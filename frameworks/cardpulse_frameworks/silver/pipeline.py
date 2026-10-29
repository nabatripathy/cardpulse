"""Silver conformance pipeline (PT-02 idempotent MERGE, PT-03 SCD Type 2, PT-04 quarantine,
PT-05 watermark, PT-12 lineage).

For each Silver entity, in spec order:

1. **Find eligible Bronze batches.** Batches LOADED in ``ops.ingest_file_log``, at or
   below the source's watermark, and not yet applied. A batch is identified by its
   *version* (batch id + file checksum), so a corrected resend is a new version.
2. **Group them into units.** Append loads take up to ``unit_max_batches`` batches per
   unit. SCD loads take one business date per unit, oldest first, because history
   must be applied in order.
3. **Transform** in DuckDB: typed columns from the spec's expressions, cast checks,
   lookups, then deduplicate on the business key (and, for append loads, against
   what Silver already holds).
4. **Check quality.** Row rules quarantine failing rows (critical) or count them
   (warning); dataset rules judge the clean set. Any critical rule over its threshold
   rejects the whole unit: nothing is written to Silver.
5. **Load** in one atomic Iceberg commit, stamped with the unit's batch versions:
   append, SCD Type 1 (replace the snapshot) or SCD Type 2 (close changed versions,
   open new ones).
6. **Record** DQ results, the unit outcome and OpenLineage events.

Exactly-once: a batch version counts as applied when the unit log says so *or* a
Silver commit carries it in its snapshot summary, so a crash between the commit and
the log write never applies data twice.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import duckdb
import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, In

from cardpulse_frameworks.common.logging import get_logger, log
from cardpulse_frameworks.ingestion.lineage import LineageEmitter, table_dataset
from cardpulse_frameworks.ingestion.pipeline import FileLog, compute_watermarks
from cardpulse_frameworks.ingestion.writers.base import TableWriter
from cardpulse_frameworks.quality.rules import RuleResult, evaluate_dataset_rules, evaluate_row_rules, lit, q

from .engine import connect
from .spec import SilverSpec

UNIT_LOG_TABLE = "ops.silver_unit_log"
DQ_RESULTS_TABLE = "ops.dq_results"
STAMP = "cardpulse.silver_batches"
OPEN_END = date(9999, 12, 31)
_LOG = get_logger("cardpulse.silver")

UNIT_LOG_SCHEMA = pa.schema(
    [
        pa.field("entity", pa.string()),
        pa.field("unit_id", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("status", pa.string()),  # APPLIED | REJECTED | PENDING_REBUILD
        pa.field("reason", pa.string()),
        pa.field("business_dates", pa.list_(pa.date32())),
        pa.field("batch_versions", pa.list_(pa.string())),
        pa.field("rows_in", pa.int64()),
        pa.field("rows_loaded", pa.int64()),
        pa.field("rows_quarantined", pa.int64()),
        pa.field("duplicates_in_batch", pa.int64()),
        pa.field("duplicates_in_target", pa.int64()),
        pa.field("versions_inserted", pa.int64()),
        pa.field("versions_changed", pa.int64()),
        pa.field("versions_closed", pa.int64()),
        pa.field("rows_unchanged", pa.int64()),
        pa.field("warning_rows", pa.int64()),
        pa.field("started_at", pa.timestamp("us", tz="UTC")),
        pa.field("finished_at", pa.timestamp("us", tz="UTC")),
    ]
)
DQ_RESULTS_SCHEMA = pa.schema(
    [
        pa.field("layer", pa.string()),
        pa.field("entity", pa.string()),
        pa.field("unit_id", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("business_dates", pa.list_(pa.date32())),
        pa.field("rule_id", pa.string()),
        pa.field("rule_name", pa.string()),
        pa.field("check_type", pa.string()),
        pa.field("severity", pa.string()),
        pa.field("rows_checked", pa.int64()),
        pa.field("rows_failed", pa.int64()),
        pa.field("fail_pct", pa.float64()),
        pa.field("threshold_pct", pa.float64()),
        pa.field("status", pa.string()),  # PASS | WARN | FAIL
        pa.field("evaluated_at", pa.timestamp("us", tz="UTC")),
    ]
)
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("entity", pa.string()),
        pa.field("unit_id", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("_bronze_batch_id", pa.string()),
        pa.field("_source_file", pa.string()),
        pa.field("_business_date", pa.date32()),
        pa.field("_row_num", pa.int32()),
        pa.field("business_key", pa.string()),
        pa.field("rule_ids", pa.list_(pa.string())),
        pa.field("record_json", pa.string()),
        pa.field("quarantined_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class BatchRef:
    batch_id: str
    version: str
    business_date: date
    seq: int
    file_name: str


@dataclass
class Eligibility:
    eligible: list[BatchRef]
    held_by_watermark: list[BatchRef]
    applied_versions: set[str]
    rejected_versions: set[str]
    last_applied_date: date | None


@dataclass
class UnitResult:
    entity: str
    unit_id: str
    business_dates: list[date]
    batches: list[BatchRef]
    status: str
    reason: str | None = None
    rows_in: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    duplicates_in_batch: int = 0
    duplicates_in_target: int = 0
    versions_inserted: int = 0
    versions_changed: int = 0
    versions_closed: int = 0
    rows_unchanged: int = 0
    warning_rows: int = 0
    dq: list[RuleResult] = field(default_factory=list)
    seconds: float = 0.0


@dataclass
class EntityResult:
    entity: str
    target_table: str
    units: list[UnitResult] = field(default_factory=list)
    held_by_watermark: list[BatchRef] = field(default_factory=list)

    def count(self, status: str) -> int:
        return sum(1 for u in self.units if u.status == status)

    def total(self, attr: str) -> int:
        return sum(getattr(u, attr) for u in self.units)


# ----------------------------------------------------------------------------- eligibility
def _version(batch_id: str, checksum: str) -> str:
    return f"{batch_id}:{checksum[:12]}"


def find_eligible(spec: SilverSpec, writer: TableWriter, retry_rejected: bool = False) -> Eligibility:
    source, entity = spec.source.ingest_source.split(".", 1)
    latest = FileLog(writer).latest_for(source, entity)
    loaded = [r for r in latest.values() if r["status"] == "LOADED"]
    marks = {w.business_date: w.watermark_seq for w in compute_watermarks(latest)}

    log_rows = writer.read(UNIT_LOG_TABLE, row_filter=EqualTo("entity", spec.entity))
    applied: set[str] = set()
    rejected: set[str] = set()
    last_date: date | None = None
    for r in log_rows.to_pylist() if log_rows.num_rows else []:
        if r["status"] == "APPLIED":
            applied.update(r["batch_versions"])
            if r["business_dates"]:
                last_date = max([last_date, *r["business_dates"]] if last_date else r["business_dates"])
        elif r["status"] == "REJECTED":
            rejected.update(r["batch_versions"])
    for stamp in writer.snapshot_values(spec.target_table, STAMP):
        applied.update(v for v in stamp.split(",") if v)
    if not retry_rejected:
        rejected -= applied
    else:
        rejected = set()

    eligible, held = [], []
    for r in sorted(loaded, key=lambda x: (x["business_date"], x["file_seq"])):
        ref = BatchRef(
            r["batch_id"],
            _version(r["batch_id"], r["checksum_sha256"]),
            r["business_date"],
            r["file_seq"],
            r["file_name"],
        )
        if ref.version in applied or ref.version in rejected:
            continue
        if spec.source.respect_watermark and ref.seq > marks.get(ref.business_date, 0):
            held.append(ref)
            continue
        eligible.append(ref)
    return Eligibility(eligible, held, applied, rejected, last_date)


def plan_units(spec: SilverSpec, eligible: list[BatchRef]) -> list[list[BatchRef]]:
    if spec.load == "append":
        n = spec.unit_max_batches
        return [eligible[i : i + n] for i in range(0, len(eligible), n)]
    by_date: dict[date, list[BatchRef]] = defaultdict(list)
    for b in eligible:
        by_date[b.business_date].append(b)
    return [by_date[d] for d in sorted(by_date)]


# ----------------------------------------------------------------------------- the pipeline
class SilverPipeline:
    def __init__(self, writer: TableWriter, lineage: LineageEmitter):
        self.writer = writer
        self.lineage = lineage

    def run_entity(self, spec: SilverSpec, retry_rejected: bool = False) -> EntityResult:
        result = EntityResult(spec.entity, spec.target_table)
        el = find_eligible(spec, self.writer, retry_rejected)
        result.held_by_watermark = el.held_by_watermark
        applied_ids = {v.split(":")[0] for v in el.applied_versions}
        for batches in plan_units(spec, el.eligible):
            if spec.load != "append" and el.last_applied_date and batches[0].business_date < el.last_applied_date:
                result.units.append(self._pending_rebuild(spec, batches, el.last_applied_date))
                continue
            reversioned = sorted({b.batch_id for b in batches} & applied_ids)
            unit = self._run_unit(spec, batches, reversioned)
            result.units.append(unit)
            if unit.status == "APPLIED" and spec.load != "append":
                el.last_applied_date = max(el.last_applied_date or unit.business_dates[0], unit.business_dates[0])
        return result

    def rebuild_entity(self, spec: SilverSpec) -> EntityResult:
        """Drop the entity's Silver and quarantine tables and its unit log, then reprocess all of Bronze."""
        self.writer.drop_table(spec.target_table)
        self.writer.drop_table(spec.quarantine_table)
        self.writer.delete(UNIT_LOG_TABLE, EqualTo("entity", spec.entity))
        log(_LOG, logging.INFO, "entity rebuilt from Bronze", entity=spec.entity)
        return self.run_entity(spec, retry_rejected=True)

    # ------------------------------------------------------------------------- one unit
    def _run_unit(self, spec: SilverSpec, batches: list[BatchRef], reversioned: list[str]) -> UnitResult:
        t0 = time.perf_counter()
        started = datetime.now(UTC)
        versions = sorted(b.version for b in batches)
        unit_id = hashlib.sha256(",".join(versions).encode()).hexdigest()[:16]
        dates = sorted({b.business_date for b in batches})
        res = UnitResult(spec.entity, unit_id, dates, batches, "APPLIED")
        run_id = self.lineage.new_run_id()
        job = f"silver.{spec.entity}"
        inputs = [{"namespace": "iceberg", "name": spec.source.bronze_table}]
        self.lineage.emit("START", run_id, job, inputs=inputs, job_type="TRANSFORM")
        con = connect()
        try:
            self._transform(con, spec, batches, res)
            self._dedup(con, spec, res, reversioned)
            res.dq = self._quality(con, spec, res)
            failed = [r for r in res.dq if r.status == "FAIL"]
            if failed:
                res.status = "REJECTED"
                res.reason = "; ".join(
                    f"{r.rule_id} {r.rows_failed}/{r.rows_checked} > {r.threshold_pct}%" for r in failed
                )
            else:
                self._write_quarantine(con, spec, res, run_id, reversioned)
                props = {STAMP: ",".join(versions), "cardpulse.silver_unit": unit_id}
                {"append": self._load_append, "scd1": self._load_scd1, "scd2": self._load_scd2}[spec.load](
                    con, spec, res, run_id, props, reversioned
                )
        except Exception as e:
            self.lineage.emit(
                "FAIL", run_id, job, inputs=inputs, error=f"{type(e).__name__}: {e}", job_type="TRANSFORM"
            )
            log(_LOG, logging.ERROR, "silver unit failed", entity=spec.entity, unit=unit_id, error=str(e))
            raise
        finally:
            con.close()

        res.seconds = round(time.perf_counter() - t0, 3)
        self._write_dq(spec, res, run_id)
        self._write_unit_log(spec, res, run_id, started, versions)
        if res.status == "APPLIED":
            cols = [(c.name, c.type) for c in spec.columns]
            self.lineage.emit(
                "COMPLETE",
                run_id,
                job,
                inputs=inputs,
                outputs=[table_dataset(spec.target_table, cols, res.rows_loaded)],
                job_type="TRANSFORM",
            )
            log(_LOG, logging.INFO, "silver unit applied", **_fields(res))
        else:
            self.lineage.emit("FAIL", run_id, job, inputs=inputs, error=res.reason, job_type="TRANSFORM")
            log(_LOG, logging.WARNING, "silver unit rejected by DQ gate", **_fields(res), reason=res.reason)
        return res

    def _pending_rebuild(self, spec: SilverSpec, batches: list[BatchRef], last: date) -> UnitResult:
        versions = sorted(b.version for b in batches)
        unit_id = hashlib.sha256(",".join(versions).encode()).hexdigest()[:16]
        res = UnitResult(
            spec.entity,
            unit_id,
            [batches[0].business_date],
            batches,
            "PENDING_REBUILD",
            reason=f"snapshot for {batches[0].business_date} arrived after {last} was applied; "
            f"run `cardpulse-silver rebuild --entity {spec.entity}`",
        )
        self._write_unit_log(spec, res, self.lineage.new_run_id(), datetime.now(UTC), versions)
        log(_LOG, logging.WARNING, "out-of-order snapshot needs a rebuild", entity=spec.entity, reason=res.reason)
        return res

    # ------------------------------------------------------------------------- steps
    def _transform(
        self, con: duckdb.DuckDBPyConnection, spec: SilverSpec, batches: list[BatchRef], res: UnitResult
    ) -> None:
        dates = sorted({b.business_date.isoformat() for b in batches})
        bronze = self.writer.read(
            spec.source.bronze_table,
            row_filter=And(In("_business_date", dates), In("_batch_id", [b.batch_id for b in batches])),
        )
        con.register("bronze", bronze)
        for lk in spec.lookups:
            if not self.writer.table_exists(lk.table):
                raise RuntimeError(f"{spec.entity} needs {lk.table}; load that entity first (spec order)")
            con.register(f"{lk.name}_src", self.writer.read(lk.table))
            cols = ", ".join(q(c) for c in lk.columns)
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW {lk.name} AS SELECT {cols} FROM {lk.name}_src WHERE {lk.where or 'TRUE'}"
            )

        exprs = [f"TRY_CAST(({c.expr}) AS {c.type}) AS {q(c.name)}" for c in spec.columns]
        exprs += [
            f"(blank({c.source}) IS NOT NULL AND TRY_CAST(({c.expr}) AS {c.type}) IS NULL) AS _castfail_{c.name}"
            for c in spec.columns
            if c.source
        ]
        exprs += [
            "b._batch_id AS _bronze_batch_id",
            "b._source_file AS _source_file",
            "b._business_date AS _business_date",
            "b._file_seq AS _file_seq",
            "b._row_num AS _row_num",
        ]
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE t0 AS SELECT row_number() OVER () AS _row_id, * "
            f"FROM (SELECT {', '.join(exprs)} FROM {spec.from_sql})"
        )
        res.rows_in = con.execute("SELECT count(*) FROM t0").fetchall()[0][0]

    def _dedup(self, con: duckdb.DuckDBPyConnection, spec: SilverSpec, res: UnitResult, reversioned: list[str]) -> None:
        keys = ", ".join(q(k) for k in spec.business_key)
        any_null = " OR ".join(f"{q(k)} IS NULL" for k in spec.business_key)
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE t1 AS SELECT * FROM t0 QUALIFY ({any_null}) OR "
            f"row_number() OVER (PARTITION BY {keys} ORDER BY {spec.dedup.order_by}) = 1"
        )
        n1 = con.execute("SELECT count(*) FROM t1").fetchall()[0][0]
        res.duplicates_in_batch = res.rows_in - n1

        if spec.dedup.against_target and self.writer.table_exists(spec.target_table):
            row_filter = AlwaysTrue()
            pc = spec.dedup.partition_column
            if pc:
                parts = [
                    r[0]
                    for r in con.execute(
                        f"SELECT DISTINCT CAST({q(pc)} AS VARCHAR) FROM t1 WHERE {q(pc)} IS NOT NULL"
                    ).fetchall()
                ]
                row_filter = In(pc, parts) if parts else EqualTo(pc, "1900-01-01")
            existing = self.writer.read(
                spec.target_table, row_filter=row_filter, columns=(*spec.business_key, "_bronze_batch_id")
            )
            con.register("existing_keys", existing)
            keep_out = (
                f"WHERE _bronze_batch_id NOT IN ({', '.join(lit(b) for b in reversioned)})" if reversioned else ""
            )
            con.execute(f"DELETE FROM t1 WHERE ({keys}) IN (SELECT ({keys}) FROM existing_keys {keep_out})")
            res.duplicates_in_target = n1 - con.execute("SELECT count(*) FROM t1").fetchall()[0][0]

    def _quality(self, con: duckdb.DuckDBPyConnection, spec: SilverSpec, res: UnitResult) -> list[RuleResult]:
        def ref_loader(table: str, column: str) -> pa.Table:
            return self.writer.read(table, columns=(column,))

        rules = spec.all_rules
        results = evaluate_row_rules(con, "t1", rules, ref_loader)
        con.execute(
            "CREATE OR REPLACE TEMP TABLE t2 AS SELECT * FROM t1 WHERE _row_id NOT IN "
            "(SELECT _row_id FROM _dq_failures WHERE severity = 'critical')"
        )
        res.rows_quarantined = con.execute(
            "SELECT count(DISTINCT _row_id) FROM _dq_failures WHERE severity = 'critical'"
        ).fetchall()[0][0]
        res.warning_rows = con.execute(
            "SELECT count(DISTINCT _row_id) FROM _dq_failures WHERE severity = 'warning' AND _row_id NOT IN "
            "(SELECT _row_id FROM _dq_failures WHERE severity = 'critical')"
        ).fetchall()[0][0]
        results += evaluate_dataset_rules(con, "t2", rules)
        attrs = ", ".join(f"coalesce(CAST({q(c)} AS VARCHAR), '\\N')" for c in spec.column_names)
        bk = ", ".join(f"CAST({q(k)} AS VARCHAR)" for k in spec.business_key)
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE t3 AS SELECT *, md5(concat_ws('|', {attrs})) AS _record_hash, "
            f"concat_ws('|', {bk}) AS _bk FROM t2"
        )
        return results

    def _write_quarantine(self, con, spec: SilverSpec, res: UnitResult, run_id: str, reversioned: list[str]) -> None:
        if res.rows_quarantined == 0 and not reversioned:
            return
        bk = ", ".join(f"CAST(t.{q(k)} AS VARCHAR)" for k in spec.business_key)
        data = (
            con.sql(f"""
            SELECT {lit(spec.entity)} AS entity, {lit(res.unit_id)} AS unit_id, {lit(run_id)} AS run_id,
                   t._bronze_batch_id, t._source_file, t._business_date, CAST(t._row_num AS INTEGER) AS _row_num,
                   concat_ws('|', {bk}) AS business_key, f.rule_ids, CAST(to_json(t) AS VARCHAR) AS record_json,
                   now() AS quarantined_at
            FROM t1 t JOIN (SELECT _row_id, list(rule_id ORDER BY rule_id) AS rule_ids FROM _dq_failures
                            WHERE severity = 'critical' GROUP BY _row_id) f USING (_row_id)
        """)
            .to_arrow_table()
            .cast(QUARANTINE_SCHEMA)
        )
        replace = In("_bronze_batch_id", reversioned) if reversioned else None
        self.writer.commit(spec.quarantine_table, data, "_business_date", replace)

    def _select_out(self, spec: SilverSpec, extra: list[str]) -> str:
        cols = [q(c) for c in spec.column_names]
        return ", ".join(cols + extra)

    def _load_append(
        self, con, spec: SilverSpec, res: UnitResult, run_id: str, props: dict[str, str], reversioned: list[str]
    ) -> None:
        out = self._select_out(
            spec,
            [
                "_record_hash",
                "_bronze_batch_id",
                "_source_file",
                "_business_date",
                f"{lit(run_id)} AS _silver_run_id",
                "now() AS _loaded_at",
            ],
        )
        data = con.sql(f"SELECT {out} FROM t3 ORDER BY _row_id").to_arrow_table()
        replace = In("_bronze_batch_id", reversioned) if reversioned else None
        self.writer.commit(spec.target_table, data, spec.partition_field, replace, props)
        res.rows_loaded = res.versions_inserted = data.num_rows

    def _load_scd1(
        self, con, spec: SilverSpec, res: UnitResult, run_id: str, props: dict[str, str], reversioned: list[str]
    ) -> None:
        out = self._select_out(
            spec,
            [
                "_record_hash",
                "_bronze_batch_id",
                "_source_file",
                "_business_date",
                f"{lit(run_id)} AS _silver_run_id",
                "now() AS _loaded_at",
            ],
        )
        data = con.sql(f"SELECT {out} FROM t3 ORDER BY _bk").to_arrow_table()
        exists = self.writer.table_exists(spec.target_table)
        if exists:
            cur = self.writer.read(spec.target_table, columns=(*spec.business_key, "_record_hash"))
            con.register("cur", cur)
            keys = ", ".join(q(k) for k in spec.business_key)
            diff = con.execute(f"""SELECT count(*) FROM (
                (SELECT {keys}, _record_hash FROM t3 EXCEPT SELECT {keys}, _record_hash FROM cur) UNION ALL
                (SELECT {keys}, _record_hash FROM cur EXCEPT SELECT {keys}, _record_hash FROM t3))""").fetchall()[0][0]
            if diff == 0:
                res.rows_unchanged = data.num_rows
                return
            res.versions_changed = diff
        self.writer.commit(spec.target_table, data, spec.partition_field, AlwaysTrue() if exists else None, props)
        res.rows_loaded = data.num_rows
        if not exists:
            res.versions_inserted = data.num_rows

    def _load_scd2(
        self, con, spec: SilverSpec, res: UnitResult, run_id: str, props: dict[str, str], reversioned: list[str]
    ) -> None:
        eff = res.business_dates[-1].isoformat()
        biz = ", ".join(q(c) for c in spec.column_names)
        if self.writer.table_exists(spec.target_table):
            con.register("cur_arrow", self.writer.read(spec.target_table, row_filter=EqualTo("_is_current", True)))
        else:
            con.register(
                "cur_arrow",
                con.sql(
                    f"SELECT {biz}, '' AS _sk, '' AS _record_hash, DATE '{eff}' AS _valid_from, "
                    f"DATE '{eff}' AS _valid_to, TRUE AS _is_current, '' AS _bronze_batch_id, '' AS _source_file, "
                    f"DATE '{eff}' AS _business_date, '' AS _silver_run_id, now() AS _loaded_at FROM t3 LIMIT 0"
                ).to_arrow_table(),
            )
        bk = ", ".join(f"CAST({q(k)} AS VARCHAR)" for k in spec.business_key)
        con.execute(f"CREATE OR REPLACE TEMP TABLE cur AS SELECT *, concat_ws('|', {bk}) AS _bk FROM cur_arrow")
        missing_sql = (
            "SELECT * FROM cur WHERE _bk NOT IN (SELECT _bk FROM t3)"
            if spec.snapshot == "full"
            else "SELECT * FROM cur WHERE FALSE"
        )
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE inc_new AS SELECT * FROM t3 WHERE _bk NOT IN (SELECT _bk FROM cur);
            CREATE OR REPLACE TEMP TABLE inc_chg AS
                SELECT t3.*, cur._sk AS _old_sk, cur._valid_from AS _old_from
                FROM t3 JOIN cur USING (_bk) WHERE t3._record_hash <> cur._record_hash;
            CREATE OR REPLACE TEMP TABLE gone AS {missing_sql};
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE scd_out AS
            -- versions closed because the record changed (a same-day change is replaced in place instead)
            SELECT {biz}, _sk, _record_hash, _valid_from, DATE '{eff}' AS _valid_to, FALSE AS _is_current,
                   _bronze_batch_id, _source_file, _business_date, _silver_run_id, _loaded_at
            FROM cur WHERE _sk IN (SELECT _old_sk FROM inc_chg WHERE _old_from < DATE '{eff}')
            UNION ALL
            -- versions closed because the key left the full snapshot
            SELECT {biz}, _sk, _record_hash, _valid_from, DATE '{eff}', FALSE,
                   _bronze_batch_id, _source_file, _business_date, _silver_run_id, _loaded_at
            FROM gone WHERE _valid_from < DATE '{eff}'
            UNION ALL
            -- new current versions (new keys and changed keys)
            SELECT {biz}, md5(_bk || '|' || '{eff}'), _record_hash, DATE '{eff}', DATE '{OPEN_END}', TRUE,
                   _bronze_batch_id, _source_file, _business_date, {lit(run_id)}, now()
            FROM (SELECT * EXCLUDE (_old_sk, _old_from) FROM inc_chg UNION ALL BY NAME SELECT * FROM inc_new)
        """)
        replaced = [
            r[0]
            for r in con.execute(f"""
            SELECT _old_sk FROM inc_chg
            UNION SELECT _sk FROM gone WHERE _valid_from <= DATE '{eff}'""").fetchall()
        ]
        # A key that left the snapshot on the same day it was opened is removed rather than closed.
        res.versions_inserted = con.execute("SELECT count(*) FROM inc_new").fetchall()[0][0]
        res.versions_changed = con.execute("SELECT count(*) FROM inc_chg").fetchall()[0][0]
        res.versions_closed = con.execute("SELECT count(*) FROM gone").fetchall()[0][0]
        res.rows_unchanged = (
            con.execute("SELECT count(*) FROM t3").fetchall()[0][0] - res.versions_inserted - res.versions_changed
        )
        data = con.sql("SELECT * FROM scd_out ORDER BY _is_current, _sk").to_arrow_table()
        if data.num_rows == 0 and not replaced:
            return
        self.writer.commit(
            spec.target_table, data, spec.partition_field, In("_sk", replaced) if replaced else None, props
        )
        res.rows_loaded = res.versions_inserted + res.versions_changed

    # ------------------------------------------------------------------------- logs
    def _write_dq(self, spec: SilverSpec, res: UnitResult, run_id: str) -> None:
        now = datetime.now(UTC)
        rows = [
            {
                "layer": "silver",
                "entity": spec.entity,
                "unit_id": res.unit_id,
                "run_id": run_id,
                "business_dates": res.business_dates,
                "rule_id": r.rule_id,
                "rule_name": r.name,
                "check_type": r.check,
                "severity": r.severity,
                "rows_checked": r.rows_checked,
                "rows_failed": r.rows_failed,
                "fail_pct": round(r.fail_pct, 4),
                "threshold_pct": r.threshold_pct,
                "status": r.status,
                "evaluated_at": now,
            }
            for r in res.dq
        ]
        if rows:
            self.writer.append(DQ_RESULTS_TABLE, pa.Table.from_pylist(rows, schema=DQ_RESULTS_SCHEMA), None)

    def _write_unit_log(
        self, spec: SilverSpec, res: UnitResult, run_id: str, started: datetime, versions: list[str]
    ) -> None:
        row = {
            k: getattr(res, k)
            for k in (
                "unit_id",
                "status",
                "reason",
                "business_dates",
                "rows_in",
                "rows_loaded",
                "rows_quarantined",
                "duplicates_in_batch",
                "duplicates_in_target",
                "versions_inserted",
                "versions_changed",
                "versions_closed",
                "rows_unchanged",
                "warning_rows",
            )
        }
        row.update(
            entity=spec.entity,
            run_id=run_id,
            batch_versions=versions,
            started_at=started,
            finished_at=datetime.now(UTC),
        )
        self.writer.append(UNIT_LOG_TABLE, pa.Table.from_pylist([row], schema=UNIT_LOG_SCHEMA), None)


def _fields(res: UnitResult) -> dict[str, object]:
    return {
        "entity": res.entity,
        "unit": res.unit_id,
        "status": res.status,
        "business_dates": [d.isoformat() for d in res.business_dates],
        "batches": len(res.batches),
        "rows_in": res.rows_in,
        "rows_loaded": res.rows_loaded,
        "rows_quarantined": res.rows_quarantined,
        "duplicates": res.duplicates_in_batch + res.duplicates_in_target,
        "seconds": res.seconds,
    }
