"""Bronze ingestion pipeline (PT-01 config-driven ingestion, PT-02 idempotent writes,
PT-04 quarantine, PT-05 micro-batch watermark, PT-12 lineage).

For each landed file of a source spec, in (business date, sequence) order:

1. Decide: load, skip, or reject, using the control log ``ops.ingest_file_log``.
2. Parse and guard (parser.py): file-level rejects, record-level quarantine.
3. Write: Bronze rows and quarantine rows, each with one atomic replace of the
   file's batch, so reruns never duplicate data.
4. Log the outcome to the control log and emit OpenLineage START/COMPLETE/FAIL.

The batch id is derived from the source and file name, so every attempt at the
same file replaces the previous attempt's rows.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa

from cardpulse_frameworks.common.logging import get_logger, log

from .lineage import LineageEmitter, file_dataset, table_dataset
from .parser import ParseResult, file_identity, parse_file, sha256_file
from .spec import SourceSpec
from .writers.base import TableWriter

FILE_LOG_TABLE = "ops.ingest_file_log"
_LOG = get_logger("cardpulse.ingest")

AUDIT_FIELDS = [
    pa.field("_source_system", pa.string()),
    pa.field("_source_file", pa.string()),
    pa.field("_batch_id", pa.string()),
    pa.field("_business_date", pa.date32()),
    pa.field("_file_seq", pa.int32()),
    pa.field("_row_num", pa.int32()),
    pa.field("_record_hash", pa.string()),
    pa.field("_schema_version", pa.int32()),
    pa.field("_run_id", pa.string()),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
]
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("_source_system", pa.string()),
        pa.field("_source_file", pa.string()),
        pa.field("_batch_id", pa.string()),
        pa.field("_business_date", pa.date32()),
        pa.field("_file_seq", pa.int32()),
        pa.field("_row_num", pa.int32()),
        pa.field("reason", pa.string()),
        pa.field("detail", pa.string()),
        pa.field("raw_record_redacted", pa.string()),
        pa.field("_run_id", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)
FILE_LOG_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string()),
        pa.field("entity", pa.string()),
        pa.field("file_name", pa.string()),
        pa.field("file_path", pa.string()),
        pa.field("checksum_sha256", pa.string()),
        pa.field("business_date", pa.date32()),
        pa.field("file_seq", pa.int32()),
        pa.field("batch_id", pa.string()),
        pa.field("status", pa.string()),  # LOADED | REJECTED
        pa.field("reject_reason", pa.string()),
        pa.field("reject_detail", pa.string()),
        pa.field("rows_read", pa.int64()),
        pa.field("rows_loaded", pa.int64()),
        pa.field("rows_quarantined", pa.int64()),
        pa.field("duplicate_keys", pa.int64()),
        pa.field("schema_version", pa.int32()),
        pa.field("run_id", pa.string()),
        pa.field("started_at", pa.timestamp("us", tz="UTC")),
        pa.field("finished_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass
class FileResult:
    file_name: str
    business_date: date
    seq: int
    status: str  # LOADED | REJECTED | SKIPPED
    rows_read: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    duplicate_keys: int = 0
    reason: str | None = None
    seconds: float = 0.0


@dataclass
class Watermark:
    business_date: date
    watermark_seq: int  # highest contiguous loaded sequence starting at 1 (0 = none)
    max_seq_seen: int
    gaps: list[int] = field(default_factory=list)  # sequences after the watermark not loaded


@dataclass
class SourceResult:
    source: str
    target_table: str
    files: list[FileResult] = field(default_factory=list)
    watermarks: list[Watermark] = field(default_factory=list)

    def count(self, status: str) -> int:
        return sum(1 for f in self.files if f.status == status)

    def total(self, attr: str) -> int:
        return sum(getattr(f, attr) for f in self.files)


def batch_id_for(spec: SourceSpec, file_name: str) -> str:
    return hashlib.sha256(f"{spec.name}:{file_name}".encode()).hexdigest()[:20]


def bronze_schema(spec: SourceSpec) -> pa.Schema:
    return pa.schema([pa.field(c, pa.string()) for c in spec.column_names] + AUDIT_FIELDS)


class FileLog:
    """Reads and writes the control table ``ops.ingest_file_log``.

    Writes are buffered and committed every ``flush_every`` files and at the end of
    a run. That is safe because data writes are exactly-once on their own (the
    writer stamps each commit with its batch id): if a run dies before a flush, the
    next run reloads those files and replaces, rather than duplicates, their rows.
    """

    def __init__(self, writer: TableWriter, flush_every: int = 50):
        self.writer = writer
        self.flush_every = flush_every
        self._pending: list[dict[str, object]] = []

    def latest(self, spec: SourceSpec) -> dict[str, dict[str, object]]:
        """Latest log entry per file name for this source."""
        t = self.writer.read(FILE_LOG_TABLE, row_filter=f"source == '{spec.source}' and entity == '{spec.entity}'")
        latest: dict[str, dict[str, object]] = {}
        for row in t.to_pylist() if t.num_rows else []:
            prev = latest.get(row["file_name"])
            if prev is None or row["finished_at"] > prev["finished_at"]:
                latest[row["file_name"]] = row
        return latest

    def write(self, entry: dict[str, object]) -> None:
        self._pending.append(entry)
        if len(self._pending) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if self._pending:
            self.writer.append(FILE_LOG_TABLE, pa.Table.from_pylist(self._pending, schema=FILE_LOG_SCHEMA), None)
            self._pending = []


def compute_watermarks(latest: dict[str, dict[str, object]], dates: set[date] | None = None) -> list[Watermark]:
    by_date: dict[date, dict[int, str]] = defaultdict(dict)
    for row in latest.values():
        by_date[row["business_date"]][row["file_seq"]] = row["status"]
    out = []
    for d in sorted(by_date):
        if dates and d not in dates:
            continue
        seqs = by_date[d]
        wm = 0
        while seqs.get(wm + 1) == "LOADED":
            wm += 1
        top = max(seqs)
        gaps = [s for s in range(wm + 1, top + 1) if seqs.get(s) != "LOADED"]
        out.append(Watermark(d, wm, top, gaps))
    return out


def discover(spec: SourceSpec, landing_root: Path, business_date: date | None = None) -> list[Path]:
    found = []
    for p in landing_root.glob(spec.file_glob):
        try:
            bdate, seq = file_identity(spec, p)
        except ValueError:
            log(_LOG, logging.WARNING, "file name does not match spec; ignored", file=str(p), source=spec.name)
            continue
        if business_date and bdate != business_date:
            continue
        found.append((bdate, seq, p))
    return [p for _, _, p in sorted(found)]


def _decide(prior: dict[str, object] | None, checksum: str, reprocess: bool) -> tuple[str, str | None]:
    """Return (action, reason): action is LOAD, SKIP or REJECT."""
    if prior is None or reprocess:
        return "LOAD", None
    same = prior["checksum_sha256"] == checksum
    if prior["status"] == "LOADED":
        return ("SKIP", "ALREADY_LOADED") if same else ("REJECT", "CHANGED_AFTER_LOAD")
    # Previously rejected: identical content stays rejected; a corrected resend loads.
    return ("SKIP", "ALREADY_REJECTED") if same else ("LOAD", None)


def _bronze_table(spec: SourceSpec, pr: ParseResult, batch_id: str, run_id: str, now: datetime) -> pa.Table:
    n = len(pr.rows)
    cols = list(zip(*pr.rows, strict=True)) if n else [[] for _ in spec.column_names]
    data = {c: list(v) for c, v in zip(spec.column_names, cols, strict=True)}
    data.update(
        {
            "_source_system": [spec.source] * n,
            "_source_file": [pr.path.name] * n,
            "_batch_id": [batch_id] * n,
            "_business_date": [pr.business_date] * n,
            "_file_seq": [pr.seq] * n,
            "_row_num": pr.row_nums,
            "_record_hash": pr.row_hashes,
            "_schema_version": [spec.schema_version] * n,
            "_run_id": [run_id] * n,
            "_ingested_at": [now] * n,
        }
    )
    return pa.table(data, schema=bronze_schema(spec))


def _quarantine_table(spec: SourceSpec, pr: ParseResult, batch_id: str, run_id: str, now: datetime) -> pa.Table:
    rows = [
        {
            "_source_system": spec.source,
            "_source_file": pr.path.name,
            "_batch_id": batch_id,
            "_business_date": pr.business_date,
            "_file_seq": pr.seq,
            "_row_num": q.row_num,
            "reason": q.reason,
            "detail": q.detail,
            "raw_record_redacted": q.raw_record,
            "_run_id": run_id,
            "_ingested_at": now,
        }
        for q in pr.quarantined
    ]
    return pa.Table.from_pylist(rows, schema=QUARANTINE_SCHEMA)


def ingest_source(
    spec: SourceSpec,
    landing_root: Path,
    writer: TableWriter,
    lineage: LineageEmitter,
    business_date: date | None = None,
    reprocess: bool = False,
) -> SourceResult:
    result = SourceResult(source=spec.name, target_table=spec.target_table)
    file_log = FileLog(writer)
    latest = file_log.latest(spec)
    job = f"ingest.{spec.name}"
    columns = [(f.name, str(f.type)) for f in bronze_schema(spec)]
    touched: set[date] = set()

    for path in discover(spec, landing_root, business_date):
        t0 = time.perf_counter()
        started = datetime.now(UTC)
        bdate, seq = file_identity(spec, path)
        touched.add(bdate)
        checksum = sha256_file(path)
        batch_id = batch_id_for(spec, path.name)
        action, why = _decide(latest.get(path.name), checksum, reprocess)
        if action == "SKIP":
            result.files.append(FileResult(path.name, bdate, seq, "SKIPPED", reason=why))
            continue
        if action == "REJECT":
            # A loaded file was changed in landing. Alert on every run until someone reprocesses it;
            # the control log keeps the LOADED entry so watermarks stay true to what is in Bronze.
            result.files.append(FileResult(path.name, bdate, seq, "REJECTED", reason=why))
            log(
                _LOG,
                logging.WARNING,
                "loaded file changed in landing; rerun with --reprocess to replace it",
                source=spec.name,
                file=path.name,
            )
            continue

        run_id = lineage.new_run_id()
        lineage.emit("START", run_id, job, inputs=[file_dataset(path)])
        entry: dict[str, object] = {
            "source": spec.source,
            "entity": spec.entity,
            "file_name": path.name,
            "file_path": str(path),
            "checksum_sha256": checksum,
            "business_date": bdate,
            "file_seq": seq,
            "batch_id": batch_id,
            "schema_version": spec.schema_version,
            "run_id": run_id,
            "started_at": started,
            "rows_read": 0,
            "rows_loaded": 0,
            "rows_quarantined": 0,
            "duplicate_keys": 0,
            "reject_reason": None,
            "reject_detail": None,
        }
        try:
            pr = parse_file(spec, path)
            now = datetime.now(UTC)
            entry.update(rows_read=pr.rows_read, rows_quarantined=len(pr.quarantined), duplicate_keys=pr.duplicate_keys)
            if pr.quarantined:
                writer.replace_batch(
                    spec.quarantine_table,
                    _quarantine_table(spec, pr, batch_id, run_id, now),
                    batch_id,
                    "_business_date",
                )
            if pr.rejected:
                entry.update(status="REJECTED", reject_reason=pr.reject_reason, reject_detail=pr.reject_detail)
            else:
                writer.replace_batch(
                    spec.target_table, _bronze_table(spec, pr, batch_id, run_id, now), batch_id, "_business_date"
                )
                entry.update(status="LOADED", rows_loaded=len(pr.rows))
            entry["finished_at"] = datetime.now(UTC)
            file_log.write(entry)
            latest[path.name] = entry
        except Exception as e:
            lineage.emit("FAIL", run_id, job, inputs=[file_dataset(path)], error=f"{type(e).__name__}: {e}")
            log(_LOG, logging.ERROR, "file load failed", source=spec.name, file=path.name, error=str(e))
            file_log.flush()  # keep the record of every file that did complete
            raise

        fr = FileResult(
            path.name,
            bdate,
            seq,
            entry["status"],
            entry["rows_read"],
            entry["rows_loaded"],
            entry["rows_quarantined"],
            entry["duplicate_keys"],
            entry["reject_reason"],
            round(time.perf_counter() - t0, 3),
        )
        result.files.append(fr)
        if fr.status == "LOADED":
            lineage.emit(
                "COMPLETE",
                run_id,
                job,
                inputs=[file_dataset(path)],
                outputs=[table_dataset(spec.target_table, columns, fr.rows_loaded)],
            )
            log(_LOG, logging.INFO, "file loaded", source=spec.name, **_fields(fr))
        else:
            lineage.emit(
                "FAIL",
                run_id,
                job,
                inputs=[file_dataset(path)],
                error=f"{entry['reject_reason']}: {entry['reject_detail']}",
            )
            log(_LOG, logging.WARNING, "file rejected", source=spec.name, **_fields(fr), detail=entry["reject_detail"])

    file_log.flush()
    if spec.watermark == "sequence":
        result.watermarks = compute_watermarks(latest, touched)
    return result


def _fields(fr: FileResult) -> dict[str, object]:
    d = asdict(fr)
    d["business_date"] = fr.business_date.isoformat()
    return d
