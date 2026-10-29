"""End-to-end Silver tests on the tiny synthetic portfolio (two master snapshots, 30 days of transactions)."""

from __future__ import annotations

from collections import Counter
from datetime import timedelta

import duckdb
import pytest

from cardpulse_frameworks.ingestion.pipeline import FileLog, compute_watermarks, ingest_source
from cardpulse_frameworks.silver import pipeline as silver_pipeline
from cardpulse_frameworks.silver.pipeline import DQ_RESULTS_TABLE, UNIT_LOG_TABLE, find_eligible
from conftest import TINY, build_lake

BD = TINY.business_date
PRIOR = BD - timedelta(days=1)


@pytest.fixture(scope="module")
def lake(tiny_landing, tmp_path_factory):
    landing, manifest = tiny_landing
    lk = build_lake(tmp_path_factory.mktemp("silver"), landing)
    lk["manifest"] = manifest
    return lk


def sql(lk, query: str, **tables):
    con = duckdb.connect()
    for name, table in tables.items():
        con.register(name, lk["writer"].read(table))
    return con.execute(query).fetchall()


def test_every_entity_applies_without_rejections(lake):
    for entity, r in lake["silver"].items():
        assert r.count("REJECTED") == 0 and r.count("PENDING_REBUILD") == 0, entity
        assert r.count("APPLIED") >= 1, entity


def test_scd2_history_matches_the_snapshot_changes(lake):
    changes = lake["manifest"].prior_snapshot["changes"]
    rows = sql(
        lake,
        "SELECT _is_current, _valid_from, _valid_to, count(*) FROM acc GROUP BY ALL ORDER BY ALL",
        acc="silver.card_account",
    )
    closed = [r for r in rows if not r[0]]
    assert closed == [(False, PRIOR, BD, changes["account_credit_limit"])]
    accounts = lake["manifest"].ground_truth["accounts"]
    assert sum(r[3] for r in rows if r[0]) == accounts
    # One current version per key, and versions never overlap.
    assert sql(
        lake,
        "SELECT max(n) FROM (SELECT count(*) n FROM acc WHERE _is_current GROUP BY account_id)",
        acc="silver.card_account",
    ) == [(1,)]
    assert sql(
        lake,
        """SELECT count(*) FROM acc a JOIN acc b USING (account_id)
                        WHERE a._sk <> b._sk AND a._valid_from < b._valid_to AND b._valid_from < a._valid_to""",
        acc="silver.card_account",
    ) == [(0,)]
    consent_closed = sql(lake, "SELECT count(*) FROM c WHERE NOT _is_current", c="silver.marketing_consent")
    assert consent_closed == [(changes["consent_changed_today"],)]


def test_changed_versions_carry_the_new_values(lake):
    rows = sql(
        lake,
        """SELECT o.credit_limit, n.credit_limit FROM acc o JOIN acc n USING (account_id)
                        WHERE NOT o._is_current AND n._is_current""",
        acc="silver.card_account",
    )
    assert rows and all(new > old for old, new in rows)  # every change was a credit line increase


def test_scd1_reference_has_no_history(lake):
    assert sql(lake, "SELECT count(*), count(DISTINCT mcc) FROM m", m="silver.mcc_reference") == [(38, 38)]


def test_transactions_are_typed_and_linked(lake):
    rows = sql(
        lake,
        """SELECT count(*), count(DISTINCT txn_id), count(customer_id),
                               count(*) FILTER (WHERE txn_type_cd = 'PAYMENT' AND amount >= 0),
                               count(*) FILTER (WHERE txn_ts IS NULL)
                        FROM t""",
        t="silver.posted_transaction",
    )
    n, distinct, linked, bad_sign, no_ts = rows[0]
    assert n == distinct and linked == n and bad_sign == 0 and no_ts == 0


def test_watermark_holds_back_later_batches(lake):
    w = lake["writer"]
    txn_spec = lake["bronze_specs"]["processor.posted_txn"]
    wm = compute_watermarks(FileLog(w).latest(txn_spec), {BD})[0]
    held = lake["silver"]["posted_transaction"].held_by_watermark
    assert {b.seq for b in held} == set(range(wm.watermark_seq + 2, wm.max_seq_seen + 1))
    loaded_dates = sql(lake, "SELECT max(_business_date) FROM t", t="silver.posted_transaction")[0][0]
    assert loaded_dates == BD  # batches up to the watermark did load


def test_bronze_to_silver_reconciles(lake):
    """Every Bronze row of an applied batch is in Silver, quarantined, or a counted duplicate."""
    w = lake["writer"]
    log = [r for r in w.read(UNIT_LOG_TABLE).to_pylist() if r["entity"] == "posted_transaction"]
    applied = {v.split(":")[0] for r in log if r["status"] == "APPLIED" for v in r["batch_versions"]}
    bronze_ids = w.read("bronze.processor_posted_txn_raw", columns=("_batch_id",)).column("_batch_id").to_pylist()
    bronze_rows = sum(1 for b in bronze_ids if b in applied)
    silver_rows = w.read("silver.posted_transaction", columns=("txn_id",)).num_rows
    quarantined = sum(r["rows_quarantined"] for r in log)
    dups = sum(r["duplicates_in_batch"] + r["duplicates_in_target"] for r in log)
    assert bronze_rows == silver_rows + quarantined + dups


def test_dq_results_are_recorded(lake):
    rows = lake["writer"].read(DQ_RESULTS_TABLE).to_pylist()
    assert {r["entity"] for r in rows} == set(lake["silver_specs"])
    assert all(r["status"] in {"PASS", "WARN"} for r in rows)
    assert {"DQ-01", "DQ-03", "DQ-08", "DQ-10", "DQ-12", "DQ-13"} <= {r["rule_id"] for r in rows}


def test_rerun_is_a_no_op(lake):
    before = {s.entity: lake["writer"].read(s.target_table).num_rows for s in lake["silver_specs"].values()}
    for spec in lake["silver_specs"].values():
        r = lake["pipe"].run_entity(spec)
        assert r.units == [], spec.entity
    after = {s.entity: lake["writer"].read(s.target_table).num_rows for s in lake["silver_specs"].values()}
    assert before == after


# ------------------------------------------------------------------ scenarios on their own lakes
def _resend_bad_file(lk) -> None:
    landing = lk["env"].landing_path()
    f = next(landing.glob(f"processor/posted_txn/business_date={BD}/*_{TINY.defects.bad_trailer_batch:04d}.dat"))
    lines = f.read_text().splitlines()
    n = sum(1 for ln in lines if ln.startswith("D|"))
    lines[-1] = f"T|{n}|{lines[-1].split('|')[2]}"
    f.write_text("\n".join(lines) + "\n")
    ingest_source(lk["bronze_specs"]["processor.posted_txn"], landing, lk["writer"], lk["lineage"])


def test_resend_releases_held_batches_with_dedup_and_warnings(tiny_landing, tmp_path):
    lk = build_lake(tmp_path, tiny_landing[0])
    defects = tiny_landing[1].defects
    _resend_bad_file(lk)
    first = lk["silver"]["posted_transaction"]
    r = lk["pipe"].run_entity(lk["silver_specs"]["posted_transaction"])
    assert r.count("APPLIED") == 1 and r.held_by_watermark == []
    units = first.units + r.units  # defects can sit below the watermark too, so count both runs
    assert sum(u.duplicates_in_batch + u.duplicates_in_target for u in units) == defects["duplicate_rows_in_bronze"]
    assert sum(d.rows_failed for u in units for d in u.dq if d.rule_id == "DQ-05") == defects["unknown_mcc_rows"]
    assert sql(lk, "SELECT count(*) FROM t WHERE mcc = '9999'", t="silver.posted_transaction") == [
        (defects["unknown_mcc_rows"],)
    ]


def test_crash_after_commit_does_not_duplicate(tiny_landing, tmp_path, monkeypatch):
    lk = build_lake(tmp_path, tiny_landing[0], silver=False)
    specs = lk["silver_specs"]
    monkeypatch.setattr(silver_pipeline.SilverPipeline, "_write_unit_log", lambda *a, **k: None)
    for s in specs.values():
        lk["pipe"].run_entity(s)
    counts = {e: lk["writer"].read(s.target_table).num_rows for e, s in specs.items()}
    monkeypatch.undo()
    for s in specs.values():  # the "next run" after the crash
        lk["pipe"].run_entity(s)
    assert counts == {e: lk["writer"].read(s.target_table).num_rows for e, s in specs.items()}


def test_dq_gate_rejects_the_whole_unit(tiny_landing, tmp_path):
    from cardpulse_frameworks.quality.rules import Rule

    lk = build_lake(tmp_path, tiny_landing[0], silver=False)
    pipe, specs = lk["pipe"], lk["silver_specs"]
    for e in ("mcc_reference", "customer", "card_account"):
        pipe.run_entity(specs[e])
    strict = specs["account_daily_balance"].model_copy(deep=True)
    strict.quality.append(Rule(id="GATE-1", check="expression", expr="current_balance < 0", max_fail_pct=0))
    r = pipe.run_entity(strict)
    assert [u.status for u in r.units] == ["REJECTED"] and "GATE-1" in r.units[0].reason
    assert not lk["writer"].table_exists(strict.target_table)  # nothing written
    assert pipe.run_entity(strict).units == []  # stays rejected until the data or the rule changes
    r = pipe.run_entity(specs["account_daily_balance"], retry_rejected=True)  # rule fixed
    assert r.count("APPLIED") == 1


def test_critical_failures_below_threshold_are_quarantined(tiny_landing, tmp_path):
    import shutil

    landing = tmp_path / "source"
    shutil.copytree(tiny_landing[0], landing)
    day = next(iter(sorted(landing.glob("processor/posted_txn/business_date=2027-02-2*/*.dat"))))
    lines = day.read_text().splitlines()
    parts = lines[1].split("|")
    parts[3] = "7099999999999999"  # account that does not exist (DQ-03)
    lines[1] = "|".join(parts)
    day.write_text("\n".join(lines) + "\n")
    lk = build_lake(tmp_path / "lake", landing)
    r = lk["silver"]["posted_transaction"]
    assert r.count("REJECTED") == 0 and r.total("rows_quarantined") == 1
    q = lk["writer"].read("silver.posted_transaction_quarantine").to_pylist()
    assert len(q) == 1 and q[0]["rule_ids"] == ["DQ-03"] and "7099999999999999" in q[0]["record_json"]


def test_out_of_order_snapshot_needs_rebuild(tiny_landing, tmp_path):
    lk = build_lake(tmp_path, tiny_landing[0])
    w, landing = lk["writer"], lk["env"].landing_path()
    # The processor corrects yesterday's customer file after today's was applied.
    f = next(landing.glob(f"processor/customer/business_date={PRIOR}/*.dat"))
    lines = f.read_text().splitlines()
    p = lines[1].split("|")
    p[8] = "MASS" if p[8] != "MASS" else "PREMIER"
    lines[1] = "|".join(p)
    f.write_text("\n".join(lines) + "\n")
    ingest_source(lk["bronze_specs"]["processor.customer"], landing, w, lk["lineage"], reprocess=True)
    spec = lk["silver_specs"]["customer"]
    r = lk["pipe"].run_entity(spec)
    assert [u.status for u in r.units] == ["PENDING_REBUILD"]
    assert find_eligible(spec, w).eligible  # still waiting

    before_current = sql(lk, "SELECT count(*) FROM c WHERE _is_current", c="silver.customer")
    r = lk["pipe"].rebuild_entity(spec)
    assert [u.status for u in r.units] == ["APPLIED", "APPLIED"]
    assert sql(lk, "SELECT count(*) FROM c WHERE _is_current", c="silver.customer") == before_current
    assert find_eligible(spec, w).eligible == []


def test_rebuild_reproduces_the_same_silver(lake):
    spec = lake["silver_specs"]["card_account"]
    cols = "account_id, credit_limit, status_cd, _valid_from, _valid_to, _is_current, _record_hash"
    before = sorted(sql(lake, f"SELECT {cols} FROM a", a=spec.target_table))
    lake["pipe"].rebuild_entity(spec)
    assert sorted(sql(lake, f"SELECT {cols} FROM a", a=spec.target_table)) == before


def test_unit_log_outcomes(lake):
    c = Counter(r["status"] for r in lake["writer"].read(UNIT_LOG_TABLE).to_pylist())
    assert c["APPLIED"] > 0 and set(c) <= {"APPLIED", "REJECTED", "PENDING_REBUILD"}
