"""End-to-end Bronze ingestion tests on a tiny synthetic portfolio.

The module-scoped fixture generates the portfolio and loads it once; the
operational scenarios then run in order against the same lakehouse.
"""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pytest

from cardpulse_frameworks.common.config import REPO_ROOT
from cardpulse_frameworks.common.pan import contains_pan, is_pan
from cardpulse_frameworks.ingestion.lineage import LineageEmitter
from cardpulse_frameworks.ingestion.parser import parse_file
from cardpulse_frameworks.ingestion.pipeline import FILE_LOG_TABLE, FileLog, compute_watermarks, ingest_source
from cardpulse_frameworks.ingestion.spec import load_spec, load_specs
from cardpulse_frameworks.ingestion.writers import make_writer
from conftest import TINY, make_env

SPECS = load_specs(REPO_ROOT / "configs" / "sources")
TXN = next(s for s in SPECS if s.name == "processor.posted_txn")
BD = TINY.business_date


@pytest.fixture(scope="module")
def lake(tiny_landing, tmp_path_factory):
    landing, manifest = tiny_landing
    tmp = tmp_path_factory.mktemp("lake")
    shutil.copytree(landing, tmp / "landing")
    env = make_env(tmp)
    writer = make_writer(env)
    lineage = LineageEmitter(env)
    results = {s.name: ingest_source(s, env.landing_path(), writer, lineage) for s in SPECS}
    return {"env": env, "writer": writer, "lineage": lineage, "results": results, "manifest": manifest, "tmp": tmp}


def test_every_source_loads(lake):
    for name, r in lake["results"].items():
        assert r.count("LOADED") >= 1, name
        if name != "processor.posted_txn":
            assert r.count("REJECTED") == 0, name


def test_rows_reconcile_to_the_landing_files(lake):
    m, r = lake["manifest"], lake["results"]["processor.posted_txn"]
    expected_read = m.rows["processor.posted_txn"]
    assert r.total("rows_read") == expected_read
    in_rejected = m.defects["rows_in_rejected_files"]
    assert r.total("rows_loaded") + r.total("rows_quarantined") + in_rejected == expected_read
    bronze = lake["writer"].read(TXN.target_table, columns=("TXN_REF_NO",))
    assert bronze.num_rows == r.total("rows_loaded")


def test_defects_are_caught(lake):
    m, r, w = lake["manifest"], lake["results"]["processor.posted_txn"], lake["writer"]
    rejected = [f.file_name for f in r.files if f.status == "REJECTED"]
    assert rejected == m.defects["rejected_files"]
    assert {f.reason for f in r.files if f.status == "REJECTED"} == {"TRAILER_MISMATCH"}
    q = w.read(TXN.quarantine_table).to_pylist()
    by_reason: dict[str, int] = {}
    for row in q:
        by_reason[row["reason"]] = by_reason.get(row["reason"], 0) + 1
    assert by_reason == m.defects["quarantine_expected"]
    assert r.total("duplicate_keys") == m.defects["duplicate_rows_in_bronze"]


def test_no_clear_card_number_reaches_any_table(lake):
    w = lake["writer"]
    bronze = w.read(TXN.target_table, columns=("PAN_TOKEN", "MERCH_NM"))
    assert not any(is_pan(v) for v in bronze.column("PAN_TOKEN").to_pylist())
    assert not any(contains_pan(v) for v in bronze.column("MERCH_NM").to_pylist())
    q = w.read(TXN.quarantine_table, columns=("raw_record_redacted",))
    assert q.num_rows > 0
    assert not any(contains_pan(v) for v in q.column("raw_record_redacted").to_pylist())


def test_audit_columns_and_partitions(lake):
    w = lake["writer"]
    t = w.read(TXN.target_table, row_filter=f"_business_date == '{BD.isoformat()}'")
    assert t.num_rows > 0
    for c in ("_batch_id", "_source_file", "_record_hash", "_ingested_at", "_run_id", "_row_num"):
        assert t.column(c).null_count == 0, c
    assert set(t.column("_business_date").to_pylist()) == {BD}
    table = w.catalog.load_table(TXN.target_table)
    assert [f.name for f in table.spec().fields] == ["_business_date"]


def test_lineage_events_are_emitted(lake):
    events = [json.loads(ln) for ln in Path(lake["env"].lineage.path).read_text().splitlines()]
    kinds = {e["eventType"] for e in events}
    assert {"START", "COMPLETE", "FAIL"} <= kinds
    complete = [e for e in events if e["eventType"] == "COMPLETE" and e["job"]["name"] == "ingest.processor.posted_txn"]
    loaded = lake["results"]["processor.posted_txn"].count("LOADED")
    assert len(complete) == loaded
    assert complete[0]["outputs"][0]["name"] == TXN.target_table
    assert complete[0]["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"] > 0


def test_operational_scenarios(lake):
    """Rerun, reprocess, corrected resend, and a file changed after load, in order."""
    env, w, lin = lake["env"], lake["writer"], lake["lineage"]
    landing = env.landing_path()
    count = lambda: w.read(TXN.target_table, columns=("TXN_REF_NO",)).num_rows  # noqa: E731
    before = count()

    # 1. Rerun: everything is skipped, nothing changes.
    r = ingest_source(TXN, landing, w, lin)
    assert r.count("SKIPPED") == len(r.files) and count() == before

    # 2. Reprocess the business date: rows are replaced, not duplicated.
    r = ingest_source(TXN, landing, w, lin, business_date=BD, reprocess=True)
    assert r.count("LOADED") > 0 and count() == before

    # 3. Watermark stops before the rejected batch.
    wm = compute_watermarks(FileLog(w).latest(TXN), {BD})[0]
    bad_seq = TINY.defects.bad_trailer_batch
    assert wm.watermark_seq == bad_seq - 1 and wm.gaps == [bad_seq]

    # 4. The processor resends a corrected file: it loads and the watermark catches up.
    bad = landing / f"processor/posted_txn/business_date={BD}/processor_posted_txn_{BD:%Y%m%d}_{bad_seq:04d}.dat"
    lines = bad.read_text().splitlines()
    n_detail = sum(1 for ln in lines if ln.startswith("D|"))
    t = lines[-1].split("|")
    lines[-1] = f"T|{n_detail}|{t[2]}"
    bad.write_text("\n".join(lines) + "\n")
    r = ingest_source(TXN, landing, w, lin, business_date=BD)
    assert [f.status for f in r.files if f.seq == bad_seq] == ["LOADED"]
    assert count() == before + n_detail
    wm = compute_watermarks(FileLog(w).latest(TXN), {BD})[0]
    assert wm.watermark_seq == wm.max_seq_seen and wm.gaps == []

    # 5. A loaded file is edited in landing: alert, keep Bronze and the control log as they were.
    after = count()
    log_rows = w.read(FILE_LOG_TABLE).num_rows
    f1 = bad.with_name(f"processor_posted_txn_{BD:%Y%m%d}_0001.dat")
    f1.write_text(f1.read_text().replace("USD", "USX", 1))
    r = ingest_source(TXN, landing, w, lin, business_date=BD)
    changed = [f for f in r.files if f.seq == 1]
    assert changed[0].status == "REJECTED" and changed[0].reason == "CHANGED_AFTER_LOAD"
    assert count() == after and w.read(FILE_LOG_TABLE).num_rows == log_rows


def _write(tmp: Path, name: str, text: str) -> Path:
    p = tmp / name
    p.write_text(text)
    return p


def test_file_level_rejections(tmp_path):
    spec = load_spec(REPO_ROOT / "configs" / "sources" / "processor_card.yaml")
    row = "9900000000000001|7010000000000001|C100000001|20270301||202703|N"
    good = f"H|CARD|20270315|0001|x\nD|{row}\nT|1|0\n"
    assert not parse_file(spec, _write(tmp_path, "processor_card_20270315_0001.dat", good)).rejected

    cases = {
        "HEADER_INVALID": good.replace("H|CARD", "H|ACCOUNT"),
        "TRAILER_MISSING": f"H|CARD|20270315|0001|x\nD|{row}\n",
        "TRAILER_MISMATCH": good.replace("T|1|0", "T|2|0"),
    }
    for reason, text in cases.items():
        pr = parse_file(spec, _write(tmp_path, "processor_card_20270315_0001.dat", text))
        assert pr.reject_reason == reason
    wrong_date = _write(tmp_path, "processor_card_20270316_0001.dat", good)
    assert parse_file(spec, wrong_date).reject_reason == "HEADER_INVALID"


def test_record_level_quarantine(tmp_path):
    spec = load_spec(REPO_ROOT / "configs" / "sources" / "processor_card.yaml")
    rows = [
        "9900000000000001|7010000000000001|C100000001|20270301||202703|N",  # good
        "4111111111111111|7010000000000002|C100000002|20270301||202703|N",  # clear PAN
        "|7010000000000005|C100000003|20270301||202703|N",  # required token missing
        "9900000000000004|7010000000000004|C100000004|20270301||202703",  # missing a field
    ]
    text = "H|CARD|20270315|0001|x\n" + "".join(f"D|{r}\n" for r in rows) + f"T|{len(rows)}|0\n"
    pr = parse_file(spec, _write(tmp_path, "processor_card_20270315_0001.dat", text))
    assert not pr.rejected and len(pr.rows) == 1
    assert [q.reason for q in pr.quarantined] == ["PAN_DETECTED", "REQUIRED_MISSING", "FIELD_COUNT"]
    assert "4111111111111111" not in pr.quarantined[0].raw_record


def test_strict_sources_reject_on_any_bad_record(tmp_path):
    spec = load_spec(REPO_ROOT / "configs" / "sources" / "prefcenter_consent.yaml")
    text = (
        ",".join(spec.column_names) + "\nC1,EMAIL,MARKETING,OPT_IN,APP,2027-01-01T00:00:00Z\nC2,EMAIL,,OPT_IN,APP,x\n"
    )
    pr = parse_file(spec, _write(tmp_path, "prefcenter_consent_20270315_0001.csv", text))
    assert pr.reject_reason == "QUARANTINE_THRESHOLD"


def test_csv_schema_mismatch(tmp_path):
    spec = load_spec(REPO_ROOT / "configs" / "sources" / "reference_mcc.yaml")
    pr = parse_file(spec, _write(tmp_path, "reference_mcc_20270315_0001.csv", "MCC,DESC\n5411,Grocery\n"))
    assert pr.reject_reason == "SCHEMA_MISMATCH"


def test_business_date_filter(lake):
    from cardpulse_frameworks.ingestion.pipeline import discover

    files = discover(TXN, lake["env"].landing_path(), date(2027, 3, 1))
    assert len(files) == 1 and "20270301" in files[0].name


def test_crash_before_control_log_write_does_not_duplicate(tiny_landing, tmp_path, monkeypatch):
    """Data commits but the run dies before the control log is written: the rerun must
    replace the batches it finds stamped in the Iceberg snapshot history, not append again."""
    from cardpulse_frameworks.ingestion import pipeline

    landing, _ = tiny_landing
    shutil.copytree(landing, tmp_path / "landing")
    env = make_env(tmp_path)
    w, lin = make_writer(env), LineageEmitter(env)

    monkeypatch.setattr(pipeline.FileLog, "flush", lambda self: None)  # simulate the crash window
    first = ingest_source(TXN, env.landing_path(), w, lin, business_date=BD)
    rows = w.read(TXN.target_table, columns=("TXN_REF_NO",)).num_rows
    assert first.count("LOADED") > 0 and w.read(FILE_LOG_TABLE).num_rows == 0
    monkeypatch.undo()

    w2 = make_writer(env)  # fresh process
    again = ingest_source(TXN, env.landing_path(), w2, lin, business_date=BD)
    assert again.count("LOADED") == first.count("LOADED")
    assert w2.read(TXN.target_table, columns=("TXN_REF_NO",)).num_rows == rows
    assert w2.read(FILE_LOG_TABLE).num_rows == len(again.files)
