"""Gold layer tests: dbt build on the tiny portfolio, contracts, reconciliation, incremental
loads, DQ results, lineage, and the trigger inputs on a portfolio with six months of history."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pytest
import yaml

from cardpulse_frameworks.common.config import REPO_ROOT
from cardpulse_frameworks.gold.cli import TRIGGER_SQL
from cardpulse_frameworks.silver.pipeline import DQ_RESULTS_TABLE
from cardpulse_synth.config import DefectConfig, SynthConfig
from cardpulse_synth.generator import SyntheticBank, generate
from conftest import TINY, build_gold, build_lake

BD = TINY.business_date
PRODUCTS = {"customer_360": "customer_360", "card_account_snapshot": "fct_account_daily_snapshot"}


@pytest.fixture(scope="module")
def gold(tiny_landing, tmp_path_factory):
    lk = build_lake(tmp_path_factory.mktemp("gold"), tiny_landing[0])
    lk["staged"], lk["run"] = build_gold(lk)
    lk["manifest"] = tiny_landing[1]
    return lk


def q(lk, sql: str):
    con = duckdb.connect(str(lk["env"].gold_path()), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def silver_count(lk, table: str) -> int:
    return lk["writer"].read(table).num_rows


def test_dbt_build_succeeds_with_every_test_passing(gold):
    run = gold["run"]
    assert run.success
    assert {m.status for m in run.models} == {"success"}
    assert len(run.tests) >= 80 and {t.status for t in run.tests} == {"pass"}


def test_every_silver_table_was_staged(gold):
    assert {s.status for s in gold["staged"]} == {"STAGED"}
    assert {s.table for s in gold["staged"]} == {s.target_table for s in gold["silver_specs"].values()}


def test_facts_and_products_reconcile_to_silver(gold):
    assert q(gold, "SELECT count(*) FROM gold.fct_transaction")[0][0] == silver_count(gold, "silver.posted_transaction")
    assert q(gold, "SELECT count(*) FROM gold.fct_account_daily_snapshot")[0][0] == silver_count(
        gold, "silver.account_daily_balance"
    )
    current = gold["writer"].read("silver.customer").column("_is_current").to_pylist()
    assert q(gold, "SELECT count(*) FROM gold.customer_360")[0][0] == sum(current)


def test_scd2_dimensions_cover_history_from_the_beginning_of_time(gold):
    rows = q(gold, "SELECT min(valid_from), count(*) FILTER (WHERE NOT is_current) FROM gold.dim_account")
    assert rows[0][0] == date(1900, 1, 1)
    assert rows[0][1] == gold["manifest"].prior_snapshot["changes"]["account_credit_limit"]
    # Transactions older than the first snapshot still found their dimension version.
    assert q(gold, "SELECT count(*) FROM gold.fct_transaction WHERE account_sk IS NULL OR customer_sk IS NULL") == [
        (0,)
    ]


@pytest.mark.parametrize("contract,model", PRODUCTS.items())
def test_contract_files_match_the_enforced_dbt_contracts(gold, contract, model):
    spec = yaml.safe_load((REPO_ROOT / "contracts" / f"{contract}.yaml").read_text())
    dbt_yml = yaml.safe_load((REPO_ROOT / "dbt/models/marts/products/_products.yml").read_text())
    dbt_model = next(m for m in dbt_yml["models"] if m["name"] == model)
    assert dbt_model["config"]["contract"]["enforced"] is True
    assert [(c["name"], c["type"]) for c in spec["schema"]] == [
        (c["name"], c["data_type"]) for c in dbt_model["columns"]
    ]
    built = q(
        gold,
        f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'gold' "
        f"AND table_name = '{model}' ORDER BY ordinal_position",
    )
    assert [r[0] for r in built] == [c["name"] for c in spec["schema"]]


def test_customer_360_flags_match_silver(gold):
    w = gold["writer"]
    consent = w.read("silver.marketing_consent").to_pylist()
    dns = {r["customer_id"] for r in consent if r["_is_current"] and r["purpose_cd"] == "DO_NOT_SOLICIT"}
    email = {
        r["customer_id"]
        for r in consent
        if r["_is_current"] and r["channel_cd"] == "EMAIL" and r["consent_status"] == "OPT_IN"
    }
    assert {r[0] for r in q(gold, "SELECT customer_id FROM gold.customer_360 WHERE is_do_not_solicit")} == dns
    assert {r[0] for r in q(gold, "SELECT customer_id FROM gold.customer_360 WHERE email_opt_in")} == email


def test_t01_population_contains_every_planted_never_activated_account(gold):
    bank = SyntheticBank(TINY).build()
    issue = bank.card_issue[: bank.na]
    planted = set(bank.acct_no[bank.never_activates & (issue <= bank.bd_o - 10) & (issue >= bank.bd_o - 60)])
    sql = next(v for k, v in TRIGGER_SQL.items() if k.startswith("T01")).replace("SELECT count(*)", "SELECT account_id")
    found = {r[0] for r in q(gold, sql)}
    assert planted and planted <= found


def test_dq_results_and_lineage_are_recorded(gold):
    rows = [r for r in gold["writer"].read(DQ_RESULTS_TABLE).to_pylist() if r["layer"] == "gold"]
    assert len(rows) == len(gold["run"].tests) and {r["status"] for r in rows} == {"PASS"}
    events = [json.loads(x) for x in Path(gold["env"].lineage.path).read_text().splitlines()]
    c360 = [e for e in events if e["job"]["name"] == "gold.customer_360" and e["eventType"] == "COMPLETE"]
    assert c360 and {"gold.fct_account_daily_snapshot", "gold.dim_customer"} <= {i["name"] for i in c360[-1]["inputs"]}
    snap = [e for e in events if e["job"]["name"] == "gold.stg_silver__posted_transaction"]
    assert {"namespace": "iceberg", "name": "silver.posted_transaction"} in snap[-1]["inputs"]


def test_incremental_run_adds_only_new_transactions(tiny_landing, tmp_path):
    from cardpulse_frameworks.ingestion.pipeline import ingest_source

    lk = build_lake(tmp_path, tiny_landing[0])
    staged, run = build_gold(lk)
    before = q(lk, "SELECT count(*) FROM gold.fct_transaction")[0][0]

    staged, run = build_gold(lk)  # nothing new
    assert {s.status for s in staged} == {"UNCHANGED"} and run.success
    assert q(lk, "SELECT count(*) FROM gold.fct_transaction")[0][0] == before

    # The rejected micro-batch is resent; Silver releases the held batches; Gold picks them up.
    landing = lk["env"].landing_path()
    f = next(landing.glob(f"processor/posted_txn/business_date={BD}/*_{TINY.defects.bad_trailer_batch:04d}.dat"))
    lines = f.read_text().splitlines()
    lines[-1] = f"T|{sum(1 for ln in lines if ln.startswith('D|'))}|{lines[-1].split('|')[2]}"
    f.write_text("\n".join(lines) + "\n")
    ingest_source(lk["bronze_specs"]["processor.posted_txn"], landing, lk["writer"], lk["lineage"])
    added = lk["pipe"].run_entity(lk["silver_specs"]["posted_transaction"]).total("rows_loaded")
    staged, run = build_gold(lk)
    assert added > 0 and run.success
    assert q(lk, "SELECT count(*) FROM gold.fct_transaction")[0][0] == before + added
    fct = next(m for m in run.models if m.name == "fct_transaction")
    assert fct.status == "success"


# ---------------------------------------------------------------- six months of history (T02)
HISTORY = SynthConfig(
    seed=23,
    business_date=date(2027, 3, 15),
    n_customers=400,
    history_days=190,
    intraday_batches=4,
    defects=DefectConfig(enabled=False),
)


@pytest.fixture(scope="module")
def history_gold(tmp_path_factory):
    landing = tmp_path_factory.mktemp("history_landing")
    generate(HISTORY, landing)
    lk = build_lake(tmp_path_factory.mktemp("history_lake"), landing)
    lk["staged"], lk["run"] = build_gold(lk)
    return lk


def test_t02_population_finds_planted_spend_decliners(history_gold):
    assert history_gold["run"].success
    bank = SyntheticBank(HISTORY).build()
    planted = set(bank.acct_no[bank.decliner])
    eligible = {
        r[0]
        for r in q(
            history_gold,
            """
        SELECT account_id FROM gold.fct_account_daily_snapshot
        WHERE status_cd = 'OPEN' AND months_on_book >= 6 AND avg_monthly_spend_6m >= 200
          AND coalesce(days_past_due, 0) = 0""",
        )
    }
    sql = next(v for k, v in TRIGGER_SQL.items() if k.startswith("T02")).replace("SELECT count(*)", "SELECT account_id")
    found = {r[0] for r in q(history_gold, sql)}
    target = planted & eligible
    assert len(target) >= 10
    # Decliners keep 10-40% of their usual purchase count in the last 30 days, and that window is part
    # of the 6-month average, so the expected ratio at the top of the range (~0.44) sits just under
    # the 0.5 cutoff; Poisson noise pushes a few over it. That is the business rule working, not a
    # pipeline defect, so the floor is set below the ~0.8 observed at seed 23.
    assert len(found & target) / len(target) >= 0.75


def test_spend_30d_matches_silver_transactions(history_gold):
    txn = history_gold["writer"].read("silver.posted_transaction").to_pylist()
    bd = HISTORY.business_date
    expected: dict[str, float] = {}
    for t in txn:
        if t["txn_type_cd"] in ("PURCHASE", "CASH") and bd - timedelta(days=30) < t["txn_local_date"] <= bd:
            expected[t["account_id"]] = expected.get(t["account_id"], 0) + float(t["amount"])
    got = dict(q(history_gold, "SELECT account_id, CAST(spend_30d AS DOUBLE) FROM gold.fct_account_daily_snapshot"))
    assert all(abs(got[a] - v) < 0.005 for a, v in expected.items())
    assert np.isclose(sum(got.values()), sum(expected.values()))
