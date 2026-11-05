"""Decisioning tests: triggers, suppression rules, holdouts, repeat rules, the decision log and
the Offer Eligibility data product, on a six-month portfolio built through Gold."""

from __future__ import annotations

import json
from collections import Counter
from datetime import timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import yaml

from cardpulse_frameworks.common.config import REPO_ROOT
from cardpulse_frameworks.decisioning.cli import RULES_PATH, TRIGGERS_DIR, decide
from cardpulse_frameworks.decisioning.engine import (
    DECISION_TABLES,
    OFFER_DECISION_TABLE,
    RuleError,
    evaluate,
    holdout_bucket,
)
from cardpulse_frameworks.decisioning.spec import load_rule_set, load_triggers
from cardpulse_frameworks.silver.pipeline import DQ_RESULTS_TABLE
from conftest import build_gold, build_lake

TRIGGERS = {t.trigger: t for t in load_triggers(TRIGGERS_DIR)}
RULES = load_rule_set(RULES_PATH)


@pytest.fixture(scope="module")
def decided(history_landing, tmp_path_factory):
    lk = build_lake(tmp_path_factory.mktemp("decide"), history_landing)
    build_gold(lk)
    lk["res"] = decide(lk["env"], lk["writer"], lk["lineage"])
    return lk


def q(lk, sql: str, params: list | None = None):
    con = duckdb.connect(str(lk["env"].gold_path()), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def test_specs_load_and_cover_the_phase_4_catalog():
    assert set(TRIGGERS) == {"T01_NEW_ACCOUNT_NOT_ACTIVATED", "T02_SPEND_DECLINE"}
    assert [r.code for r in RULES.rules] == [f"S{i:02d}" for i in range(1, 13)]
    assert [r.code for r in RULES.rules if not r.enabled] == ["S11"]  # R2 offers only


def test_decision_run_publishes_to_gold_with_every_test_passing(decided):
    res = decided["res"]
    assert res.build.success
    assert {t.status for t in res.build.tests} == {"pass"} and len(res.build.tests) >= 25
    assert {s.status for s in res.staged} == {"STAGED"}
    events, decisions = res.run.trigger_events, res.run.decisions
    fired = pc.sum(pc.equal(events["status"], "FIRED")).as_py()
    assert decisions.num_rows == fired > 0
    assert {d["trigger_cd"] for d in decisions.to_pylist()} == set(TRIGGERS)
    assert q(decided, "SELECT count(*) FROM gold.fct_offer_decision")[0][0] == decisions.num_rows
    customers = q(decided, "SELECT count(*) FROM gold.customer_360")[0][0]
    assert q(decided, "SELECT count(*) FROM gold.offer_eligibility")[0][0] == customers * 4


def test_offer_eligibility_contract_matches_the_enforced_dbt_contract(decided):
    spec = yaml.safe_load((REPO_ROOT / "contracts" / "offer_eligibility.yaml").read_text())
    dbt_yml = yaml.safe_load((REPO_ROOT / "dbt/models/marts/decisioning/_decisioning.yml").read_text())
    model = next(m for m in dbt_yml["models"] if m["name"] == "offer_eligibility")
    assert model["config"]["contract"]["enforced"] is True
    assert [(c["name"], c["type"]) for c in spec["schema"]] == [(c["name"], c["data_type"]) for c in model["columns"]]
    built = q(
        decided,
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'gold' "
        "AND table_name = 'offer_eligibility' ORDER BY ordinal_position",
    )
    assert [r[0] for r in built] == [c["name"] for c in spec["schema"]]


def test_suppression_codes_follow_the_rules(decided):
    c360 = {
        r[0]: r[1:]
        for r in q(
            decided,
            """
        SELECT customer_id, is_deceased, is_do_not_solicit, email_opt_in, push_opt_in, has_acquisition_bonus,
               open_account_count, max_days_past_due, has_fraud_flag FROM gold.customer_360""",
        )
    }
    rows = q(decided, "SELECT customer_id, offer_code, is_eligible, suppression_codes FROM gold.offer_eligibility")
    seen = Counter()
    for cust, offer, eligible, codes in rows:
        codes = codes or []
        deceased, dns, email, push, bonus, open_accts, dpd, fraud = c360[cust]
        assert eligible == (codes == [])
        assert codes == sorted(codes)  # catalog order; the first is the primary reason
        assert ("S01" in codes) == (open_accts == 0)
        assert ("S02" in codes) == (dpd > 0)
        assert ("S03" in codes) == fraud
        assert ("S04" in codes) == deceased
        assert ("S06" in codes) == (not email and not push)
        assert ("S07" in codes) == dns
        assert ("S12" in codes) == (offer == "ACT_BONUS_TIERED" and bonus)
        seen.update(codes)
    # The 400-customer portfolio exercises most of the catalog (S04 and S05 are 1-in-1000 populations).
    assert {"S01", "S02", "S06", "S07", "S08"} <= set(seen)


def test_contact_cap_counts_marketing_contacts_only(decided):
    caps = dict(
        q(
            decided,
            """
        SELECT customer_id, count(*) FILTER (WHERE contact_date > ? - 30) >= 2
                            OR count(*) FILTER (WHERE contact_date > ? - 7) >= 1
        FROM gold.fct_offer_contact WHERE purpose_cd = 'MARKETING' GROUP BY customer_id""",
            [decided["res"].run.business_date] * 2,
        )
    )
    s08 = {
        r[0]
        for r in q(
            decided,
            "SELECT DISTINCT customer_id FROM gold.offer_eligibility WHERE list_contains(suppression_codes, 'S08')",
        )
    }
    assert s08 == {c for c, capped in caps.items() if capped}
    service_only = {
        r[0]
        for r in q(
            decided,
            """SELECT customer_id FROM gold.fct_offer_contact GROUP BY customer_id
                                                HAVING bool_and(purpose_cd = 'SERVICE')""",
        )
    }
    assert service_only and not (service_only & s08)


def test_decisions_take_the_best_eligible_offer_and_hold_out_by_bucket(decided):
    sent_per_customer = Counter()
    for d in decided["res"].run.decisions.to_pylist():
        t = TRIGGERS[d["trigger_cd"]]
        offers = json.loads(d["offers_evaluated"])
        assert [o["offer"] for o in offers[: len(t.offers)]] == t.offers
        assert d["holdout_bucket"] == holdout_bucket(d["customer_id"], t.trigger)
        if d["outcome"] == "SUPPRESSED":
            assert d["reason_codes"] and d["primary_reason_cd"] == d["reason_codes"][0]
            assert all(o["codes"] for o in offers)
        else:
            chosen = offers[d["offer_rank"] - 1]
            assert chosen["offer"] == d["offer_code"] and not chosen["codes"]
            assert all(o["codes"] for o in offers[: d["offer_rank"] - 1])  # every better offer was blocked
            assert (d["outcome"] == "HOLDOUT") == (d["holdout_bucket"] < t.holdout_pct)
        if d["outcome"] == "SENT":
            sent_per_customer[d["customer_id"]] += 1
    assert sent_per_customer and max(sent_per_customer.values()) == 1  # one marketing message a day


def test_repeat_rules_hold_back_recently_triggered_accounts(decided):
    bd = decided["res"].run.business_date
    events = {
        e["account_id"]: e
        for e in decided["res"].run.trigger_events.to_pylist()
        if e["trigger_cd"] == "T01_NEW_ACCOUNT_NOT_ACTIVATED"
    }
    recent = {
        r[0]
        for r in q(
            decided,
            """
        SELECT account_id FROM gold.fct_offer_contact
        WHERE trigger_cd = 'T01_NEW_ACCOUNT_NOT_ACTIVATED' AND contact_date > ? - 15""",
            [bd],
        )
    }
    held = [a for a in recent if a in events]
    assert held, "the portfolio should plant recently triggered T01 accounts"
    assert all(events[a]["status"] == "SKIPPED_REPEAT_GAP" for a in held)
    for e in events.values():
        assert (e["status"] == "FIRED") == (
            e["prior_fires"] < 2 and (e["last_fire_date"] is None or e["last_fire_date"] <= bd - timedelta(days=15))
        )


def test_yesterdays_sends_count_as_contacts_and_fires(decided):
    run = decided["res"].run
    bd = run.business_date
    yesterday = run.decisions.set_column(
        0, "business_date", pa.array([bd - timedelta(days=1)] * run.decisions.num_rows, pa.date32())
    )
    again = evaluate(decided["env"].gold_path(), list(TRIGGERS.values()), RULES, "test", bd, yesterday)
    sent = {d["account_id"]: d for d in run.decisions.to_pylist() if d["outcome"] == "SENT"}
    treated = {d["account_id"] for d in run.decisions.to_pylist() if d["outcome"] in ("SENT", "HOLDOUT")}
    status = {(e["trigger_cd"], e["account_id"]): e["status"] for e in again.trigger_events.to_pylist()}
    assert treated and all(
        status[(d["trigger_cd"], a)] == "SKIPPED_REPEAT_GAP"
        for d in run.decisions.to_pylist()
        for a in [d["account_id"]]
        if a in treated
    )
    elig = {(r["customer_id"], r["offer_code"]): r["suppression_codes"] or [] for r in again.eligibility.to_pylist()}
    for d in sent.values():
        assert "S08" in elig[(d["customer_id"], d["offer_code"])]  # 1 contact in 7 days
        assert "S09" in elig[(d["customer_id"], d["offer_code"])]  # same offer in 90 days


def test_unknown_or_inactive_offer_is_suppressed_with_s10(decided):
    t01 = TRIGGERS["T01_NEW_ACCOUNT_NOT_ACTIVATED"].model_copy(update={"offers": ["NO_SUCH_OFFER"]})
    run = evaluate(decided["env"].gold_path(), [t01], RULES, "test")
    assert run.decisions.num_rows > 0
    assert all(d["outcome"] == "SUPPRESSED" and "S10" in d["reason_codes"] for d in run.decisions.to_pylist())


def test_a_rule_that_does_not_compile_is_reported(decided):
    bad = RULES.model_copy(
        update={"rules": [*RULES.rules[:-1], RULES.rules[-1].model_copy(update={"when": "nope > 1"})]}
    )
    with pytest.raises(RuleError, match="suppression rules"):
        evaluate(decided["env"].gold_path(), list(TRIGGERS.values()), bad, "test")
    t02 = TRIGGERS["T02_SPEND_DECLINE"].model_copy(update={"when": ["no_such_column > 1"]})
    with pytest.raises(RuleError, match="T02_SPEND_DECLINE"):
        evaluate(decided["env"].gold_path(), [t02], RULES, "test")


def test_holdout_buckets_are_stable_and_uniform():
    ids = [f"C{100_000_001 + i}" for i in range(20_000)]
    share = sum(holdout_bucket(c, "T01_NEW_ACCOUNT_NOT_ACTIVATED") < 25 for c in ids) / len(ids)
    assert abs(share - 0.25) < 0.01
    assert holdout_bucket("C100000001", "T02_SPEND_DECLINE") == holdout_bucket("C100000001", "T02_SPEND_DECLINE")


def test_lineage_and_dq_results_are_recorded(decided):
    events = [json.loads(x) for x in Path(decided["env"].lineage.path).read_text().splitlines()]
    engine = [e for e in events if e["job"]["name"] == "decide.engine" and e["eventType"] == "COMPLETE"]
    assert engine and {o["name"] for o in engine[-1]["outputs"]} == set(DECISION_TABLES)
    assert {i["name"] for i in engine[-1]["inputs"]} >= {"gold.fct_account_daily_snapshot", "gold.customer_360"}
    stg = [e for e in events if e["job"]["name"] == "gold.stg_decision__offer_eligibility"]
    assert {"namespace": "iceberg", "name": "decision.offer_eligibility"} in stg[-1]["inputs"]
    fct = [e for e in events if e["job"]["name"] == "gold.fct_offer_decision" and e["eventType"] == "COMPLETE"]
    assert "gold.dim_customer" in {i["name"] for i in fct[-1]["inputs"]}  # built by the earlier Gold run
    dq = [
        r
        for r in decided["writer"].read(DQ_RESULTS_TABLE).to_pylist()
        if r["entity"] in ("offer_eligibility", "fct_offer_decision")
    ]
    assert dq and {r["status"] for r in dq} == {"PASS"}


def test_rerun_replaces_the_business_date(decided):
    w = decided["writer"]
    before = {t: w.read(t).num_rows for t in DECISION_TABLES}
    first = sorted((d["decision_id"], d["outcome"]) for d in w.read(OFFER_DECISION_TABLE).to_pylist())
    res = decide(decided["env"], w, decided["lineage"])
    assert res.build.success
    assert {t: w.read(t).num_rows for t in DECISION_TABLES} == before
    assert sorted((d["decision_id"], d["outcome"]) for d in w.read(OFFER_DECISION_TABLE).to_pylist()) == first
