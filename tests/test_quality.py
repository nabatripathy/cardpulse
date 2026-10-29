"""Unit tests for the YAML data quality rules engine."""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest
from pydantic import ValidationError

from cardpulse_frameworks.quality.rules import Rule, evaluate_dataset_rules, evaluate_row_rules


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 'A1', 'OPEN',   100.0, 'M1'),
        (2, 'A2', 'OPEN',   0.0,   'M2'),
        (3, 'A3', 'CLOSED', 0.0,   NULL),
        (4, NULL, 'WEIRD',  50.0,  'M9'),
        (5, 'A1', 'OPEN',   20.0,  'M1')
    ) v(_row_id, account_id, status_cd, credit_limit, mcc)""")
    return c


def run(con, rules, refs=None):
    def loader(table, column):
        return refs[table]

    results = {r.rule_id: r for r in evaluate_row_rules(con, "t", rules, loader)}
    failed = {(rid, row) for row, rid, _ in con.execute("SELECT * FROM _dq_failures").fetchall()}
    return results, failed


def test_not_null(con):
    res, failed = run(con, [Rule(id="R1", check="not_null", columns=["account_id"])])
    assert (res["R1"].rows_checked, res["R1"].rows_failed) == (5, 1)
    assert failed == {("R1", 4)}


def test_accepted_values_and_null_passes(con):
    res, failed = run(con, [Rule(id="R1", check="accepted_values", column="status_cd", values=["OPEN", "CLOSED"])])
    assert failed == {("R1", 4)}


def test_range_with_where(con):
    rule = Rule(id="R1", check="range", column="credit_limit", min=0.01, where="status_cd = 'OPEN'")
    res, failed = run(con, [rule])
    assert res["R1"].rows_checked == 3 and failed == {("R1", 2)}


def test_expression(con):
    res, failed = run(con, [Rule(id="R1", check="expression", expr="credit_limit < 60")])
    assert failed == {("R1", 1)}


def test_referential(con):
    refs = {"silver.merchant": pa.table({"merchant_id": ["M1", "M2"]})}
    rule = Rule(id="R1", check="referential", column="mcc", ref_table="silver.merchant", ref_column="merchant_id")
    res, failed = run(con, [rule], refs)
    assert failed == {("R1", 4)}  # NULL passes; M9 is not in the reference


def test_status_and_thresholds():
    from cardpulse_frameworks.quality.rules import RuleResult

    assert RuleResult("R", "R", "x", "critical", 1000, 5, 1.0).status == "PASS"  # 0.5% <= 1%
    assert RuleResult("R", "R", "x", "critical", 100, 5, 1.0).status == "FAIL"
    assert RuleResult("R", "R", "x", "warning", 100, 5, 1.0).status == "WARN"
    assert RuleResult("R", "R", "x", "critical", 0, 0, 0.0).status == "PASS"


def test_dataset_rules(con):
    rules = [
        Rule(id="U1", check="unique", columns=["account_id"]),
        Rule(
            id="S1",
            check="dataset_sql",
            sql="SELECT count(*) FROM {table} WHERE status_cd = 'WEIRD'",
            severity="warning",
        ),
    ]
    res = {r.rule_id: r for r in evaluate_dataset_rules(con, "t", rules)}
    assert res["U1"].rows_failed == 1 and res["U1"].status == "FAIL"  # A1 twice
    assert res["S1"].rows_failed == 1 and res["S1"].status == "WARN"


@pytest.mark.parametrize(
    "bad",
    [
        {"id": "X", "check": "not_null"},
        {"id": "X", "check": "range", "column": "c"},
        {"id": "X", "check": "referential", "column": "c"},
        {"id": "X", "check": "unique", "columns": ["c"], "where": "TRUE"},
    ],
)
def test_invalid_rules_are_refused(bad):
    with pytest.raises(ValidationError):
        Rule.model_validate(bad)
