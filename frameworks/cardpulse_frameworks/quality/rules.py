"""Data quality rules declared as configuration and evaluated in DuckDB.

Rules come in two kinds:

* **Row rules** judge each row. A row failing a *critical* rule is quarantined; a
  row failing a *warning* rule is kept and counted.
  Checks: ``not_null``, ``accepted_values``, ``range``, ``expression``, ``referential``.
* **Dataset rules** judge the batch as a whole, after quarantine.
  Checks: ``unique``, ``dataset_sql``.

Every rule has a threshold (``max_fail_pct``). A critical rule whose failure rate
exceeds it FAILS, and the caller rejects the whole batch (the DQ gate, PT-04); a
warning rule above its threshold is reported as WARN. ``where`` limits a rule to
the rows it applies to (for example, credit limit > 0 only for open accounts).

The engine knows nothing about Iceberg or files: it works on a DuckDB table that
has a unique ``_row_id`` column, and asks a callback for referential lookups.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import duckdb
import pyarrow as pa
from pydantic import BaseModel, model_validator

ROW_CHECKS = {"not_null", "accepted_values", "range", "expression", "referential"}
DATASET_CHECKS = {"unique", "dataset_sql"}
DEFAULT_THRESHOLD_PCT = {"critical": 1.0, "warning": 1.0}


def q(name: str) -> str:
    """Quote an identifier for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def lit(value: object) -> str:
    """Render a SQL literal."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


class Rule(BaseModel):
    id: str
    check: Literal["not_null", "accepted_values", "range", "expression", "referential", "unique", "dataset_sql"]
    name: str | None = None
    severity: Literal["critical", "warning"] = "critical"
    columns: list[str] = []  # not_null, unique
    column: str | None = None  # accepted_values, range, referential
    values: list[str | int | bool] | None = None  # accepted_values
    min: float | None = None  # range (inclusive)
    max: float | None = None
    expr: str | None = None  # expression: SQL that is TRUE for a good row
    sql: str | None = None  # dataset_sql: query over {table} returning the number of violations
    ref_table: str | None = None  # referential
    ref_column: str | None = None
    where: str | None = None  # row rules: only rows where this is TRUE are checked
    max_fail_pct: float | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Rule:
        need: dict[str, tuple[str, ...]] = {
            "not_null": ("columns",),
            "accepted_values": ("column", "values"),
            "range": ("column",),
            "expression": ("expr",),
            "referential": ("column", "ref_table", "ref_column"),
            "unique": ("columns",),
            "dataset_sql": ("sql",),
        }
        missing = [f for f in need[self.check] if not getattr(self, f)]
        if missing:
            raise ValueError(f"rule {self.id}: {self.check} needs {', '.join(missing)}")
        if self.check == "range" and self.min is None and self.max is None:
            raise ValueError(f"rule {self.id}: range needs min or max")
        if self.check in DATASET_CHECKS and self.where:
            raise ValueError(f"rule {self.id}: 'where' applies to row rules only")
        return self

    @property
    def is_row_rule(self) -> bool:
        return self.check in ROW_CHECKS

    @property
    def threshold(self) -> float:
        return self.max_fail_pct if self.max_fail_pct is not None else DEFAULT_THRESHOLD_PCT[self.severity]

    def pass_sql(self, ref_view: str | None = None) -> str:
        """SQL that is TRUE when a row passes. NULLs pass every check except not_null."""
        if self.check == "not_null":
            return " AND ".join(f"{q(c)} IS NOT NULL" for c in self.columns)
        col = q(self.column) if self.column else ""
        if self.check == "accepted_values":
            return f"{col} IS NULL OR {col} IN ({', '.join(lit(v) for v in self.values or [])})"
        if self.check == "range":
            parts = []
            if self.min is not None:
                parts.append(f"{col} >= {lit(self.min)}")
            if self.max is not None:
                parts.append(f"{col} <= {lit(self.max)}")
            return f"{col} IS NULL OR ({' AND '.join(parts)})"
        if self.check == "expression":
            return f"({self.expr})"
        if self.check == "referential":
            return f"{col} IS NULL OR {col} IN (SELECT k FROM {ref_view})"
        raise ValueError(f"{self.check} is not a row rule")


@dataclass
class RuleResult:
    rule_id: str
    name: str
    check: str
    severity: str
    rows_checked: int
    rows_failed: int
    threshold_pct: float

    @property
    def fail_pct(self) -> float:
        return 100.0 * self.rows_failed / self.rows_checked if self.rows_checked else 0.0

    @property
    def status(self) -> str:
        if self.rows_failed == 0 or self.fail_pct <= self.threshold_pct:
            return "PASS"
        return "FAIL" if self.severity == "critical" else "WARN"


class DatasetRuleResult(RuleResult):
    @property
    def fail_pct(self) -> float:
        return 100.0 if self.rows_failed else 0.0

    @property
    def status(self) -> str:
        if self.rows_failed == 0:
            return "PASS"
        return "FAIL" if self.severity == "critical" else "WARN"


RefLoader = Callable[[str, str], pa.Table]


def evaluate_row_rules(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rules: list[Rule],
    ref_loader: RefLoader | None = None,
    fail_table: str = "_dq_failures",
) -> list[RuleResult]:
    """Evaluate row rules over ``table``; failing (_row_id, rule_id, severity) go to ``fail_table``."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE {fail_table} (_row_id BIGINT, rule_id VARCHAR, severity VARCHAR)")
    results = []
    for i, rule in enumerate(r for r in rules if r.is_row_rule):
        ref_view = None
        if rule.check == "referential":
            if ref_loader is None:
                raise ValueError(f"rule {rule.id} is referential but no ref_loader was given")
            ref_view = f"_dq_ref_{i}"
            keys = ref_loader(rule.ref_table, rule.ref_column)  # type: ignore[arg-type]
            con.register(f"{ref_view}_arrow", keys)
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {ref_view} AS "
                f"SELECT DISTINCT CAST({q(rule.ref_column)} AS VARCHAR) AS k FROM {ref_view}_arrow"
                if keys.num_columns
                else f"CREATE OR REPLACE TEMP TABLE {ref_view} (k VARCHAR)"
            )
            con.unregister(f"{ref_view}_arrow")
        scope = f"({rule.where})" if rule.where else "TRUE"
        checked = con.execute(f"SELECT count(*) FROM {table} WHERE {scope}").fetchall()[0][0]
        con.execute(
            f"INSERT INTO {fail_table} SELECT _row_id, {lit(rule.id)}, {lit(rule.severity)} FROM {table} "
            f"WHERE {scope} AND NOT coalesce({rule.pass_sql(ref_view)}, FALSE)"
        )
        failed = con.execute(f"SELECT count(*) FROM {fail_table} WHERE rule_id = {lit(rule.id)}").fetchall()[0][0]
        results.append(
            RuleResult(rule.id, rule.name or rule.id, rule.check, rule.severity, checked, failed, rule.threshold)
        )
    return results


def evaluate_dataset_rules(con: duckdb.DuckDBPyConnection, table: str, rules: list[Rule]) -> list[RuleResult]:
    results: list[RuleResult] = []
    total = con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
    for rule in (r for r in rules if not r.is_row_rule):
        if rule.check == "unique":
            cols = ", ".join(q(c) for c in rule.columns)
            sql = f"SELECT count(*) FROM (SELECT {cols} FROM {table} GROUP BY {cols} HAVING count(*) > 1)"
        else:
            sql = (rule.sql or "").format(table=table)
        violations = int(con.execute(sql).fetchall()[0][0] or 0)
        results.append(
            DatasetRuleResult(rule.id, rule.name or rule.id, rule.check, rule.severity, total, violations, 0.0)
        )
    return results
