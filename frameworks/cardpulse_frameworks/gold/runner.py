"""Run dbt for the Gold layer and turn its artifacts into CardPulse results.

dbt runs in-process (dbtRunner). Its artifacts (manifest.json, run_results.json) are
then read to produce: a per-model and per-test summary, data quality rows in
``ops.dq_results`` (layer = gold, alongside Silver's), and OpenLineage events per model
(Silver Iceberg tables -> staging views -> Gold tables).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from cardpulse_frameworks.common.config import REPO_ROOT, EnvConfig
from cardpulse_frameworks.ingestion.lineage import LineageEmitter, table_dataset
from cardpulse_frameworks.ingestion.writers.base import TableWriter
from cardpulse_frameworks.silver.pipeline import DQ_RESULTS_SCHEMA, DQ_RESULTS_TABLE

DBT_DIR = REPO_ROOT / "dbt"


@dataclass
class NodeResult:
    unique_id: str
    name: str
    resource_type: str  # model | test
    status: str  # success | error | skipped | pass | fail | warn
    seconds: float
    failures: int = 0
    message: str | None = None
    relation: str | None = None  # schema.name for models
    attached_model: str | None = None  # for tests
    severity: str = "error"
    test_type: str | None = None
    depends_on: list[str] = field(default_factory=list)
    rows: int | None = None


@dataclass
class GoldRun:
    success: bool
    models: list[NodeResult]
    tests: list[NodeResult]
    seconds: float
    target_path: Path


def dbt_paths(env: EnvConfig) -> tuple[Path, Path]:
    work = env.gold_path().parent / "dbt"
    return work / "target", work / "logs"


def invoke_dbt(env: EnvConfig, args: list[str], verbose: bool = False) -> bool:
    """Run dbt in a child process; returns True on success.

    A child process keeps dbt's DuckDB connection (and its settings) out of this process,
    so CardPulse can open the warehouse afterwards to count rows and read columns.
    """
    target_path, log_path = dbt_paths(env)
    child_env = {
        **os.environ,
        "CARDPULSE_GOLD_PATH": str(env.gold_path()),
        "CARDPULSE_DBT_TARGET": env.gold.target,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    flags = ["--no-use-colors", "--no-version-check", "--no-send-anonymous-usage-stats"]
    if not verbose:
        flags.append("--quiet")
    cmd = [
        sys.executable,
        "-c",
        "from dbt.cli.main import cli; cli()",
        *flags,
        *args,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
    ]
    return subprocess.run(cmd, env=child_env, check=False).returncode == 0  # noqa: S603 - fixed command


def run_build(
    env: EnvConfig, select: list[str] | None = None, full_refresh: bool = False, verbose: bool = False
) -> GoldRun:
    args = ["build"]
    if select:
        args += ["--select", *select]
    if full_refresh:
        args.append("--full-refresh")
    t0 = datetime.now(UTC)
    ok = invoke_dbt(env, args, verbose)
    seconds = (datetime.now(UTC) - t0).total_seconds()
    target_path, _ = dbt_paths(env)
    models, tests = read_artifacts(target_path)
    _count_rows(env, models)
    return GoldRun(ok, models, tests, round(seconds, 1), target_path)


def read_artifacts(target_path: Path) -> tuple[list[NodeResult], list[NodeResult]]:
    run_results = json.loads((target_path / "run_results.json").read_text())
    manifest = json.loads((target_path / "manifest.json").read_text())
    nodes = manifest["nodes"]
    models, tests = [], []
    for r in run_results["results"]:
        node = nodes.get(r["unique_id"])
        if node is None:
            continue
        nr = NodeResult(
            unique_id=r["unique_id"],
            name=node["name"],
            resource_type=node["resource_type"],
            status=r["status"],
            seconds=round(r.get("execution_time") or 0.0, 2),
            failures=int(r.get("failures") or 0),
            message=r.get("message"),
            depends_on=list(node.get("depends_on", {}).get("nodes", [])),
        )
        if node["resource_type"] == "model":
            nr.relation = f"{node['schema']}.{node['alias']}"
            models.append(nr)
        elif node["resource_type"] == "test":
            nr.severity = (node.get("config", {}).get("severity") or "error").lower()
            meta = node.get("test_metadata") or {}
            nr.test_type = meta.get("name") or "singular"
            attached = node.get("attached_node") or next((d for d in nr.depends_on if d.startswith("model.")), None)
            nr.attached_model = attached.split(".")[-1] if attached else None
            tests.append(nr)
    return models, tests


def _count_rows(env: EnvConfig, models: list[NodeResult]) -> None:
    path = env.gold_path()
    if env.gold.target != "local" or not path.exists():
        return
    con = duckdb.connect(str(path), read_only=True)
    try:
        for m in models:
            if m.status == "success" and m.relation:
                schema, name = m.relation.split(".", 1)
                m.rows = con.execute(f'SELECT count(*) FROM "{schema}"."{name}"').fetchall()[0][0]
    finally:
        con.close()


def record_dq(writer: TableWriter, run: GoldRun, run_id: str) -> int:
    """Write one ops.dq_results row per dbt test (layer = gold)."""
    rows_by_model = {m.name: m.rows or 0 for m in run.models}
    status_map = {"pass": "PASS", "warn": "WARN", "fail": "FAIL", "error": "FAIL", "skipped": "FAIL"}
    now = datetime.now(UTC)
    rows = [
        {
            "layer": "gold",
            "entity": t.attached_model or "gold",
            "unit_id": run_id[:16],
            "run_id": run_id,
            "business_dates": [],
            "rule_id": t.name,
            "rule_name": t.name,
            "check_type": t.test_type,
            "severity": "warning" if t.severity == "warn" else "critical",
            "rows_checked": rows_by_model.get(t.attached_model or "", 0),
            "rows_failed": t.failures,
            "fail_pct": 0.0,
            "threshold_pct": 0.0,
            "status": status_map.get(t.status, "FAIL"),
            "evaluated_at": now,
        }
        for t in run.tests
    ]
    if rows:
        writer.append(DQ_RESULTS_TABLE, pa.Table.from_pylist(rows, schema=DQ_RESULTS_SCHEMA), None)
    return len(rows)


def emit_lineage(env: EnvConfig, lineage: LineageEmitter, run: GoldRun) -> int:
    """One START + COMPLETE/FAIL event per model: inputs are its Silver sources and upstream models."""
    ns = env.gold.lineage_namespace
    columns = _relation_columns(env)
    by_id = {m.unique_id: m for m in run.models}
    n = 0
    for m in run.models:
        inputs = []
        for dep in m.depends_on:
            if dep.startswith("source."):
                inputs.append({"namespace": "iceberg", "name": f"silver.{dep.split('.')[-1]}"})
            elif dep in by_id and by_id[dep].relation:
                inputs.append({"namespace": ns, "name": by_id[dep].relation})
        run_id = lineage.new_run_id()
        job = f"gold.{m.name}"
        lineage.emit("START", run_id, job, inputs=inputs, job_type="MODEL")
        if m.status == "success":
            out = table_dataset(m.relation or m.name, columns.get(m.relation or "", []), m.rows)
            out["namespace"] = ns
            lineage.emit("COMPLETE", run_id, job, inputs=inputs, outputs=[out], job_type="MODEL")
        else:
            lineage.emit("FAIL", run_id, job, inputs=inputs, error=m.message or m.status, job_type="MODEL")
        n += 1
    return n


def _relation_columns(env: EnvConfig) -> dict[str, list[tuple[str, str]]]:
    path = env.gold_path()
    if env.gold.target != "local" or not path.exists():
        return {}
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            "SELECT table_schema || '.' || table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema IN ('gold', 'staging') ORDER BY table_schema, table_name, ordinal_position"
        ).fetchall()
    finally:
        con.close()
    out: dict[str, list[tuple[str, str]]] = {}
    for rel, col, typ in rows:
        out.setdefault(rel, []).append((col, typ))
    return out
