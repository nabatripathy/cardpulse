"""cardpulse-gold: build the Gold layer with dbt and report on it.

cardpulse-gold run --env local                  # stage Silver, dbt build (models + tests), record results
cardpulse-gold run --env local --select customer_360+ --full-refresh
cardpulse-gold report --env local
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter

import duckdb

from cardpulse_frameworks.common.config import REPO_ROOT, load_env
from cardpulse_frameworks.ingestion.cli import _table
from cardpulse_frameworks.ingestion.lineage import LineageEmitter
from cardpulse_frameworks.ingestion.writers import make_writer
from cardpulse_frameworks.silver.spec import load_silver_specs

from .runner import dbt_paths, emit_lineage, read_artifacts, record_dq, run_build
from .stage import stage_silver

# MVP trigger populations, read straight from the Card Account Snapshot (Phase 4 trigger specs).
# They preview Increment 4: the trigger engine will apply the same rules, then suppression.
TRIGGER_SQL = {
    "T01 new account not activated (card issued 10-60 days ago, no purchase)": """
        SELECT count(*) FROM gold.fct_account_daily_snapshot
        WHERE snapshot_date = (SELECT max(snapshot_date) FROM gold.fct_account_daily_snapshot)
          AND status_cd = 'OPEN' AND lifetime_purchase_count = 0
          AND days_since_card_issue BETWEEN 10 AND 60""",
    "T02 spend decline (30-day spend < 50% of 6-month average)": """
        SELECT count(*) FROM gold.fct_account_daily_snapshot
        WHERE snapshot_date = (SELECT max(snapshot_date) FROM gold.fct_account_daily_snapshot)
          AND status_cd = 'OPEN' AND months_on_book >= 6 AND avg_monthly_spend_6m >= 200
          AND spend_30d < 0.5 * avg_monthly_spend_6m AND coalesce(days_past_due, 0) = 0""",
}


def cmd_run(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    writer = make_writer(env)
    lineage = LineageEmitter(env)
    t0 = time.perf_counter()
    if env.gold.target != "local":
        print(f"gold target is {env.gold.target}: Silver is read in place, nothing to stage")
        staged = []
    else:
        tables = [s.target_table for s in load_silver_specs(REPO_ROOT / "configs" / "silver")]
        staged = stage_silver(writer, tables, env.gold_path())
    run = run_build(env, args.select, args.full_refresh, args.verbose)
    dq_rows = record_dq(writer, run, lineage.new_run_id())
    events = emit_lineage(env, lineage, run)

    print(f"\nGold build ({env.env}) -> {env.gold_path() if env.gold.target == 'local' else env.gold.target}\n")
    if staged:
        print(
            _table(
                ["Silver table staged", "Snapshot", "Rows", "Status"],
                [[s.table, str(s.snapshot_id or "-"), s.rows, s.status] for s in staged],
                "lrrl",
            )
        )
        print()
    marts = [m for m in run.models if not m.name.startswith("stg_")]
    print(
        _table(
            ["Gold model", "Status", "Rows", "Seconds"],
            [[m.relation, m.status, m.rows if m.rows is not None else "-", m.seconds] for m in marts],
            "llrr",
        )
    )
    views = len(run.models) - len(marts)
    c = Counter(t.status for t in run.tests)
    print(
        f"\n{views} staging views built; {len(run.tests)} dbt tests: "
        + ", ".join(f"{n} {s}" for s, n in sorted(c.items()))
    )
    bad = [t for t in run.tests if t.status not in ("pass",)]
    if bad:
        print("\nTests not passing\n")
        print(
            _table(
                ["Test", "Model", "Status", "Failing rows"],
                [[t.name[:70], t.attached_model or "-", t.status, t.failures] for t in bad],
                "lllr",
            )
        )
    errors = [m for m in run.models if m.status not in ("success",)]
    for m in errors:
        print(f"\nMODEL {m.status.upper()}: {m.name}: {(m.message or '').strip()[:500]}")
    print(
        f"\nDQ results recorded: {dq_rows}; lineage events: {events * 2}; completed in "
        f"{time.perf_counter() - t0:.1f}s (dbt {run.seconds}s)"
    )
    return 0 if run.success else 1


def cmd_report(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    path = env.gold_path()
    if not path.exists():
        print(f"no Gold warehouse at {path}; run `cardpulse-gold run` first")
        return 1
    con = duckdb.connect(str(path), read_only=True)
    try:
        rels = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold' ORDER BY table_name"
        ).fetchall()
        rows = []
        for (name,) in rels:
            n = con.execute(f'SELECT count(*) FROM gold."{name}"').fetchall()[0][0]
            kind = (
                "data product"
                if name in ("customer_360", "fct_account_daily_snapshot")
                else ("fact" if name.startswith("fct_") else "dimension")
            )
            rows.append([f"gold.{name}", kind, n])
        print(f"\nGold report ({env.env})\n")
        print(_table(["Gold table", "Kind", "Rows"], rows, "llr"))

        if any(r[0] == "gold.customer_360" for r in rows):
            kpi = con.execute("""
                SELECT max(as_of_date), count(*), sum(open_account_count), sum(total_credit_limit),
                       sum(total_current_balance), sum(spend_30d),
                       count(*) FILTER (WHERE email_opt_in), count(*) FILTER (WHERE push_opt_in),
                       count(*) FILTER (WHERE is_do_not_solicit), count(*) FILTER (WHERE max_days_past_due > 0)
                FROM gold.customer_360""").fetchall()[0]
            print("\nPortfolio as of", kpi[0], "\n")
            print(
                _table(
                    ["Measure", "Value"],
                    [
                        ["Customers", kpi[1]],
                        ["Open accounts", int(kpi[2] or 0)],
                        ["Total credit limit (USD)", f"{kpi[3]:,.0f}"],
                        ["Receivables (USD)", f"{kpi[4]:,.0f}"],
                        ["Spend, last 30 days (USD)", f"{kpi[5]:,.0f}"],
                        ["Customers opted in to marketing email", kpi[6]],
                        ["Customers opted in to marketing push", kpi[7]],
                        ["Customers on do-not-solicit", kpi[8]],
                        ["Customers with a delinquent account", kpi[9]],
                    ],
                    "lr",
                )
            )
            print("\nTrigger populations before suppression (preview of Increment 4)\n")
            print(
                _table(
                    ["Trigger", "Accounts"],
                    [[k, con.execute(sql).fetchall()[0][0]] for k, sql in TRIGGER_SQL.items()],
                    "lr",
                )
            )
    finally:
        con.close()

    target_path, _ = dbt_paths(env)
    if (target_path / "run_results.json").exists():
        models, tests = read_artifacts(target_path)
        c = Counter(t.status for t in tests)
        print(
            f"\nLast dbt build: {len(models)} models, {len(tests)} tests ("
            + ", ".join(f"{n} {s}" for s, n in sorted(c.items()))
            + ")"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="cardpulse-gold", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "report"):
        sp = sub.add_parser(name)
        sp.add_argument("--env", default="local", help="environment name (configs/env) or path")
        if name == "run":
            sp.add_argument("--select", nargs="+", help="dbt selection, e.g. customer_360+")
            sp.add_argument("--full-refresh", action="store_true", help="rebuild incremental models from scratch")
            sp.add_argument("--verbose", action="store_true", help="show dbt's own log output")
    args = p.parse_args(argv)
    return cmd_run(args) if args.cmd == "run" else cmd_report(args)


if __name__ == "__main__":
    sys.exit(main())
