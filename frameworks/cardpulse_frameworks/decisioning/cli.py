"""cardpulse-decide: evaluate triggers and suppression rules, log decisions, publish to Gold.

    cardpulse-decide run --env local                    # business date = latest Card Account Snapshot
    cardpulse-decide run --env local --date 2027-03-15
    cardpulse-decide report --env local

`run` needs a built Gold layer (`cardpulse-gold run`). It writes the decision log to Iceberg
(namespace `decision`), then stages it into the Gold warehouse and builds the decisioning
models (fct_trigger_event, fct_offer_decision and the Offer Eligibility data product) with
their dbt tests. Rerunning a business date replaces that date's decisions.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date

import duckdb
from pyiceberg.expressions import AlwaysTrue, EqualTo

from cardpulse_frameworks.common.config import REPO_ROOT, EnvConfig, load_env
from cardpulse_frameworks.gold.runner import GoldRun, dbt_paths, emit_lineage, read_artifacts, record_dq, run_build
from cardpulse_frameworks.gold.stage import StagedTable, stage_tables
from cardpulse_frameworks.ingestion.cli import _table
from cardpulse_frameworks.ingestion.lineage import LineageEmitter, table_dataset
from cardpulse_frameworks.ingestion.writers import make_writer
from cardpulse_frameworks.ingestion.writers.base import TableWriter

from .engine import (
    DECISION_TABLES,
    OFFER_DECISION_TABLE,
    OFFER_ELIGIBILITY_TABLE,
    TRIGGER_EVENT_TABLE,
    DecisionRun,
    evaluate,
)
from .spec import RuleSet, TriggerSpec, load_rule_set, load_triggers

TRIGGERS_DIR = REPO_ROOT / "configs" / "triggers"
RULES_PATH = REPO_ROOT / "configs" / "eligibility" / "suppression_rules.yaml"
ENGINE_INPUTS = ["gold.fct_account_daily_snapshot", "gold.customer_360", "gold.dim_offer", "gold.fct_offer_contact"]


@dataclass
class DecideResult:
    run: DecisionRun
    staged: list[StagedTable]
    build: GoldRun
    dq_rows: int
    engine_seconds: float


def decide(
    env: EnvConfig,
    writer: TableWriter,
    lineage: LineageEmitter,
    business_date: date | None = None,
    verbose: bool = False,
    triggers: list[TriggerSpec] | None = None,
    rules: RuleSet | None = None,
) -> DecideResult:
    """Evaluate, write the decision log to Iceberg, stage it into Gold and build the decisioning models."""
    triggers = triggers if triggers is not None else load_triggers(TRIGGERS_DIR)
    rules = rules if rules is not None else load_rule_set(RULES_PATH)
    t0 = time.perf_counter()
    run_id = lineage.new_run_id()
    job = "decide.engine"
    inputs = [{"namespace": env.gold.lineage_namespace, "name": n} for n in ENGINE_INPUTS]
    lineage.emit("START", run_id, job, inputs=inputs, job_type="DECISION")
    prior = writer.read(OFFER_DECISION_TABLE) if writer.table_exists(OFFER_DECISION_TABLE) else None
    try:
        run = evaluate(env.gold_path(), triggers, rules, run_id, business_date, prior)
    except Exception as e:
        lineage.emit("FAIL", run_id, job, inputs=inputs, error=str(e)[:2000], job_type="DECISION")
        raise
    # Rerunning a business date replaces it: one atomic commit per table.
    by_date = EqualTo("business_date", run.business_date.isoformat())
    writer.commit(TRIGGER_EVENT_TABLE, run.trigger_events, "business_date", by_date)
    writer.commit(OFFER_DECISION_TABLE, run.decisions, "business_date", by_date)
    writer.commit(OFFER_ELIGIBILITY_TABLE, run.eligibility, None, AlwaysTrue())
    outputs = [
        table_dataset(t, [(f.name, str(f.type)) for f in tbl.schema], tbl.num_rows)
        for t, tbl in zip(DECISION_TABLES, (run.trigger_events, run.decisions, run.eligibility), strict=True)
    ]
    lineage.emit("COMPLETE", run_id, job, inputs=inputs, outputs=outputs, job_type="DECISION")
    engine_s = time.perf_counter() - t0

    staged = stage_tables(writer, DECISION_TABLES, env.gold_path())
    build = run_build(env, ["tag:decisioning"], verbose=verbose, run_name="decide")
    dq_rows = record_dq(writer, build, lineage.new_run_id())
    emit_lineage(env, lineage, build)
    return DecideResult(run, staged, build, dq_rows, engine_s)


def cmd_run(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    if env.gold.target != "local":
        print(f"gold target is {env.gold.target}: the decision engine runs as the eligibility service there")
        return 1
    if not env.gold_path().exists():
        print(f"no Gold warehouse at {env.gold_path()}; run `cardpulse-gold run` first")
        return 1
    t0 = time.perf_counter()
    triggers, rules = load_triggers(TRIGGERS_DIR), load_rule_set(RULES_PATH)
    res = decide(
        env,
        make_writer(env),
        LineageEmitter(env),
        date.fromisoformat(args.date) if args.date else None,
        args.verbose,
        triggers,
        rules,
    )
    run, staged, build, dq_rows, engine_s = res.run, res.staged, res.build, res.dq_rows, res.engine_seconds

    print(
        f"\nDecision run ({env.env}) for business date {run.business_date}: triggers "
        + ", ".join(f"{t.trigger} {t.version}" for t in triggers)
        + f"; rule set {rules.version}\n"
    )
    _print_summary(run.trigger_events.to_pylist(), run.decisions.to_pylist())
    elig = run.eligibility.to_pylist()
    print(f"\nOffer Eligibility: {len(elig):,} customer-offer rows, {sum(r['is_eligible'] for r in elig):,} eligible")
    print()
    print(_table(["Decision table", "Rows", "Status"], [[s.table, s.rows, s.status] for s in staged], "lrl"))
    print()
    marts = [m for m in build.models if not m.name.startswith("stg_")]
    print(_table(["Gold model", "Status", "Rows"], [[m.relation, m.status, m.rows] for m in marts], "llr"))
    c = Counter(t.status for t in build.tests)
    print(f"\n{len(build.tests)} dbt tests: " + ", ".join(f"{n} {s}" for s, n in sorted(c.items())))
    for t in build.tests:
        if t.status != "pass":
            print(f"  {t.status.upper()}: {t.name} ({t.failures} rows)")
    for m in build.models:
        if m.status != "success":
            print(f"  MODEL {m.status.upper()}: {m.name}: {(m.message or '').strip()[:400]}")
    print(f"\nDQ results recorded: {dq_rows}; engine {engine_s:.1f}s, total {time.perf_counter() - t0:.1f}s")
    return 0 if build.success else 1


def _print_summary(events: list[dict], decisions: list[dict]) -> None:
    triggers = sorted({e["trigger_cd"] for e in events} | {d["trigger_cd"] for d in decisions})
    ev = Counter((e["trigger_cd"], e["status"]) for e in events)
    de = Counter((d["trigger_cd"], d["outcome"]) for d in decisions)
    rows = [
        [
            t,
            sum(n for (tt, _), n in ev.items() if tt == t),
            ev[(t, "SKIPPED_REPEAT_GAP")] + ev[(t, "SKIPPED_MAX_FIRES")],
            ev[(t, "FIRED")],
            de[(t, "SUPPRESSED")],
            de[(t, "HOLDOUT")],
            de[(t, "SENT")],
        ]
        for t in triggers
    ]
    print(
        _table(
            ["Trigger", "Met conditions", "Held by repeat rule", "Fired", "Suppressed", "Holdout", "Sent"],
            rows,
            "lrrrrrr",
        )
    )
    primary = Counter((d["trigger_cd"], d["primary_reason_cd"]) for d in decisions if d["outcome"] == "SUPPRESSED")
    if primary:
        print("\nSuppression by primary reason (first matching rule)\n")
        print(_table(["Trigger", "Reason", "Decisions"], [[t, r, n] for (t, r), n in sorted(primary.items())], "llr"))
    mix = Counter(
        (d["trigger_cd"], d["offer_code"], d["channel_cd"], d["offer_rank"])
        for d in decisions
        if d["outcome"] == "SENT"
    )
    if mix:
        print("\nOffers sent\n")
        print(
            _table(
                ["Trigger", "Offer", "Channel", "Rank", "Sent"],
                [[t, o, ch, r, n] for (t, o, ch, r), n in sorted(mix.items())],
                "lllrr",
            )
        )


def cmd_report(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    path = env.gold_path()
    con = duckdb.connect(str(path), read_only=True) if path.exists() else None
    try:
        if (
            con is None
            or not con.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'gold' AND table_name = 'fct_offer_decision'"
            ).fetchall()[0][0]
        ):
            print("no decisions in Gold yet; run `cardpulse-decide run` first")
            return 1
        bd = con.execute("SELECT max(business_date) FROM gold.fct_offer_decision").fetchall()[0][0]
        print(f"\nDecision report ({env.env}), business date {bd}\n")
        events = [
            dict(zip(("trigger_cd", "status"), r, strict=True))
            for r in con.execute(
                "SELECT trigger_cd, status FROM gold.fct_trigger_event WHERE business_date = ?", [bd]
            ).fetchall()
        ]
        decisions = [
            dict(
                zip(
                    ("trigger_cd", "outcome", "primary_reason_cd", "offer_code", "channel_cd", "offer_rank"),
                    r,
                    strict=True,
                )
            )
            for r in con.execute(
                "SELECT trigger_cd, outcome, primary_reason_cd, offer_code, channel_cd, offer_rank "
                "FROM gold.fct_offer_decision WHERE business_date = ?",
                [bd],
            ).fetchall()
        ]
        _print_summary(events, decisions)
        print("\nOffer Eligibility (gold.offer_eligibility)\n")
        totals = con.execute(
            "SELECT offer_code, count(*), count(*) FILTER (WHERE is_eligible) FROM gold.offer_eligibility "
            "GROUP BY offer_code ORDER BY offer_code"
        ).fetchall()
        reasons: dict[str, list[str]] = {}
        for offer, code, n in con.execute(
            "SELECT offer_code, suppression_codes[1], count(*) FROM gold.offer_eligibility WHERE NOT is_eligible "
            "GROUP BY ALL ORDER BY offer_code, 2"
        ).fetchall():
            reasons.setdefault(offer, []).append(f"{code} {n:,}")
        print(
            _table(
                ["Offer", "Customers", "Eligible", "Blocked, by primary reason"],
                [[o, n, e, ", ".join(reasons.get(o, [])) or "-"] for o, n, e in totals],
                "lrrl",
            )
        )
    finally:
        if con is not None:
            con.close()
    target_path, _ = dbt_paths(env, "decide")
    if (target_path / "run_results.json").exists():
        models, tests = read_artifacts(target_path)
        c = Counter(t.status for t in tests)
        print(
            f"\nLast decisioning dbt build: {len(models)} models, {len(tests)} tests ("
            + ", ".join(f"{n} {s}" for s, n in sorted(c.items()))
            + ")"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="cardpulse-decide", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "report"):
        sp = sub.add_parser(name)
        sp.add_argument("--env", default="local", help="environment name (configs/env) or path")
        if name == "run":
            sp.add_argument("--date", help="business date, YYYY-MM-DD (default: latest snapshot)")
            sp.add_argument("--verbose", action="store_true", help="show dbt's own log output")
    args = p.parse_args(argv)
    return cmd_run(args) if args.cmd == "run" else cmd_report(args)


if __name__ == "__main__":
    sys.exit(main())
