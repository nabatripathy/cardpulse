"""cardpulse-silver: conform Bronze into Silver, with data quality gates.

cardpulse-silver run --env local                     # every entity, in dependency order
cardpulse-silver run --env local --entity posted_transaction
cardpulse-silver rebuild --env local --entity card_account
cardpulse-silver report --env local
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

from cardpulse_frameworks.common.config import REPO_ROOT, load_env
from cardpulse_frameworks.ingestion.cli import _table
from cardpulse_frameworks.ingestion.lineage import LineageEmitter
from cardpulse_frameworks.ingestion.writers import make_writer

from .pipeline import DQ_RESULTS_TABLE, UNIT_LOG_TABLE, EntityResult, SilverPipeline, find_eligible
from .spec import SilverSpec, load_silver_specs

DEFAULT_SPECS = REPO_ROOT / "configs" / "silver"


def _select(args: argparse.Namespace) -> list[SilverSpec]:
    specs = load_silver_specs(args.specs_dir)
    if args.entity:
        wanted = set(args.entity)
        unknown = wanted - {s.entity for s in specs}
        if unknown:
            raise SystemExit(f"unknown entity: {', '.join(sorted(unknown))}")
        specs = [s for s in specs if s.entity in wanted]
    return specs


def _print_results(results: list[EntityResult]) -> None:
    rows = [
        [
            r.entity,
            len(r.units),
            r.count("APPLIED"),
            r.count("REJECTED"),
            r.count("PENDING_REBUILD"),
            r.total("rows_in"),
            r.total("versions_inserted"),
            r.total("versions_changed"),
            r.total("versions_closed"),
            r.total("rows_quarantined"),
            r.total("duplicates_in_batch") + r.total("duplicates_in_target"),
            r.total("warning_rows"),
            len(r.held_by_watermark),
        ]
        for r in results
    ]
    print(
        _table(
            [
                "Entity",
                "Units",
                "Applied",
                "Rejected",
                "Rebuild",
                "Rows in",
                "New",
                "Changed",
                "Removed",
                "Quarantined",
                "Duplicates",
                "Warn rows",
                "Held",
            ],
            rows,
        )
    )
    problems = [(r.entity, u) for r in results for u in r.units if u.status != "APPLIED"]
    if problems:
        print("\nUnits not applied\n")
        print(
            _table(
                ["Entity", "Business dates", "Status", "Reason"],
                [[e, ", ".join(d.isoformat() for d in u.business_dates), u.status, u.reason] for e, u in problems],
                "llll",
            )
        )
    warns = [
        (r.entity, d)
        for r in results
        for u in r.units
        for d in u.dq
        if d.status != "PASS" or (d.severity == "warning" and d.rows_failed)
    ]
    if warns:
        print("\nData quality findings (rules with failures)\n")
        print(
            _table(
                ["Entity", "Rule", "Name", "Severity", "Failed", "Checked", "Status"],
                [[e, d.rule_id, d.name, d.severity, d.rows_failed, d.rows_checked, d.status] for e, d in warns],
                "lllllrl",
            )
        )
    held = [(r.entity, r.held_by_watermark) for r in results if r.held_by_watermark]
    for entity, refs in held:
        dates = sorted({b.business_date for b in refs})
        print(
            f"\n{entity}: {len(refs)} Bronze batches held above the watermark "
            f"({', '.join(d.isoformat() for d in dates)}); they load once the missing file arrives"
        )


def cmd_run(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    pipe = SilverPipeline(make_writer(env), LineageEmitter(env))
    t0 = time.perf_counter()
    results = []
    for spec in _select(args):
        if args.cmd == "rebuild":
            results.append(pipe.rebuild_entity(spec))
        else:
            results.append(pipe.run_entity(spec, retry_rejected=args.retry_rejected))
    print(f"\nSilver {'rebuild' if args.cmd == 'rebuild' else 'run'} ({env.env})\n")
    _print_results(results)
    print(f"\nCompleted in {time.perf_counter() - t0:.1f}s")
    bad = any(u.status == "REJECTED" for r in results for u in r.units)
    return 1 if args.fail_on_reject and bad else 0


def cmd_report(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    w = make_writer(env)
    specs = _select(args)
    print(f"\nSilver report ({env.env})\n")
    rows = []
    for s in specs:
        t = w.read(s.target_table)
        qn = w.read(s.quarantine_table, columns=("entity",)).num_rows
        if t.num_rows and "_is_current" in t.column_names:
            current = sum(1 for v in t.column("_is_current").to_pylist() if v)
            history = t.num_rows - current
        else:
            current, history = t.num_rows, 0
        el = find_eligible(s, w)
        rows.append(
            [s.target_table, s.load, t.num_rows, current, history, qn, len(el.eligible), len(el.held_by_watermark)]
        )
    print(
        _table(["Silver table", "Load", "Rows", "Current", "Closed versions", "Quarantined", "Pending", "Held"], rows)
    )

    dq = w.read(DQ_RESULTS_TABLE)
    if dq.num_rows:
        agg: dict[tuple[str, str], list[int]] = {}
        meta: dict[tuple[str, str], tuple[str, str]] = {}
        status: dict[tuple[str, str], Counter] = {}
        for r in dq.to_pylist():
            if r["layer"] != "silver" or r["entity"] not in {s.entity for s in specs}:
                continue
            k = (r["entity"], r["rule_id"])
            a = agg.setdefault(k, [0, 0])
            a[0] += r["rows_checked"]
            a[1] += r["rows_failed"]
            meta[k] = (r["rule_name"], r["severity"])
            status.setdefault(k, Counter())[r["status"]] += 1
        hits = sorted(k for k, a in agg.items() if a[1])
        print(f"\nData quality: {len(agg)} rules evaluated, {len(hits)} with failures (all runs)\n")
        if hits:
            print(
                _table(
                    ["Entity", "Rule", "Name", "Severity", "Rows failed", "Rows checked", "Fail %", "Outcomes"],
                    [
                        [
                            k[0],
                            k[1],
                            meta[k][0],
                            meta[k][1],
                            agg[k][1],
                            agg[k][0],
                            f"{100 * agg[k][1] / agg[k][0]:.3f}" if agg[k][0] else "-",
                            ", ".join(f"{s} {n}" for s, n in sorted(status[k].items())),
                        ]
                        for k in hits
                    ],
                    "llllrrrl",
                )
            )
    log = w.read(UNIT_LOG_TABLE)
    if log.num_rows:
        c = Counter((r["entity"], r["status"]) for r in log.to_pylist())
        print("\nUnit outcomes (all runs)\n")
        print(_table(["Entity", "Status", "Units"], [[e, s, n] for (e, s), n in sorted(c.items())], "llr"))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="cardpulse-silver", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "rebuild", "report"):
        sp = sub.add_parser(name)
        sp.add_argument("--env", default="local", help="environment name (configs/env) or path")
        sp.add_argument(
            "--entity", action="append", help="entity name; repeatable (default: all)", required=name == "rebuild"
        )
        sp.add_argument("--specs-dir", type=Path, default=DEFAULT_SPECS)
        if name != "report":
            sp.add_argument("--fail-on-reject", action="store_true", help="exit 1 if a unit is rejected")
        if name == "run":
            sp.add_argument(
                "--retry-rejected",
                action="store_true",
                help="re-evaluate batches a DQ gate rejected before (e.g. after a threshold change)",
            )
    args = p.parse_args(argv)
    return cmd_report(args) if args.cmd == "report" else cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
