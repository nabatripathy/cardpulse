"""cardpulse-ingest: load landed files into Bronze, and report on what is there.

cardpulse-ingest run --env local --all
cardpulse-ingest run --env local --source configs/sources/processor_posted_txn.yaml --business-date 2027-03-15
cardpulse-ingest report --env local
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

from cardpulse_frameworks.common.config import REPO_ROOT, load_env

from .lineage import LineageEmitter
from .pipeline import FILE_LOG_TABLE, FileLog, SourceResult, compute_watermarks, ingest_source
from .spec import SourceSpec, load_spec, load_specs
from .writers import make_writer

DEFAULT_SPECS = REPO_ROOT / "configs" / "sources"


def _table(headers: list[str], rows: list[list[object]], align: str | None = None) -> str:
    """Render a Markdown table (readable in a terminal and pastes cleanly into docs)."""
    cells = [[str(h) for h in headers]] + [[f"{c:,}" if isinstance(c, int) else str(c) for c in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]
    align = align or "l" + "r" * (len(headers) - 1)
    out = []
    for n, r in enumerate(cells):
        out.append(
            "| "
            + " | ".join(
                c.ljust(w) if align[i] == "l" else c.rjust(w) for i, (c, w) in enumerate(zip(r, widths, strict=True))
            )
            + " |"
        )
        if n == 0:
            out.append("|" + "|".join(("-" * (w + 2)) for w in widths) + "|")
    return "\n".join(out)


def _specs(args: argparse.Namespace) -> list[SourceSpec]:
    if args.source:
        return [load_spec(s) for s in args.source]
    return load_specs(args.specs_dir)


def cmd_run(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    writer = make_writer(env)
    lineage = LineageEmitter(env)
    landing = env.landing_path()
    bdate = date.fromisoformat(args.business_date) if args.business_date else None
    t0 = time.perf_counter()
    results: list[SourceResult] = []
    for spec in _specs(args):
        results.append(ingest_source(spec, landing, writer, lineage, bdate, args.reprocess))

    print(f"\nBronze ingestion ({env.env}) from {landing}\n")
    rows = [
        [
            r.source,
            r.count("LOADED"),
            r.count("REJECTED"),
            r.count("SKIPPED"),
            r.total("rows_read"),
            r.total("rows_loaded"),
            r.total("rows_quarantined"),
            r.total("duplicate_keys"),
        ]
        for r in results
    ]
    print(
        _table(["Source", "Loaded", "Rejected", "Skipped", "Rows read", "Rows loaded", "Quarantined", "Dup keys"], rows)
    )
    rejected = [(r.source, f) for r in results for f in r.files if f.status == "REJECTED"]
    if rejected:
        print("\nRejected files\n")
        print(_table(["Source", "File", "Reason"], [[s, f.file_name, f.reason] for s, f in rejected], "lll"))
    gaps = [(r.source, w) for r in results for w in r.watermarks if w.gaps]
    if gaps:
        print("\nWatermarks with gaps (downstream micro-batches stop at the watermark)\n")
        print(
            _table(
                ["Source", "Business date", "Watermark seq", "Max seq seen", "Missing or rejected"],
                [[s, w.business_date, w.watermark_seq, w.max_seq_seen, ", ".join(map(str, w.gaps))] for s, w in gaps],
                "lrrrl",
            )
        )
    print(f"\nCompleted in {time.perf_counter() - t0:.1f}s")
    return 1 if args.fail_on_reject and rejected else 0


def cmd_report(args: argparse.Namespace) -> int:
    env = load_env(args.env)
    writer = make_writer(env)
    specs = _specs(args)
    print(f"\nBronze report ({env.env})\n")
    rows = []
    for s in specs:
        bronze = writer.read(s.target_table, columns=("_business_date",))
        q = writer.read(s.quarantine_table, columns=("reason",))
        dates = bronze.column("_business_date").to_pylist() if bronze.num_rows else []
        rows.append([s.target_table, bronze.num_rows, len(set(dates)), q.num_rows])
    print(_table(["Bronze table", "Rows", "Business dates", "Quarantined"], rows))

    log = writer.read(FILE_LOG_TABLE)
    if log.num_rows:
        status = Counter(
            zip(
                log.column("source").to_pylist(),
                log.column("entity").to_pylist(),
                log.column("status").to_pylist(),
                strict=True,
            )
        )
        print("\nControl log: file outcomes (all attempts)\n")
        print(
            _table(
                ["Source", "Status", "Files"], [[f"{s}.{e}", st, n] for (s, e, st), n in sorted(status.items())], "llr"
            )
        )

    reasons: Counter[tuple[str, str]] = Counter()
    for s in specs:
        q = writer.read(s.quarantine_table, columns=("reason",))
        for r in q.column("reason").to_pylist() if q.num_rows else []:
            reasons[(s.quarantine_table, r)] += 1
    if reasons:
        print("\nQuarantine by reason\n")
        print(
            _table(
                ["Quarantine table", "Reason", "Records"], [[t, r, n] for (t, r), n in sorted(reasons.items())], "llr"
            )
        )

    for s in specs:
        if s.watermark != "sequence":
            continue
        wms = compute_watermarks(FileLog(writer).latest(s))
        if wms and (wms[-1].max_seq_seen > 1 or wms[-1].gaps):
            w = wms[-1]
            print(
                f"\nWatermark for {s.name} on {w.business_date}: seq {w.watermark_seq} of {w.max_seq_seen}"
                + (f" (gaps: {', '.join(map(str, w.gaps))})" if w.gaps else "")
            )
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="cardpulse-ingest", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "report"):
        sp = sub.add_parser(name)
        sp.add_argument("--env", default="local", help="environment name (configs/env) or path")
        g = sp.add_mutually_exclusive_group()
        g.add_argument("--source", action="append", type=Path, help="spec file; repeatable")
        g.add_argument("--all", action="store_true", help="every spec in --specs-dir (default)")
        sp.add_argument("--specs-dir", type=Path, default=DEFAULT_SPECS)
        if name == "run":
            sp.add_argument("--business-date", help="only files for this date (YYYY-MM-DD)")
            sp.add_argument("--reprocess", action="store_true", help="reload files even if already processed")
            sp.add_argument("--fail-on-reject", action="store_true", help="exit 1 if any file is rejected")
    args = p.parse_args(argv)
    return cmd_run(args) if args.cmd == "run" else cmd_report(args)


if __name__ == "__main__":
    sys.exit(main())
