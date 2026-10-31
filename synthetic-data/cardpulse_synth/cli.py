"""cardpulse-synth: generate a synthetic card portfolio into the landing zone.

cardpulse-synth --config synthetic-data/config/small.yaml --env local
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

from cardpulse_frameworks.common.config import load_env

from .config import load_config
from .generator import generate


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="synthetic-data/config/default.yaml", help="generator settings (YAML)")
    p.add_argument("--env", default="local", help="environment name or path; decides the landing root")
    p.add_argument("--out", help="override the landing root")
    p.add_argument("--clean", action="store_true", help="empty the landing root first")
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    root = Path(args.out) if args.out else load_env(args.env).landing_path()
    if args.clean and root.exists():
        # Empty the directory rather than remove it: in docker it is a volume mount point.
        for child in root.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
    t0 = time.perf_counter()
    m = generate(cfg, root)
    secs = time.perf_counter() - t0

    print(f"\nSynthetic portfolio for business date {m.business_date} (seed {m.seed}) -> {root}\n")
    print(f"| {'Entity':<34} | {'Files':>6} | {'Rows':>10} |")
    print(f"|{'-' * 36}|{'-' * 8}|{'-' * 12}|")
    for k in sorted(m.files):
        print(f"| {k:<34} | {m.files[k]:>6,} | {m.rows[k]:>10,} |")
    print(f"\n| {'Ground truth':<40} | {'Count':>8} |\n|{'-' * 42}|{'-' * 10}|")
    for k, v in m.ground_truth.items():
        print(f"| {k:<40} | {v:>8,} |")
    if m.defects:
        print(f"\nDefects injected: {m.defects}")
    print(f"\nGenerated in {secs:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
