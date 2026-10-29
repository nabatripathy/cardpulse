"""Check that a docker-profile run really used the docker services.

Run after `cardpulse-synth`, `cardpulse-ingest` and `cardpulse-silver` with
`--env docker`. It fails (exit 1) unless:

* every Bronze and Silver table exists in the Iceberg REST catalog and has rows;
* the tables live in MinIO (``s3://``), not on a local disk;
* Marquez received lineage for every Bronze ingest job and every Silver transform job.

    python tools/verify_docker_profile.py --env docker
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    from cardpulse_frameworks.common.config import load_env
    from cardpulse_frameworks.ingestion.spec import load_specs
    from cardpulse_frameworks.ingestion.writers import make_writer
    from cardpulse_frameworks.silver.spec import load_silver_specs

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--env", default="docker")
    args = p.parse_args(argv)
    env = load_env(args.env)
    w = make_writer(env)
    bronze = load_specs(ROOT / "configs" / "sources")
    silver = load_silver_specs(ROOT / "configs" / "silver")

    failures: list[str] = []
    rows = []
    for table in [s.target_table for s in bronze] + [s.target_table for s in silver]:
        if not w.table_exists(table):
            failures.append(f"{table}: missing")
            continue
        location = w.catalog.load_table(table).metadata.location
        n = w.read(table, columns=()).num_rows
        rows.append((table, n, location))
        if n == 0:
            failures.append(f"{table}: no rows")
        if not location.startswith("s3://"):
            failures.append(f"{table}: stored at {location}, expected s3:// (MinIO)")

    jobs: set[str] = set()
    if env.lineage.transport == "http" and env.lineage.url:
        url = f"{env.lineage.url.rstrip('/')}/api/v1/namespaces/{env.lineage.namespace}/jobs?limit=200"
        with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 - URL comes from env config
            jobs = {j["name"] for j in json.load(resp).get("jobs", [])}
        expected = {f"ingest.{s.name}" for s in bronze} | {f"silver.{s.entity}" for s in silver}
        missing = sorted(expected - jobs)
        if missing:
            failures.append(f"Marquez is missing lineage for: {', '.join(missing)}")
    else:
        failures.append("lineage transport is not http; this check expects Marquez")

    print(f"\nDocker profile verification ({env.env})\n")
    print("| Table | Rows | Location |\n|---|---:|---|")
    for table, n, loc in rows:
        print(f"| {table} | {n:,} | {loc} |")
    print(f"\nMarquez jobs in namespace {env.lineage.namespace}: {len(jobs)}")
    if failures:
        print("\nFAILED\n" + "\n".join(f"- {f}" for f in failures))
        return 1
    print("\nPASSED: tables in MinIO through the Iceberg REST catalog, lineage in Marquez")
    return 0


if __name__ == "__main__":
    sys.exit(main())
