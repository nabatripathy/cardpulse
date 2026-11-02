"""Check that a docker-profile run really used the docker services.

Run after `cardpulse-synth`, `cardpulse-ingest`, `cardpulse-silver` and `cardpulse-gold`
with `--env docker`. It fails (exit 1) unless:

* every Bronze and Silver table exists in the Iceberg REST catalog and has rows;
* the tables live in the S3 object store (``s3://``), not on a local disk;
* every Gold model was built with rows, and every Gold dbt test passed;
* Marquez received lineage for every Bronze ingest job, every Silver transform job and
  every Gold model.

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
            failures.append(f"{table}: stored at {location}, expected s3:// (object store)")

    gold_rows = []
    from cardpulse_frameworks.gold.runner import _count_rows, dbt_paths, read_artifacts

    target_path, _ = dbt_paths(env)
    models, tests = read_artifacts(target_path) if (target_path / "run_results.json").exists() else ([], [])
    _count_rows(env, models)
    if not models:
        failures.append("no Gold dbt run results; run `cardpulse-gold run` first")
    for m in models:
        if m.status != "success":
            failures.append(f"gold model {m.name}: {m.status}")
        elif not m.name.startswith("stg_"):
            gold_rows.append((m.relation, m.rows))
            if not m.rows:
                failures.append(f"{m.relation}: no rows")
    failing_tests = [t.name for t in tests if t.status != "pass"]
    if failing_tests:
        failures.append(f"Gold dbt tests not passing: {', '.join(failing_tests)}")

    jobs: set[str] = set()
    if env.lineage.transport == "http" and env.lineage.url:
        url = f"{env.lineage.url.rstrip('/')}/api/v1/namespaces/{env.lineage.namespace}/jobs?limit=200"
        with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 - URL comes from env config
            jobs = {j["name"] for j in json.load(resp).get("jobs", [])}
        expected = (
            {f"ingest.{s.name}" for s in bronze}
            | {f"silver.{s.entity}" for s in silver}
            | {f"gold.{m.name}" for m in models}
        )
        missing = sorted(expected - jobs)
        if missing:
            failures.append(f"Marquez is missing lineage for: {', '.join(missing)}")
    else:
        failures.append("lineage transport is not http; this check expects Marquez")

    print(f"\nDocker profile verification ({env.env})\n")
    print("| Table | Rows | Location |\n|---|---:|---|")
    for table, n, loc in rows:
        print(f"| {table} | {n:,} | {loc} |")
    print("\n| Gold model | Rows |\n|---|---:|")
    for rel, n in gold_rows:
        print(f"| {rel} | {n:,} |")
    print(f"\nGold dbt tests: {len(tests)}, passing: {len(tests) - len(failing_tests)}")
    print(f"\nMarquez jobs in namespace {env.lineage.namespace}: {len(jobs)}")
    if failures:
        print("\nFAILED\n" + "\n".join(f"- {f}" for f in failures))
        return 1
    print(
        "\nPASSED: tables in the S3 object store through the Iceberg REST catalog, "
        "Gold built and tested, lineage in Marquez"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
