# CardPulse

Real-time card customer intelligence and next-best-offer platform for a US card
issuer. This repository is the build for Phase 5 of the CardPulse portfolio
project; the design lives in the Phase 1–4 documents (business case, roadmap,
architecture, detailed design).

**All data is synthetic.** No real people, accounts or card numbers are used or
generated (ADR-004), and CI blocks card numbers from entering the repository.

## Status: Increment 1 of Phase 5

| Increment | Scope | Status |
|---|---|---|
| 1 | Repository skeleton, local profile, synthetic data generator, config-driven Bronze ingestion | **Done** (this commit) |
| 2 | Silver conformance: typing, dedup, SCD Type 2, DQ gates (Great Expectations) | Next |
| 3 | Gold in dbt: star schema, Customer 360, Card Account Snapshot | Planned |
| 4 | Triggers T01/T02, eligibility and suppression service, decision logging | Planned |
| 5 | Observability, CI/CD to AWS, Terraform landing zone, thin cloud slice | Planned |

## Quickstart

Python 3.11+ and nothing else; no Docker, JVM or cloud account needed.

```bash
make setup        # virtualenv + editable install with dev tools
make demo         # reset, generate 10,000 customers / 6 months, load Bronze, report
make test         # 33 tests, about 30 seconds
```

`make help` lists every target. `PROFILE=small make synth` generates a 1,000-customer portfolio.

## What Increment 1 delivers

| Component | Where | Roadmap epic | Patterns |
|---|---|---|---|
| Synthetic data generator | `synthetic-data/cardpulse_synth/` | E03 | Seeded, deterministic; builds T01, T02 and every suppression population on purpose |
| Config-driven ingestion framework | `frameworks/cardpulse_frameworks/ingestion/` | E04 | PT-01, PT-02, PT-04, PT-05, PT-12 |
| 10 source specs (YAML) | `configs/sources/` | E04 | P6 configuration over code |
| PCI guard and redaction | `frameworks/cardpulse_frameworks/common/pan.py` | E06 | DQ-02, P2 compliance by design |
| Lineage (OpenLineage) | `ingestion/lineage.py` | E06 | PT-12 lineage by default |
| Environment profiles | `configs/env/` (local, docker, aws-dev) | E01, E05 | Same code, different config |
| CI pipeline | `.github/workflows/ci.yml` | E05 | Lint, format, PAN scan, tests, end-to-end smoke run |
| Docker profile | `docker-compose.yml`, `docker/Dockerfile` | E05 | MinIO, Iceberg REST catalog, Marquez |

## How Bronze ingestion works

For every landed file of a source spec, in business-date and sequence order:

1. **Decide** from the control log (`ops.ingest_file_log`): load, skip (already loaded or rejected), or alert (a loaded file changed in landing).
2. **Parse and guard** the file (below).
3. **Write** Bronze rows and quarantine rows. Each commit is stamped with the file's batch id in the Iceberg snapshot summary. A first load is a fast append. A rerun, reprocess, or a crash before the control log was written becomes an atomic replace of that batch, so data is never duplicated.
4. **Record** the outcome in the control log, emit OpenLineage START and COMPLETE/FAIL events, and update the watermark.

| Guard | Level | Effect | Reason code |
|---|---|---|---|
| Header type, date and sequence match the file name | File | Reject file | `HEADER_INVALID` |
| Trailer present; record count and amount total reconcile | File | Reject file | `TRAILER_MISSING`, `TRAILER_MISMATCH` |
| CSV header matches the spec | File | Reject file | `SCHEMA_MISMATCH` |
| Quarantined records above max(1%, 10 records); strict sources allow 0 | File | Reject file | `QUARANTINE_THRESHOLD` |
| Clear card number in a token column, or inside free text | Record | Quarantine, redacted | `PAN_DETECTED` |
| Wrong number of fields | Record | Quarantine, redacted | `FIELD_COUNT` |
| Required value missing | Record | Quarantine, redacted | `REQUIRED_MISSING` |
| Duplicate business keys | Record | Counted, kept (Silver deduplicates) | metric only |

Bronze keeps every value exactly as received, as strings, plus audit columns
(`_source_system`, `_source_file`, `_batch_id`, `_business_date`, `_file_seq`,
`_row_num`, `_record_hash`, `_schema_version`, `_run_id`, `_ingested_at`), and is
partitioned by `_business_date`.

**Watermark:** the highest sequence number per business date with every earlier
file loaded. A rejected or missing micro-batch holds the watermark until a
corrected file arrives, so Silver never skips over missing data.

## Demo results (default profile, local run)

| Measure | Result |
|---|---|
| Portfolio | 10,000 customers, 11,200 accounts, 12,136 cards, 2,020 merchants |
| Transactions | 792,677 posted transactions over 181 days, in 276 files (180 daily + 96 fifteen-minute files) |
| Generation time | 5.6 s |
| Ingestion time, all 285 files | 55 s (149 s before the fast-append and buffered-log optimizations) |
| Rows loaded to Bronze | 792,650 transactions, plus all master and reference data |
| Injected defects caught | 4 card numbers (3 in the token column, 1 in a merchant name), 2 malformed records, 1 file with a bad trailer |
| Reconciliation | 792,650 loaded + 6 quarantined + 21 in the rejected file = 792,677 read |
| Clear card numbers in any table | 0 (quarantined records are redacted) |
| Watermark on the business date | Sequence 6 of 96; held at the rejected file 7 until it is resent |

## Tests

| Suite | What it proves |
|---|---|
| `test_pan.py` | Luhn checks, separators, free-text detection, redaction |
| `test_synth.py` | Same seed gives byte-identical files; card tokens and account numbers are never Luhn-valid; trailers reconcile; T01 accounts never transact; T02 decliners meet the rule |
| `test_spec.py` | All 10 repo specs validate; malformed specs are refused |
| `test_ingest.py` | End-to-end reconciliation; every injected defect caught; no PAN in any table; audit columns and partitioning; lineage events; rerun skips; reprocess replaces; corrected resend loads and the watermark catches up; changed-after-load alerts; crash before the log write does not duplicate; file- and record-level rejections |

## Repository layout

| Path | Contents |
|---|---|
| `frameworks/` | Reusable engineering frameworks (ingestion now; DQ and streaming later) |
| `synthetic-data/` | Generator package and profiles (`small`, `default`, `scale`) |
| `configs/env/` | Environment profiles: `local`, `docker`, `aws-dev` |
| `configs/sources/` | One YAML spec per landed entity |
| `contracts/` | Data product contracts (Offer Eligibility from Phase 4) |
| `tools/` | Repository card-number scan used in CI |
| `docs/adr/` | ADR index and implementation decisions |
| `infra/`, `pipelines/`, `dbt/`, `services/`, `ml/`, `observability/` | Placeholders for later increments |

## What is and is not verified yet

| Item | Status |
|---|---|
| Local profile (PyIceberg, SQLite catalog, local files) | Verified: tests and demo |
| Docker profile (MinIO, Iceberg REST, Marquez) | Written, not started: the build workspace had no Docker daemon. CI validates the compose file. |
| Spark writer for EMR | Written, not exercised: the Iceberg Spark runtime jar could not be downloaded in the build workspace. Integration tests come with the EMR profile. |
| AWS dev profile (Glue catalog) | Config only; needs the Terraform landing zone (Increment 5) |
| Scale beyond the default profile | The 1M-customer `scale` profile is untested; 10M accounts needs chunked generation on EMR |

## Known simplifications

- Refunds are not yet linked to their original purchase (`ORIG_TXN_REF_NO` is blank).
- Balances are generated for the business date only, not as daily history.
- Landing is read from a local path in every profile; native S3 reads come with the EMR profile.
