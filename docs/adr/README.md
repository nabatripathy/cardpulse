# Architecture Decision Records

The full decision log, with rationale and alternatives, lives in the CardPulse
phase documents (Phase 1 business case through Phase 4 detailed design). This
index records the decisions that shape this repository.

| ADR | Decision | Where it shows up in the code |
|---|---|---|
| ADR-003 | AWS + Snowflake, with a local Docker profile | `configs/env/*.yaml` |
| ADR-004 | Synthetic data only | `synthetic-data/`, `tools/scan_repo_for_pans.py` |
| ADR-006 | Posted transactions every 15 minutes in the MVP | `processor_posted_txn.yaml`, intraday batch files |
| ADR-008 | Bronze and Silver in Iceberg on S3; Gold in Snowflake | Bronze tables written as Iceberg |
| ADR-017 | Glue Data Catalog as the Iceberg catalog | `configs/env/aws-dev.yaml` |
| ADR-018 | Spark on EMR Serverless; dbt on Snowflake | `writers/spark_writer.py` (not yet exercised) |
| ADR-022 | OpenLineage + Marquez, Great Expectations, dbt tests | `ingestion/lineage.py` |
| ADR-023 | Terraform, GitHub Actions, monorepo | `.github/workflows/ci.yml` |
| ADR-025 | Local profile primary; thin AWS slice later | `configs/env/local.yaml` |
| ADR-029 | Hash-based keys | `_batch_id`, `_record_hash` |

## Decisions made during Increment 1

| ID | Decision | Rationale |
|---|---|---|
| IMP-001 | Two interchangeable writers: PyIceberg (local, docker, small AWS loads) and Spark (EMR) | Runs anywhere without a JVM; the same pipeline code targets EMR later |
| IMP-002 | Batch id = hash(source, file name), stamped on every Iceberg commit; a first load is a fast append, any later load of the same file is an atomic replace of that batch | Reruns, reprocessing and crash recovery never duplicate rows (PT-02), without paying for a delete on every first load (149 s to 55 s on the demo) |
| IMP-003 | Bronze keeps values as received (strings); typing, dedup and reference checks are Silver's job | Bronze stays replayable and faithful to the source (P7) |
| IMP-004 | File-level quarantine limit = max(percentage, record floor); strict sources (consent, campaign) use zero | One bad record should not reject a small micro-batch; consent errors must never pass |
| IMP-005 | Quarantined records are always PAN-redacted before they are written | A quarantine table must not become a PCI data store |
| IMP-006 | A loaded file that changes in landing raises an alert but keeps the control log and Bronze unchanged until reprocessed | Watermarks stay true to what is in Bronze |
| IMP-007 | Watermark = highest contiguous loaded sequence per business date, computed from the control log | Silver micro-batches never skip over a missing or rejected file (PT-05) |
| IMP-008 | Synthetic account numbers, like card tokens, are Luhn-invalid by construction | The repository PAN scan flagged a Luhn-valid account number in a test on its first run; card-like identifiers must never look like PANs to DLP tools |

## Decisions made during Increment 2 (Silver)

| ID | Decision | Rationale |
|---|---|---|
| IMP-009 | DuckDB transforms over Arrow; PyIceberg reads and writes the tables (no DuckDB Iceberg extension) | Runs on a laptop with no network access or JVM; the transform logic is plain SQL that maps to Spark SQL or Snowflake later |
| IMP-010 | Silver specs declare each column as a SQL expression plus a type; the framework wraps it in `TRY_CAST` and generates `NN-` (required) and `CAST-` (conversion) rules | Specs stay readable and reviewable; a bad value is caught by a named rule instead of failing the job |
| IMP-011 | SCD Type 2 in one atomic commit: overwrite filtered on the surrogate keys of the versions being closed, adding closed and new versions together | Readers never see a half-applied change; a same-day correction replaces in place rather than creating a zero-length version |
| IMP-012 | Batch *versions* (batch id + checksum) are the unit of work, stamped on Silver commits and recorded in `ops.silver_unit_log` | Exactly-once across crashes; a corrected resend is recognised as new work |
| IMP-013 | History entities apply one business date per unit, in order; an older snapshot arriving late is held as `PENDING_REBUILD`, and `rebuild` replays the entity from Bronze | Applying an old snapshot after a newer one would corrupt history; Bronze is the replayable source of truth (P7) |
| IMP-014 | DQ gate: critical row failures are quarantined; if any critical rule exceeds its threshold the whole unit is rejected and nothing is written; `--retry-rejected` re-evaluates after a fix | Bad data never reaches Silver silently, and a systemic problem stops the line instead of being partially loaded |
| IMP-015 | Silver follows the Bronze watermark: batches above it wait | Downstream facts never skip a missing micro-batch |
| IMP-016 | The generator writes a prior-day master snapshot from a separate random stream | Silver history is exercised in every demo and test, while transaction files stay identical to Increment 1 |

## Decisions made during container packaging

| ID | Decision | Rationale |
|---|---|---|
| IMP-017 | SeaweedFS replaces MinIO as the S3-compatible store in the docker profile; the bucket is created by `tools/ensure_bucket.py` in the runner | The first CI run found that Docker Hub no longer serves `minio/minio` or `minio/mc` ("repository does not exist"). SeaweedFS is maintained, S3-compatible and widely used with Iceberg. AWS S3 remains the cloud target (ADR-008), so this changes only the local stand-in. |
| IMP-018 | Docker work is built and verified in GitHub Actions, not in the build workspace | The build workspace has no Docker daemon; CI runners do, so every pull request proves the image and the full docker profile |
| IMP-019 | The image is published to GitHub Container Registry on merge to `main`, tagged with the commit and `latest` | Versioned, pullable artifact next to the code; AWS ECR takes over with the landing zone (Increment 5) |

## Decisions made during Increment 3 (Gold)

| ID | Decision | Rationale |
|---|---|---|
| IMP-020 | Locally, Gold is built in a DuckDB file standing in for Snowflake; `cardpulse-gold` stages each Silver Iceberg table into it at its current snapshot, skipping unchanged snapshots | dbt runs on a laptop with no account; on Snowflake the staging step is removed and dbt reads Silver in place through Iceberg tables (ADR-008, ADR-017) |
| IMP-021 | dbt runs as a subprocess, not in-process | dbt-duckdb holds the warehouse open with its own settings, so an in-process run blocked CardPulse from reading results afterwards; a subprocess also matches how Airflow (MWAA) will call dbt |
| IMP-022 | The first version of every SCD2 member is valid from 1900-01-01, not its first load date | History starts loading at go-live, but transactions and balances go back further; without this, older facts find no dimension version |
| IMP-023 | Each data product has one contract in two places, `contracts/*.yaml` (consumers) and an enforced dbt contract (build), and a test fails if they differ | Consumers and the build cannot drift apart silently |
| IMP-024 | dbt test results are written to `ops.dq_results` with layer `gold`, and each model emits an OpenLineage MODEL job | One place for data quality and one lineage graph from Bronze to Gold |
| IMP-025 | The 6-month spend average divides by the history actually observed (minimum 30 days, maximum 180), not always by 6 months | A portfolio, or a new data platform, with less than six months of history would otherwise understate the average and miss decliners (the small profile found 5 of 62 before the change) |
| IMP-026 | The trigger populations are previewed in `cardpulse-gold report` with the Phase 4 rule as written; T02's precision finding is recorded for Increment 4 rather than changing the rule here | Changes to a business rule go through the trigger owner, measured against the planted population |
| IMP-027 | The image is built for `linux/amd64` and `linux/arm64` with buildx and QEMU; arm64 is built on every pull request and published on merge | Apple-chip Macs run it natively without `--platform` |

