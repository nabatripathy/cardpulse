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
