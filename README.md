# CardPulse

Real-time card customer intelligence and next-best-offer platform for a US card
issuer. This repository is the build for Phase 5 of the CardPulse portfolio
project; the design lives in the Phase 1–4 documents (business case, roadmap,
architecture, detailed design).

**All data is synthetic.** No real people, accounts or card numbers are used or
generated (ADR-004), and CI blocks card numbers from entering the repository.

## Status: Increment 3 of Phase 5

| Increment | Scope | Status |
|---|---|---|
| 1 | Repository skeleton, local profile, synthetic data generator, config-driven Bronze ingestion | Done |
| 2 | Silver conformance: typing, dedup, SCD Type 2 history, YAML data quality engine and gates | Done |
| 2a | Container image and docker profile verified in CI; image published to GitHub Container Registry | Done |
| 3 | Gold in dbt: star schema, Customer 360, Card Account Snapshot; multi-arch (Apple-chip) image | **Done** (this pull request) |
| 4 | Triggers T01/T02, eligibility and suppression service, decision logging | Next |
| 5 | Observability, CI/CD to AWS, Terraform landing zone, thin cloud slice | Planned |

## Quickstart

Python 3.11+ and nothing else; no Docker, JVM or cloud account needed.

```bash
make setup        # virtualenv + editable install with dev tools
make demo         # reset, generate 10,000 customers / 6 months, Bronze, Silver, Gold (dbt), report
make test         # 72 tests, about 2.5 minutes
make dbt-docs     # dbt docs site: model lineage graph, model and column documentation
```

### Or run it with Docker

The image is built and tested on every pull request and published on every merge to `main`, for both
Intel (`linux/amd64`) and Apple-chip (`linux/arm64`) machines, so no `--platform` flag is needed:

```bash
docker run --rm ghcr.io/nabatripathy/cardpulse:latest sh -c "
  cardpulse-synth --config synthetic-data/config/small.yaml --env local --clean &&
  cardpulse-ingest run --env local --all &&
  cardpulse-silver run --env local &&
  cardpulse-gold run --env local && cardpulse-gold report --env local"
```

The full docker profile (SeaweedFS as the S3 store, the Iceberg REST catalog, Marquez for lineage) needs Docker Desktop:

```bash
make up                         # start SeaweedFS (S3), Iceberg REST, Marquez, Marquez UI
PROFILE=small make demo-docker  # generate, Bronze, Silver, Gold, then verify the S3 store, Gold tests and lineage in Marquez
open http://localhost:3000      # Marquez lineage UI
make down
```

`make help` lists every target. `PROFILE=small make synth` generates a 1,000-customer portfolio.
After `make synth && make ingest`, `make silver` and `make gold` run each layer on its own and `make report` shows all three.

## What is built

| Component | Where | Roadmap epic | Patterns |
|---|---|---|---|
| Synthetic data generator | `synthetic-data/cardpulse_synth/` | E03 | Seeded, deterministic; builds T01, T02 and every suppression population on purpose; writes a prior-day master snapshot so Silver builds real history |
| Config-driven ingestion framework | `frameworks/cardpulse_frameworks/ingestion/` | E04 | PT-01, PT-02, PT-04, PT-05, PT-12 |
| 10 source specs (YAML) | `configs/sources/` | E04 | P6 configuration over code |
| **Silver conformance framework** | `frameworks/cardpulse_frameworks/silver/` | E07 | PT-02, PT-03, PT-04, PT-05, PT-12 |
| **10 Silver entity specs (YAML)** | `configs/silver/` | E07 | Column expressions, load type, dedup, DQ rules |
| **Data quality rules engine** | `frameworks/cardpulse_frameworks/quality/` | E12 | Phase 4 DQ rules as configuration; results in `ops.dq_results` |
| PCI guard and redaction | `frameworks/cardpulse_frameworks/common/pan.py` | E06 | DQ-02, P2 compliance by design |
| **Gold layer in dbt** | `dbt/`, `frameworks/cardpulse_frameworks/gold/` | E08, E09 | Star schema with as-of SCD2 joins, incremental fact, enforced model contracts, 85 dbt tests |
| **Data products** | `gold.customer_360`, `gold.fct_account_daily_snapshot`; contracts in `contracts/` | E09 | Contract file and dbt contract kept identical by a test |
| Lineage (OpenLineage) | `ingestion/lineage.py`, `gold/runner.py` | E06 | PT-12; Bronze INGEST, Silver TRANSFORM and Gold MODEL jobs |
| Environment profiles | `configs/env/` (local, docker, aws-dev) | E01, E05 | Same code, different config |
| CI pipeline | `.github/workflows/ci.yml` | E05 | Lint, format, PAN scan, spec validation, tests, Bronze + Silver + Gold smoke run, Gold rerun no-op |
| Container pipeline | `.github/workflows/docker.yml` | E05 | Builds and smoke-tests the image on every PR; builds amd64 + arm64 and publishes `ghcr.io/nabatripathy/cardpulse` on merge; runs the full docker profile end to end |
| Docker profile | `docker-compose.yml`, `docker/Dockerfile`, `tools/wait_for.py`, `tools/ensure_bucket.py`, `tools/verify_docker_profile.py` | E05 | SeaweedFS (S3), Iceberg REST catalog, Marquez; verified in CI |

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

Bronze keeps every value exactly as received, as strings, plus audit columns, and
is partitioned by `_business_date`.

**Watermark:** the highest sequence number per business date with every earlier
file loaded. A rejected or missing micro-batch holds the watermark until a
corrected file arrives, so Silver never skips over missing data.

## How Silver conformance works

DuckDB runs the transforms over Arrow tables that PyIceberg reads from Bronze and
writes to Silver, so no engine extension or network access is needed. For each
entity, in dependency order (reference data, customers, accounts, roles, cards,
merchants, consent, campaigns, balances, transactions):

1. **Find eligible Bronze batches:** loaded, at or below the watermark, and not yet applied. A batch is tracked by its *version* (batch id + file checksum), so a corrected resend is new work.
2. **Group into units:** up to 60 batches per commit for append loads; one business date per unit for history loads, oldest first.
3. **Transform:** each column is a SQL expression in the spec, wrapped in a safe cast. Macros keep specs short: `blank()`, `ymd()`, `cents()`, `yn()`, and `local_ts()` (merchant local time and time zone to a UTC instant). Lookups join other Silver tables, for example the primary holder of each transaction's account.
4. **Deduplicate** on the business key within the unit and, for append loads, against Silver.
5. **Check quality:** row rules quarantine failing rows (critical) or count them (warning); dataset rules judge the clean set. Any critical rule above its threshold **rejects the whole unit**, and nothing is written.
6. **Load** in one atomic Iceberg commit, stamped with the unit's batch versions:

| Load type | Used for | What a commit does |
|---|---|---|
| SCD Type 2 | Customer, card account, role, card, merchant, consent, campaign | Closes the current version of each changed key (`_valid_to` = business date, `_is_current` = false), opens a new version, and closes keys that left the full snapshot. Surrogate key `_sk` = hash of business key + valid-from date. |
| SCD Type 1 | MCC reference | Replaces the table when the snapshot differs; no history |
| Append | Posted transactions, daily balances | Adds new rows; a re-versioned batch replaces its earlier rows |

**Exactly-once:** a batch version counts as applied if the unit log says so *or* a Silver
commit carries it in its snapshot summary, so a crash between the commit and the log
write never applies data twice. **History order:** a snapshot that arrives for a date
older than one already applied is held as `PENDING_REBUILD`;
`cardpulse-silver rebuild --entity <name>` replays the entity from Bronze in date order.

| Data quality check | Kind | Examples from Phase 4 |
|---|---|---|
| `not_null`, `accepted_values`, `range`, `expression` | Row | DQ-08 account status list, DQ-09 credit limit > 0 for open accounts, DQ-04 amount within limits, DQ-12 consent not in the future |
| `referential` | Row | DQ-03 transaction account exists in `silver.card_account` |
| `unique`, `dataset_sql` | Dataset | DQ-01 transaction id unique, DQ-10 exactly one primary holder per account, DQ-13 one balance per account per day |
| Generated from the column specs | Row | `NN-<column>` for required columns, `CAST-<column>` when a present value fails to convert |

Every rule result (rows checked, failed, threshold, PASS/WARN/FAIL) is written to
`ops.dq_results`, and every unit outcome to `ops.silver_unit_log`.

## How the Gold layer works

| Step | What happens |
|---|---|
| Stage | `cardpulse-gold run` copies each Silver Iceberg table, at its current snapshot, into the DuckDB warehouse (schema `silver`). A table whose snapshot has not changed is skipped, so a rerun stages nothing. On Snowflake this step disappears: dbt reads Silver through Iceberg tables (ADR-008, ADR-017). |
| Staging views | `stg_silver__*` select Silver without its load-audit columns and rename the SCD2 columns to `valid_from`, `valid_to`, `is_current`. |
| Core star schema | `dim_customer`, `dim_account`, `dim_date`, `dim_merchant_category` (with an Unknown member), `dim_offer`; `fct_transaction` is incremental (`delete+insert` on `txn_id`) and joins each transaction to the dimension version **valid on the transaction date**. |
| Data products | `gold.fct_account_daily_snapshot` (Card Account Snapshot: balances, utilisation, delinquency, 30-day and 6-month spend, activation) and `gold.customer_360` (one row per current customer: accounts, exposure, spend, consent and do-not-solicit flags). Both have enforced dbt contracts, and `contracts/*.yaml` must match them column for column (a test checks). |
| Tests | 85 dbt tests: keys, not-null, accepted values, relationships, plus singular tests for one current version per member, no overlapping versions, fact-to-Silver reconciliation, snapshot-to-Silver balance reconciliation (DQ-15), snapshot grain, and Customer 360 coverage. |
| Results and lineage | Every dbt test result is written to `ops.dq_results` with layer `gold`, next to Silver's rules. Each model emits an OpenLineage MODEL job whose inputs are the Silver Iceberg tables or upstream Gold models, so Marquez shows Bronze to Gold end to end. |

## Demo results (default profile, local run)

| Measure | Result |
|---|---|
| Portfolio | 10,000 customers, 11,200 accounts, 12,136 cards, 2,020 merchants; master snapshots for 2027-03-14 and 2027-03-15 |
| Transactions | 792,677 posted transactions over 181 days, in 276 files (180 daily + 96 fifteen-minute files) |
| Bronze | 792,650 transactions loaded; 4 card numbers and 2 malformed records quarantined; 1 file rejected for a bad trailer; watermark at sequence 6 of 96 |
| Silver run time, all 10 entities | 21 s |
| Silver transactions | 788,462 (everything at or below the watermark); 89 fifteen-minute batches held until file 7 is resent |
| SCD Type 2 history built | 331 credit line increases, 172 segment changes, 789 consent changes, 14 card activations, 14 merchant renames, each closing the old version and opening a new one |
| Data quality | 96 rules evaluated; 0 critical failures; DQ-05 (unknown MCC) flagged as a warning |
| Gold build | 8 models and 9 staging views in 9 s (14 s with staging); 85 of 85 dbt tests pass; rerun stages nothing and adds no rows |
| Gold tables | `fct_transaction` 788,462 rows (reconciles to Silver); `fct_account_daily_snapshot` 11,200; `customer_360` 10,000; `dim_account` 11,531 versions; `dim_customer` 10,172 versions |
| Portfolio KPIs (`gold.customer_360`) | 10,771 open accounts; credit limit USD 92.7M; receivables USD 24.9M; 30-day spend USD 9.4M; 8,551 email and 5,478 push opt-ins; 111 do-not-solicit; 350 customers with a delinquent account |
| T01 preview (before suppression) | 121 accounts; includes all 42 planted never-activated accounts in the 10–60 day window; the rest are genuinely new accounts with no purchase yet |
| T02 preview (before suppression) | 1,692 accounts; finds 499 of 616 eligible planted decliners (recall 0.81) |
| Full `make demo` | About 1 minute 45 seconds |

**Finding for Increment 4 (T02 precision).** The Phase 4 rule (30-day spend under half the 6-month
average) flags about 1 in 5 eligible accounts, and only 3 in 10 flagged accounts are planted
decliners. That is real behaviour, not a pipeline defect: a typical account has about 10 purchases a month
and ticket sizes are skewed, so a single 30-day total is usually below the long-run average (median ratio
0.88) and about 13% of ordinary accounts fall under the 50% line in any month. Increment 4 will
measure two refinements against the planted population before the trigger goes live: a minimum
transaction count in the baseline window, and confirmation over two consecutive snapshots.

## Tests

| Suite | What it proves |
|---|---|
| `test_pan.py` | Luhn checks, separators, free-text detection, redaction |
| `test_synth.py` | Same seed gives byte-identical files; card tokens and account numbers are never Luhn-valid; trailers reconcile; T01 accounts never transact; T02 decliners meet the rule |
| `test_spec.py` | All 10 source specs validate; malformed specs are refused |
| `test_ingest.py` | Bronze reconciliation, defect detection, PAN safety, lineage, reruns, reprocessing, resends, crash recovery |
| `test_quality.py` | Each DQ check type, `where` scoping, thresholds and statuses, dataset rules, invalid rules refused |
| `test_gold.py` | dbt build passes every test; every Silver table staged; facts and products reconcile to Silver; SCD2 dimensions start at the beginning of time so old transactions find a version; contract files match the enforced dbt contracts and the built columns; Customer 360 consent and do-not-solicit flags match Silver; T01 contains every planted never-activated account; T02 recall on planted decliners with six months of history; 30-day spend matches Silver to the cent; DQ results and lineage recorded; rerun is a no-op and a resend adds exactly the released transactions |
| `test_silver.py` | Type 2 history matches the generated changes (no overlapping versions, one current per key); Type 1 has no history; transactions typed and linked; watermark holds later batches; Bronze-to-Silver reconciliation; DQ results recorded; rerun is a no-op; resend releases held batches with dedup and warnings; crash after commit does not duplicate; DQ gate rejects a whole unit and `--retry-rejected` re-evaluates; critical failures under threshold are quarantined; out-of-order snapshot needs a rebuild; rebuild reproduces Silver exactly |

## Repository layout

| Path | Contents |
|---|---|
| `frameworks/` | Reusable engineering frameworks: `ingestion`, `silver`, `quality`, `gold`, `common` |
| `synthetic-data/` | Generator package and profiles (`small`, `default`, `scale`) |
| `configs/env/` | Environment profiles: `local`, `docker`, `aws-dev` |
| `configs/sources/` | One YAML spec per landed entity (Bronze) |
| `configs/silver/` | One YAML spec per conformed entity (Silver), prefixed by run order |
| `contracts/` | Data product contracts: Customer 360, Card Account Snapshot, Offer Eligibility (Phase 4) |
| `dbt/` | Gold dbt project: staging views over Silver, core star schema, data products, singular tests, macros |
| `tools/` | Repository card-number scan used in CI |
| `docs/adr/` | ADR index and implementation decisions |
| `infra/`, `pipelines/`, `services/`, `ml/`, `observability/` | Placeholders for later increments |

## What is and is not verified yet

| Item | Status |
|---|---|
| Local profile (PyIceberg, DuckDB, SQLite catalog, local files) | Verified: tests and demo |
| Container image | Built and smoke-tested in CI on every pull request; published to GitHub Container Registry on merge |
| Docker profile (SeaweedFS as S3, Iceberg REST, Marquez) | Verified in CI on every pull request: Bronze and Silver tables land in the S3 store through the REST catalog, Gold builds with every dbt test passing, every job's lineage (including each Gold model) reaches Marquez, and a rerun is a no-op |
| Gold on DuckDB (Snowflake stand-in) | Verified: tests, demo, CI smoke run and docker profile |
| Gold on Snowflake | dbt `snowflake` output defined in `dbt/profiles.yml`; not exercised until the AWS slice (Increment 5). Models use portable SQL except `select * exclude(...)`, which Snowflake also supports |
| arm64 (Apple-chip) image | Built by buildx under QEMU on every pull request; published on merge |
| Spark writer for EMR | Bronze path written, not exercised; Silver operations on Spark are stubs until the EMR profile (they become `MERGE INTO`) |
| AWS dev profile (Glue catalog) | Config only; needs the Terraform landing zone (Increment 5) |
| Scale beyond the default profile | The 1M-customer `scale` profile is untested. Silver reads the current versions of a history table into DuckDB for each unit, which is fine at demo scale; at 10M accounts this moves to partition-scoped `MERGE INTO` on Spark or Snowflake. |

## Known simplifications

- Refunds are not yet linked to their original purchase (`ORIG_TXN_REF_NO` is blank).
- Balances are generated for the business date only, not as daily history.
- A transaction's customer is the account's *current* primary holder, not the holder as of the transaction date (in Silver and in `gold.fct_transaction`).
- The Card Account Snapshot exists for the business date only, because balances are generated for that date only; the model is built to take daily history when it arrives.
- Gold is staged from Silver into DuckDB as a full copy of each changed table; on Snowflake, dbt reads the Silver Iceberg tables in place.
- Landing is read from a local path in every profile; native S3 reads come with the EMR profile.
