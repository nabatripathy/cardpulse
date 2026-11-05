"""Decision engine: triggers -> offer eligibility and suppression -> decision log.

For a business date it reads the Gold warehouse (Card Account Snapshot, Customer 360,
offers and contact history) and produces three tables:

* ``decision.trigger_event``: every account that met a trigger's conditions, and whether it
  fired or was held back by the trigger's repeat rules.
* ``decision.offer_decision``: one decision per fired trigger event: SENT, HOLDOUT or
  SUPPRESSED, the offer and channel chosen, every suppression code that matched, the rule
  and trigger versions and the holdout bucket (the audit trail Compliance asks for).
* ``decision.offer_eligibility``: the Offer Eligibility data product, one row per customer
  and active offer with the reason codes that block it.

Triggers and suppression rules are versioned YAML (configs/triggers, configs/eligibility)
whose conditions are SQL over a fixed set of columns, evaluated here with DuckDB. In the AWS
target the same rules run in the eligibility service against the DynamoDB snapshot.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from .spec import RuleSet, TriggerSpec

TRIGGER_EVENT_TABLE = "decision.trigger_event"
OFFER_DECISION_TABLE = "decision.offer_decision"
OFFER_ELIGIBILITY_TABLE = "decision.offer_eligibility"
DECISION_TABLES = [TRIGGER_EVENT_TABLE, OFFER_DECISION_TABLE, OFFER_ELIGIBILITY_TABLE]
MODEL_VERSION = "rules-only"  # offers are taken in configured rank order until the propensity model ships

# The columns a suppression rule's `when` may use, for both evaluations.
EVAL_COLUMNS = {
    "customer_id": "Customer",
    "account_id": "Triggering account (NULL in the customer-level product)",
    "offer_code": "Offer being evaluated",
    "status_cd": "Account status; customer level: OPEN if any account is open",
    "days_past_due": "Account days past due; customer level: the maximum",
    "is_fraud_flagged": "Open fraud or dispute flag; customer level: any account",
    "has_acquisition_bonus": "Earning a new-account sign-up bonus; customer level: any account",
    "is_deceased": "Customer deceased flag",
    "email_opt_in": "Marketing email consent",
    "push_opt_in": "Marketing push consent",
    "sms_opt_in": "Marketing SMS consent",
    "mail_opt_in": "Marketing mail consent",
    "is_do_not_solicit": "On the internal do-not-solicit list",
    "channel_cd": "First of the offer's channels the customer consents to (NULL if none)",
    "marketing_contacts_7d": "Marketing contacts in the 7 days before the business date",
    "marketing_contacts_30d": "Marketing contacts in the 30 days before the business date",
    "same_offer_contacts_90d": "Contacts with this offer in the 90 days before the business date",
    "last_contact_ts": "Last marketing contact",
    "offer_active": "Business date within the offer's campaign dates",
    "compliance_approval_id": "Campaign's Compliance approval id",
    "requires_credit_decision": "Offer needs a credit decision (R2 offers; none in the MVP)",
    "passes_credit_policy": "Credit policy check result (R2)",
}

TS = pa.timestamp("us", tz="UTC")
TRIGGER_EVENT_SCHEMA = pa.schema(
    [
        ("business_date", pa.date32()),
        ("trigger_event_id", pa.string()),
        ("trigger_cd", pa.string()),
        ("trigger_version", pa.string()),
        ("account_id", pa.string()),
        ("customer_id", pa.string()),
        ("status", pa.string()),  # FIRED | SKIPPED_REPEAT_GAP | SKIPPED_MAX_FIRES
        ("prior_fires", pa.int32()),
        ("last_fire_date", pa.date32()),
        ("inputs", pa.string()),  # JSON of the recorded snapshot values
        ("evaluated_at", TS),
        ("run_id", pa.string()),
    ]
)
OFFER_DECISION_SCHEMA = pa.schema(
    [
        ("business_date", pa.date32()),
        ("decision_id", pa.string()),
        ("trigger_event_id", pa.string()),
        ("trigger_cd", pa.string()),
        ("trigger_version", pa.string()),
        ("customer_id", pa.string()),
        ("account_id", pa.string()),
        ("outcome", pa.string()),  # SENT | HOLDOUT | SUPPRESSED
        ("offer_code", pa.string()),  # offer sent, or held out; NULL when suppressed
        ("offer_rank", pa.int32()),
        ("channel_cd", pa.string()),
        ("primary_reason_cd", pa.string()),
        ("reason_codes", pa.list_(pa.string())),
        ("offers_evaluated", pa.string()),  # JSON: every offer considered and its codes
        ("holdout_bucket", pa.int32()),
        ("rule_set_version", pa.string()),
        ("model_version", pa.string()),
        ("decided_at", TS),
        ("run_id", pa.string()),
    ]
)
OFFER_ELIGIBILITY_SCHEMA = pa.schema(
    [
        ("customer_id", pa.string()),
        ("offer_code", pa.string()),
        ("is_eligible", pa.bool_()),
        ("suppression_codes", pa.list_(pa.string())),
        ("last_contact_ts", TS),
        ("contacts_30d", pa.int32()),
        ("rule_set_version", pa.string()),
        ("evaluated_at", TS),
        ("business_date", pa.date32()),
    ]
)


@dataclass
class DecisionRun:
    business_date: date
    run_id: str
    trigger_events: pa.Table
    decisions: pa.Table
    eligibility: pa.Table
    rule_set_version: str


class RuleError(ValueError):
    """A trigger condition or suppression rule does not compile against its columns."""


def holdout_bucket(customer_id: str, trigger: str) -> int:
    """Stable 0-99 bucket per customer and trigger, the same on every run and every engine."""
    return int(hashlib.sha256(f"{customer_id}|{trigger}".encode()).hexdigest()[:8], 16) % 100


def _id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:32]


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _codes_sql(rules: RuleSet) -> str:
    parts = [f"CASE WHEN coalesce(({r.when}), false) THEN {_lit(r.code)} END" for r in rules.active]
    return f"[x FOR x IN [{', '.join(parts)}] IF x IS NOT NULL]"


def _channel_sql(channels: list[str]) -> str:
    whens = " ".join(f"WHEN coalesce({c.lower()}_opt_in, false) THEN {_lit(c)}" for c in channels)
    return f"CASE {whens} END"


def evaluate(
    gold_path: Path,
    triggers: list[TriggerSpec],
    rules: RuleSet,
    run_id: str,
    business_date: date | None = None,
    prior_decisions: pa.Table | None = None,
    evaluated_at: datetime | None = None,
) -> DecisionRun:
    """Evaluate every trigger and the suppression rules for one business date."""
    now = evaluated_at or datetime.now(UTC)
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {_lit(str(gold_path))} AS wh (READ_ONLY)")
        if business_date is None:
            business_date = con.execute("SELECT max(snapshot_date) FROM wh.gold.fct_account_daily_snapshot").fetchall()[
                0
            ][0]
        assert business_date is not None, "the Card Account Snapshot is empty"
        bd = f"DATE '{business_date.isoformat()}'"
        con.register("prior", prior_decisions if prior_decisions is not None else OFFER_DECISION_SCHEMA.empty_table())
        _prepare(con, bd)
        events = _trigger_events(con, triggers, bd, business_date, run_id, now)
        con.register("event", events)
        evals = _offer_evaluations(con, triggers, rules, bd)
        decisions = _decide(evals, events, triggers, rules, business_date, run_id, now)
        eligibility = _eligibility(con, triggers, rules, bd, business_date, now)
        return DecisionRun(business_date, run_id, events, decisions, eligibility, rules.version)
    finally:
        con.close()


def _prepare(con: duckdb.DuckDBPyConnection, bd: str) -> None:
    # Contacts: the delivery platforms' history plus this platform's own earlier SENT decisions
    # (until delivery feedback flows back, a sent decision counts as a contact).
    con.execute(f"""
        CREATE TEMP VIEW contact AS
        SELECT customer_id, nullif(account_id, '') AS account_id, offer_code, nullif(trigger_cd, '') AS trigger_cd,
               purpose_cd, contact_date, contact_ts
        FROM wh.gold.fct_offer_contact WHERE contact_date < {bd}
        UNION ALL
        SELECT customer_id, account_id, offer_code, trigger_cd, 'MARKETING', business_date, decided_at
        FROM prior WHERE outcome = 'SENT' AND business_date < {bd}""")
    # A trigger "fired" on a date if the account was contacted for it, or held out, that day.
    con.execute(f"""
        CREATE TEMP VIEW fire AS
        SELECT account_id, trigger_cd, contact_date AS fire_date FROM contact
        WHERE trigger_cd IS NOT NULL AND account_id IS NOT NULL
        UNION
        SELECT account_id, trigger_cd, business_date FROM prior
        WHERE outcome IN ('SENT', 'HOLDOUT') AND business_date < {bd}""")
    con.execute(f"""
        CREATE TEMP TABLE cust AS
        SELECT c.customer_id, c.is_deceased, c.email_opt_in, c.push_opt_in, c.sms_opt_in, c.mail_opt_in,
               c.is_do_not_solicit, c.open_account_count, c.max_days_past_due, c.has_fraud_flag,
               c.has_acquisition_bonus,
               coalesce(k.m7, 0) AS marketing_contacts_7d, coalesce(k.m30, 0) AS marketing_contacts_30d,
               k.last_ts AS last_contact_ts
        FROM wh.gold.customer_360 c
        LEFT JOIN (
            SELECT customer_id,
                   count(*) FILTER (WHERE contact_date > {bd} - 7) AS m7,
                   count(*) FILTER (WHERE contact_date > {bd} - 30) AS m30,
                   max(contact_ts) AS last_ts
            FROM contact WHERE purpose_cd = 'MARKETING' GROUP BY customer_id
        ) k USING (customer_id)""")
    con.execute(f"""
        CREATE TEMP TABLE same_offer AS
        SELECT customer_id, offer_code, count(*) AS n FROM contact
        WHERE purpose_cd = 'MARKETING' AND contact_date > {bd} - 90 GROUP BY ALL""")
    con.execute(f"""
        CREATE TEMP TABLE offer AS
        SELECT offer_code, trigger_cd, compliance_approval_id,
               ({bd} BETWEEN start_date AND end_date) AS offer_active
        FROM wh.gold.dim_offer WHERE is_current""")
    con.execute(f"""
        CREATE TEMP TABLE snap AS
        SELECT * FROM wh.gold.fct_account_daily_snapshot WHERE snapshot_date = {bd}""")


def _trigger_events(
    con: duckdb.DuckDBPyConnection,
    triggers: list[TriggerSpec],
    bd: str,
    business_date: date,
    run_id: str,
    now: datetime,
) -> pa.Table:
    tables = []
    for t in triggers:
        conds = " AND ".join(f"({c})" for c in t.when)
        record = ", ".join(f"{c} := {c}" for c in t.record) or "none := NULL"
        sql = f"""
            WITH cand AS (
                SELECT account_id, customer_id, to_json(struct_pack({record}))::VARCHAR AS inputs
                FROM snap WHERE {conds}
            ),
            fired AS (
                SELECT account_id, max(fire_date) AS last_fire_date, count(*) AS fires FROM fire
                WHERE trigger_cd = {_lit(t.trigger)} AND fire_date > {bd} - {t.repeat.lookback_days}
                GROUP BY account_id
            )
            SELECT c.account_id, c.customer_id, c.inputs, coalesce(f.fires, 0)::INTEGER AS prior_fires,
                   f.last_fire_date,
                   CASE WHEN f.last_fire_date > {bd} - {t.repeat.min_gap_days} THEN 'SKIPPED_REPEAT_GAP'
                        WHEN coalesce(f.fires, 0) >= {t.repeat.max_fires} THEN 'SKIPPED_MAX_FIRES'
                        ELSE 'FIRED' END AS status
            FROM cand c LEFT JOIN fired f USING (account_id)
            ORDER BY c.account_id"""
        try:
            rows = con.execute(sql).fetchall()
        except duckdb.Error as e:
            raise RuleError(f"trigger {t.trigger}: {e}") from e
        tables.append(
            pa.Table.from_pylist(
                [
                    {
                        "business_date": business_date,
                        "trigger_event_id": _id(business_date, t.trigger, acct),
                        "trigger_cd": t.trigger,
                        "trigger_version": t.version,
                        "account_id": acct,
                        "customer_id": cust,
                        "status": status,
                        "prior_fires": fires,
                        "last_fire_date": last,
                        "inputs": inputs,
                        "evaluated_at": now,
                        "run_id": run_id,
                    }
                    for acct, cust, inputs, fires, last, status in rows
                ],
                schema=TRIGGER_EVENT_SCHEMA,
            )
        )
    return pa.concat_tables(tables) if tables else TRIGGER_EVENT_SCHEMA.empty_table()


def _offer_evaluations(
    con: duckdb.DuckDBPyConnection, triggers: list[TriggerSpec], rules: RuleSet, bd: str
) -> list[tuple]:
    trig_offer = [(t.trigger, rank, code) for t in triggers for rank, code in enumerate(t.offers, 1)]
    con.execute("CREATE OR REPLACE TEMP TABLE trig_offer (trigger_cd VARCHAR, offer_rank INTEGER, offer_code VARCHAR)")
    if trig_offer:
        con.executemany("INSERT INTO trig_offer VALUES (?, ?, ?)", trig_offer)
    channel = (
        "CASE e.trigger_cd "
        + " ".join(f"WHEN {_lit(t.trigger)} THEN ({_channel_sql(list(t.channels))})" for t in triggers)
        + " END"
    )
    sql = f"""
        WITH r AS (
            SELECT e.trigger_event_id, e.trigger_cd, e.customer_id, e.account_id, o.offer_rank, o.offer_code,
                   s.status_cd, s.days_past_due, s.is_fraud_flagged, s.has_acquisition_bonus,
                   c.is_deceased, c.email_opt_in, c.push_opt_in, c.sms_opt_in, c.mail_opt_in, c.is_do_not_solicit,
                   c.marketing_contacts_7d, c.marketing_contacts_30d, c.last_contact_ts,
                   coalesce(so.n, 0) AS same_offer_contacts_90d,
                   coalesce(f.offer_active, false) AS offer_active, f.compliance_approval_id,
                   false AS requires_credit_decision, true AS passes_credit_policy,
                   {channel} AS channel_cd
            FROM event e
            JOIN trig_offer o ON o.trigger_cd = e.trigger_cd
            JOIN snap s ON s.account_id = e.account_id
            LEFT JOIN cust c ON c.customer_id = e.customer_id
            LEFT JOIN offer f ON f.offer_code = o.offer_code
            LEFT JOIN same_offer so ON so.customer_id = e.customer_id AND so.offer_code = o.offer_code
            WHERE e.status = 'FIRED'
        )
        SELECT trigger_event_id, offer_rank, offer_code, channel_cd, {_codes_sql(rules)} AS codes
        FROM r ORDER BY trigger_event_id, offer_rank"""
    try:
        return con.execute(sql).fetchall()
    except duckdb.Error as e:
        raise RuleError(f"suppression rules {rules.version}: {e}") from e


def _decide(
    evals: list[tuple],
    events: pa.Table,
    triggers: list[TriggerSpec],
    rules: RuleSet,
    business_date: date,
    run_id: str,
    now: datetime,
) -> pa.Table:
    spec = {t.trigger: t for t in triggers}
    by_event: dict[str, list[tuple]] = {}
    for row in evals:
        by_event.setdefault(row[0], []).append(row)
    fired = [e for e in events.to_pylist() if e["status"] == "FIRED"]
    # One marketing message per customer per day: triggers in configured order, then account.
    order = {t.trigger: i for i, t in enumerate(triggers)}
    fired.sort(key=lambda e: (order[e["trigger_cd"]], e["account_id"]))
    sent_customers: set[str] = set()
    out = []
    for e in fired:
        t = spec[e["trigger_cd"]]
        offers = by_event.get(e["trigger_event_id"], [])
        evaluated = [{"offer": o[2], "rank": o[1], "channel": o[3], "codes": list(o[4])} for o in offers]
        chosen = next((o for o in offers if not o[4]), None)
        bucket = holdout_bucket(e["customer_id"], t.trigger)
        row = {
            "business_date": business_date,
            "decision_id": _id("decision", e["trigger_event_id"]),
            "trigger_event_id": e["trigger_event_id"],
            "trigger_cd": t.trigger,
            "trigger_version": t.version,
            "customer_id": e["customer_id"],
            "account_id": e["account_id"],
            "holdout_bucket": bucket,
            "rule_set_version": rules.version,
            "model_version": MODEL_VERSION,
            "decided_at": now,
            "run_id": run_id,
        }
        if chosen is None:
            codes = list(offers[0][4]) if offers else ["S10"]
            row.update(
                outcome="SUPPRESSED",
                offer_code=None,
                offer_rank=None,
                channel_cd=None,
                primary_reason_cd=codes[0],
                reason_codes=codes,
            )
        elif bucket < t.holdout_pct:
            row.update(
                outcome="HOLDOUT",
                offer_code=chosen[2],
                offer_rank=chosen[1],
                channel_cd=chosen[3],
                primary_reason_cd=None,
                reason_codes=[],
            )
        elif e["customer_id"] in sent_customers:
            # Contact cap within the day: another trigger already reached this customer today.
            # The cap is per customer, so it blocks every offer that was otherwise eligible.
            for o in evaluated:
                if not o["codes"]:
                    o.update(codes=["S08"], note="another offer sent to this customer today")
            row.update(
                outcome="SUPPRESSED",
                offer_code=None,
                offer_rank=None,
                channel_cd=None,
                primary_reason_cd="S08",
                reason_codes=["S08"],
            )
        else:
            sent_customers.add(e["customer_id"])
            row.update(
                outcome="SENT",
                offer_code=chosen[2],
                offer_rank=chosen[1],
                channel_cd=chosen[3],
                primary_reason_cd=None,
                reason_codes=[],
            )
        row["offers_evaluated"] = json.dumps(evaluated, separators=(",", ":"))
        out.append(row)
    return pa.Table.from_pylist(out, schema=OFFER_DECISION_SCHEMA)


def _eligibility(
    con: duckdb.DuckDBPyConnection,
    triggers: list[TriggerSpec],
    rules: RuleSet,
    bd: str,
    business_date: date,
    now: datetime,
) -> pa.Table:
    """Offer Eligibility data product: every customer x active offer, at customer level."""
    offer_channels = {code: list(t.channels) for t in triggers for code in t.offers}
    default = ["EMAIL", "PUSH"]
    channel = (
        "CASE f.offer_code "
        + " ".join(f"WHEN {_lit(code)} THEN ({_channel_sql(ch)})" for code, ch in offer_channels.items())
        + f" ELSE ({_channel_sql(default)}) END"
    )
    sql = f"""
        WITH r AS (
            SELECT c.customer_id, NULL::VARCHAR AS account_id, f.offer_code,
                   CASE WHEN c.open_account_count > 0 THEN 'OPEN' ELSE 'CLOSED' END AS status_cd,
                   c.max_days_past_due AS days_past_due, c.has_fraud_flag AS is_fraud_flagged,
                   c.has_acquisition_bonus, c.is_deceased, c.email_opt_in, c.push_opt_in, c.sms_opt_in,
                   c.mail_opt_in, c.is_do_not_solicit, c.marketing_contacts_7d, c.marketing_contacts_30d,
                   c.last_contact_ts, coalesce(so.n, 0) AS same_offer_contacts_90d, f.offer_active,
                   f.compliance_approval_id, false AS requires_credit_decision, true AS passes_credit_policy,
                   {channel} AS channel_cd
            FROM cust c CROSS JOIN offer f
            LEFT JOIN same_offer so ON so.customer_id = c.customer_id AND so.offer_code = f.offer_code
            WHERE f.offer_active
        ), e AS (SELECT *, {_codes_sql(rules)} AS codes FROM r)
        SELECT customer_id, offer_code, len(codes) = 0 AS is_eligible,
               CASE WHEN len(codes) > 0 THEN codes END AS suppression_codes,
               last_contact_ts, marketing_contacts_30d::INTEGER AS contacts_30d
        FROM e ORDER BY customer_id, offer_code"""
    try:
        tbl = con.sql(sql).to_arrow_table()
    except duckdb.Error as e:
        raise RuleError(f"suppression rules {rules.version}: {e}") from e
    n = tbl.num_rows
    tbl = tbl.append_column("rule_set_version", pa.array([rules.version] * n, pa.string()))
    tbl = tbl.append_column("evaluated_at", pa.array([now] * n, TS))
    tbl = tbl.append_column("business_date", pa.array([business_date] * n, pa.date32()))
    return tbl.cast(OFFER_ELIGIBILITY_SCHEMA)
