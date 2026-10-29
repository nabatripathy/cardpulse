"""Generator settings. Every rate is a planning assumption, not a measured value."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class DefectConfig(BaseModel):
    """Deliberate defects injected into the business-date micro-batch files.

    They exist to prove the Bronze guards work: the PAN guard, structural
    quarantine and trailer reconciliation.
    """

    enabled: bool = False
    clear_pan_rows: int = 3  # PAN_TOKEN replaced by a public test card number
    pan_in_merchant_name_rows: int = 1  # card number typed into free text
    malformed_rows: int = 2  # unescaped delimiter in MERCH_NM -> wrong field count
    duplicate_rows: int = 5  # same TXN_REF_NO sent twice (kept in Bronze, deduped in Silver)
    unknown_mcc_rows: int = 5  # MCC not in reference (Silver warning, DQ-05)
    bad_trailer_batch: int | None = 7  # this batch's trailer count is off by one -> file rejected


class SynthConfig(BaseModel):
    seed: int = 42
    business_date: date = date(2027, 3, 15)
    n_customers: int = Field(10_000, ge=100)
    history_days: int = Field(180, ge=30, le=730)
    intraday_batches: int = Field(96, ge=1, le=288)  # 96 = one file every 15 minutes

    second_account_rate: float = 0.12  # share of customers with a second card account
    authorized_user_rate: float = 0.08  # share of accounts with an authorized user
    new_account_rate: float = 0.04  # opened in the last 60 days
    non_activation_rate: float = 0.15  # of new accounts, never make a purchase (T01 population)
    spend_decline_rate: float = 0.08  # of seasoned accounts, spend falls in the last 30 days (T02)
    dormant_rate: float = 0.05
    closed_rate: float = 0.03
    charged_off_rate: float = 0.004
    blocked_rate: float = 0.003
    delinquent_rate: float = 0.03
    revolver_rate: float = 0.42
    fraud_flag_rate: float = 0.003
    deceased_rate: float = 0.001
    do_not_solicit_rate: float = 0.01
    acquisition_bonus_rate: float = 0.6  # of new rewards/travel accounts (drives S12)
    median_monthly_spend: float = 900.0
    avg_ticket: float = 90.0

    # Prior-day master snapshot (business date - 1) so Silver builds SCD Type 2 history.
    prior_snapshot: bool = True
    prior_cli_rate: float = 0.03  # open accounts whose credit limit was raised today
    prior_segment_change_rate: float = 0.015
    prior_merchant_rename_rate: float = 0.01
    prior_consent_change_rate: float = 0.02  # marketing preferences changed today

    defects: DefectConfig = Field(default_factory=DefectConfig)


def load_config(path: str | Path) -> SynthConfig:
    with open(path) as fh:
        return SynthConfig.model_validate(yaml.safe_load(fh) or {})
