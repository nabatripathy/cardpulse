"""Trigger and suppression-rule specifications (versioned YAML that Compliance reviews as text)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

Channel = Literal["EMAIL", "PUSH", "SMS", "MAIL"]
SEMVER = r"^\d+\.\d+\.\d+$"


class Repeat(BaseModel):
    max_fires: int = Field(ge=1)
    min_gap_days: int = Field(ge=0)
    lookback_days: int = Field(ge=1)


class TriggerSpec(BaseModel):
    trigger: str = Field(pattern=r"^T\d{2}_[A-Z0-9_]+$")
    version: str = Field(pattern=SEMVER)
    description: str
    entity: Literal["account"] = "account"
    evaluate: Literal["daily"] = "daily"
    when: list[str] = Field(min_length=1)  # SQL over a gold.fct_account_daily_snapshot row; all must hold
    record: list[str] = []  # snapshot columns kept on the trigger event
    repeat: Repeat
    offers: list[str] = Field(min_length=1)  # rank order
    channels: list[Channel] = Field(min_length=1)  # preference order
    holdout_pct: int = Field(ge=0, le=100)

    @property
    def code(self) -> str:
        return self.trigger.split("_", 1)[0]  # T01


class SuppressionRule(BaseModel):
    code: str = Field(pattern=r"^S\d{2}$")
    name: str
    when: str
    basis: str
    enabled: bool = True


class RuleSet(BaseModel):
    rule_set: str
    version: str = Field(pattern=SEMVER)
    rules: list[SuppressionRule] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self) -> RuleSet:
        codes = [r.code for r in self.rules]
        if len(codes) != len(set(codes)):
            raise ValueError("suppression rule codes must be unique")
        return self

    @property
    def active(self) -> list[SuppressionRule]:
        return [r for r in self.rules if r.enabled]


def load_triggers(directory: str | Path) -> list[TriggerSpec]:
    specs = [TriggerSpec.model_validate(yaml.safe_load(p.read_text())) for p in sorted(Path(directory).glob("*.yaml"))]
    names = [s.trigger for s in specs]
    if len(names) != len(set(names)):
        raise ValueError("trigger names must be unique")
    return specs


def load_rule_set(path: str | Path) -> RuleSet:
    return RuleSet.model_validate(yaml.safe_load(Path(path).read_text()))
