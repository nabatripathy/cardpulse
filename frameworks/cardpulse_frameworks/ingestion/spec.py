"""Source specifications: one YAML file per landed entity (configs/sources/*.yaml).

A new source is onboarded by adding a spec, not by writing code (P6, PT-01).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class ColumnSpec(BaseModel):
    name: str
    required: bool = False
    pan_guard: Literal["none", "exact", "scan"] = "none"  # exact = whole value; scan = free text
    pii: bool = False
    description: str | None = None


class TrailerSpec(BaseModel):
    record_count: bool = True
    amount_total_field: str | None = None


class SourceSpec(BaseModel):
    source: str
    entity: str
    description: str
    owner: str
    classification: Literal["public", "internal", "confidential", "restricted"] = "confidential"
    format: Literal["hdt", "csv"]
    delimiter: str = "|"
    file_type: str | None = None  # H record file type for hdt files
    file_glob: str
    file_name_regex: str
    target_table: str
    quarantine_table: str
    schema_version: int = 1
    key_columns: list[str] = Field(default_factory=list)
    columns: list[ColumnSpec]
    trailer: TrailerSpec | None = None
    # The file is rejected when quarantined records exceed BOTH limits: the percentage
    # catches systemic problems in large files, the floor stops one bad record from
    # rejecting a small micro-batch.
    max_quarantine_pct: float = Field(1.0, ge=0, le=100)
    quarantine_floor: int = Field(10, ge=0)
    watermark: Literal["sequence", "none"] = "sequence"

    @model_validator(mode="after")
    def _check(self) -> SourceSpec:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"{self.name}: duplicate column names")
        groups = re.compile(self.file_name_regex).groupindex
        for g in ("business_date", "seq"):
            if g not in groups:
                raise ValueError(f"{self.name}: file_name_regex needs a (?P<{g}>...) group")
        for k in self.key_columns:
            if k not in names:
                raise ValueError(f"{self.name}: key column {k} is not a column")
        if self.format == "hdt":
            if not self.file_type:
                raise ValueError(f"{self.name}: hdt sources need file_type")
            if self.trailer and self.trailer.amount_total_field and self.trailer.amount_total_field not in names:
                raise ValueError(f"{self.name}: trailer amount field is not a column")
        for t in (self.target_table, self.quarantine_table):
            if t.count(".") != 1:
                raise ValueError(f"{self.name}: table names are <namespace>.<table>, got {t}")
        return self

    @property
    def name(self) -> str:
        return f"{self.source}.{self.entity}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def load_spec(path: str | Path) -> SourceSpec:
    with open(path) as fh:
        return SourceSpec.model_validate(yaml.safe_load(fh))


def load_specs(directory: str | Path) -> list[SourceSpec]:
    return [load_spec(p) for p in sorted(Path(directory).glob("*.yaml"))]
