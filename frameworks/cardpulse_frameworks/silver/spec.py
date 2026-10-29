"""Silver entity specifications: one YAML file per conformed entity (configs/silver/*.yaml).

A spec says where the entity comes from (a Bronze source), how each column is
derived (a SQL expression, usually one of the macros in engine.py), how it is
loaded (SCD Type 2, Type 1, or append-only), how duplicates are resolved, and
which data quality rules guard it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from cardpulse_frameworks.quality.rules import Rule

AUDIT_COLUMNS = ("_bronze_batch_id", "_source_file", "_business_date")


class ColumnSpec(BaseModel):
    name: str
    expr: str
    type: str  # DuckDB type, e.g. VARCHAR, DATE, DECIMAL(18,2), TIMESTAMPTZ
    source: str | None = None  # Bronze column(s) whose presence means a value is expected (cast check)
    nullable: bool = True
    pii: bool = False
    description: str | None = None


class Lookup(BaseModel):
    """A Silver table exposed to the transform SQL as a DuckDB view."""

    name: str
    table: str
    columns: list[str]
    where: str | None = None  # applied in DuckDB after reading


class Dedup(BaseModel):
    order_by: str = "_business_date DESC, _file_seq DESC, _row_num DESC"  # first row per key wins
    against_target: bool = False  # append loads: drop keys already in Silver
    partition_column: str | None = None  # narrows the target lookup to the batch's partitions


class SourceRef(BaseModel):
    bronze_table: str
    ingest_source: str  # "<source>.<entity>" in ops.ingest_file_log
    respect_watermark: bool = True


class SilverSpec(BaseModel):
    entity: str
    description: str
    owner: str
    classification: Literal["public", "internal", "confidential", "restricted"] = "confidential"
    order: int = 100  # run order: lookups and referential targets must load first
    source: SourceRef
    target_table: str
    quarantine_table: str
    load: Literal["scd2", "scd1", "append"]
    snapshot: Literal["full", "delta"] = "full"  # scd loads: full snapshots close keys that disappear
    business_key: list[str]
    partition_field: str | None = None
    unit_max_batches: int = Field(60, ge=1)  # append loads: Bronze batches per Silver commit
    from_sql: str = "bronze b"
    lookups: list[Lookup] = Field(default_factory=list)
    columns: list[ColumnSpec]
    dedup: Dedup = Field(default_factory=Dedup)
    quality: list[Rule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> SilverSpec:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"{self.entity}: duplicate column names")
        for k in self.business_key:
            if k not in names:
                raise ValueError(f"{self.entity}: business key {k} is not a column")
        if self.partition_field and self.partition_field not in names:
            raise ValueError(f"{self.entity}: partition field {self.partition_field} is not a column")
        if self.dedup.against_target and self.load != "append":
            raise ValueError(f"{self.entity}: against_target dedup applies to append loads")
        ids = [r.id for r in self.all_rules]
        if len(ids) != len(set(ids)):
            raise ValueError(f"{self.entity}: duplicate rule ids")
        return self

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def generated_rules(self) -> list[Rule]:
        """Rules implied by the column definitions: required columns and cast validity."""
        rules = [
            Rule(id=f"NN-{c.name}", check="not_null", columns=[c.name], name=f"{c.name} is required")
            for c in self.columns
            if not c.nullable
        ]
        rules += [
            Rule(
                id=f"CAST-{c.name}",
                check="expression",
                expr=f"NOT _castfail_{c.name}",
                name=f"{c.name} converts to {c.type}",
            )
            for c in self.columns
            if c.source
        ]
        return rules

    @property
    def all_rules(self) -> list[Rule]:
        return self.generated_rules + self.quality


def load_silver_spec(path: str | Path) -> SilverSpec:
    with open(path) as fh:
        return SilverSpec.model_validate(yaml.safe_load(fh))


def load_silver_specs(directory: str | Path) -> list[SilverSpec]:
    specs = [load_silver_spec(p) for p in sorted(Path(directory).glob("*.yaml"))]
    return sorted(specs, key=lambda s: (s.order, s.entity))
