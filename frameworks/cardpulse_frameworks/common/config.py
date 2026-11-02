"""Environment configuration (local, docker, aws-dev).

The same code runs in every environment; only this config changes (Phase 3,
environments and CI/CD). Relative paths are resolved against the repo root so the
local profile works from any working directory.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

REPO_ROOT = Path(__file__).resolve().parents[3]


class CatalogConfig(BaseModel):
    name: str = "cardpulse"
    type: Literal["sql", "rest", "glue"] = "sql"
    properties: dict[str, str] = Field(default_factory=dict)


class LineageConfig(BaseModel):
    transport: Literal["file", "http", "none"] = "file"
    path: str | None = None  # file transport
    url: str | None = None  # http transport, e.g. http://marquez:5000
    namespace: str = "cardpulse"


class GoldConfig(BaseModel):
    """Where the Gold layer is built. ``duckdb`` is the local stand-in for Snowflake."""

    target: Literal["local", "snowflake"] = "local"
    warehouse_path: str = ".lakehouse/gold/warehouse.duckdb"  # DuckDB file (local target)
    lineage_namespace: str = "duckdb://gold"


class EnvConfig(BaseModel):
    env: str
    landing_root: str
    writer: Literal["pyiceberg", "spark"] = "pyiceberg"
    catalog: CatalogConfig
    lineage: LineageConfig = Field(default_factory=LineageConfig)
    gold: GoldConfig = Field(default_factory=GoldConfig)

    def landing_path(self) -> Path:
        return _resolve(self.landing_root)

    def gold_path(self) -> Path:
        return _resolve(self.gold.warehouse_path)

    def catalog_properties(self) -> dict[str, str]:
        props = {"type": self.catalog.type, **self.catalog.properties}
        if self.catalog.type == "sql":
            # Make local sqlite + file warehouse paths absolute and make sure they exist.
            uri = props.get("uri", "")
            if uri.startswith("sqlite:///") and not uri.startswith("sqlite:////"):
                db = _resolve(uri.removeprefix("sqlite:///"))
                db.parent.mkdir(parents=True, exist_ok=True)
                props["uri"] = f"sqlite:///{db}"
            wh = props.get("warehouse", "")
            if wh.startswith("file://") and not wh.startswith("file:///"):
                path = _resolve(wh.removeprefix("file://"))
                path.mkdir(parents=True, exist_ok=True)
                props["warehouse"] = f"file://{path}"
        return props


def _resolve(p: str) -> Path:
    path = Path(p).expanduser()
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def load_env(env: str | Path) -> EnvConfig:
    """Load ``configs/env/<env>.yaml`` or an explicit path."""
    path = Path(env)
    if not path.suffix:
        path = REPO_ROOT / "configs" / "env" / f"{env}.yaml"
    with open(path) as fh:
        return EnvConfig.model_validate(yaml.safe_load(fh))
