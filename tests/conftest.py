from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cardpulse_frameworks.common.config import CatalogConfig, EnvConfig, LineageConfig
from cardpulse_synth.config import DefectConfig, SynthConfig

TINY = SynthConfig(
    seed=11,
    business_date=date(2027, 3, 15),
    n_customers=300,
    history_days=30,
    intraday_batches=8,
    defects=DefectConfig(enabled=True, bad_trailer_batch=3),
)


def make_env(tmp: Path) -> EnvConfig:
    (tmp / "warehouse").mkdir(parents=True, exist_ok=True)
    return EnvConfig(
        env="test",
        landing_root=str(tmp / "landing"),
        writer="pyiceberg",
        catalog=CatalogConfig(
            type="sql",
            properties={"uri": f"sqlite:///{tmp / 'catalog.db'}", "warehouse": f"file://{tmp / 'warehouse'}"},
        ),
        lineage=LineageConfig(transport="file", path=str(tmp / "lineage.jsonl"), namespace="cardpulse-test"),
    )


@pytest.fixture(scope="module")
def tiny_landing(tmp_path_factory) -> tuple[Path, object]:
    from cardpulse_synth.generator import generate

    root = tmp_path_factory.mktemp("landing")
    manifest = generate(TINY, root)
    return root, manifest


def build_lake(tmp: Path, landing: Path, silver: bool = True) -> dict:
    """Copy a landing zone into ``tmp``, load Bronze and (optionally) Silver; return the handles."""
    import shutil

    from cardpulse_frameworks.common.config import REPO_ROOT
    from cardpulse_frameworks.ingestion.lineage import LineageEmitter
    from cardpulse_frameworks.ingestion.pipeline import ingest_source
    from cardpulse_frameworks.ingestion.spec import load_specs
    from cardpulse_frameworks.ingestion.writers import make_writer
    from cardpulse_frameworks.silver.pipeline import SilverPipeline
    from cardpulse_frameworks.silver.spec import load_silver_specs

    shutil.copytree(landing, tmp / "landing", dirs_exist_ok=True)
    env = make_env(tmp)
    writer, lineage = make_writer(env), LineageEmitter(env)
    bronze_specs = load_specs(REPO_ROOT / "configs" / "sources")
    silver_specs = load_silver_specs(REPO_ROOT / "configs" / "silver")
    bronze = {s.name: ingest_source(s, env.landing_path(), writer, lineage) for s in bronze_specs}
    pipe = SilverPipeline(writer, lineage)
    silver = {s.entity: pipe.run_entity(s) for s in silver_specs} if silver else {}
    return {
        "env": env,
        "writer": writer,
        "lineage": lineage,
        "pipe": pipe,
        "bronze": bronze,
        "silver": silver,
        "bronze_specs": {s.name: s for s in bronze_specs},
        "silver_specs": {s.entity: s for s in silver_specs},
    }


def build_gold(lk: dict, select: list[str] | None = None, full_refresh: bool = False):
    """Stage Silver into the lake's Gold warehouse, run dbt build, record DQ and lineage."""
    from cardpulse_frameworks.common.config import GoldConfig
    from cardpulse_frameworks.gold.runner import emit_lineage, record_dq, run_build
    from cardpulse_frameworks.gold.stage import stage_silver

    env = lk["env"]
    if env.gold.warehouse_path == GoldConfig().warehouse_path:
        env.gold = GoldConfig(warehouse_path=str(Path(env.lineage.path).parent / "gold" / "warehouse.duckdb"))
    staged = stage_silver(lk["writer"], [s.target_table for s in lk["silver_specs"].values()], env.gold_path())
    run = run_build(env, select, full_refresh)
    record_dq(lk["writer"], run, lk["lineage"].new_run_id())
    emit_lineage(env, lk["lineage"], run)
    return staged, run
