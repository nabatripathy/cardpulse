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
