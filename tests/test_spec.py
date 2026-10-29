from __future__ import annotations

import pytest
from pydantic import ValidationError

from cardpulse_frameworks.common.config import REPO_ROOT
from cardpulse_frameworks.ingestion.spec import SourceSpec, load_specs

BASE = {
    "source": "processor",
    "entity": "thing",
    "description": "test",
    "owner": "me",
    "format": "hdt",
    "file_type": "THING",
    "file_glob": "processor/thing/*/*.dat",
    "file_name_regex": r"processor_thing_(?P<business_date>\d{8})_(?P<seq>\d{4})\.dat",
    "target_table": "bronze.processor_thing_raw",
    "quarantine_table": "bronze.processor_thing_raw_quarantine",
    "columns": [{"name": "ID", "required": True}, {"name": "AMT"}],
    "key_columns": ["ID"],
    "trailer": {"record_count": True, "amount_total_field": "AMT"},
}


def test_all_repo_specs_are_valid():
    specs = load_specs(REPO_ROOT / "configs" / "sources")
    assert len(specs) == 10
    assert len({s.target_table for s in specs}) == 10


def test_valid_spec():
    assert SourceSpec.model_validate(BASE).name == "processor.thing"


@pytest.mark.parametrize(
    "change",
    [
        {"file_name_regex": r"processor_thing_(\d{8})\.dat"},  # missing named groups
        {"key_columns": ["NOPE"]},
        {"columns": [{"name": "ID"}, {"name": "ID"}]},
        {"file_type": None},
        {"target_table": "processor_thing_raw"},
        {"trailer": {"amount_total_field": "NOPE"}},
    ],
)
def test_invalid_specs_are_refused(change):
    with pytest.raises(ValidationError):
        SourceSpec.model_validate({**BASE, **change})
