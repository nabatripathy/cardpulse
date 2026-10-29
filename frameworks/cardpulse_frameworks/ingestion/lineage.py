"""Minimal OpenLineage client (PT-12, lineage by default).

Emits RunEvents for every file load to a JSONL file (local) or to Marquez over HTTP
(docker, aws-dev). Lineage must never break a pipeline: transport errors are
logged and swallowed.
"""

from __future__ import annotations

import json
import logging
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path

from cardpulse_frameworks.common.config import EnvConfig, _resolve
from cardpulse_frameworks.common.logging import get_logger, log

PRODUCER = "https://github.com/cardpulse/cardpulse/frameworks/ingestion"
RUN_EVENT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
_LOG = get_logger("cardpulse.lineage")


def _facet(schema: str, **fields: object) -> dict[str, object]:
    return {"_producer": PRODUCER, "_schemaURL": f"https://openlineage.io/spec/facets/{schema}", **fields}


class LineageEmitter:
    def __init__(self, env: EnvConfig):
        self.cfg = env.lineage
        self.path = _resolve(self.cfg.path) if self.cfg.transport == "file" and self.cfg.path else None
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def new_run_id(self) -> str:
        return str(uuid.uuid4())

    def emit(
        self,
        event_type: str,
        run_id: str,
        job_name: str,
        inputs: list[dict[str, object]] | None = None,
        outputs: list[dict[str, object]] | None = None,
        error: str | None = None,
        job_type: str = "INGEST",
    ) -> dict[str, object]:
        run_facets: dict[str, object] = {}
        if error:
            run_facets["errorMessage"] = _facet(
                "1-0-1/ErrorMessageRunFacet.json", message=error, programmingLanguage="python"
            )
        event = {
            "eventType": event_type,
            "eventTime": datetime.now(UTC).isoformat(),
            "producer": PRODUCER,
            "schemaURL": RUN_EVENT_SCHEMA,
            "run": {"runId": run_id, "facets": run_facets},
            "job": {
                "namespace": self.cfg.namespace,
                "name": job_name,
                "facets": {
                    "jobType": _facet(
                        "2-0-3/JobTypeJobFacet.json", processingType="BATCH", integration="CARDPULSE", jobType=job_type
                    )
                },
            },
            "inputs": inputs or [],
            "outputs": outputs or [],
        }
        self._send(event)
        return event

    def _send(self, event: dict[str, object]) -> None:
        try:
            if self.cfg.transport == "file" and self.path:
                with open(self.path, "a") as fh:
                    fh.write(json.dumps(event) + "\n")
            elif self.cfg.transport == "http" and self.cfg.url:
                req = urllib.request.Request(  # noqa: S310 - http(s) URL from env config
                    self.cfg.url.rstrip("/") + "/api/v1/lineage",
                    data=json.dumps(event).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=5).close()  # noqa: S310
        except Exception as e:  # noqa: BLE001 - lineage must never fail the job
            log(_LOG, logging.WARNING, "lineage emit failed", error=str(e), job=event["job"]["name"])


def file_dataset(path: Path) -> dict[str, object]:
    return {"namespace": "file", "name": str(path)}


def table_dataset(table: str, columns: list[tuple[str, str]], row_count: int | None) -> dict[str, object]:
    ds: dict[str, object] = {
        "namespace": "iceberg",
        "name": table,
        "facets": {
            "schema": _facet("1-1-1/SchemaDatasetFacet.json", fields=[{"name": n, "type": t} for n, t in columns])
        },
    }
    if row_count is not None:
        ds["outputFacets"] = {
            "outputStatistics": _facet("1-0-2/OutputStatisticsOutputDatasetFacet.json", rowCount=row_count)
        }
    return ds
