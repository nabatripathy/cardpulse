"""Stage Silver Iceberg tables into the Gold warehouse (DuckDB, the local Snowflake stand-in).

Each table is copied from one Iceberg snapshot, so dbt sees a consistent version of
every table. A table whose snapshot has not changed since the last staging is skipped.
On Snowflake this step disappears: Silver is read in place through Snowflake Iceberg
tables over the Glue catalog (ADR-008, ADR-017).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from cardpulse_frameworks.ingestion.writers.base import TableWriter

MANIFEST = "silver._staged"


@dataclass
class StagedTable:
    table: str  # Iceberg name, e.g. silver.customer
    snapshot_id: int | None
    rows: int
    status: str  # STAGED | UNCHANGED | MISSING


def stage_silver(writer: TableWriter, tables: list[str], warehouse: Path) -> list[StagedTable]:
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {MANIFEST} "
            "(table_name VARCHAR PRIMARY KEY, snapshot_id BIGINT, row_count BIGINT, staged_at TIMESTAMPTZ)"
        )
        previous = dict(con.execute(f"SELECT table_name, snapshot_id FROM {MANIFEST}").fetchall())
        out = []
        for name in tables:
            if not writer.table_exists(name):
                out.append(StagedTable(name, None, 0, "MISSING"))
                continue
            tbl = writer.catalog.load_table(name)  # type: ignore[attr-defined]
            snap = tbl.current_snapshot()
            snap_id = snap.snapshot_id if snap else None
            local = name.split(".", 1)[1]
            exists = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'silver' AND table_name = ?",
                [local],
            ).fetchall()[0][0]
            if exists and previous.get(name) == snap_id:
                n = con.execute(f'SELECT count(*) FROM silver."{local}"').fetchall()[0][0]
                out.append(StagedTable(name, snap_id, n, "UNCHANGED"))
                continue
            data: pa.Table = tbl.scan(snapshot_id=snap_id).to_arrow() if snap_id else tbl.scan().to_arrow()
            con.register("_staging", data)
            con.execute(f'CREATE OR REPLACE TABLE silver."{local}" AS SELECT * FROM _staging')
            con.unregister("_staging")
            con.execute(
                f"INSERT OR REPLACE INTO {MANIFEST} VALUES (?, ?, ?, ?)",
                [name, snap_id, data.num_rows, datetime.now(UTC)],
            )
            out.append(StagedTable(name, snap_id, data.num_rows, "STAGED"))
        return out
    finally:
        con.close()
