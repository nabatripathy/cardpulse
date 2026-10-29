from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class TableWriter(Protocol):
    """What the ingestion pipeline needs from a table engine."""

    def replace_batch(self, table: str, data: pa.Table, batch_id: str, partition_field: str | None) -> None:
        """Atomically replace every row with ``_batch_id == batch_id`` by ``data``.

        This is what makes a file load idempotent (PT-02): a rerun of the same file
        replaces its rows instead of duplicating them, in one commit.
        """

    def append(self, table: str, data: pa.Table, partition_field: str | None) -> None: ...

    def read(self, table: str, row_filter: str | None = None, columns: tuple[str, ...] = ()) -> pa.Table:
        """Read a table (empty table if it does not exist)."""

    def table_exists(self, table: str) -> bool: ...
