from __future__ import annotations

from typing import Any, Protocol

import pyarrow as pa


class TableWriter(Protocol):
    """What the Bronze and Silver pipelines need from a table engine."""

    def replace_batch(self, table: str, data: pa.Table, batch_id: str, partition_field: str | None) -> None:
        """Exactly-once write of one Bronze batch (see the PyIceberg writer for the mechanism)."""

    def append(self, table: str, data: pa.Table, partition_field: str | None) -> None: ...

    def commit(
        self,
        table: str,
        data: pa.Table,
        partition_field: str | None,
        replace_filter: Any | None = None,
        properties: dict[str, str] | None = None,
    ) -> bool:
        """One atomic commit: delete rows matching ``replace_filter`` and add ``data``."""

    def snapshot_values(self, table: str, key: str) -> list[str]:
        """Values of a snapshot-summary property across the table history."""

    def delete(self, table: str, delete_filter: Any) -> None: ...

    def drop_table(self, table: str) -> None: ...

    def read(self, table: str, row_filter: Any | None = None, columns: tuple[str, ...] = ()) -> pa.Table:
        """Read a table (empty table if it does not exist)."""

    def table_exists(self, table: str) -> bool: ...
