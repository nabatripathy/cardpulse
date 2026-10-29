"""Iceberg writer built on PyIceberg (no JVM).

Used by the local and docker profiles, and valid on AWS with the Glue catalog for
modest volumes. Tables are created on first write from the Arrow schema and
partitioned by an identity field (``_business_date`` for Bronze).
"""

from __future__ import annotations

import warnings

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table

from cardpulse_frameworks.common.config import EnvConfig

BATCH_PROPERTY = "cardpulse.batch_id"


class PyIcebergWriter:
    def __init__(self, env: EnvConfig):
        self.catalog: Catalog = load_catalog(env.catalog.name, **env.catalog_properties())
        self._tables: dict[str, Table] = {}

    def _table(self, name: str, schema: pa.Schema, partition_field: str | None) -> Table:
        if name in self._tables:
            return self._tables[name]
        ns = name.split(".")[0]
        try:
            self.catalog.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass
        try:
            table = self.catalog.load_table(name)
        except NoSuchTableError:
            table = self.catalog.create_table(name, schema=schema)
            if partition_field:
                with table.update_spec() as spec:
                    spec.add_identity(partition_field)
            with table.transaction() as tx:
                tx.set_properties(
                    {
                        "write.parquet.compression-codec": "zstd",
                        "cardpulse.layer": ns,
                        "cardpulse.managed-by": "cardpulse_frameworks.ingestion",
                    }
                )
            table = self.catalog.load_table(name)
        self._tables[name] = table
        return table

    def replace_batch(self, table: str, data: pa.Table, batch_id: str, partition_field: str | None) -> None:
        """Exactly-once write of one batch.

        Every commit is stamped with ``cardpulse.batch_id`` in the Iceberg snapshot
        summary. If no snapshot carries this batch id, nothing of it can be in the
        table, so a fast append is enough. Otherwise (a rerun, a reprocess, or a crash
        after the data commit but before the control-log write) the batch is replaced
        with a delete-and-append in one atomic commit.
        """
        t = self._table(table, data.schema, partition_field)
        props = {BATCH_PROPERTY: batch_id}
        if not self._has_batch(t, batch_id):
            t.append(data, snapshot_properties=props)
            return
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Delete operation did not match any records")
            t.overwrite(data, overwrite_filter=EqualTo("_batch_id", batch_id), snapshot_properties=props)

    @staticmethod
    def _has_batch(t: Table, batch_id: str) -> bool:
        # Snapshot history is retained for 7 days (Phase 4 conventions), far longer than any retry window.
        return any(
            s.summary is not None and s.summary.additional_properties.get(BATCH_PROPERTY) == batch_id
            for s in t.metadata.snapshots
        )

    def append(self, table: str, data: pa.Table, partition_field: str | None) -> None:
        self._table(table, data.schema, partition_field).append(data)

    def table_exists(self, table: str) -> bool:
        return self.catalog.table_exists(table)

    def read(self, table: str, row_filter: str | None = None, columns: tuple[str, ...] = ()) -> pa.Table:
        if not self.table_exists(table):
            return pa.table({})
        t = self.catalog.load_table(table)
        kwargs: dict = {"selected_fields": columns or ("*",)}
        if row_filter:
            kwargs["row_filter"] = row_filter
        return t.scan(**kwargs).to_arrow()
