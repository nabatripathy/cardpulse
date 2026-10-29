"""Iceberg writer for Spark on EMR Serverless (ADR-018).

STATUS: written against the Spark 3.5 DataFrameWriterV2 API but NOT exercised in
Increment 1. The build workspace cannot download the Iceberg Spark runtime jar,
and EMR arrives with the landing zone (E01). It is wired in through
``writer: spark`` in the environment config and gets integration tests when the
EMR profile exists.

Expected Spark settings (EMR Serverless job or spark-submit):
  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  spark.sql.catalog.cardpulse=org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.cardpulse.type=glue
  spark.sql.catalog.cardpulse.warehouse=s3://cardpulse-<env>-lakehouse/
"""

from __future__ import annotations

import pyarrow as pa

from cardpulse_frameworks.common.config import EnvConfig


class SparkIcebergWriter:
    def __init__(self, env: EnvConfig, spark=None):
        from pyspark.sql import SparkSession  # optional dependency: pip install .[spark]

        self.catalog = env.catalog.name
        self.spark = spark or SparkSession.builder.appName("cardpulse-ingest").getOrCreate()

    def _fq(self, table: str) -> str:
        return f"{self.catalog}.{table}"

    def _df(self, data: pa.Table):
        return self.spark.createDataFrame(data.to_pandas())

    def table_exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(self._fq(table))

    def _create_if_missing(self, table: str, data: pa.Table, partition_field: str | None) -> None:
        if self.table_exists(table):
            return
        ns = table.split(".")[0]
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{ns}")
        writer = self._df(data.slice(0, 0)).writeTo(self._fq(table)).using("iceberg")
        writer = writer.tableProperty("write.parquet.compression-codec", "zstd")
        if partition_field:
            from pyspark.sql.functions import col

            writer = writer.partitionedBy(col(partition_field))
        writer.create()

    def replace_batch(self, table: str, data: pa.Table, batch_id: str, partition_field: str | None) -> None:
        from pyspark.sql.functions import col, lit

        self._create_if_missing(table, data, partition_field)
        # Atomic replace-where: deletes this batch's rows and writes the new ones in one commit.
        self._df(data).writeTo(self._fq(table)).overwrite(col("_batch_id") == lit(batch_id))

    def append(self, table: str, data: pa.Table, partition_field: str | None) -> None:
        self._create_if_missing(table, data, partition_field)
        self._df(data).writeTo(self._fq(table)).append()

    def read(self, table: str, row_filter: str | None = None, columns: tuple[str, ...] = ()) -> pa.Table:
        if not self.table_exists(table):
            return pa.table({})
        df = self.spark.table(self._fq(table))
        if row_filter:
            df = df.where(row_filter)
        if columns:
            df = df.select(*columns)
        return pa.Table.from_pandas(df.toPandas(), preserve_index=False)

    # Silver operations (Increment 2). On Spark these become MERGE INTO statements;
    # they are implemented with the EMR profile, alongside its integration tests.
    def commit(self, table, data, partition_field, replace_filter=None, properties=None) -> bool:
        raise NotImplementedError("Silver commits on Spark arrive with the EMR profile")

    def snapshot_values(self, table: str, key: str) -> list[str]:
        raise NotImplementedError("Silver commits on Spark arrive with the EMR profile")

    def delete(self, table: str, delete_filter) -> None:
        self.spark.sql(f"DELETE FROM {self._fq(table)} WHERE {delete_filter}")  # noqa: S608 - filter built internally

    def drop_table(self, table: str) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {self._fq(table)}")
