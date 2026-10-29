"""Table writers. The pipeline talks to a writer, never to a specific engine."""

from __future__ import annotations

from cardpulse_frameworks.common.config import EnvConfig

from .base import TableWriter


def make_writer(env: EnvConfig) -> TableWriter:
    if env.writer == "spark":
        from .spark_writer import SparkIcebergWriter

        return SparkIcebergWriter(env)
    from .pyiceberg_writer import PyIcebergWriter

    return PyIcebergWriter(env)


__all__ = ["TableWriter", "make_writer"]
