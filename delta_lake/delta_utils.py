"""
Delta Lake utility helpers: compaction, vacuuming, schema inspection.
"""
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


def compact_table(spark: SparkSession, path: str, target_file_size_mb: int = 128):
    """Compact small files into larger ones (OPTIMIZE equivalent)."""
    dt = DeltaTable.forPath(spark, path)
    dt.optimize().executeCompaction()
    print(f"Compacted Delta table at {path}")


def vacuum_table(spark: SparkSession, path: str, retention_hours: int = 168):
    """Remove files older than retention_hours (default 7 days)."""
    dt = DeltaTable.forPath(spark, path)
    dt.vacuum(retention_hours)
    print(f"Vacuumed Delta table at {path} (retention={retention_hours}h)")


def show_history(spark: SparkSession, path: str, limit: int = 10):
    """Print the last N operations from the Delta transaction log."""
    dt = DeltaTable.forPath(spark, path)
    dt.history(limit).show(truncate=False)


def read_table(spark: SparkSession, path: str, version: int | None = None):
    """Read Delta table, optionally time-travelling to a specific version."""
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)
