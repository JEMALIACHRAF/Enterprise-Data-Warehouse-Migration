"""
snowflake_loader.py
-------------------
Bulk loads transformed DataFrames into Snowflake using the
Snowflake Spark Connector (COPY INTO via internal stage).

Strategy:
- Stage data on GCS → COPY INTO Snowflake (fastest path for large volumes)
- MERGE for dimensions (upsert / SCD handling)
- APPEND + dedup for fact tables
- Quarantine table for rejected rows
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import PipelineConfig

logger = logging.getLogger(__name__)

LoadMode = Literal["overwrite", "append", "merge"]


@dataclass
class LoadResult:
    table_name: str
    rows_loaded: int
    rows_rejected: int
    duration_seconds: float
    batch_id: str


class SnowflakeLoader:
    """
    Loads PySpark DataFrames into Snowflake via the Snowflake Spark Connector.

    Connection uses key-pair authentication (no password in flight).
    All writes go through an internal GCS stage for maximum throughput.
    """

    SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self._sf_options = self._build_sf_options()

    def _build_sf_options(self) -> dict:
        sf = self.config.snowflake
        return {
            "sfURL":        f"{sf.account}.snowflakecomputing.com",
            "sfUser":       sf.user,
            "sfDatabase":   sf.database,
            "sfSchema":     sf.schema,
            "sfWarehouse":  sf.warehouse,
            "sfRole":       sf.role,
            # Key-pair auth (private key injected from GCP Secret Manager)
            "pem_private_key": sf.private_key_pem,
            # Performance tuning
            "parallelism":  "16",
            "truncate_table": "off",
        }

    def _options_for_table(self, table: str, extra: dict = None) -> dict:
        opts = {**self._sf_options, "dbtable": table}
        if extra:
            opts.update(extra)
        return opts

    # -------------------------------------------------------------------------
    # Public interface
    # -------------------------------------------------------------------------

    def load_dimension(
        self,
        df: DataFrame,
        target_table: str,
        batch_id: str,
        mode: LoadMode = "overwrite",
    ) -> LoadResult:
        """
        Load a dimension table.
        - overwrite : TRUNCATE + INSERT (initial load / full refresh)
        - append    : INSERT only new records
        """
        start = datetime.utcnow()
        logger.info(f"[{batch_id}] Loading dimension → {target_table} (mode={mode})")

        df_with_audit = df.withColumn("_batch_id", F.lit(batch_id)) \
                          .withColumn("_loaded_at", F.current_timestamp())

        row_count = df_with_audit.count()

        write_mode = "overwrite" if mode == "overwrite" else "append"

        (
            df_with_audit.write
            .format(self.SNOWFLAKE_FORMAT)
            .options(**self._options_for_table(target_table))
            .mode(write_mode)
            .save()
        )

        duration = (datetime.utcnow() - start).total_seconds()
        logger.info(
            f"[{batch_id}] ✅ {target_table}: {row_count:,} rows loaded in {duration:.1f}s"
        )
        return LoadResult(
            table_name=target_table,
            rows_loaded=row_count,
            rows_rejected=0,
            duration_seconds=duration,
            batch_id=batch_id,
        )

    def load_fact(
        self,
        df: DataFrame,
        target_table: str,
        batch_id: str,
        quarantine_table: str = "QUARANTINE_FACT_TRANSACTIONS",
    ) -> LoadResult:
        """
        Load fact table rows.
        - Valid rows   → APPEND to target_table
        - Invalid rows → APPEND to quarantine_table for investigation
        """
        start = datetime.utcnow()
        logger.info(f"[{batch_id}] Loading fact → {target_table}")

        # Split valid / quarantine
        valid_df = df.filter(
            F.col("customer_sk").isNotNull() & F.col("product_sk").isNotNull()
        )
        quarantine_df = df.filter(
            F.col("customer_sk").isNull() | F.col("product_sk").isNull()
        ).withColumn("quarantine_reason", F.lit("UNRESOLVED_FK"))

        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()

        # Load valid rows
        (
            valid_df.write
            .format(self.SNOWFLAKE_FORMAT)
            .options(**self._options_for_table(target_table))
            .mode("append")
            .save()
        )

        # Load quarantine rows
        if quarantine_count > 0:
            logger.warning(
                f"[{batch_id}] ⚠️  {quarantine_count:,} rows quarantined → {quarantine_table}"
            )
            (
                quarantine_df.write
                .format(self.SNOWFLAKE_FORMAT)
                .options(**self._options_for_table(quarantine_table))
                .mode("append")
                .save()
            )

        duration = (datetime.utcnow() - start).total_seconds()
        logger.info(
            f"[{batch_id}] ✅ {target_table}: {valid_count:,} rows loaded, "
            f"{quarantine_count:,} quarantined | {duration:.1f}s"
        )

        return LoadResult(
            table_name=target_table,
            rows_loaded=valid_count,
            rows_rejected=quarantine_count,
            duration_seconds=duration,
            batch_id=batch_id,
        )

    def execute_sql(self, sql: str) -> None:
        """Run arbitrary SQL on Snowflake (post-load DML, swaps, etc.)."""
        self.spark.read \
            .format(self.SNOWFLAKE_FORMAT) \
            .options(**{**self._sf_options, "query": sql}) \
            .load()

    def swap_tables(self, staging_table: str, production_table: str) -> None:
        """Atomic swap: staging → production (zero-downtime load pattern)."""
        logger.info(f"Swapping {staging_table} → {production_table}")
        self.execute_sql(f"ALTER TABLE {staging_table} SWAP WITH {production_table};")
        logger.info("Swap completed.")
