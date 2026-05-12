"""
oracle_extractor.py
-------------------
Extracts data from Oracle legacy DWH using PySpark JDBC.

Features:
- Parallel partitioned reads (avoids single-thread bottleneck)
- Incremental extraction via watermark column
- Automatic GCS landing zone upload
- Retry logic with exponential backoff
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.utils.config import PipelineConfig, retry_with_backoff
from src.utils.gcs_helper import GCSHelper

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    table_name: str
    row_count: int
    gcs_path: str
    extraction_start: datetime
    extraction_end: datetime
    batch_id: str


class OracleExtractor:
    """
    Handles full and incremental extraction from Oracle via JDBC.

    Parallelism strategy:
        - Partitions the JDBC read on a numeric/date column (partition_column)
        - Uses num_partitions parallel connections to Oracle
        - Each partition is written as a separate Parquet file to GCS
    """

    JDBC_DRIVER = "oracle.jdbc.OracleDriver"
    DEFAULT_FETCH_SIZE = 50_000
    DEFAULT_NUM_PARTITIONS = 20

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.gcs = GCSHelper(config.gcs_bucket)
        self._jdbc_url = self._build_jdbc_url()

    def _build_jdbc_url(self) -> str:
        cfg = self.config.oracle
        return (
            f"jdbc:oracle:thin:@//{cfg.host}:{cfg.port}/{cfg.service_name}"
        )

    def _jdbc_options(self, extra: Optional[dict] = None) -> dict:
        opts = {
            "url": self._jdbc_url,
            "user": self.config.oracle.user,
            "password": self.config.oracle.password,
            "driver": self.JDBC_DRIVER,
            "fetchsize": str(self.DEFAULT_FETCH_SIZE),
            "oracle.jdbc.mapDateToTimestamp": "false",
        }
        if extra:
            opts.update(extra)
        return opts

    @retry_with_backoff(max_retries=3, base_delay=30)
    def extract_table(
        self,
        table_name: str,
        batch_date: date,
        partition_column: str = "ROWID_HASH",
        num_partitions: int = DEFAULT_NUM_PARTITIONS,
        watermark_column: Optional[str] = None,
        schema: Optional[StructType] = None,
    ) -> ExtractionResult:
        """
        Extract a full Oracle table (or incremental slice) to GCS as Parquet.

        Args:
            table_name:        Oracle table (schema.table)
            batch_date:        Logical date of the batch
            partition_column:  Column used to split parallel reads
            num_partitions:    Number of parallel JDBC connections
            watermark_column:  If set, extract only rows updated >= last_run
            schema:            Optional explicit schema (avoids Oracle type inference issues)

        Returns:
            ExtractionResult with metadata about the extraction.
        """
        extraction_start = datetime.utcnow()
        batch_id = f"{table_name}_{batch_date.strftime('%Y%m%d')}_{extraction_start.strftime('%H%M%S')}"

        logger.info(f"[{batch_id}] Starting extraction of {table_name}")

        # Build extraction query
        query = self._build_query(table_name, watermark_column, batch_date)

        # Compute partition bounds for parallel read
        lower_bound, upper_bound = self._compute_partition_bounds(
            query, partition_column
        )

        logger.info(
            f"[{batch_id}] Partition bounds: [{lower_bound}, {upper_bound}] "
            f"over {num_partitions} partitions"
        )

        # Read from Oracle with partitioned JDBC
        read_opts = self._jdbc_options(
            {
                "dbtable": f"({query}) TMP",
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
                "numPartitions": str(num_partitions),
            }
        )

        reader = self.spark.read.format("jdbc").options(**read_opts)
        if schema:
            reader = reader.schema(schema)

        df: DataFrame = reader.load()

        # Add audit columns
        df = df.withColumn("_extracted_at", F.current_timestamp()) \
               .withColumn("_batch_id", F.lit(batch_id)) \
               .withColumn("_source_table", F.lit(table_name))

        row_count = df.count()
        logger.info(f"[{batch_id}] Extracted {row_count:,} rows from {table_name}")

        # Write to GCS landing zone (Parquet, snappy compressed)
        gcs_path = self._build_gcs_path(table_name, batch_date)
        df.write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(gcs_path)

        logger.info(f"[{batch_id}] Written to GCS: {gcs_path}")

        return ExtractionResult(
            table_name=table_name,
            row_count=row_count,
            gcs_path=gcs_path,
            extraction_start=extraction_start,
            extraction_end=datetime.utcnow(),
            batch_id=batch_id,
        )

    def _build_query(
        self,
        table_name: str,
        watermark_column: Optional[str],
        batch_date: date,
    ) -> str:
        """Build extraction SQL. Incremental if watermark_column is set."""
        if watermark_column:
            # Incremental: only rows modified since last successful batch
            return (
                f"SELECT *, ORA_HASH(ROWID, {self.DEFAULT_NUM_PARTITIONS}) AS ROWID_HASH "
                f"FROM {table_name} "
                f"WHERE {watermark_column} >= TRUNC(SYSDATE) - 1 "
                f"  AND {watermark_column} <  TRUNC(SYSDATE)"
            )
        # Full load
        return (
            f"SELECT *, ORA_HASH(ROWID, {self.DEFAULT_NUM_PARTITIONS}) AS ROWID_HASH "
            f"FROM {table_name}"
        )

    def _compute_partition_bounds(
        self, query: str, partition_column: str
    ) -> tuple[int, int]:
        """Fetch min/max of partition column to define JDBC split bounds."""
        bounds_query = (
            f"SELECT MIN({partition_column}), MAX({partition_column}) "
            f"FROM ({query}) TMP"
        )
        row = (
            self.spark.read.format("jdbc")
            .options(**self._jdbc_options({"dbtable": f"({bounds_query}) B"}))
            .load()
            .collect()[0]
        )
        lower = int(row[0]) if row[0] is not None else 0
        upper = int(row[1]) if row[1] is not None else self.DEFAULT_NUM_PARTITIONS
        return lower, upper

    def _build_gcs_path(self, table_name: str, batch_date: date) -> str:
        table_clean = table_name.replace(".", "/").lower()
        return (
            f"gs://{self.config.gcs_bucket}/raw/{table_clean}"
            f"/year={batch_date.year}"
            f"/month={batch_date.month:02d}"
            f"/day={batch_date.day:02d}"
        )
