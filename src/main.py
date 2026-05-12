"""
main.py
-------
Pipeline entry point — orchestrates the full Oracle → Snowflake migration batch.

Execution order:
  1. Extract all tables from Oracle (parallelized JDBC)
  2. Transform: geography dims → product dims → customers → fact
  3. Validate each layer before loading
  4. Load: dims first, then fact (FK dependency order)
  5. Post-load: reconciliation report + Slack notification

Usage:
    python src/main.py --env prod --batch-date 2024-01-15
    python src/main.py --env dev  --batch-date 2024-01-15 --tables dim_product
"""

import argparse
import logging
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession

from src.extraction.oracle_extractor import OracleExtractor
from src.loading.snowflake_loader import SnowflakeLoader
from src.transformation.spark_transformer import SparkTransformer
from src.utils.config import load_config
from src.utils.notifier import PipelineNotifier
from src.utils.spark_session import build_spark_session
from src.validation.data_quality import DataQualityValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Oracle → Snowflake migration pipeline")
    parser.add_argument("--env",        required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--batch-date", required=True, help="YYYY-MM-DD logical date")
    parser.add_argument("--tables",     nargs="*",     help="Subset of tables to process")
    parser.add_argument("--dry-run",    action="store_true", help="Extract & transform only, skip load")
    return parser.parse_args()


def run_pipeline(
    spark: SparkSession,
    args: argparse.Namespace,
    batch_date: date,
) -> None:
    config   = load_config(args.env)
    batch_id = f"batch_{batch_date.strftime('%Y%m%d')}_{datetime.utcnow().strftime('%H%M%S')}"

    extractor   = OracleExtractor(spark, config)
    transformer = SparkTransformer(spark)
    loader      = SnowflakeLoader(spark, config)
    validator   = DataQualityValidator(spark)
    notifier    = PipelineNotifier(config.slack_webhook_url)

    logger.info(f"{'='*60}")
    logger.info(f"  Pipeline started  | env={args.env} | batch_id={batch_id}")
    logger.info(f"  batch_date={batch_date} | dry_run={args.dry_run}")
    logger.info(f"{'='*60}")

    pipeline_start = datetime.utcnow()
    load_results   = []

    try:
        # =====================================================================
        # STEP 1 — EXTRACT
        # =====================================================================
        logger.info("── STEP 1: EXTRACTION ────────────────────────────────────")

        ext_continent   = extractor.extract_table("ERP.LKP_CONTINENT",  batch_date)
        ext_country     = extractor.extract_table("ERP.LKP_COUNTRY",    batch_date)
        ext_region      = extractor.extract_table("ERP.LKP_REGION",     batch_date)
        ext_city        = extractor.extract_table("ERP.LKP_CITY",       batch_date)
        ext_prod_line   = extractor.extract_table("ERP.LKP_PROD_LINE",  batch_date)
        ext_prod_family = extractor.extract_table("ERP.LKP_PROD_FAM",   batch_date)
        ext_product     = extractor.extract_table("ERP.DIM_PRODUCT",    batch_date, watermark_column="LAST_UPD_DT")
        ext_customer    = extractor.extract_table("ERP.DIM_CUSTOMER",   batch_date, watermark_column="LAST_UPD_DT")
        ext_txn         = extractor.extract_table("ERP.FACT_TXN",       batch_date, watermark_column="TXN_DT", num_partitions=40)

        # Read back from GCS landing zone
        raw = {
            "continent":   spark.read.parquet(ext_continent.gcs_path),
            "country":     spark.read.parquet(ext_country.gcs_path),
            "region":      spark.read.parquet(ext_region.gcs_path),
            "city":        spark.read.parquet(ext_city.gcs_path),
            "prod_line":   spark.read.parquet(ext_prod_line.gcs_path),
            "prod_family": spark.read.parquet(ext_prod_family.gcs_path),
            "product":     spark.read.parquet(ext_product.gcs_path),
            "customer":    spark.read.parquet(ext_customer.gcs_path),
            "txn":         spark.read.parquet(ext_txn.gcs_path),
        }

        # =====================================================================
        # STEP 2 — TRANSFORM  (dependency order: geography → product → fact)
        # =====================================================================
        logger.info("── STEP 2: TRANSFORMATION ────────────────────────────────")

        dim_continent   = transformer.transform_dim_continent(raw["continent"]).cache()
        dim_country     = transformer.transform_dim_country(raw["country"], dim_continent).cache()
        dim_region      = transformer.transform_dim_region(raw["region"], dim_country).cache()
        dim_city        = transformer.transform_dim_city(raw["city"], dim_region).cache()

        dim_prod_line   = transformer.transform_dim_product_line(raw["prod_line"]).cache()
        dim_prod_family = transformer.transform_dim_product_family(raw["prod_family"], dim_prod_line).cache()
        dim_product     = transformer.transform_dim_product(raw["product"], dim_prod_family).cache()

        dim_customer    = transformer.transform_dim_customer(raw["customer"], dim_city).cache()

        fact_txn        = transformer.transform_fact_transactions(
            raw_df=raw["txn"],
            dim_time=_load_existing_dim(spark, loader, "FINANCE_DWH.CORE.DIM_TIME"),
            dim_customer=dim_customer,
            dim_product=dim_product,
            dim_city=dim_city,
            batch_id=batch_id,
        ).cache()

        # =====================================================================
        # STEP 3 — VALIDATE
        # =====================================================================
        logger.info("── STEP 3: VALIDATION ────────────────────────────────────")

        validator.validate_fact_transactions(
            df=fact_txn,
            dim_customer=dim_customer,
            dim_product=dim_product,
            dim_time=_load_existing_dim(spark, loader, "FINANCE_DWH.CORE.DIM_TIME"),
            batch_id=batch_id,
            oracle_row_count=ext_txn.row_count,
            oracle_net_total=_get_oracle_net_total(spark, extractor),
        )

        if args.dry_run:
            logger.info("DRY RUN — skipping load steps.")
            return

        # =====================================================================
        # STEP 4 — LOAD  (dims first → fact)
        # =====================================================================
        logger.info("── STEP 4: LOADING ───────────────────────────────────────")

        for table, df in [
            ("FINANCE_DWH.CORE.DIM_CONTINENT",      dim_continent),
            ("FINANCE_DWH.CORE.DIM_COUNTRY",        dim_country),
            ("FINANCE_DWH.CORE.DIM_REGION",         dim_region),
            ("FINANCE_DWH.CORE.DIM_CITY",           dim_city),
            ("FINANCE_DWH.CORE.DIM_PRODUCT_LINE",   dim_prod_line),
            ("FINANCE_DWH.CORE.DIM_PRODUCT_FAMILY", dim_prod_family),
            ("FINANCE_DWH.CORE.DIM_PRODUCT",        dim_product),
            ("FINANCE_DWH.CORE.DIM_CUSTOMER",       dim_customer),
        ]:
            result = loader.load_dimension(df, table, batch_id, mode="overwrite")
            load_results.append(result)

        fact_result = loader.load_fact(
            df=fact_txn,
            target_table="FINANCE_DWH.CORE.FACT_TRANSACTIONS",
            batch_id=batch_id,
        )
        load_results.append(fact_result)

        # =====================================================================
        # STEP 5 — POST-LOAD
        # =====================================================================
        logger.info("── STEP 5: POST-LOAD ─────────────────────────────────────")

        # Refresh Snowflake search optimization
        loader.execute_sql("ALTER TABLE FINANCE_DWH.CORE.FACT_TRANSACTIONS RESUME RECLUSTER;")

        duration = (datetime.utcnow() - pipeline_start).total_seconds()
        total_loaded = sum(r.rows_loaded for r in load_results)
        total_rejected = sum(r.rows_rejected for r in load_results)

        logger.info(f"Pipeline completed in {duration:.0f}s | "
                    f"loaded={total_loaded:,} | rejected={total_rejected:,}")

        notifier.send_success(
            batch_id=batch_id,
            batch_date=batch_date,
            duration_seconds=duration,
            rows_loaded=total_loaded,
            rows_rejected=total_rejected,
        )

    except Exception as exc:
        duration = (datetime.utcnow() - pipeline_start).total_seconds()
        logger.exception(f"Pipeline FAILED after {duration:.0f}s: {exc}")
        notifier.send_failure(batch_id=batch_id, error=str(exc))
        sys.exit(1)


def _load_existing_dim(spark, loader, table_name):
    """Load an already-existing Snowflake dim into a DataFrame (for SCD lookups)."""
    return (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**loader._options_for_table(table_name))
        .load()
    )


def _get_oracle_net_total(spark, extractor) -> float:
    """Compute total net_amount directly from Oracle for reconciliation."""
    row = (
        spark.read.format("jdbc")
        .options(**extractor._jdbc_options({"dbtable": "(SELECT SUM(NET_AMT) AS TOTAL FROM ERP.FACT_TXN WHERE TXN_DT >= TRUNC(SYSDATE)-1) T"}))
        .load()
        .collect()[0]
    )
    return float(row[0]) if row[0] else 0.0


if __name__ == "__main__":
    args       = parse_args()
    batch_date = date.fromisoformat(args.batch_date)
    spark      = build_spark_session(env=args.env)
    run_pipeline(spark, args, batch_date)
