"""
test_pipeline_integration.py
-----------------------------
Test d'intégration complet du pipeline :
Simule exactement ce que fait main.py mais sur un petit jeu de données.

Pré-requis :
- Oracle Docker démarré + tables créées (make oracle-setup)
- Snowflake DDL exécuté (sql/ddl/snowflake_schema.sql)
- GCS bucket créé et accessible
- Variables d'environnement chargées (export $(cat .env | xargs))

Usage:
    pytest tests/integration/test_pipeline_integration.py -v -s
"""

import os
import sys
from pathlib import Path

# Charge .env depuis la racine du projet
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

# Fix PySpark Python path (Conda/Anaconda)
if sys.executable not in os.environ.get("PYSPARK_PYTHON", ""):
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark_real():
    """SparkSession avec vrais connecteurs GCS + Snowflake."""
    gcs_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    jdbc_jar  = os.path.abspath("drivers/ojdbc8.jar")

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("integration-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_creds)
        .config("spark.jars.packages",
                "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
                "net.snowflake:snowflake-jdbc:3.14.4,"
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14")
        .config("spark.jars", jdbc_jar)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


def sf_options(table: str) -> dict:
    return {
        "sfURL":       f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser":      os.environ["SNOWFLAKE_USER"],
        "sfPassword":  os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase":  os.environ.get("SNOWFLAKE_DATABASE", "FINANCE_DWH_DEV"),
        "sfSchema":    os.environ.get("SNOWFLAKE_SCHEMA", "CORE"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "MIGRATION_WH"),
        "dbtable":     table,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_oracle(spark, table: str, num_partitions: int = 2):
    jdbc_url = (
        f"jdbc:oracle:thin:@//"
        f"{os.environ.get('ORACLE_HOST','localhost')}:"
        f"{os.environ.get('ORACLE_PORT','1521')}/"
        f"{os.environ.get('ORACLE_SERVICE','XEPDB1')}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable",
                f"(SELECT t.*, ORA_HASH(ROWID,{num_partitions}) PART_ID FROM {table} t) Q")
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("user", os.environ.get("ORACLE_USER", "migration_reader"))
        .option("password", os.environ["ORACLE_PASSWORD"])
        .option("partitionColumn", "PART_ID")
        .option("lowerBound", "0")
        .option("upperBound", str(num_partitions))
        .option("numPartitions", str(num_partitions))
        .load()
    )


def read_snowflake(spark, table: str):
    return (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options(table))
        .load()
    )


def write_snowflake(df, table: str, mode: str = "overwrite"):
    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options(table))
        .mode(mode)
        .save()
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestOracleRead:

    def test_read_lkp_continent(self, spark_real):
        df = read_oracle(spark_real, "LKP_CONTINENT")
        assert df.count() >= 3
        assert "CONTINENT_CD" in df.columns
        assert "CONTINENT_NM" in df.columns

    def test_read_lkp_country(self, spark_real):
        df = read_oracle(spark_real, "LKP_COUNTRY")
        assert df.count() >= 5

    def test_read_fact_txn(self, spark_real):
        df = read_oracle(spark_real, "FACT_TXN")
        count = df.count()
        assert count >= 10, f"Attendu ≥ 10 transactions Oracle, trouvé : {count}"
        assert "TXN_ID" in df.columns
        assert "NET_AMT" in df.columns

    def test_fact_txn_net_positive(self, spark_real):
        df = read_oracle(spark_real, "FACT_TXN")
        negatives = df.filter(F.col("NET_AMT") < 0).count()
        assert negatives == 0, f"{negatives} montants négatifs dans FACT_TXN Oracle"

    def test_fact_txn_no_null_id(self, spark_real):
        df = read_oracle(spark_real, "FACT_TXN")
        nulls = df.filter(F.col("TXN_ID").isNull()).count()
        assert nulls == 0


@pytest.mark.integration
class TestGCSLandingZone:

    GCS_TEST_PREFIX = "tests/integration"

    def test_write_parquet_to_gcs(self, spark_real):
        bucket = os.environ.get("GCS_BUCKET", "ton-bucket-migration-dev")
        df     = read_oracle(spark_real, "LKP_CONTINENT")
        path   = f"gs://{bucket}/{self.GCS_TEST_PREFIX}/lkp_continent"
        df.write.mode("overwrite").parquet(path)

        # Relire et vérifier
        df_back = spark_real.read.parquet(path)
        assert df_back.count() == df.count()

    def test_schema_preserved_after_gcs_roundtrip(self, spark_real):
        bucket = os.environ.get("GCS_BUCKET", "ton-bucket-migration-dev")
        df     = read_oracle(spark_real, "FACT_TXN")
        path   = f"gs://{bucket}/{self.GCS_TEST_PREFIX}/fact_txn"
        df.write.mode("overwrite").option("compression", "snappy").parquet(path)

        df_back = spark_real.read.parquet(path)
        original_cols = set(df.columns)
        restored_cols = set(df_back.columns)
        assert original_cols == restored_cols


@pytest.mark.integration
class TestSnowflakeWrite:

    def test_write_dim_continent(self, spark_real):
        df_oracle = read_oracle(spark_real, "LKP_CONTINENT")
        df_staged = (
            df_oracle
            .select(
                F.col("CONTINENT_CD").alias("continent_code"),
                F.initcap(F.col("CONTINENT_NM")).alias("continent_name"),
            )
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )
        write_snowflake(df_staged, "DIM_CONTINENT")

        df_sf = read_snowflake(spark_real, "DIM_CONTINENT")
        assert df_sf.count() == df_staged.count()

    def test_write_dim_country(self, spark_real):
        df_oracle = read_oracle(spark_real, "LKP_COUNTRY")
        df_staged = (
            df_oracle
            .select(
                F.col("COUNTRY_CD").alias("country_code"),
                F.col("COUNTRY_NM").alias("country_name"),
                F.col("CURRENCY_CD").alias("currency_code"),
                F.col("IS_EU_FLG").cast("boolean").alias("is_eu_member"),
                F.col("ACTIVE_FLG").cast("boolean").alias("is_active"),
            )
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )
        write_snowflake(df_staged, "DIM_COUNTRY")
        df_sf = read_snowflake(spark_real, "DIM_COUNTRY")
        assert df_sf.count() == df_staged.count()


@pytest.mark.integration
class TestEndToEndReconciliation:

    def test_fact_txn_count_matches(self, spark_real):
        """Le nombre de transactions Snowflake doit correspondre à Oracle."""
        oracle_count = read_oracle(spark_real, "FACT_TXN").count()

        # Charger les transactions dans Snowflake (version simplifiée)
        df = (
            read_oracle(spark_real, "FACT_TXN")
            .select(
                F.col("TXN_ID").alias("transaction_bk"),
                F.col("TXN_TYPE_CD").alias("transaction_type"),
                F.col("STATUS_CD").alias("transaction_status"),
                F.col("CHANNEL_CD").alias("channel"),
                F.col("GROSS_AMT").cast("decimal(18,4)").alias("gross_amount"),
                F.col("NET_AMT").cast("decimal(18,4)").alias("net_amount"),
                F.col("FEE_AMT").cast("decimal(18,4)").alias("fee_amount"),
                F.col("TAX_AMT").cast("decimal(18,4)").alias("tax_amount"),
                F.col("CURRENCY_CD").alias("currency_code"),
                F.col("FX_RATE_EUR").cast("float").alias("exchange_rate_to_eur"),
                F.round(
                    F.col("NET_AMT").cast("float") * F.col("FX_RATE_EUR").cast("float"), 4
                ).alias("amount_eur"),
                F.regexp_replace(F.col("TXN_DT"), "-", "").cast("int").alias("time_sk"),
                F.lit("ORACLE_ERP").alias("source_system"),
                F.lit("INTEGRATION_TEST").alias("batch_id"),
                F.current_timestamp().alias("loaded_at"),
            )
        )
        write_snowflake(df, "FACT_TRANSACTIONS")

        sf_count = read_snowflake(spark_real, "FACT_TRANSACTIONS").count()

        variance = abs(oracle_count - sf_count) / oracle_count
        assert variance < 0.005, (
            f"Variance trop haute : Oracle={oracle_count}, Snowflake={sf_count}, "
            f"variance={variance:.2%}"
        )

    def test_net_amount_total_matches(self, spark_real):
        """Le total NET_AMT Oracle doit correspondre à SNOWFLAKE à 0.1% près."""
        oracle_total = (
            read_oracle(spark_real, "FACT_TXN")
            .agg(F.sum(F.col("NET_AMT").cast("double")))
            .collect()[0][0]
        ) or 0.0

        sf_total = (
            read_snowflake(spark_real, "FACT_TRANSACTIONS")
            .agg(F.sum(F.col("NET_AMOUNT").cast("double")))
            .collect()[0][0]
        ) or 0.0

        variance = abs(oracle_total - sf_total) / oracle_total if oracle_total else 0
        assert variance < 0.001, (
            f"Variance montants : Oracle={oracle_total:,.2f}, "
            f"Snowflake={sf_total:,.2f}, variance={variance:.4%}"
        )
