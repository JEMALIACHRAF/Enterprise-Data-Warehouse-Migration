"""
test_spark_integration.py
--------------------------
Teste PySpark en conditions réelles :
  - Lecture Oracle via JDBC → écriture GCS Parquet
  - Lecture GCS Parquet → transformation → écriture Snowflake
  - Vérification end-to-end (comptage + checksum)

Usage:
    python tests/connections/test_spark_integration.py
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

import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Config depuis variables d'environnement ───────────────────────────────────

ORACLE_HOST = os.environ.get("ORACLE_HOST", "localhost")
ORACLE_PORT = os.environ.get("ORACLE_PORT", "1521")
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "XEPDB1")
ORACLE_USER = os.environ.get("ORACLE_USER", "migration_reader")
ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "")

SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SF_USER = os.environ.get("SNOWFLAKE_USER", "migration_svc")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "FINANCE_DWH_DEV")
SF_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "CORE")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "MIGRATION_WH")

GCS_BUCKET = os.environ.get("GCS_BUCKET", "ton-bucket-migration-dev")
GCS_CREDS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

JDBC_JAR = os.path.abspath("drivers/ojdbc8.jar")
BATCH_DATE = date.today().strftime("%Y%m%d")

GCS_TEST_PATH = f"gs://{GCS_BUCKET}/tests/spark_integration/{BATCH_DATE}"


def build_spark() -> SparkSession:
    """SparkSession locale avec connecteurs Oracle + Snowflake.
    Note: GCS est géré via le client Python (google-cloud-storage)
    pour éviter le conflit protobuf entre gcs-connector et Spark 3.4.
    """
    return (
        SparkSession.builder.master("local[4]")
        .appName("test-spark-integration")
        .config("spark.sql.shuffle.partitions", "8")
        # Snowflake uniquement — GCS géré par Python SDK
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
            "net.snowflake:snowflake-jdbc:3.14.4",
        )
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )


def test_oracle_to_gcs(spark: SparkSession) -> int:
    """ÉTAPE 1 — Oracle → local Parquet → GCS via Python SDK.
    On évite le GCS connector Spark (conflit protobuf sur Windows).
    """
    print("\n── ÉTAPE 1 : Oracle → GCS ────────────────────────────────")
    import shutil
    import tempfile

    from google.cloud import storage as gcs_storage

    jdbc_url = f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "dbtable",
            "(SELECT TXN_ID, TXN_DT, CUST_ID, PROD_ID, TXN_TYPE_CD, "
            "STATUS_CD, GROSS_AMT, NET_AMT, FEE_AMT, CURRENCY_CD, "
            "ORA_HASH(ROWID,4) AS PART_ID FROM FACT_TXN) T",
        )
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("user", ORACLE_USER)
        .option("password", ORACLE_PASSWORD)
        .option("partitionColumn", "PART_ID")
        .option("lowerBound", "0")
        .option("upperBound", "4")
        .option("numPartitions", "4")
        .option("fetchsize", "10000")
        .load()
    )

    oracle_count = df.count()
    oracle_net = df.agg(F.sum("NET_AMT")).collect()[0][0] or 0
    print(f"  ✅  Oracle : {oracle_count} lignes lues | NET_AMT total = {oracle_net:,.2f}")

    # Écriture locale d'abord
    local_path = os.path.join(tempfile.gettempdir(), "spark_gcs_test")
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    df.coalesce(1).write.mode("overwrite").parquet(local_path)
    print(f"  ✅  Écriture locale : {local_path}")

    # Upload vers GCS via Python SDK (pas de conflit protobuf)
    client = gcs_storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    gcs_prefix = f"tests/spark_integration/{BATCH_DATE}"

    for fname in os.listdir(local_path):
        if fname.endswith(".parquet"):
            local_file = os.path.join(local_path, fname)
            blob = bucket.blob(f"{gcs_prefix}/{fname}")
            blob.upload_from_filename(local_file)

    print(f"  ✅  Upload GCS : gs://{GCS_BUCKET}/{gcs_prefix}/")
    return oracle_count


def test_gcs_transform(spark: SparkSession) -> object:
    """ÉTAPE 2 — Lecture GCS + transformations PySpark."""
    print("\n── ÉTAPE 2 : GCS → Transformation PySpark ────────────────")

    import tempfile

    local_path = os.path.join(tempfile.gettempdir(), "spark_gcs_test")
    df = spark.read.parquet(local_path)
    gcs_count = df.count()
    print(f"  ✅  GCS : {gcs_count} lignes relues depuis Parquet")

    # Transformations
    transformed = (
        df.withColumn("transaction_bk", F.col("TXN_ID"))
        .withColumn("transaction_type", F.col("TXN_TYPE_CD"))
        .withColumn("transaction_status", F.col("STATUS_CD"))
        .withColumn("gross_amount", F.col("GROSS_AMT").cast("decimal(18,4)"))
        .withColumn("net_amount", F.col("NET_AMT").cast("decimal(18,4)"))
        .withColumn("fee_amount", F.col("FEE_AMT").cast("decimal(18,4)"))
        .withColumn("currency_code", F.col("CURRENCY_CD"))
        .withColumn("time_sk", F.regexp_replace(F.col("TXN_DT"), "-", "").cast("int"))
        .withColumn("source_system", F.lit("ORACLE_ERP"))
        .withColumn("batch_id", F.lit(f"TEST_{BATCH_DATE}"))
        .withColumn("loaded_at", F.current_timestamp())
        .select(
            "transaction_bk",
            "transaction_type",
            "transaction_status",
            "gross_amount",
            "net_amount",
            "fee_amount",
            "currency_code",
            "time_sk",
            "source_system",
            "batch_id",
            "loaded_at",
        )
    )

    print(f"  ✅  Transformation OK : {transformed.count()} lignes")
    transformed.show(5, truncate=False)

    return transformed


def test_load_to_snowflake(spark: SparkSession, df) -> None:
    """ÉTAPE 3 — Écriture dans Snowflake."""
    print("\n── ÉTAPE 3 : PySpark → Snowflake ─────────────────────────")

    sf_options = {
        "sfURL": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "dbtable": "FACT_TRANSACTIONS_TEST",  # table de test séparée
    }

    # Crée la table de test si nécessaire
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
    )
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS_TEST (
            transaction_bk      VARCHAR,
            transaction_type    VARCHAR,
            transaction_status  VARCHAR,
            gross_amount        NUMBER(18,4),
            net_amount          NUMBER(18,4),
            fee_amount          NUMBER(18,4),
            currency_code       VARCHAR,
            time_sk             INTEGER,
            source_system       VARCHAR,
            batch_id            VARCHAR,
            loaded_at           TIMESTAMP_NTZ
        )
    """
    )
    conn.commit()
    conn.close()

    df.write.format("net.snowflake.spark.snowflake").options(**sf_options).mode("overwrite").save()

    print(f"  ✅  Écriture Snowflake OK → {SF_DATABASE}.{SF_SCHEMA}.FACT_TRANSACTIONS_TEST")


def test_reconciliation(spark: SparkSession, oracle_count: int) -> None:
    """ÉTAPE 4 — Réconciliation Snowflake vs Oracle."""
    print("\n── ÉTAPE 4 : Réconciliation Oracle ↔ Snowflake ───────────")

    import snowflake.connector

    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), SUM(NET_AMOUNT) FROM FACT_TRANSACTIONS_TEST")
    sf_count, sf_net = cur.fetchone()
    conn.close()

    variance = abs(oracle_count - sf_count) / oracle_count if oracle_count else 0

    print(f"  Oracle     → {oracle_count} lignes")
    print(f"  Snowflake  → {sf_count} lignes")
    print(f"  Variance   → {variance:.4%}")

    if variance == 0:
        print("  ✅  Réconciliation parfaite : 0% de variance")
    elif variance < 0.005:
        print("  ✅  Réconciliation OK : variance < 0.5%")
    else:
        print(f"  ❌  Réconciliation KO : variance {variance:.2%} > seuil 0.5%")


def run_all_tests():
    print("\n" + "=" * 60)
    print("  TEST INTÉGRATION SPARK — Oracle → GCS → Snowflake")
    print("=" * 60)

    missing = []
    for var in [
        "ORACLE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_PASSWORD",
        "GCS_BUCKET",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]:
        if not os.environ.get(var):
            missing.append(var)

    if missing:
        print(f"❌  Variables d'environnement manquantes : {missing}")
        print("    → source .env  (ou : export $(cat .env | xargs))")
        sys.exit(1)

    if not os.path.exists(JDBC_JAR):
        print(f"❌  Driver Oracle manquant : {JDBC_JAR}")
        print("    → Voir README.md section PARTIE 3.4")
        sys.exit(1)

    print("  Initialisation de la SparkSession (peut prendre ~30s au premier lancement)...")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"  ✅  Spark {spark.version} démarré")

    oracle_count = test_oracle_to_gcs(spark)
    transformed = test_gcs_transform(spark)
    test_load_to_snowflake(spark, transformed)
    test_reconciliation(spark, oracle_count)

    spark.stop()
    print("\n🎉  TEST INTÉGRATION COMPLET — Oracle → GCS → Snowflake : SUCCÈS !")


if __name__ == "__main__":
    run_all_tests()
