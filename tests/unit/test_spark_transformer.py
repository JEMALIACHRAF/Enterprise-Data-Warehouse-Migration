"""
test_spark_transformer.py
--------------------------
Tests unitaires pour SparkTransformer.
Vérifie les transformations PySpark sans connexion externe.
"""


import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformation.spark_transformer import SparkTransformer


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-transformer")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def transformer(spark):
    return SparkTransformer(spark)


# ─────────────────────────────────────────────────────────────────────────────
# DIM_CONTINENT
# ─────────────────────────────────────────────────────────────────────────────


class TestTransformDimContinent:
    def test_basic_transform(self, spark, transformer):
        raw = spark.createDataFrame(
            [("EU", "europe"), ("AM", "amerique")], schema=["CONTINENT_CD", "CONTINENT_NM"]
        )
        result = transformer.transform_dim_continent(raw)
        assert result.count() == 2
        cols = result.columns
        assert "continent_code" in cols
        assert "continent_name" in cols

    def test_deduplication(self, spark, transformer):
        raw = spark.createDataFrame(
            [("EU", "europe"), ("EU", "europe_dup")], schema=["CONTINENT_CD", "CONTINENT_NM"]
        )
        result = transformer.transform_dim_continent(raw)
        assert result.count() == 1

    def test_null_filtered(self, spark, transformer):
        raw = spark.createDataFrame(
            [(None, "inconnu"), ("EU", "europe")], schema=["CONTINENT_CD", "CONTINENT_NM"]
        )
        result = transformer.transform_dim_continent(raw)
        assert result.count() == 1
        assert result.collect()[0]["continent_code"] == "EU"

    def test_initcap_applied(self, spark, transformer):
        raw = spark.createDataFrame([("EU", "EUROPE")], schema=["CONTINENT_CD", "CONTINENT_NM"])
        result = transformer.transform_dim_continent(raw)
        row = result.collect()[0]
        assert row["continent_name"] == "Europe"


# ─────────────────────────────────────────────────────────────────────────────
# FACT_TRANSACTIONS — transformations
# ─────────────────────────────────────────────────────────────────────────────


class TestTransformFactTransactions:
    def _make_raw_txn(self, spark):
        return spark.createDataFrame(
            [
                (
                    "TXN001",
                    "20240101",
                    "CUST001",
                    "PROD001",
                    "BUY",
                    "SETTLED",
                    "ONLINE",
                    1100.0,
                    1000.0,
                    100.0,
                    0.0,
                    10.0,
                    100.0,
                    "EUR",
                    1.0,
                ),
                (
                    "TXN002",
                    "20240102",
                    "CUST002",
                    "PROD002",
                    "SELL",
                    "SETTLED",
                    "BRANCH",
                    550.0,
                    500.0,
                    50.0,
                    0.0,
                    5.0,
                    100.0,
                    "EUR",
                    1.0,
                ),
                (
                    "TXN003",
                    "20240103",
                    "CUST001",
                    "PROD003",
                    "BUY",
                    "PENDING",
                    "API",
                    220.0,
                    200.0,
                    20.0,
                    0.0,
                    2.0,
                    100.0,
                    "USD",
                    1.08,
                ),
            ],
            schema=[
                "TXN_ID",
                "TXN_DT",
                "CUST_ID",
                "PROD_ID",
                "TXN_TYPE_CD",
                "STATUS_CD",
                "CHANNEL_CD",
                "GROSS_AMT",
                "NET_AMT",
                "FEE_AMT",
                "TAX_AMT",
                "QTY",
                "UNIT_PRICE",
                "CURRENCY_CD",
                "FX_RATE_EUR",
            ],
        )

    def _make_dim_customer(self, spark):
        return spark.createDataFrame(
            [("CUST001", 1, 10, True), ("CUST002", 2, 20, True)],
            schema=["customer_bk", "customer_sk", "city_sk", "is_current"],
        )

    def _make_dim_product(self, spark):
        return spark.createDataFrame(
            [("PROD001", 1, True), ("PROD002", 2, True), ("PROD003", 3, True)],
            schema=["product_bk", "product_sk", "is_current"],
        )

    def _make_dim_city(self, spark):
        return spark.createDataFrame([(10, "Paris"), (20, "Lyon")], schema=["city_sk", "city_name"])

    def _make_dim_time(self, spark):
        return spark.createDataFrame(
            [(20240101, "2024-01-01"), (20240102, "2024-01-02"), (20240103, "2024-01-03")],
            schema=["time_sk", "full_date"],
        )

    def test_row_count(self, spark, transformer):
        raw = self._make_raw_txn(spark)
        dim_cu = self._make_dim_customer(spark)
        dim_pr = self._make_dim_product(spark)
        dim_ci = self._make_dim_city(spark)
        dim_ti = self._make_dim_time(spark)

        result = transformer.transform_fact_transactions(
            raw_df=raw,
            dim_time=dim_ti,
            dim_customer=dim_cu,
            dim_product=dim_pr,
            dim_city=dim_ci,
            batch_id="TEST_001",
        )
        assert result.count() == 3

    def test_time_sk_computed(self, spark, transformer):
        raw = self._make_raw_txn(spark)
        dim_cu = self._make_dim_customer(spark)
        dim_pr = self._make_dim_product(spark)
        dim_ci = self._make_dim_city(spark)
        dim_ti = self._make_dim_time(spark)

        result = transformer.transform_fact_transactions(
            raw_df=raw,
            dim_time=dim_ti,
            dim_customer=dim_cu,
            dim_product=dim_pr,
            dim_city=dim_ci,
            batch_id="TEST_002",
        )
        time_sks = {row["time_sk"] for row in result.select("time_sk").collect()}
        assert 20240101 in time_sks
        assert 20240102 in time_sks

    def test_amount_eur_computed(self, spark, transformer):
        raw = self._make_raw_txn(spark)
        dim_cu = self._make_dim_customer(spark)
        dim_pr = self._make_dim_product(spark)
        dim_ci = self._make_dim_city(spark)
        dim_ti = self._make_dim_time(spark)

        result = transformer.transform_fact_transactions(
            raw_df=raw,
            dim_time=dim_ti,
            dim_customer=dim_cu,
            dim_product=dim_pr,
            dim_city=dim_ci,
            batch_id="TEST_003",
        )
        txn3 = result.filter(F.col("transaction_bk") == "TXN003").collect()[0]
        # 200 * 1.08 = 216
        assert abs(float(txn3["amount_eur"]) - 216.0) < 0.01

    def test_deduplication_keeps_latest(self, spark, transformer):
        # TXN001 en double — doit garder 1 seul
        raw = spark.createDataFrame(
            [
                (
                    "TXN001",
                    "20240101",
                    "CUST001",
                    "PROD001",
                    "BUY",
                    "SETTLED",
                    "ONLINE",
                    1100.0,
                    1000.0,
                    100.0,
                    0.0,
                    10.0,
                    100.0,
                    "EUR",
                    1.0,
                ),
                (
                    "TXN001",
                    "20240102",
                    "CUST001",
                    "PROD001",
                    "BUY",
                    "SETTLED",
                    "ONLINE",
                    1100.0,
                    1000.0,
                    100.0,
                    0.0,
                    10.0,
                    100.0,
                    "EUR",
                    1.0,
                ),
            ],
            schema=[
                "TXN_ID",
                "TXN_DT",
                "CUST_ID",
                "PROD_ID",
                "TXN_TYPE_CD",
                "STATUS_CD",
                "CHANNEL_CD",
                "GROSS_AMT",
                "NET_AMT",
                "FEE_AMT",
                "TAX_AMT",
                "QTY",
                "UNIT_PRICE",
                "CURRENCY_CD",
                "FX_RATE_EUR",
            ],
        )
        dim_cu = self._make_dim_customer(spark)
        dim_pr = self._make_dim_product(spark)
        dim_ci = self._make_dim_city(spark)
        dim_ti = self._make_dim_time(spark)

        result = transformer.transform_fact_transactions(
            raw_df=raw,
            dim_time=dim_ti,
            dim_customer=dim_cu,
            dim_product=dim_pr,
            dim_city=dim_ci,
            batch_id="TEST_004",
        )
        assert result.count() == 1

    def test_batch_id_set(self, spark, transformer):
        raw = self._make_raw_txn(spark)
        dim_cu = self._make_dim_customer(spark)
        dim_pr = self._make_dim_product(spark)
        dim_ci = self._make_dim_city(spark)
        dim_ti = self._make_dim_time(spark)

        result = transformer.transform_fact_transactions(
            raw_df=raw,
            dim_time=dim_ti,
            dim_customer=dim_cu,
            dim_product=dim_pr,
            dim_city=dim_ci,
            batch_id="BATCH_XYZ",
        )
        batch_ids = {row["batch_id"] for row in result.select("batch_id").collect()}
        assert batch_ids == {"BATCH_XYZ"}

    def test_surrogate_keys_resolved(self, spark, transformer):
        raw = self._make_raw_txn(spark)
        dim_cu = self._make_dim_customer(spark)
        dim_pr = self._make_dim_product(spark)
        dim_ci = self._make_dim_city(spark)
        dim_ti = self._make_dim_time(spark)

        result = transformer.transform_fact_transactions(
            raw_df=raw,
            dim_time=dim_ti,
            dim_customer=dim_cu,
            dim_product=dim_pr,
            dim_city=dim_ci,
            batch_id="TEST_005",
        )
        row = result.filter(F.col("transaction_bk") == "TXN001").collect()[0]
        assert row["customer_sk"] == 1
        assert row["product_sk"] == 1
