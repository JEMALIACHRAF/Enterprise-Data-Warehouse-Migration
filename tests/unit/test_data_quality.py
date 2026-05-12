"""
test_data_quality.py
--------------------
Unit tests for the DataQualityValidator.
Uses a local SparkSession (no cluster needed).
"""


import pytest
from pyspark.sql import SparkSession

from src.validation.data_quality import DataQualityValidator, Severity


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-data-quality")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def validator(spark):
    return DataQualityValidator(spark)


# ─────────────────────────────────────────────────────────────────────────────
# check_not_null
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckNotNull:
    def test_passes_when_no_nulls(self, spark, validator):
        df = spark.createDataFrame([("TXN001",), ("TXN002",)], schema=["transaction_bk"])
        check = validator.check_not_null(df, "transaction_bk")
        assert check.passed is True
        assert check.failed_rows == 0

    def test_fails_when_null_present(self, spark, validator):
        df = spark.createDataFrame([("TXN001",), (None,)], schema=["transaction_bk"])
        check = validator.check_not_null(df, "transaction_bk")
        assert check.passed is False
        assert check.failed_rows == 1

    def test_passes_within_tolerance(self, spark, validator):
        # 1 null in 1000 rows = 0.1% — under default max_null_rate=0 → should fail
        data = [("TXN",)] * 999 + [(None,)]
        df = spark.createDataFrame(data, schema=["col"])
        check = validator.check_not_null(df, "col", max_null_rate=0.005)
        assert check.passed is True  # 0.1% < 0.5%

    def test_severity_is_critical_by_default(self, spark, validator):
        df = spark.createDataFrame([("A",)], schema=["col"])
        check = validator.check_not_null(df, "col")
        assert check.severity == Severity.CRITICAL


# ─────────────────────────────────────────────────────────────────────────────
# check_unique
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckUnique:
    def test_passes_when_unique(self, spark, validator):
        df = spark.createDataFrame([("A",), ("B",), ("C",)], schema=["transaction_bk"])
        check = validator.check_unique(df, ["transaction_bk"])
        assert check.passed is True

    def test_fails_when_duplicates_exist(self, spark, validator):
        df = spark.createDataFrame([("A",), ("A",), ("B",)], schema=["transaction_bk"])
        check = validator.check_unique(df, ["transaction_bk"])
        assert check.passed is False
        assert check.failed_rows == 1  # 1 duplicate

    def test_composite_key_unique(self, spark, validator):
        df = spark.createDataFrame(
            [("A", "2024-01"), ("A", "2024-02"), ("B", "2024-01")], schema=["customer_bk", "month"]
        )
        check = validator.check_unique(df, ["customer_bk", "month"])
        assert check.passed is True

    def test_composite_key_duplicate(self, spark, validator):
        df = spark.createDataFrame(
            [("A", "2024-01"), ("A", "2024-01")], schema=["customer_bk", "month"]
        )
        check = validator.check_unique(df, ["customer_bk", "month"])
        assert check.passed is False


# ─────────────────────────────────────────────────────────────────────────────
# check_accepted_values
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckAcceptedValues:
    def test_passes_all_valid(self, spark, validator):
        df = spark.createDataFrame(
            [("BUY",), ("SELL",), ("TRANSFER",)], schema=["transaction_type"]
        )
        check = validator.check_accepted_values(df, "transaction_type", ["BUY", "SELL", "TRANSFER"])
        assert check.passed is True

    def test_fails_on_unknown_value(self, spark, validator):
        df = spark.createDataFrame([("BUY",), ("HACK",)], schema=["transaction_type"])
        check = validator.check_accepted_values(df, "transaction_type", ["BUY", "SELL"])
        assert check.passed is False
        assert check.failed_rows == 1


# ─────────────────────────────────────────────────────────────────────────────
# check_not_negative
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckNotNegative:
    def test_passes_all_positive(self, spark, validator):
        df = spark.createDataFrame([(100.0,), (0.0,), (50.5,)], schema=["net_amount"])
        check = validator.check_not_negative(df, "net_amount")
        assert check.passed is True

    def test_fails_on_negative(self, spark, validator):
        df = spark.createDataFrame([(100.0,), (-5.0,)], schema=["net_amount"])
        check = validator.check_not_negative(df, "net_amount")
        assert check.passed is False
        assert check.failed_rows == 1


# ─────────────────────────────────────────────────────────────────────────────
# check_gross_gte_net
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckGrossGteNet:
    def test_passes_gross_gt_net(self, spark, validator):
        df = spark.createDataFrame(
            [(1100.0, 1000.0), (500.0, 500.0)], schema=["gross_amount", "net_amount"]
        )
        check = validator.check_gross_gte_net(df)
        assert check.passed is True

    def test_fails_gross_lt_net(self, spark, validator):
        df = spark.createDataFrame([(900.0, 1000.0)], schema=["gross_amount", "net_amount"])
        check = validator.check_gross_gte_net(df)
        assert check.passed is False


# ─────────────────────────────────────────────────────────────────────────────
# reconcile_row_counts
# ─────────────────────────────────────────────────────────────────────────────


class TestReconcileRowCounts:
    def test_passes_exact_match(self, validator):
        check = validator.reconcile_row_counts(1_000_000, 1_000_000, "fact_transactions")
        assert check.passed is True

    def test_passes_within_variance(self, validator):
        # 0.3% variance — under 0.5% threshold
        check = validator.reconcile_row_counts(1_000_000, 997_000, "fact_transactions")
        assert check.passed is True

    def test_fails_above_variance(self, validator):
        # 1% variance — above 0.5% threshold
        check = validator.reconcile_row_counts(1_000_000, 990_000, "fact_transactions")
        assert check.passed is False


# ─────────────────────────────────────────────────────────────────────────────
# reconcile_amounts
# ─────────────────────────────────────────────────────────────────────────────


class TestReconcileAmounts:
    def test_passes_exact_match(self, validator):
        check = validator.reconcile_amounts(
            5_000_000.00, 5_000_000.00, "net_amount", "fact_transactions"
        )
        assert check.passed is True

    def test_passes_within_tolerance(self, validator):
        # 0.005% variance — under 0.1% threshold
        check = validator.reconcile_amounts(
            5_000_000.00, 4_999_750.00, "net_amount", "fact_transactions"
        )
        assert check.passed is True

    def test_fails_above_tolerance(self, validator):
        check = validator.reconcile_amounts(
            5_000_000.00, 4_940_000.00, "net_amount", "fact_transactions"
        )
        assert check.passed is False
