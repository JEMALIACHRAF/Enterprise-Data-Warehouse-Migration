"""
data_quality.py
---------------
3-layer data validation suite for the Oracle → Snowflake migration.

Layer 1 — Schema validation   : column presence, type conformance, nullability
Layer 2 — Business rules      : referential integrity, value ranges, formats
Layer 3 — Reconciliation      : row count & checksum comparison Oracle vs Snowflake
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    CRITICAL = "CRITICAL"  # Pipeline must stop
    WARNING = "WARNING"  # Log and continue
    INFO = "INFO"  # Informational only


@dataclass
class ValidationCheck:
    name: str
    severity: Severity
    passed: bool
    message: str
    failed_rows: int = 0
    total_rows: int = 0

    @property
    def pass_rate(self) -> Optional[float]:
        if self.total_rows == 0:
            return None
        return (self.total_rows - self.failed_rows) / self.total_rows


@dataclass
class ValidationReport:
    table_name: str
    batch_id: str
    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks if c.severity == Severity.CRITICAL)

    @property
    def critical_failures(self) -> list[ValidationCheck]:
        return [c for c in self.checks if not c.passed and c.severity == Severity.CRITICAL]

    def summary(self) -> str:
        total = len(self.checks)
        passed = sum(1 for c in self.checks if c.passed)
        return (
            f"[{self.table_name}] Validation: {passed}/{total} checks passed. "
            f"Status: {'✅ PASSED' if self.passed else '❌ FAILED'}"
        )


class DataQualityValidator:
    """
    Runs multi-layer validation on transformed DataFrames before Snowflake load.
    Failures at CRITICAL severity raise an exception and halt the pipeline.
    """

    # Acceptable thresholds
    MAX_NULL_RATE_PK = 0.0  # Primary/business keys must never be null
    MAX_NULL_RATE_FK = 0.005  # Foreign keys: max 0.5% unresolved
    MAX_AMOUNT_VARIANCE = 0.001  # Reconciliation: max 0.1% total amount variance
    MAX_ROW_COUNT_VARIANCE = 0.005  # Reconciliation: max 0.5% row count variance

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # =========================================================================
    # LAYER 1 — Schema Validation
    # =========================================================================

    def check_not_null(
        self,
        df: DataFrame,
        column: str,
        severity: Severity = Severity.CRITICAL,
        max_null_rate: float = 0.0,
    ) -> ValidationCheck:
        total = df.count()
        nulls = df.filter(F.col(column).isNull()).count()
        null_rate = nulls / total if total > 0 else 0
        passed = null_rate <= max_null_rate
        return ValidationCheck(
            name=f"not_null:{column}",
            severity=severity,
            passed=passed,
            message=(
                f"Column '{column}': {nulls:,} nulls / {total:,} rows "
                f"(rate={null_rate:.4%}, threshold={max_null_rate:.4%})"
            ),
            failed_rows=nulls,
            total_rows=total,
        )

    def check_unique(
        self,
        df: DataFrame,
        columns: list[str],
        severity: Severity = Severity.CRITICAL,
    ) -> ValidationCheck:
        total = df.count()
        distinct = df.dropDuplicates(columns).count()
        duplicates = total - distinct
        passed = duplicates == 0
        return ValidationCheck(
            name=f"unique:{'+'.join(columns)}",
            severity=severity,
            passed=passed,
            message=f"Uniqueness on {columns}: {duplicates:,} duplicates found in {total:,} rows",
            failed_rows=duplicates,
            total_rows=total,
        )

    def check_accepted_values(
        self,
        df: DataFrame,
        column: str,
        accepted: list[str],
        severity: Severity = Severity.WARNING,
    ) -> ValidationCheck:
        total = df.count()
        invalid = df.filter(~F.col(column).isin(accepted)).count()
        passed = invalid == 0
        return ValidationCheck(
            name=f"accepted_values:{column}",
            severity=severity,
            passed=passed,
            message=(f"Column '{column}': {invalid:,} rows with values outside {accepted}"),
            failed_rows=invalid,
            total_rows=total,
        )

    def check_not_negative(
        self,
        df: DataFrame,
        column: str,
        severity: Severity = Severity.CRITICAL,
    ) -> ValidationCheck:
        total = df.count()
        negatives = df.filter(F.col(column) < 0).count()
        passed = negatives == 0
        return ValidationCheck(
            name=f"not_negative:{column}",
            severity=severity,
            passed=passed,
            message=f"Column '{column}': {negatives:,} negative values found",
            failed_rows=negatives,
            total_rows=total,
        )

    # =========================================================================
    # LAYER 2 — Business Rules
    # =========================================================================

    def check_referential_integrity(
        self,
        fact_df: DataFrame,
        dim_df: DataFrame,
        fact_fk: str,
        dim_pk: str,
        severity: Severity = Severity.CRITICAL,
        max_unresolved_rate: float = MAX_NULL_RATE_FK,
    ) -> ValidationCheck:
        """Verify all FK values in fact_df exist in dim_df."""
        total = fact_df.count()
        unresolved = fact_df.join(
            dim_df.select(F.col(dim_pk).alias(fact_fk)), on=fact_fk, how="left_anti"
        ).count()
        rate = unresolved / total if total > 0 else 0
        passed = rate <= max_unresolved_rate
        return ValidationCheck(
            name=f"referential_integrity:{fact_fk}→{dim_pk}",
            severity=severity,
            passed=passed,
            message=(
                f"FK '{fact_fk}' → '{dim_pk}': {unresolved:,} unresolved "
                f"({rate:.4%}, threshold={max_unresolved_rate:.4%})"
            ),
            failed_rows=unresolved,
            total_rows=total,
        )

    def check_gross_gte_net(self, df: DataFrame) -> ValidationCheck:
        """Business rule: gross_amount must always be >= net_amount."""
        total = df.count()
        violations = df.filter(F.col("gross_amount") < F.col("net_amount")).count()
        return ValidationCheck(
            name="business_rule:gross_gte_net",
            severity=Severity.CRITICAL,
            passed=violations == 0,
            message=f"gross_amount < net_amount: {violations:,} violations",
            failed_rows=violations,
            total_rows=total,
        )

    # =========================================================================
    # LAYER 3 — Reconciliation (Oracle vs Snowflake)
    # =========================================================================

    def reconcile_row_counts(
        self,
        source_count: int,
        target_count: int,
        table_name: str,
    ) -> ValidationCheck:
        delta = abs(source_count - target_count)
        variance = delta / source_count if source_count > 0 else 0
        passed = variance <= self.MAX_ROW_COUNT_VARIANCE
        return ValidationCheck(
            name=f"reconciliation:row_count:{table_name}",
            severity=Severity.CRITICAL,
            passed=passed,
            message=(
                f"Row count — Oracle: {source_count:,} | Snowflake: {target_count:,} "
                f"| Δ={delta:,} ({variance:.4%})"
            ),
        )

    def reconcile_amounts(
        self,
        source_total: float,
        target_total: float,
        column: str,
        table_name: str,
    ) -> ValidationCheck:
        variance = abs(source_total - target_total) / source_total if source_total else 0
        passed = variance <= self.MAX_AMOUNT_VARIANCE
        return ValidationCheck(
            name=f"reconciliation:amount:{column}:{table_name}",
            severity=Severity.CRITICAL,
            passed=passed,
            message=(
                f"Amount '{column}' — Oracle: {source_total:,.2f} | "
                f"Snowflake: {target_total:,.2f} | variance={variance:.6%}"
            ),
        )

    # =========================================================================
    # Orchestration
    # =========================================================================

    def validate_fact_transactions(
        self,
        df: DataFrame,
        dim_customer: DataFrame,
        dim_product: DataFrame,
        dim_time: DataFrame,
        batch_id: str,
        oracle_row_count: int,
        oracle_net_total: float,
    ) -> ValidationReport:
        report = ValidationReport(table_name="fact_transactions", batch_id=batch_id)

        # Layer 1 — Schema
        report.checks.append(self.check_not_null(df, "transaction_bk"))
        report.checks.append(self.check_not_null(df, "time_sk"))
        report.checks.append(self.check_not_null(df, "customer_sk"))
        report.checks.append(self.check_not_null(df, "product_sk"))
        report.checks.append(self.check_not_null(df, "gross_amount"))
        report.checks.append(self.check_not_null(df, "net_amount"))
        report.checks.append(self.check_unique(df, ["transaction_bk"]))

        # Layer 2 — Business rules
        report.checks.append(
            self.check_accepted_values(df, "transaction_type", ["BUY", "SELL", "TRANSFER"])
        )
        report.checks.append(
            self.check_accepted_values(
                df, "transaction_status", ["SETTLED", "PENDING", "CANCELLED"]
            )
        )
        report.checks.append(self.check_not_negative(df, "fee_amount", Severity.WARNING))
        report.checks.append(self.check_not_negative(df, "net_amount"))
        report.checks.append(self.check_gross_gte_net(df))
        report.checks.append(
            self.check_referential_integrity(df, dim_customer, "customer_sk", "customer_sk")
        )
        report.checks.append(
            self.check_referential_integrity(df, dim_product, "product_sk", "product_sk")
        )
        report.checks.append(self.check_referential_integrity(df, dim_time, "time_sk", "time_sk"))

        # Layer 3 — Reconciliation
        target_count = df.count()
        target_net_total = df.agg(F.sum("net_amount")).collect()[0][0] or 0.0
        report.checks.append(
            self.reconcile_row_counts(oracle_row_count, target_count, "fact_transactions")
        )
        report.checks.append(
            self.reconcile_amounts(
                oracle_net_total, float(target_net_total), "net_amount", "fact_transactions"
            )
        )

        logger.info(report.summary())
        for c in report.critical_failures:
            logger.error(f"  ❌ {c.name}: {c.message}")

        if not report.passed:
            raise ValueError(
                "Critical validation failures in fact_transactions: "
                + ", ".join(c.name for c in report.critical_failures)
            )

        return report
