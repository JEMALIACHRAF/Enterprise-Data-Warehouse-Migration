"""
spark_transformer.py
--------------------
PySpark transformation layer: Raw Oracle data → Snowflake-ready dimensional model.

Handles:
- Type casting & null normalization
- SCD Type 2 logic for slowly changing dimensions
- Business rule enforcement
- Deduplication
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DecimalType, FloatType, IntegerType, StringType

logger = logging.getLogger(__name__)


class SparkTransformer:
    """Transforms raw Oracle extracts into dimensional model DataFrames."""

    NULL_SENTINEL_STR = "UNKNOWN"
    NULL_SENTINEL_INT = -1

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # -------------------------------------------------------------------------
    # GEOGRAPHY DIMENSION
    # -------------------------------------------------------------------------

    def transform_dim_continent(self, raw_df: DataFrame) -> DataFrame:
        logger.info("Transforming dim_continent...")
        return (
            raw_df
            .select(
                F.col("CONTINENT_CD").cast(StringType()).alias("continent_code"),
                F.initcap(F.col("CONTINENT_NM")).alias("continent_name"),
            )
            .filter(F.col("continent_code").isNotNull())
            .dropDuplicates(["continent_code"])
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

    def transform_dim_country(
        self, raw_df: DataFrame, dim_continent: DataFrame
    ) -> DataFrame:
        logger.info("Transforming dim_country...")
        return (
            raw_df
            .select(
                F.col("COUNTRY_CD").cast(StringType()).alias("country_code"),
                F.col("COUNTRY_NM").alias("country_name"),
                F.col("CONTINENT_CD").alias("continent_code"),
                F.col("CURRENCY_CD").cast(StringType()).alias("currency_code"),
                F.col("IS_EU_FLG").cast(BooleanType()).alias("is_eu_member"),
                F.col("ACTIVE_FLG").cast(BooleanType()).alias("is_active"),
            )
            .filter(F.col("country_code").isNotNull())
            .join(
                dim_continent.select("continent_code", "continent_sk"),
                on="continent_code",
                how="left",
            )
            .drop("continent_code")
            .dropDuplicates(["country_code"])
        )

    def transform_dim_region(
        self, raw_df: DataFrame, dim_country: DataFrame
    ) -> DataFrame:
        logger.info("Transforming dim_region...")
        return (
            raw_df
            .select(
                F.col("REGION_CD").alias("region_code"),
                F.col("REGION_NM").alias("region_name"),
                F.col("COUNTRY_CD").alias("country_code"),
            )
            .filter(F.col("region_code").isNotNull())
            .join(
                dim_country.select("country_code", "country_sk"),
                on="country_code",
                how="left",
            )
            .drop("country_code")
            .dropDuplicates(["region_code"])
        )

    def transform_dim_city(
        self, raw_df: DataFrame, dim_region: DataFrame
    ) -> DataFrame:
        logger.info("Transforming dim_city...")
        return (
            raw_df
            .select(
                F.col("CITY_NM").alias("city_name"),
                F.col("POSTAL_CD").alias("postal_code"),
                F.col("REGION_CD").alias("region_code"),
                F.col("LAT").cast(FloatType()).alias("latitude"),
                F.col("LNG").cast(FloatType()).alias("longitude"),
            )
            .join(
                dim_region.select("region_code", "region_sk"),
                on="region_code",
                how="left",
            )
            .drop("region_code")
        )

    # -------------------------------------------------------------------------
    # PRODUCT DIMENSION sub-dims
    # -------------------------------------------------------------------------

    def transform_dim_product_line(self, raw_df: DataFrame) -> DataFrame:
        logger.info("Transforming dim_product_line...")
        return (
            raw_df
            .select(
                F.col("PROD_LINE_CD").cast(StringType()).alias("product_line_code"),
                F.col("PROD_LINE_NM").alias("product_line_name"),
                F.col("BUSINESS_UNIT").alias("business_unit"),
            )
            .filter(F.col("product_line_code").isNotNull())
            .dropDuplicates(["product_line_code"])
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

    def transform_dim_product_family(
        self, raw_df: DataFrame, dim_product_line: DataFrame
    ) -> DataFrame:
        logger.info("Transforming dim_product_family...")
        return (
            raw_df
            .select(
                F.col("FAMILY_CD").cast(StringType()).alias("product_family_code"),
                F.col("FAMILY_NM").alias("product_family_name"),
                F.col("PROD_LINE_CD").alias("product_line_code"),
                F.col("RISK_CAT").alias("risk_category"),
                F.col("IS_REGULATED").cast(BooleanType()).alias("is_regulated"),
            )
            .filter(F.col("product_family_code").isNotNull())
            .join(
                dim_product_line.select("product_line_code", "product_line_sk"),
                on="product_line_code",
                how="left",
            )
            .drop("product_line_code")
            .dropDuplicates(["product_family_code"])
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

    # -------------------------------------------------------------------------
    # CUSTOMER DIMENSION — SCD Type 2
    # -------------------------------------------------------------------------

    def transform_dim_customer(
        self,
        raw_df: DataFrame,
        dim_city: DataFrame,
        existing_dim: Optional[DataFrame] = None,
    ) -> DataFrame:
        logger.info("Transforming dim_customer (SCD Type 2)...")

        staged = (
            raw_df
            .select(
                F.col("CUST_ID").cast(StringType()).alias("customer_bk"),
                F.col("CUST_NM").alias("customer_name"),
                F.col("CUST_TYPE").alias("customer_type"),
                F.col("CITY_ID").cast("long").alias("city_sk"),
                F.col("SEGMENT").alias("segment"),
                F.col("KYC_STATUS").alias("kyc_status"),
            )
            .filter(F.col("customer_bk").isNotNull())
            .withColumn("valid_from", F.current_date())
            .withColumn("valid_to", F.lit(None).cast(DateType()))
            .withColumn("is_current", F.lit(True))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        if existing_dim is None:
            return staged

        changed_keys = (
            existing_dim.filter(F.col("is_current") == True)
            .join(staged, on="customer_bk", how="inner")
            .filter(
                (existing_dim["customer_name"] != staged["customer_name"]) |
                (existing_dim["kyc_status"]    != staged["kyc_status"])
            )
            .select(existing_dim["customer_bk"])
        )

        expired = (
            existing_dim.join(changed_keys, on="customer_bk", how="inner")
            .withColumn("valid_to", F.date_sub(F.current_date(), 1))
            .withColumn("is_current", F.lit(False))
            .withColumn("updated_at", F.current_timestamp())
        )

        unchanged      = existing_dim.join(changed_keys, on="customer_bk", how="left_anti")
        new_customers  = staged.join(
            existing_dim.filter(F.col("is_current") == True).select("customer_bk"),
            on="customer_bk", how="left_anti"
        )
        updated_customers = staged.join(changed_keys, on="customer_bk", how="inner")

        return unchanged.unionByName(expired) \
                        .unionByName(new_customers) \
                        .unionByName(updated_customers)

    # -------------------------------------------------------------------------
    # PRODUCT DIMENSION — SCD Type 2
    # -------------------------------------------------------------------------

    def transform_dim_product(
        self,
        raw_df: DataFrame,
        dim_product_family: DataFrame,
        existing_dim: Optional[DataFrame] = None,
    ) -> DataFrame:
        """
        Applies SCD Type 2 logic:
        - New products → insert as current record
        - Changed products → expire old record, insert new current record
        - Unchanged products → no-op
        """
        logger.info("Transforming dim_product (SCD Type 2)...")

        staged = (
            raw_df
            .select(
                F.col("PRODUCT_ID").cast(StringType()).alias("product_bk"),
                F.col("PRODUCT_NM").alias("product_name"),
                F.col("FAMILY_CD").alias("product_family_code"),
                F.col("ISIN_CD").alias("isin_code"),
                F.col("CURRENCY_CD").alias("currency_code"),
                F.to_date(F.col("MATURITY_DT"), "yyyyMMdd").alias("maturity_date"),
                F.col("ACTIVE_FLG").cast(BooleanType()).alias("is_active"),
            )
            .join(
                dim_product_family.select("product_family_code", "product_family_sk"),
                on="product_family_code",
                how="left",
            )
            .drop("product_family_code")
            .withColumn("valid_from", F.current_date())
            .withColumn("valid_to", F.lit(None).cast(DateType()))
            .withColumn("is_current", F.lit(True))
        )

        if existing_dim is None:
            return staged

        # Identify changed records
        changed_keys = (
            existing_dim
            .filter(F.col("is_current") == True)
            .join(staged, on="product_bk", how="inner")
            .filter(
                (existing_dim["product_name"] != staged["product_name"]) |
                (existing_dim["isin_code"] != staged["isin_code"]) |
                (existing_dim["is_active"] != staged["is_active"])
            )
            .select(existing_dim["product_bk"])
        )

        # Expire old current records
        expired = (
            existing_dim
            .join(changed_keys, on="product_bk", how="inner")
            .withColumn("valid_to", F.date_sub(F.current_date(), 1))
            .withColumn("is_current", F.lit(False))
            .withColumn("updated_at", F.current_timestamp())
        )

        # Unchanged records
        unchanged = existing_dim.join(changed_keys, on="product_bk", how="left_anti")

        # New records = staged products not yet in dim
        new_products = staged.join(
            existing_dim.filter(F.col("is_current") == True).select("product_bk"),
            on="product_bk",
            how="left_anti",
        )

        # Changed products → insert new version
        updated_products = staged.join(changed_keys, on="product_bk", how="inner")

        return unchanged.unionByName(expired) \
                        .unionByName(new_products) \
                        .unionByName(updated_products)

    # -------------------------------------------------------------------------
    # FACT TABLE
    # -------------------------------------------------------------------------

    def transform_fact_transactions(
        self,
        raw_df: DataFrame,
        dim_time: DataFrame,
        dim_customer: DataFrame,
        dim_product: DataFrame,
        dim_city: DataFrame,
        batch_id: str,
    ) -> DataFrame:
        logger.info("Transforming fact_transactions...")

        # Resolve surrogate keys
        current_customers = dim_customer.filter(F.col("is_current") == True)
        current_products = dim_product.filter(F.col("is_current") == True)

        fact = (
            raw_df
            .select(
                F.col("TXN_ID").alias("transaction_bk"),
                F.to_date(F.col("TXN_DT"), "yyyyMMdd").alias("txn_date"),
                F.col("CUST_ID").alias("customer_bk"),
                F.col("PROD_ID").alias("product_bk"),
                F.col("TXN_TYPE_CD").alias("transaction_type"),
                F.col("STATUS_CD").alias("transaction_status"),
                F.col("CHANNEL_CD").alias("channel"),
                F.col("GROSS_AMT").cast(DecimalType(18, 4)).alias("gross_amount"),
                F.col("NET_AMT").cast(DecimalType(18, 4)).alias("net_amount"),
                F.col("FEE_AMT").cast(DecimalType(18, 4)).alias("fee_amount"),
                F.col("TAX_AMT").cast(DecimalType(18, 4)).alias("tax_amount"),
                F.col("QTY").cast(DecimalType(18, 6)).alias("quantity"),
                F.col("UNIT_PRICE").cast(DecimalType(18, 6)).alias("unit_price"),
                F.col("CURRENCY_CD").alias("currency_code"),
                F.col("FX_RATE_EUR").cast(FloatType()).alias("exchange_rate_to_eur"),
            )
            # Deduplication: keep latest version of each transaction
            .withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy("transaction_bk")
                          .orderBy(F.col("txn_date").desc())
                ),
            )
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            # Compute EUR amount
            .withColumn(
                "amount_eur",
                F.round(F.col("net_amount") * F.col("exchange_rate_to_eur"), 4),
            )
            # Resolve time_sk
            .withColumn("time_sk", F.date_format(F.col("txn_date"), "yyyyMMdd").cast(IntegerType()))
            .drop("txn_date")
        )

        # Join surrogate keys
        fact = (
            fact
            .join(
                current_customers.select(
                    F.col("customer_bk"), F.col("customer_sk"), F.col("city_sk")
                ),
                on="customer_bk", how="left",
            )
            .join(
                current_products.select(F.col("product_bk"), F.col("product_sk")),
                on="product_bk", how="left",
            )
            .drop("customer_bk", "product_bk")
            .withColumn("source_system", F.lit("ORACLE_ERP"))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("loaded_at", F.current_timestamp())
        )

        null_sk_count = fact.filter(
            F.col("customer_sk").isNull() | F.col("product_sk").isNull()
        ).count()

        if null_sk_count > 0:
            logger.warning(
                f"fact_transactions: {null_sk_count:,} rows with unresolved surrogate keys. "
                "These will be written to the quarantine table."
            )

        return fact
