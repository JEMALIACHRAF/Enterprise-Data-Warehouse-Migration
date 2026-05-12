"""
conftest.py
-----------
Configuration globale pytest.
Fixtures partagées entre tous les tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession partagée pour tous les tests de la session."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("migration-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def reset_spark_context(spark):
    """Nettoie le cache Spark entre les tests."""
    yield
    spark.catalog.clearCache()
