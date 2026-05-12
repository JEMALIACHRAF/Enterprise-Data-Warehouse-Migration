"""
spark_session.py
----------------
Builds and configures the SparkSession for the migration pipeline.
Tuned for large-scale GCS ↔ Snowflake data movement on GKE.
"""

from pyspark.sql import SparkSession


def build_spark_session(env: str = "prod") -> SparkSession:
    """
    Create a SparkSession with:
    - Snowflake Spark Connector
    - GCS connector (Hadoop-GCS)
    - Adaptive Query Execution (AQE) enabled
    - Optimized shuffle & memory settings for large joins
    """

    is_prod = env == "prod"

    builder = (
        SparkSession.builder
        .appName(f"oracle-snowflake-migration-{env}")

        # ── Adaptive Query Execution ──────────────────────────────────────
        .config("spark.sql.adaptive.enabled",                        "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",     "true")
        .config("spark.sql.adaptive.skewJoin.enabled",               "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled",     "true")

        # ── Shuffle & Parallelism ─────────────────────────────────────────
        # 200 is Spark default; for 500TB we need much higher parallelism
        .config("spark.sql.shuffle.partitions",                      "800" if is_prod else "50")
        .config("spark.default.parallelism",                         "800" if is_prod else "50")

        # ── Memory ───────────────────────────────────────────────────────
        .config("spark.executor.memory",                             "28g" if is_prod else "4g")
        .config("spark.executor.memoryOverhead",                     "4g"  if is_prod else "512m")
        .config("spark.driver.memory",                               "8g"  if is_prod else "2g")
        .config("spark.memory.fraction",                             "0.8")
        .config("spark.memory.storageFraction",                      "0.3")

        # ── Serialization ─────────────────────────────────────────────────
        .config("spark.serializer",                                  "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired",                   "false")

        # ── GCS connector ─────────────────────────────────────────────────
        .config("spark.hadoop.fs.gs.impl",                           "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # Workload Identity on GKE (no service account key file needed)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")

        # ── GCS performance ───────────────────────────────────────────────
        .config("spark.hadoop.fs.gs.block.size",                     str(128 * 1024 * 1024))  # 128 MB
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", str(64  * 1024 * 1024))  # 64 MB
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

        # ── Snowflake connector ───────────────────────────────────────────
        .config("spark.jars.packages",
                "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
                "net.snowflake:snowflake-jdbc:3.14.4,"
                "com.oracle.database.jdbc:ojdbc8:21.9.0.0,"
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14")

        # ── Dynamic allocation (GKE autoscaling) ─────────────────────────
        .config("spark.dynamicAllocation.enabled",                   "true")
        .config("spark.dynamicAllocation.minExecutors",              "5")
        .config("spark.dynamicAllocation.maxExecutors",              "120" if is_prod else "10")
        .config("spark.dynamicAllocation.initialExecutors",          "20"  if is_prod else "2")
        .config("spark.dynamicAllocation.executorIdleTimeout",       "120s")
        .config("spark.dynamicAllocation.schedulerBacklogTimeout",   "30s")

        # ── Parquet ───────────────────────────────────────────────────────
        .config("spark.sql.parquet.compression.codec",               "snappy")
        .config("spark.sql.parquet.mergeSchema",                     "false")
        .config("spark.sql.parquet.filterPushdown",                  "true")

        # ── Misc ──────────────────────────────────────────────────────────
        .config("spark.sql.broadcastTimeout",                        "600")
        .config("spark.network.timeout",                             "800s")
        .config("spark.sql.legacy.timeParserPolicy",                 "LEGACY")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark
