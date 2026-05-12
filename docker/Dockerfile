# =============================================================================
# Dockerfile — Oracle → Snowflake Migration Spark Job
# Base: official Spark 3.4 image + Oracle JDBC + GCS connector
# =============================================================================

ARG SPARK_VERSION=3.4.1
ARG PYTHON_VERSION=3.10

FROM apache/spark-py:v${SPARK_VERSION}

USER root

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ── Download JAR dependencies ─────────────────────────────────────────────────
# GCS Connector
RUN wget -q -P /opt/spark/jars \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# Snowflake Spark Connector
RUN wget -q -P /opt/spark/jars \
    https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.4/spark-snowflake_2.12-2.12.0-spark_3.4.jar \
    https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.4/snowflake-jdbc-3.14.4.jar

# Oracle JDBC (ojdbc8 — must be placed manually due to Oracle license)
COPY drivers/ojdbc8.jar /opt/spark/jars/ojdbc8.jar

# ── Application code ──────────────────────────────────────────────────────────
COPY src/       /app/src/
COPY config/    /app/config/

WORKDIR /app

# ── Spark config defaults ─────────────────────────────────────────────────────
COPY docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ── Non-root user for security ────────────────────────────────────────────────
RUN useradd -m -u 1001 sparkuser && chown -R sparkuser:sparkuser /app
USER sparkuser

ENTRYPOINT ["/opt/entrypoint.sh"]
