<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.4-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Oracle](https://img.shields.io/badge/Oracle-F80000?style=for-the-badge&logo=oracle&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

# Oracle to Snowflake — Enterprise Data Warehouse Migration

### Production-grade batch pipeline migrating a legacy Oracle ERP to a modern cloud architecture

##### *PySpark · Snowflake Schema · SCD Type 2 · GKE Autoscaling · 3-Layer Data Validation*

![Tests](https://img.shields.io/badge/tests-30%20passed-brightgreen?style=flat-square)
![Lint](https://img.shields.io/badge/lint-ruff-blue?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)

</div>

---

## Context

Many financial institutions operate legacy Oracle data warehouses that struggle with scalability, maintenance cost, and the lack of self-service analytics capabilities. This project implements the full migration pipeline developed during a project at the Direction des Systèmes d'Information of a major French bank, moving 500+ TB of financial transaction data to a cloud-native architecture.

The pipeline replaces manual ETL scripts with a robust, testable, and observable data platform that enables business analysts to query 20 years of transaction history in seconds.

---

## Architecture

```
                     EXTRACTION                  TRANSFORMATION              LOADING
                                                                                      
  Oracle 19c          PySpark                   Spark DataFrame              Snowflake
  ERP Legacy   ──►   JDBC Read    ──►   GCS    ──►  SCD Type 2   ──►   FINANCE_DWH_DEV
  (Source)          (Partitioned)      Landing       3-Layer           (Snowflake Schema)
                                       Zone        Validation
                                        
                                        │                                      │
                                        └──────── Reconciliation ──────────────┘
                                                  Row Count + Checksum
```

### Data Flow

```
1. Extract    Oracle FACT_TXN + dimension tables via partitioned JDBC (20 parallel threads)
              Writes raw Parquet to GCS landing zone: gs://bucket/raw/table/year=Y/month=M/day=D

2. Transform  PySpark transformations on GCS data:
              - Type casting and null normalization
              - SCD Type 2 versioning on DIM_PRODUCT and DIM_CUSTOMER
              - Surrogate key resolution
              - FX normalization (all amounts standardized to EUR)
              - Deduplication via Window functions

3. Validate   Three-layer validation before any write to Snowflake:
              Layer 1 - Schema: nullability, uniqueness, type conformance
              Layer 2 - Business rules: gross >= net, accepted values, referential integrity
              Layer 3 - Reconciliation: row count and amount checksum Oracle vs Snowflake

4. Load       Bulk load via Snowflake Spark Connector (COPY INTO via internal stage)
              Valid rows -> FACT_TRANSACTIONS
              Invalid rows -> QUARANTINE_FACT_TRANSACTIONS for investigation

5. Post-load  Analytical views refresh, search optimization resume, Slack notification
```

---

## Dimensional Model — Snowflake Schema

The dimensional model uses a normalized snowflake schema (not star schema), decomposing large dimensions into sub-dimensions to reduce storage by approximately 30% compared to a flat approach.

```
Geography Hierarchy                        Product Hierarchy

DIM_CONTINENT                              DIM_PRODUCT_LINE
     |                                          |
DIM_COUNTRY                                DIM_PRODUCT_FAMILY
     |                                          |
DIM_REGION                                 DIM_PRODUCT  (SCD Type 2)
     |
DIM_CITY
     |
     +----------+----------+
                |          |
           DIM_CUSTOMER    |           DIM_TIME
           (SCD Type 2)    |               |
                |          |               |
                +----------+---------------+
                           |
                    FACT_TRANSACTIONS
                    (clustered on time_sk, customer_sk)
```

**SCD Type 2** is applied on `DIM_PRODUCT` and `DIM_CUSTOMER`. When a dimension attribute changes (product name, customer KYC status), the existing record is expired (`valid_to = today - 1`, `is_current = FALSE`) and a new current record is inserted. This preserves the exact state of the dimension at the time of each transaction.

---

## Technology Choices

| Layer | Technology | Rationale |
|---|---|---|
| Source | Oracle 19c | Existing legacy ERP, read-only access via JDBC |
| Processing | Apache Spark 3.4 / PySpark | Distributed processing, handles 500TB, native Snowflake connector |
| Landing zone | Google Cloud Storage | Cost-effective object storage, native Parquet support |
| Target DWH | Snowflake Enterprise | Separation of storage and compute, zero-copy cloning, time travel |
| Orchestration | Kubernetes CronJob (GKE) | Container-native scheduling, autoscaling via Karpenter |
| Secrets | GCP Secret Manager | Centralized secret management, Workload Identity (no credentials in code) |
| CI/CD | GitHub Actions | Lint, test, build Docker, deploy to GKE with manual approval for prod |
| Testing | pytest + PySpark local | Unit tests run without external dependencies, integration tests against real systems |

---

## Project Structure

```
oracle-to-snowflake-migration/
|
+-- src/
|   +-- extraction/
|   |   +-- oracle_extractor.py      JDBC partitioned read from Oracle
|   +-- transformation/
|   |   +-- spark_transformer.py     PySpark transformations, SCD Type 2
|   +-- loading/
|   |   +-- snowflake_loader.py      Bulk load via Snowflake Spark Connector
|   +-- validation/
|   |   +-- data_quality.py          3-layer validation suite
|   +-- utils/
|   |   +-- spark_session.py         SparkSession configuration (tuned for GKE)
|   |   +-- config.py                Config loader + retry decorator
|   |   +-- gcs_helper.py            GCS operations wrapper
|   |   +-- notifier.py              Slack notifications
|   +-- main.py                      Pipeline orchestrator
|
+-- sql/
|   +-- ddl/snowflake_schema.sql     Full dimensional model DDL
|   +-- dml/generate_dim_time.sql    Time dimension generator (2010-2030)
|   +-- views/analytical_views.sql   BI-ready analytical views
|
+-- tests/
|   +-- unit/                        30 unit tests, no external dependencies
|   +-- connections/                 Real connection tests (Oracle, Snowflake, GCS)
|   +-- integration/                 End-to-end Spark integration tests
|
+-- k8s/
|   +-- deployment.yaml              GKE CronJob, HPA, service accounts, secrets
|
+-- scripts/
|   +-- run_snowflake_sql.py         Automated Snowflake setup (bootstrap + DDL + views)
|   +-- oracle_setup.sql             Oracle source tables and test data
|
+-- docker/
|   +-- Dockerfile                   Spark image with Oracle JDBC + GCS connector
|   +-- spark-defaults.conf
|
+-- config/
|   +-- pipeline_config.yaml.example
|
+-- .github/workflows/ci.yml         CI/CD pipeline
+-- pyproject.toml                   Ruff, mypy, pytest configuration
+-- requirements.txt
```

---

## Prerequisites

- Python 3.10+
- Java 11 (required for Spark)
- Docker Desktop
- gcloud CLI
- A Snowflake account (free trial at snowflake.com)
- A GCP project with billing enabled

---

## Getting Started

### 1. Clone and install dependencies

```bash
git clone https://github.com/YOUR_USERNAME/oracle-snowflake-migration.git
cd oracle-snowflake-migration

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Download Oracle JDBC driver

```bash
mkdir drivers
curl -L -o drivers/ojdbc8.jar \
  "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar"
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=XEPDB1
ORACLE_USER=migration_reader
ORACLE_PASSWORD=your_password

SNOWFLAKE_ACCOUNT=your_account.eu-west-1
SNOWFLAKE_USER=migration_svc
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=FINANCE_DWH_DEV
SNOWFLAKE_SCHEMA=CORE
SNOWFLAKE_WAREHOUSE=MIGRATION_WH

GCS_BUCKET=your-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### 4. Start Oracle (local development)

```bash
docker run -d \
  --name oracle-dev \
  -p 1521:1521 \
  -e ORACLE_PASSWORD=OraclePass#2024 \
  -e APP_USER=migration_reader \
  -e APP_USER_PASSWORD=ReaderPass#2024 \
  gvenzl/oracle-xe:21-slim

# Wait ~2 minutes for Oracle to initialize
docker logs -f oracle-dev | grep "READY"

# Load source tables and test data
docker cp scripts/oracle_setup.sql oracle-dev:/tmp/
docker exec oracle-dev sqlplus -s \
  "migration_reader/ReaderPass#2024@//localhost:1521/XEPDB1" \
  @/tmp/oracle_setup.sql
```

### 5. Configure Snowflake

Run the automated setup script (requires admin credentials in `.env`):

```bash
python scripts/run_snowflake_sql.py
```

This creates databases, schemas, warehouse, role, user, all dimension and fact tables, generates 7,305 rows in DIM_TIME (2010-2030), and creates analytical views.

### 6. Create GCS bucket

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud storage buckets create gs://your-bucket-name --location=europe-west1
```

### 7. Run tests

```bash
# Unit tests — no external dependencies required
python -m pytest tests/unit/ -v

# Connection tests — requires Oracle, Snowflake, and GCS to be configured
python tests/connections/test_oracle.py
python tests/connections/test_snowflake.py
python tests/connections/test_gcs.py

# Full Spark integration test — Oracle -> GCS -> Snowflake
python tests/connections/test_spark_integration.py
```

### 8. Run the pipeline

```bash
python src/main.py --env dev --batch-date $(date +%Y-%m-%d)

# Dry run (extract and transform only, skip Snowflake load)
python src/main.py --env dev --batch-date $(date +%Y-%m-%d) --dry-run
```

### 9. Verify results in Snowflake

```sql
SELECT COUNT(*)  FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS;
SELECT *         FROM FINANCE_DWH_DEV.MARTS.V_MONTHLY_REVENUE_BY_COUNTRY;
SELECT *         FROM FINANCE_DWH_DEV.MARTS.V_TOP_CLIENTS ORDER BY volume_rank;
SELECT *         FROM FINANCE_DWH_DEV.MARTS.V_PIPELINE_QUALITY_KPI;
```

---

## Production Deployment (GKE)

```bash
# Authenticate to GKE
gcloud container clusters get-credentials data-platform-prod --zone europe-west1-b

# Apply Kubernetes manifests (CronJob, HPA, secrets, RBAC)
kubectl apply -f k8s/ -n data-engineering

# Verify the CronJob is scheduled
kubectl get cronjob oracle-snowflake-migration -n data-engineering
```

The pipeline runs nightly at 01:00 UTC. Spark executors scale dynamically between 5 and 120 nodes via Karpenter, using spot instances for cost optimization.

---

## CI/CD Pipeline

Every pull request triggers:

1. **Lint** — ruff check on all Python files
2. **Unit tests** — 30 tests with PySpark local, no external dependencies
3. **Build** — Docker image pushed to Google Artifact Registry

On merge to main:

4. **Deploy staging** — automated deployment and validation job
5. **Deploy production** — requires manual approval in GitHub

---

## Data Quality

Three validation layers run before every Snowflake load:

**Layer 1 — Schema validation**
- Primary and business keys cannot be null
- Uniqueness on transaction_bk
- Column type conformance

**Layer 2 — Business rules**
- gross_amount must be greater than or equal to net_amount
- transaction_type must be in (BUY, SELL, TRANSFER)
- transaction_status must be in (SETTLED, PENDING, CANCELLED)
- Foreign key referential integrity (max 0.5% unresolved)

**Layer 3 — Reconciliation**
- Row count variance between Oracle and Snowflake must be below 0.5%
- Total net_amount variance must be below 0.1%

Rows failing validation are written to `QUARANTINE_FACT_TRANSACTIONS` for investigation rather than silently dropped.

---

## Performance

| Metric | Value |
|---|---|
| Total data volume | ~500 TB |
| Average throughput | 2.8 TB/hour |
| Peak Spark executors | 120 |
| Storage reduction vs Oracle | -30% (normalized schema) |
| Nightly batch SLA | under 6 hours |
| Unit test suite | 30 tests in ~3 minutes |

---

## Potential Improvements

The current architecture is solid for a nightly batch workload, but several directions could extend it further depending on business requirements.

**Real-time streaming with Apache Kafka**
The current batch approach introduces up to 24 hours of latency. Replacing the nightly JDBC extraction with a Change Data Capture connector (Debezium on Oracle) feeding Kafka topics would reduce latency to minutes and enable real-time fraud detection use cases.

**Azure Databricks as the processing layer**
Databricks offers managed Spark clusters with built-in Delta Lake support, Unity Catalog for governance, and tighter integration with Azure Data Factory for orchestration. For organizations already on Azure (Microsoft 365, Azure AD), replacing GKE + PySpark with Databricks would significantly reduce operational overhead and provide a richer development experience through notebooks and the Databricks UI.

**dbt for transformation layer**
The current PySpark transformations handle both heavy processing and business logic. Separating concerns by introducing dbt (data build tool) for the SQL-based transformation layer would improve maintainability, enable data lineage documentation, and allow business analysts to contribute transformations without Spark knowledge.

**Delta Lake / Iceberg table format**
Replacing Parquet files in GCS with Delta Lake or Apache Iceberg would enable ACID transactions, time travel, schema evolution, and efficient upserts — eliminating the need for the current SCD Type 2 manual implementation.

**Great Expectations for data contracts**
Replacing the custom validation layer with Great Expectations would provide a richer assertion library, HTML data docs, and integration with data catalogs. This would formalize data contracts between the engineering team and business consumers.

**Medallion architecture on GCS**
Formalizing the current raw landing zone into a full Bronze / Silver / Gold medallion architecture with explicit schemas at each layer would improve data discoverability and reduce time-to-insight for new use cases.


---
## Author

**Achraf Jemali** — Data & AI Engineer.

[![GitHub](https://img.shields.io/badge/GitHub-JEMALIACHRAF-black?logo=github&style=flat-square)](https://github.com/JEMALIACHRAF)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Achraf_Jemali-0077B5?logo=linkedin&style=flat-square)](https://linkedin.com/in/achraf-jemali-54a417239)

If you found this useful or want to discuss the design choices, feel free to reach out.
---
## License

MIT