-- =============================================================================
-- SNOWFLAKE SCHEMA — Financial Data Warehouse
-- Société Générale — DSI Migration Project
-- Author: Data Engineering Team
-- Description: Normalized dimensional model (Snowflake Schema)
--              replacing Oracle legacy star schema.
--              Geography and Product dimensions are decomposed
--              into sub-dimensions to reduce redundancy by ~30%.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- DATABASE & SCHEMAS
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS FINANCE_DWH;

CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.RAW;        -- Landing zone (raw Oracle data)
CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.STAGING;    -- Cleaned & typed
CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.CORE;       -- Dimensional model (Snowflake schema)
CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.MARTS;      -- Aggregated analytical views


-- =============================================================================
-- CORE LAYER — SNOWFLAKE SCHEMA (normalized dimensions)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- GEOGRAPHY DIMENSION — decomposed into 4 sub-dimensions
-- Normalisation reduces storage vs flat dim_geography by ~30%
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_CONTINENT (
    continent_sk        NUMBER AUTOINCREMENT PRIMARY KEY,
    continent_code      VARCHAR(2)      NOT NULL UNIQUE,   -- EU, AM, AS, AF, OC
    continent_name      VARCHAR(100)    NOT NULL,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_COUNTRY (
    country_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    country_code        VARCHAR(3)      NOT NULL UNIQUE,   -- ISO 3166-1 alpha-3
    country_name        VARCHAR(200)    NOT NULL,
    continent_sk        NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_CONTINENT(continent_sk),
    currency_code       VARCHAR(3),                        -- ISO 4217
    is_eu_member        BOOLEAN         DEFAULT FALSE,
    is_active           BOOLEAN         DEFAULT TRUE,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_REGION (
    region_sk           NUMBER AUTOINCREMENT PRIMARY KEY,
    region_code         VARCHAR(10)     NOT NULL,
    region_name         VARCHAR(200)    NOT NULL,
    country_sk          NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_COUNTRY(country_sk),
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (region_code, country_sk)
);

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_CITY (
    city_sk             NUMBER AUTOINCREMENT PRIMARY KEY,
    city_name           VARCHAR(200)    NOT NULL,
    postal_code         VARCHAR(20),
    region_sk           NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_REGION(region_sk),
    latitude            FLOAT,
    longitude           FLOAT,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);


-- ---------------------------------------------------------------------------
-- PRODUCT DIMENSION — decomposed into 3 sub-dimensions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_PRODUCT_LINE (
    product_line_sk     NUMBER AUTOINCREMENT PRIMARY KEY,
    product_line_code   VARCHAR(20)     NOT NULL UNIQUE,
    product_line_name   VARCHAR(200)    NOT NULL,
    business_unit       VARCHAR(100),
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_PRODUCT_FAMILY (
    product_family_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
    product_family_code VARCHAR(20)     NOT NULL UNIQUE,
    product_family_name VARCHAR(200)    NOT NULL,
    product_line_sk     NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_PRODUCT_LINE(product_line_sk),
    risk_category       VARCHAR(50),                       -- LOW / MEDIUM / HIGH
    is_regulated        BOOLEAN         DEFAULT FALSE,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_PRODUCT (
    product_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    product_bk          VARCHAR(50)     NOT NULL UNIQUE,   -- Business key (Oracle PK)
    product_name        VARCHAR(300)    NOT NULL,
    product_family_sk   NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_PRODUCT_FAMILY(product_family_sk),
    isin_code           VARCHAR(12),
    currency_code       VARCHAR(3),
    maturity_date       DATE,
    is_active           BOOLEAN         DEFAULT TRUE,
    -- SCD Type 2 columns
    valid_from          DATE            NOT NULL DEFAULT CURRENT_DATE(),
    valid_to            DATE,
    is_current          BOOLEAN         DEFAULT TRUE,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);


-- ---------------------------------------------------------------------------
-- TIME DIMENSION
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_TIME (
    time_sk             NUMBER          PRIMARY KEY,       -- YYYYMMDD integer
    full_date           DATE            NOT NULL UNIQUE,
    day_of_week         NUMBER(1)       NOT NULL,          -- 1=Monday ... 7=Sunday
    day_name            VARCHAR(20)     NOT NULL,
    day_of_month        NUMBER(2)       NOT NULL,
    day_of_year         NUMBER(3)       NOT NULL,
    week_of_year        NUMBER(2)       NOT NULL,
    month_number        NUMBER(2)       NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    quarter             NUMBER(1)       NOT NULL,
    year                NUMBER(4)       NOT NULL,
    is_weekend          BOOLEAN         NOT NULL,
    is_public_holiday   BOOLEAN         DEFAULT FALSE,
    fiscal_year         NUMBER(4),
    fiscal_quarter      NUMBER(1)
);


-- ---------------------------------------------------------------------------
-- CUSTOMER DIMENSION — SCD Type 2
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.DIM_CUSTOMER (
    customer_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_bk         VARCHAR(50)     NOT NULL,          -- Oracle source key
    customer_name       VARCHAR(300)    NOT NULL,
    customer_type       VARCHAR(50),                       -- RETAIL / CORPORATE / INSTITUTIONAL
    city_sk             NUMBER          REFERENCES FINANCE_DWH.CORE.DIM_CITY(city_sk),
    segment             VARCHAR(100),
    kyc_status          VARCHAR(20),                       -- APPROVED / PENDING / REJECTED
    -- SCD Type 2
    valid_from          DATE            NOT NULL DEFAULT CURRENT_DATE(),
    valid_to            DATE,
    is_current          BOOLEAN         DEFAULT TRUE,
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);


-- =============================================================================
-- FACT TABLE — Financial Transactions
-- =============================================================================

CREATE TABLE IF NOT EXISTS FINANCE_DWH.CORE.FACT_TRANSACTIONS (
    transaction_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    transaction_bk          VARCHAR(100)    NOT NULL UNIQUE, -- Oracle source PK
    -- Dimension foreign keys
    time_sk                 NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_TIME(time_sk),
    customer_sk             NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_CUSTOMER(customer_sk),
    product_sk              NUMBER          NOT NULL REFERENCES FINANCE_DWH.CORE.DIM_PRODUCT(product_sk),
    city_sk                 NUMBER          REFERENCES FINANCE_DWH.CORE.DIM_CITY(city_sk),
    -- Degenerate dimensions
    transaction_type        VARCHAR(50)     NOT NULL,        -- BUY / SELL / TRANSFER
    transaction_status      VARCHAR(50)     NOT NULL,        -- SETTLED / PENDING / CANCELLED
    channel                 VARCHAR(50),                     -- BRANCH / ONLINE / API
    -- Measures
    gross_amount            NUMBER(18,4)    NOT NULL,
    net_amount              NUMBER(18,4)    NOT NULL,
    fee_amount              NUMBER(18,4)    DEFAULT 0,
    tax_amount              NUMBER(18,4)    DEFAULT 0,
    quantity                NUMBER(18,6),
    unit_price              NUMBER(18,6),
    currency_code           VARCHAR(3)      NOT NULL,
    exchange_rate_to_eur    FLOAT           DEFAULT 1.0,
    amount_eur              NUMBER(18,4),                    -- normalized amount
    -- Audit
    source_system           VARCHAR(50)     DEFAULT 'ORACLE_ERP',
    batch_id                VARCHAR(100),
    loaded_at               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (time_sk, customer_sk);   -- Micro-partitioning optimization


-- =============================================================================
-- INDEXES & SEARCH OPTIMIZATION
-- =============================================================================

ALTER TABLE FINANCE_DWH.CORE.FACT_TRANSACTIONS ADD SEARCH OPTIMIZATION ON EQUALITY(transaction_bk);
ALTER TABLE FINANCE_DWH.CORE.DIM_PRODUCT ADD SEARCH OPTIMIZATION ON EQUALITY(product_bk, isin_code);
ALTER TABLE FINANCE_DWH.CORE.DIM_CUSTOMER ADD SEARCH OPTIMIZATION ON EQUALITY(customer_bk);


-- =============================================================================
-- MARTS LAYER — Pre-aggregated analytical views
-- =============================================================================

CREATE OR REPLACE VIEW FINANCE_DWH.MARTS.V_MONTHLY_REVENUE_BY_COUNTRY AS
SELECT
    dt.year,
    dt.month_number,
    dt.month_name,
    dc.country_name,
    dco.continent_name,
    dpl.product_line_name,
    COUNT(ft.transaction_sk)            AS total_transactions,
    SUM(ft.amount_eur)                  AS total_revenue_eur,
    AVG(ft.amount_eur)                  AS avg_transaction_eur,
    SUM(ft.fee_amount)                  AS total_fees_eur
FROM FINANCE_DWH.CORE.FACT_TRANSACTIONS ft
JOIN FINANCE_DWH.CORE.DIM_TIME          dt  ON ft.time_sk = dt.time_sk
JOIN FINANCE_DWH.CORE.DIM_CUSTOMER      dcu ON ft.customer_sk = dcu.customer_sk
JOIN FINANCE_DWH.CORE.DIM_CITY          dci ON dcu.city_sk = dci.city_sk
JOIN FINANCE_DWH.CORE.DIM_REGION        dr  ON dci.region_sk = dr.region_sk
JOIN FINANCE_DWH.CORE.DIM_COUNTRY       dc  ON dr.country_sk = dc.country_sk
JOIN FINANCE_DWH.CORE.DIM_CONTINENT     dco ON dc.continent_sk = dco.continent_sk
JOIN FINANCE_DWH.CORE.DIM_PRODUCT       dp  ON ft.product_sk = dp.product_sk
JOIN FINANCE_DWH.CORE.DIM_PRODUCT_FAMILY dpf ON dp.product_family_sk = dpf.product_family_sk
JOIN FINANCE_DWH.CORE.DIM_PRODUCT_LINE  dpl ON dpf.product_line_sk = dpl.product_line_sk
WHERE ft.transaction_status = 'SETTLED'
  AND dcu.is_current = TRUE
  AND dp.is_current = TRUE
GROUP BY 1,2,3,4,5,6;
