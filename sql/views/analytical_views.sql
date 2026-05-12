-- =============================================================================
-- analytical_views.sql
-- Vues analytiques prêtes à l'emploi pour les équipes BI & Data Science
-- À exécuter dans Snowflake après chargement des données
-- =============================================================================

USE DATABASE FINANCE_DWH_DEV;
USE SCHEMA MARTS;

-- ---------------------------------------------------------------------------
-- Vue 1 — Revenue mensuel par pays et ligne produit
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_MONTHLY_REVENUE_BY_COUNTRY AS
SELECT
    dt.year,
    dt.month_number,
    dt.month_name,
    dco.continent_name,
    dc.country_name,
    dpl.product_line_name,
    dpf.product_family_name,
    ft.transaction_type,
    COUNT(*)                            AS nb_transactions,
    SUM(ft.net_amount)                  AS revenue_eur,
    AVG(ft.net_amount)                  AS avg_transaction_eur,
    SUM(ft.fee_amount)                  AS total_fees_eur,
    SUM(ft.quantity)                    AS total_quantity
FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS ft
JOIN FINANCE_DWH_DEV.CORE.DIM_TIME          dt   ON ft.time_sk    = dt.time_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_CUSTOMER      dcu  ON ft.customer_sk = dcu.customer_sk AND dcu.is_current
JOIN FINANCE_DWH_DEV.CORE.DIM_CITY          dci  ON dcu.city_sk    = dci.city_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_REGION        dr   ON dci.region_sk  = dr.region_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_COUNTRY       dc   ON dr.country_sk  = dc.country_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_CONTINENT     dco  ON dc.continent_sk = dco.continent_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_PRODUCT       dp   ON ft.product_sk   = dp.product_sk AND dp.is_current
JOIN FINANCE_DWH_DEV.CORE.DIM_PRODUCT_FAMILY dpf ON dp.product_family_sk = dpf.product_family_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_PRODUCT_LINE  dpl  ON dpf.product_line_sk  = dpl.product_line_sk
WHERE ft.transaction_status = 'SETTLED'
GROUP BY 1,2,3,4,5,6,7,8;

-- ---------------------------------------------------------------------------
-- Vue 2 — Top clients par volume (ranking)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_TOP_CLIENTS AS
SELECT
    dcu.customer_bk,
    dcu.customer_name,
    dcu.customer_type,
    dcu.segment,
    dc.country_name,
    COUNT(*)                                          AS nb_transactions,
    SUM(ft.net_amount)                                AS total_volume_eur,
    AVG(ft.net_amount)                                AS avg_transaction_eur,
    SUM(ft.fee_amount)                                AS total_fees_generated,
    RANK() OVER (ORDER BY SUM(ft.net_amount) DESC)    AS volume_rank
FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS ft
JOIN FINANCE_DWH_DEV.CORE.DIM_CUSTOMER      dcu ON ft.customer_sk = dcu.customer_sk AND dcu.is_current
JOIN FINANCE_DWH_DEV.CORE.DIM_CITY          dci ON dcu.city_sk    = dci.city_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_REGION        dr  ON dci.region_sk  = dr.region_sk
JOIN FINANCE_DWH_DEV.CORE.DIM_COUNTRY       dc  ON dr.country_sk  = dc.country_sk
WHERE ft.transaction_status = 'SETTLED'
GROUP BY 1,2,3,4,5;

-- ---------------------------------------------------------------------------
-- Vue 3 — Analyse des transactions par canal et statut
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_CHANNEL_ANALYSIS AS
SELECT
    dt.year,
    dt.quarter,
    ft.channel,
    ft.transaction_status,
    ft.transaction_type,
    COUNT(*)                AS nb_transactions,
    SUM(ft.net_amount)      AS total_volume_eur,
    SUM(ft.fee_amount)      AS total_fees_eur,
    AVG(ft.net_amount)      AS avg_amount_eur,
    MIN(ft.net_amount)      AS min_amount_eur,
    MAX(ft.net_amount)      AS max_amount_eur
FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS ft
JOIN FINANCE_DWH_DEV.CORE.DIM_TIME dt ON ft.time_sk = dt.time_sk
GROUP BY 1,2,3,4,5;

-- ---------------------------------------------------------------------------
-- Vue 4 — KPIs globaux du pipeline (monitoring qualité)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_PIPELINE_QUALITY_KPI AS
SELECT
    MAX(loaded_at)                                        AS last_load_ts,
    COUNT(*)                                              AS total_rows,
    COUNT(DISTINCT batch_id)                              AS nb_batches,
    SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END)  AS null_customer_sk,
    SUM(CASE WHEN product_sk  IS NULL THEN 1 ELSE 0 END)  AS null_product_sk,
    SUM(CASE WHEN net_amount < 0     THEN 1 ELSE 0 END)   AS negative_amounts,
    SUM(CASE WHEN gross_amount < net_amount THEN 1 ELSE 0 END) AS gross_lt_net,
    SUM(net_amount)                                       AS total_volume_eur,
    AVG(net_amount)                                       AS avg_transaction_eur
FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS;
