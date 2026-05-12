-- =============================================================================
-- generate_dim_time.sql
-- Génère la table DIM_TIME pour les années 2010 à 2030
-- À exécuter dans Snowflake après le DDL principal
-- =============================================================================

-- Jours fériés France (à adapter selon besoins)
CREATE OR REPLACE TEMPORARY TABLE TEMP_HOLIDAYS (holiday_date DATE);
INSERT INTO TEMP_HOLIDAYS VALUES
    ('2024-01-01'), -- Jour de l'An
    ('2024-04-01'), -- Lundi de Pâques
    ('2024-05-01'), -- Fête du Travail
    ('2024-05-08'), -- Victoire 1945
    ('2024-05-09'), -- Ascension
    ('2024-05-20'), -- Lundi de Pentecôte
    ('2024-07-14'), -- Fête Nationale
    ('2024-08-15'), -- Assomption
    ('2024-11-01'), -- Toussaint
    ('2024-11-11'), -- Armistice
    ('2024-12-25'); -- Noël

-- Génération de la dimension temps (2010–2030)
INSERT INTO FINANCE_DWH_DEV.CORE.DIM_TIME
SELECT
    TO_NUMBER(TO_CHAR(d.dt, 'YYYYMMDD'))            AS time_sk,
    d.dt                                             AS full_date,
    DAYOFWEEKISO(d.dt)                               AS day_of_week,
    DAYNAME(d.dt)                                    AS day_name,
    DAY(d.dt)                                        AS day_of_month,
    DAYOFYEAR(d.dt)                                  AS day_of_year,
    WEEKOFYEAR(d.dt)                                 AS week_of_year,
    MONTH(d.dt)                                      AS month_number,
    MONTHNAME(d.dt)                                  AS month_name,
    QUARTER(d.dt)                                    AS quarter,
    YEAR(d.dt)                                       AS year,
    CASE WHEN DAYOFWEEKISO(d.dt) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END   AS is_public_holiday,
    YEAR(d.dt)                                       AS fiscal_year,
    QUARTER(d.dt)                                    AS fiscal_quarter
FROM (
    SELECT DATEADD(DAY, seq4(), '2010-01-01'::DATE) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 7305))  -- 20 ans
    WHERE dt <= '2029-12-31'
) d
LEFT JOIN TEMP_HOLIDAYS h ON d.dt = h.holiday_date;

SELECT COUNT(*) AS nb_jours_generes FROM FINANCE_DWH_DEV.CORE.DIM_TIME;
-- → 7305 jours
