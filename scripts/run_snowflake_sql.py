"""
scripts/run_snowflake_sql.py
----------------------------
Setup COMPLET Snowflake en un seul script :
  1. Bootstrap  — DB, schemas, warehouse, role, user
  2. DDL        — Toutes les tables dans FINANCE_DWH_DEV.CORE
  3. DIM_TIME   — 7305 jours générés (2010-2030)
  4. Vues       — Vues analytiques dans FINANCE_DWH_DEV.MARTS

Usage:
    python scripts/run_snowflake_sql.py              # tout faire en une fois
    python scripts/run_snowflake_sql.py --bootstrap  # bootstrap seulement
    python scripts/run_snowflake_sql.py --ddl        # DDL seulement
    python scripts/run_snowflake_sql.py --reset      # tout supprimer et recrée
"""

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    env_file = Path(__file__).parents[1] / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

import snowflake.connector
from snowflake.connector.errors import ProgrammingError

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"{GREEN}✅  {msg}{RESET}")
def err(msg):  print(f"{RED}❌  {msg}{RESET}")
def warn(msg): print(f"{YELLOW}⚠️   {msg}{RESET}")
def info(msg): print(f"{BLUE}ℹ️   {msg}{RESET}")
def section(title): print(f"\n{BOLD}{BLUE}── {title} {'─'*(50-len(title))}{RESET}")


def get_connection(database=None, schema=None):
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing  = [v for v in required if not os.environ.get(v)]
    if missing:
        err(f"Variables manquantes dans .env : {missing}")
        sys.exit(1)
    kwargs = dict(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "MIGRATION_WH"),
    )
    if database: kwargs["database"] = database
    if schema:   kwargs["schema"]   = schema
    return snowflake.connector.connect(**kwargs)


def run(cur, sql, label=None):
    preview = (label or sql).replace("\n", " ").strip()[:70]
    try:
        cur.execute(sql)
        print(f"    {GREEN}✓{RESET} {preview}")
        return True
    except ProgrammingError as e:
        msg = str(e).lower()
        if any(x in msg for x in ["already exists", "duplicate", "object exists"]):
            print(f"    {YELLOW}~{RESET} {preview} {YELLOW}(déjà existant){RESET}")
        else:
            print(f"    {RED}✗{RESET} {preview}")
            print(f"      {RED}→ {str(e)[:120]}{RESET}")
        return False


# =============================================================================
# STEP 1 — BOOTSTRAP
# =============================================================================

def step_bootstrap(cur):
    section("STEP 1 — Bootstrap (DB, Schemas, Warehouse, Role, User)")
    cmds = [
        # Databases
        ("CREATE DATABASE IF NOT EXISTS FINANCE_DWH_DEV",                              "DB: FINANCE_DWH_DEV"),
        ("CREATE DATABASE IF NOT EXISTS FINANCE_DWH",                                  "DB: FINANCE_DWH"),
        # Schemas DEV
        ("CREATE SCHEMA IF NOT EXISTS FINANCE_DWH_DEV.RAW",                            "Schema: FINANCE_DWH_DEV.RAW"),
        ("CREATE SCHEMA IF NOT EXISTS FINANCE_DWH_DEV.CORE",                           "Schema: FINANCE_DWH_DEV.CORE"),
        ("CREATE SCHEMA IF NOT EXISTS FINANCE_DWH_DEV.MARTS",                          "Schema: FINANCE_DWH_DEV.MARTS"),
        # Schemas PROD
        ("CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.RAW",                                "Schema: FINANCE_DWH.RAW"),
        ("CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.CORE",                               "Schema: FINANCE_DWH.CORE"),
        ("CREATE SCHEMA IF NOT EXISTS FINANCE_DWH.MARTS",                              "Schema: FINANCE_DWH.MARTS"),
        # Warehouse
        ("CREATE WAREHOUSE IF NOT EXISTS MIGRATION_WH WAREHOUSE_SIZE='SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE", "Warehouse: MIGRATION_WH"),
        # Role
        ("CREATE ROLE IF NOT EXISTS DATA_ENGINEER",                                    "Role: DATA_ENGINEER"),
        ("GRANT USAGE ON WAREHOUSE MIGRATION_WH TO ROLE DATA_ENGINEER",               "Grant: warehouse → DATA_ENGINEER"),
        ("GRANT ALL ON DATABASE FINANCE_DWH_DEV TO ROLE DATA_ENGINEER",               "Grant: FINANCE_DWH_DEV → DATA_ENGINEER"),
        ("GRANT ALL ON DATABASE FINANCE_DWH TO ROLE DATA_ENGINEER",                   "Grant: FINANCE_DWH → DATA_ENGINEER"),
        ("GRANT ALL ON ALL SCHEMAS IN DATABASE FINANCE_DWH_DEV TO ROLE DATA_ENGINEER","Grant: all schemas DEV → DATA_ENGINEER"),
        ("GRANT ALL ON ALL SCHEMAS IN DATABASE FINANCE_DWH TO ROLE DATA_ENGINEER",    "Grant: all schemas PROD → DATA_ENGINEER"),
        ("GRANT ALL ON FUTURE SCHEMAS IN DATABASE FINANCE_DWH_DEV TO ROLE DATA_ENGINEER", "Grant: future schemas DEV → DATA_ENGINEER"),
        ("GRANT ALL ON FUTURE SCHEMAS IN DATABASE FINANCE_DWH TO ROLE DATA_ENGINEER", "Grant: future schemas PROD → DATA_ENGINEER"),
        ("GRANT ALL ON FUTURE TABLES IN DATABASE FINANCE_DWH_DEV TO ROLE DATA_ENGINEER",  "Grant: future tables DEV → DATA_ENGINEER"),
        ("GRANT ALL ON FUTURE TABLES IN DATABASE FINANCE_DWH TO ROLE DATA_ENGINEER",  "Grant: future tables PROD → DATA_ENGINEER"),
        # Service user
        ("CREATE USER IF NOT EXISTS migration_svc PASSWORD='MotDePasseForT#2024!' DEFAULT_ROLE=DATA_ENGINEER DEFAULT_WAREHOUSE=MIGRATION_WH", "User: migration_svc"),
        ("GRANT ROLE DATA_ENGINEER TO USER migration_svc",                             "Grant: DATA_ENGINEER → migration_svc"),
    ]
    for sql, label in cmds:
        run(cur, sql, label)
    ok("Bootstrap terminé")


# =============================================================================
# STEP 2 — DDL (toutes les tables dans FINANCE_DWH_DEV.CORE)
# =============================================================================

def step_ddl(cur):
    section("STEP 2 — DDL (tables dans FINANCE_DWH_DEV.CORE)")

    tables = [

        ("DIM_CONTINENT", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_CONTINENT (
            continent_sk    NUMBER AUTOINCREMENT PRIMARY KEY,
            continent_code  VARCHAR(2)    NOT NULL UNIQUE,
            continent_name  VARCHAR(100)  NOT NULL,
            created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("DIM_COUNTRY", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_COUNTRY (
            country_sk      NUMBER AUTOINCREMENT PRIMARY KEY,
            country_code    VARCHAR(3)    NOT NULL UNIQUE,
            country_name    VARCHAR(200)  NOT NULL,
            continent_sk    NUMBER        REFERENCES FINANCE_DWH_DEV.CORE.DIM_CONTINENT(continent_sk),
            currency_code   VARCHAR(3),
            is_eu_member    BOOLEAN       DEFAULT FALSE,
            is_active       BOOLEAN       DEFAULT TRUE,
            created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("DIM_REGION", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_REGION (
            region_sk       NUMBER AUTOINCREMENT PRIMARY KEY,
            region_code     VARCHAR(10)   NOT NULL,
            region_name     VARCHAR(200)  NOT NULL,
            country_sk      NUMBER        REFERENCES FINANCE_DWH_DEV.CORE.DIM_COUNTRY(country_sk),
            created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE (region_code, country_sk)
        )"""),

        ("DIM_CITY", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_CITY (
            city_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
            city_name       VARCHAR(200)  NOT NULL,
            postal_code     VARCHAR(20),
            region_sk       NUMBER        REFERENCES FINANCE_DWH_DEV.CORE.DIM_REGION(region_sk),
            latitude        FLOAT,
            longitude       FLOAT,
            created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("DIM_PRODUCT_LINE", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_PRODUCT_LINE (
            product_line_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
            product_line_code VARCHAR(20)  NOT NULL UNIQUE,
            product_line_name VARCHAR(200) NOT NULL,
            business_unit     VARCHAR(100),
            created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("DIM_PRODUCT_FAMILY", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_PRODUCT_FAMILY (
            product_family_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
            product_family_code VARCHAR(20)  NOT NULL UNIQUE,
            product_family_name VARCHAR(200) NOT NULL,
            product_line_sk     NUMBER       REFERENCES FINANCE_DWH_DEV.CORE.DIM_PRODUCT_LINE(product_line_sk),
            risk_category       VARCHAR(50),
            is_regulated        BOOLEAN      DEFAULT FALSE,
            created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("DIM_PRODUCT", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_PRODUCT (
            product_sk        NUMBER AUTOINCREMENT PRIMARY KEY,
            product_bk        VARCHAR(50)   NOT NULL UNIQUE,
            product_name      VARCHAR(300)  NOT NULL,
            product_family_sk NUMBER        REFERENCES FINANCE_DWH_DEV.CORE.DIM_PRODUCT_FAMILY(product_family_sk),
            isin_code         VARCHAR(12),
            currency_code     VARCHAR(3),
            maturity_date     DATE,
            is_active         BOOLEAN       DEFAULT TRUE,
            valid_from        DATE          DEFAULT CURRENT_DATE(),
            valid_to          DATE,
            is_current        BOOLEAN       DEFAULT TRUE,
            created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("DIM_TIME", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_TIME (
            time_sk           NUMBER        PRIMARY KEY,
            full_date         DATE          NOT NULL UNIQUE,
            day_of_week       NUMBER(1)     NOT NULL,
            day_name          VARCHAR(20)   NOT NULL,
            day_of_month      NUMBER(2)     NOT NULL,
            day_of_year       NUMBER(3)     NOT NULL,
            week_of_year      NUMBER(2)     NOT NULL,
            month_number      NUMBER(2)     NOT NULL,
            month_name        VARCHAR(20)   NOT NULL,
            quarter           NUMBER(1)     NOT NULL,
            year              NUMBER(4)     NOT NULL,
            is_weekend        BOOLEAN       NOT NULL,
            is_public_holiday BOOLEAN       DEFAULT FALSE,
            fiscal_year       NUMBER(4),
            fiscal_quarter    NUMBER(1)
        )"""),

        ("DIM_CUSTOMER", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.DIM_CUSTOMER (
            customer_sk     NUMBER AUTOINCREMENT PRIMARY KEY,
            customer_bk     VARCHAR(50)   NOT NULL,
            customer_name   VARCHAR(300)  NOT NULL,
            customer_type   VARCHAR(50),
            city_sk         NUMBER        REFERENCES FINANCE_DWH_DEV.CORE.DIM_CITY(city_sk),
            segment         VARCHAR(100),
            kyc_status      VARCHAR(20),
            valid_from      DATE          DEFAULT CURRENT_DATE(),
            valid_to        DATE,
            is_current      BOOLEAN       DEFAULT TRUE,
            created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        ("FACT_TRANSACTIONS", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS (
            transaction_sk       NUMBER AUTOINCREMENT PRIMARY KEY,
            transaction_bk       VARCHAR(100)  NOT NULL UNIQUE,
            time_sk              NUMBER,
            customer_sk          NUMBER,
            product_sk           NUMBER,
            city_sk              NUMBER,
            transaction_type     VARCHAR(50),
            transaction_status   VARCHAR(50),
            channel              VARCHAR(50),
            gross_amount         NUMBER(18,4),
            net_amount           NUMBER(18,4),
            fee_amount           NUMBER(18,4)  DEFAULT 0,
            tax_amount           NUMBER(18,4)  DEFAULT 0,
            quantity             NUMBER(18,6),
            unit_price           NUMBER(18,6),
            currency_code        VARCHAR(3),
            exchange_rate_to_eur FLOAT         DEFAULT 1.0,
            amount_eur           NUMBER(18,4),
            source_system        VARCHAR(50)   DEFAULT 'ORACLE_ERP',
            batch_id             VARCHAR(100),
            loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),

        # Table quarantaine
        ("QUARANTINE_FACT_TRANSACTIONS", """
        CREATE TABLE IF NOT EXISTS FINANCE_DWH_DEV.CORE.QUARANTINE_FACT_TRANSACTIONS (
            transaction_bk    VARCHAR(100),
            quarantine_reason VARCHAR(200),
            raw_data          VARIANT,
            batch_id          VARCHAR(100),
            quarantined_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""),
    ]

    for name, sql in tables:
        run(cur, sql, f"Table: {name}")

    ok("DDL terminé — toutes les tables créées")


# =============================================================================
# STEP 3 — GÉNÉRATION DIM_TIME (2010-2030)
# =============================================================================

def step_dim_time(cur):
    section("STEP 3 — Génération DIM_TIME (2010-2030)")

    # Vérifier si déjà peuplée
    cur.execute("SELECT COUNT(*) FROM FINANCE_DWH_DEV.CORE.DIM_TIME")
    count = cur.fetchone()[0]
    if count > 0:
        warn(f"DIM_TIME déjà peuplée ({count:,} jours) — skip")
        return

    run(cur, """
    INSERT INTO FINANCE_DWH_DEV.CORE.DIM_TIME
    SELECT
        TO_NUMBER(TO_CHAR(d.dt, 'YYYYMMDD'))                                    AS time_sk,
        d.dt                                                                     AS full_date,
        DAYOFWEEKISO(d.dt)                                                       AS day_of_week,
        DAYNAME(d.dt)                                                            AS day_name,
        DAY(d.dt)                                                                AS day_of_month,
        DAYOFYEAR(d.dt)                                                          AS day_of_year,
        WEEKOFYEAR(d.dt)                                                         AS week_of_year,
        MONTH(d.dt)                                                              AS month_number,
        MONTHNAME(d.dt)                                                          AS month_name,
        QUARTER(d.dt)                                                            AS quarter,
        YEAR(d.dt)                                                               AS year,
        CASE WHEN DAYOFWEEKISO(d.dt) IN (6,7) THEN TRUE ELSE FALSE END          AS is_weekend,
        FALSE                                                                    AS is_public_holiday,
        YEAR(d.dt)                                                               AS fiscal_year,
        QUARTER(d.dt)                                                            AS fiscal_quarter
    FROM (
        SELECT DATEADD(DAY, seq4(), '2010-01-01'::DATE) AS dt
        FROM TABLE(GENERATOR(ROWCOUNT => 7305))
        WHERE dt <= '2029-12-31'
    ) d
    """, "INSERT DIM_TIME 2010-2030")

    cur.execute("SELECT COUNT(*) FROM FINANCE_DWH_DEV.CORE.DIM_TIME")
    count = cur.fetchone()[0]
    ok(f"DIM_TIME peuplée : {count:,} jours générés")


# =============================================================================
# STEP 4 — VUES ANALYTIQUES
# =============================================================================

def step_views(cur):
    section("STEP 4 — Vues analytiques (FINANCE_DWH_DEV.MARTS)")

    views = [

        ("V_MONTHLY_REVENUE_BY_COUNTRY", """
        CREATE OR REPLACE VIEW FINANCE_DWH_DEV.MARTS.V_MONTHLY_REVENUE_BY_COUNTRY AS
        SELECT
            dt.year, dt.month_number, dt.month_name,
            dco.continent_name, dc.country_name,
            dpl.product_line_name, dpf.product_family_name,
            ft.transaction_type,
            COUNT(*)               AS nb_transactions,
            SUM(ft.net_amount)     AS revenue_eur,
            AVG(ft.net_amount)     AS avg_transaction_eur,
            SUM(ft.fee_amount)     AS total_fees_eur
        FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS ft
        JOIN FINANCE_DWH_DEV.CORE.DIM_TIME          dt   ON ft.time_sk     = dt.time_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_CUSTOMER      dcu  ON ft.customer_sk = dcu.customer_sk AND dcu.is_current
        JOIN FINANCE_DWH_DEV.CORE.DIM_CITY          dci  ON dcu.city_sk    = dci.city_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_REGION        dr   ON dci.region_sk  = dr.region_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_COUNTRY       dc   ON dr.country_sk  = dc.country_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_CONTINENT     dco  ON dc.continent_sk= dco.continent_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_PRODUCT       dp   ON ft.product_sk  = dp.product_sk AND dp.is_current
        JOIN FINANCE_DWH_DEV.CORE.DIM_PRODUCT_FAMILY dpf ON dp.product_family_sk = dpf.product_family_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_PRODUCT_LINE  dpl  ON dpf.product_line_sk  = dpl.product_line_sk
        WHERE ft.transaction_status = 'SETTLED'
        GROUP BY 1,2,3,4,5,6,7,8
        """),

        ("V_TOP_CLIENTS", """
        CREATE OR REPLACE VIEW FINANCE_DWH_DEV.MARTS.V_TOP_CLIENTS AS
        SELECT
            dcu.customer_bk, dcu.customer_name, dcu.customer_type, dcu.segment,
            dc.country_name,
            COUNT(*)                                            AS nb_transactions,
            SUM(ft.net_amount)                                  AS total_volume_eur,
            AVG(ft.net_amount)                                  AS avg_transaction_eur,
            SUM(ft.fee_amount)                                  AS total_fees_generated,
            RANK() OVER (ORDER BY SUM(ft.net_amount) DESC)      AS volume_rank
        FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS ft
        JOIN FINANCE_DWH_DEV.CORE.DIM_CUSTOMER  dcu ON ft.customer_sk = dcu.customer_sk AND dcu.is_current
        JOIN FINANCE_DWH_DEV.CORE.DIM_CITY      dci ON dcu.city_sk    = dci.city_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_REGION    dr  ON dci.region_sk  = dr.region_sk
        JOIN FINANCE_DWH_DEV.CORE.DIM_COUNTRY   dc  ON dr.country_sk  = dc.country_sk
        WHERE ft.transaction_status = 'SETTLED'
        GROUP BY 1,2,3,4,5
        """),

        ("V_CHANNEL_ANALYSIS", """
        CREATE OR REPLACE VIEW FINANCE_DWH_DEV.MARTS.V_CHANNEL_ANALYSIS AS
        SELECT
            dt.year, dt.quarter,
            ft.channel, ft.transaction_status, ft.transaction_type,
            COUNT(*)            AS nb_transactions,
            SUM(ft.net_amount)  AS total_volume_eur,
            SUM(ft.fee_amount)  AS total_fees_eur,
            AVG(ft.net_amount)  AS avg_amount_eur,
            MIN(ft.net_amount)  AS min_amount_eur,
            MAX(ft.net_amount)  AS max_amount_eur
        FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS ft
        JOIN FINANCE_DWH_DEV.CORE.DIM_TIME dt ON ft.time_sk = dt.time_sk
        GROUP BY 1,2,3,4,5
        """),

        ("V_PIPELINE_QUALITY_KPI", """
        CREATE OR REPLACE VIEW FINANCE_DWH_DEV.MARTS.V_PIPELINE_QUALITY_KPI AS
        SELECT
            MAX(loaded_at)                                             AS last_load_ts,
            COUNT(*)                                                   AS total_rows,
            COUNT(DISTINCT batch_id)                                   AS nb_batches,
            SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END)       AS null_customer_sk,
            SUM(CASE WHEN product_sk  IS NULL THEN 1 ELSE 0 END)       AS null_product_sk,
            SUM(CASE WHEN net_amount  < 0     THEN 1 ELSE 0 END)       AS negative_amounts,
            SUM(CASE WHEN gross_amount < net_amount THEN 1 ELSE 0 END) AS gross_lt_net,
            SUM(net_amount)                                            AS total_volume_eur,
            AVG(net_amount)                                            AS avg_transaction_eur
        FROM FINANCE_DWH_DEV.CORE.FACT_TRANSACTIONS
        """),
    ]

    for name, sql in views:
        run(cur, sql, f"View: {name}")

    ok("Vues analytiques créées")


# =============================================================================
# RESET
# =============================================================================

def step_reset(cur):
    warn("RESET — suppression complète de FINANCE_DWH_DEV et FINANCE_DWH")
    confirm = input("  Tape 'OUI' pour confirmer : ").strip()
    if confirm != "OUI":
        info("Reset annulé.")
        return
    for db in ["FINANCE_DWH_DEV", "FINANCE_DWH"]:
        run(cur, f"DROP DATABASE IF EXISTS {db} CASCADE", f"DROP {db}")
    ok("Reset terminé — relance sans --reset pour tout recréer")


# =============================================================================
# VÉRIFICATION FINALE
# =============================================================================

def verify(cur):
    section("Vérification finale")
    try:
        cur.execute("SHOW TABLES IN SCHEMA FINANCE_DWH_DEV.CORE")
        tables = sorted([row[1] for row in cur.fetchall()])

        cur.execute("SELECT COUNT(*) FROM FINANCE_DWH_DEV.CORE.DIM_TIME")
        time_count = cur.fetchone()[0]

        cur.execute("SHOW VIEWS IN SCHEMA FINANCE_DWH_DEV.MARTS")
        views = sorted([row[1] for row in cur.fetchall()])

        print(f"\n  Tables ({len(tables)}) :")
        for t in tables:
            print(f"    {GREEN}✓{RESET}  {t}")

        print(f"\n  Vues ({len(views)}) :")
        for v in views:
            print(f"    {GREEN}✓{RESET}  {v}")

        print(f"\n  DIM_TIME : {time_count:,} jours")
        print(f"\n{GREEN}{BOLD}🎉  Snowflake setup 100% terminé !{RESET}")
        print(f"\n  Tu peux maintenant lancer :")
        print(f"  → python tests/connections/test_snowflake.py")

    except Exception as e:
        warn(f"Vérification impossible : {e}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Setup complet Snowflake en un seul script")
    parser.add_argument("--bootstrap", action="store_true", help="Bootstrap seulement (DB/role/user)")
    parser.add_argument("--ddl",       action="store_true", help="DDL seulement (tables)")
    parser.add_argument("--reset",     action="store_true", help="Tout supprimer (irréversible)")
    args = parser.parse_args()

    print(f"\n{BOLD}{BLUE}{'='*60}{RESET}")
    print(f"{BOLD}{BLUE}  Snowflake Setup — Oracle Migration Pipeline{RESET}")
    print(f"{BOLD}{BLUE}{'='*60}{RESET}")

    info("Connexion à Snowflake...")
    try:
        conn = get_connection()
        cur  = conn.cursor()
        ok(f"Connecté à {os.environ['SNOWFLAKE_ACCOUNT']}")
    except Exception as e:
        err(f"Connexion échouée : {e}")
        sys.exit(1)

    if args.reset:
        step_reset(cur)
        conn.close()
        return

    if args.bootstrap:
        step_bootstrap(cur)
    elif args.ddl:
        step_ddl(cur)
        step_dim_time(cur)
        step_views(cur)
        verify(cur)
    else:
        # TOUT faire dans l'ordre
        step_bootstrap(cur)
        step_ddl(cur)
        step_dim_time(cur)
        step_views(cur)
        verify(cur)

    conn.close()


if __name__ == "__main__":
    main()