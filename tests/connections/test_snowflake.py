"""
test_snowflake.py
-----------------
Teste la connexion réelle à Snowflake.
Vérifie : authentification, warehouse, DDL, lecture/écriture.

Usage:
    python tests/connections/test_snowflake.py
"""

import os
from pathlib import Path

# Charge .env depuis la racine du projet
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

import snowflake.connector
from snowflake.connector.errors import DatabaseError, OperationalError


def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],    # ex: abc12345.eu-central-1
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "MIGRATION_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE", "FINANCE_DWH_DEV"),
        schema    = os.environ.get("SNOWFLAKE_SCHEMA", "CORE"),
    )


def test_snowflake_connection():
    print("\n" + "="*60)
    print("  TEST CONNEXION — Snowflake")
    print("="*60)

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing  = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"❌  Variables manquantes : {missing}")
        print("    → Définis-les dans ton fichier .env puis : export $(cat .env | xargs)")
        return False

    # ── 1. Connexion ─────────────────────────────────────────────
    try:
        conn = get_connection()
        cur  = conn.cursor()
        print("✅  Connexion établie")
    except (DatabaseError, OperationalError) as e:
        print(f"❌  Connexion échouée : {e}")
        return False

    # ── 2. Infos de session ───────────────────────────────────────
    cur.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    version, user, role, warehouse = cur.fetchone()
    print(f"✅  Version Snowflake : {version}")
    print(f"    User      : {user}")
    print(f"    Role      : {role}")
    print(f"    Warehouse : {warehouse}")

    # ── 3. Vérifier la base et le schema ─────────────────────────
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    db, schema = cur.fetchone()
    print(f"✅  Database : {db} | Schema : {schema}")

    # ── 4. Lister les tables créées par le DDL ────────────────────
    cur.execute("SHOW TABLES IN SCHEMA CORE")
    tables = [row[1] for row in cur.fetchall()]

    expected_tables = [
        "DIM_CONTINENT", "DIM_COUNTRY", "DIM_REGION", "DIM_CITY",
        "DIM_PRODUCT_LINE", "DIM_PRODUCT_FAMILY", "DIM_PRODUCT",
        "DIM_CUSTOMER", "DIM_TIME", "FACT_TRANSACTIONS"
    ]

    print(f"\n  Tables présentes ({len(tables)}) :")
    all_present = True
    for t in expected_tables:
        found = t in tables
        status = "✅" if found else "❌"
        print(f"    {status}  {t}")
        if not found:
            all_present = False

    if not all_present:
        print("\n  ⚠️  Tables manquantes → exécute sql/ddl/snowflake_schema.sql dans Snowflake")

    # ── 5. Test écriture / lecture ────────────────────────────────
    cur.execute("""
        INSERT INTO DIM_CONTINENT (continent_code, continent_name)
        VALUES ('TS', 'Test Continent')
    """)
    conn.commit()
    print("\n✅  INSERT dans DIM_CONTINENT OK")

    cur.execute("SELECT continent_code, continent_name FROM DIM_CONTINENT WHERE continent_code = 'TS'")
    row = cur.fetchone()
    assert row is not None, "Ligne insérée introuvable !"
    print(f"✅  SELECT OK : {row[0]} → {row[1]}")

    # ── 6. Nettoyage ──────────────────────────────────────────────
    cur.execute("DELETE FROM DIM_CONTINENT WHERE continent_code = 'TS'")
    conn.commit()
    print("✅  Nettoyage OK")

    # ── 7. Test warehouse (compute) ───────────────────────────────
    cur.execute("SELECT COUNT(*) FROM FACT_TRANSACTIONS")
    count = cur.fetchone()[0]
    print(f"✅  Warehouse actif — FACT_TRANSACTIONS : {count} lignes")

    conn.close()
    print("\n🎉  Snowflake : tous les tests passés avec succès !")
    return True


if __name__ == "__main__":
    success = test_snowflake_connection()
    exit(0 if success else 1)
