"""
test_oracle.py
--------------
Teste la connexion réelle à Oracle (local Docker ou distant).
Vérifie : JDBC, tables sources, données de test.

Usage:
    python tests/connections/test_oracle.py
"""

import os
import sys
from pathlib import Path

# Charge .env depuis la racine du projet
try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

import subprocess
import sys

import jaydebeapi

JDBC_DRIVER_PATH = "drivers/ojdbc8.jar"
JDBC_DRIVER_CLASS = "oracle.jdbc.OracleDriver"


def check_driver():
    if not os.path.exists(JDBC_DRIVER_PATH):
        print(f"❌  Driver JDBC introuvable : {JDBC_DRIVER_PATH}")
        print("    → Télécharge-le :")
        print("      mkdir -p drivers && curl -L -o drivers/ojdbc8.jar \\")
        print(
            '        "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar"'
        )
        return False
    print(f"✅  Driver JDBC trouvé : {JDBC_DRIVER_PATH}")
    return True


def check_docker_oracle():
    """Vérifie si le conteneur Oracle Docker tourne localement."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", "oracle-dev"],
            capture_output=True,
            text=True,
        )
        status = result.stdout.strip()
        if status == "running":
            print("✅  Conteneur Docker 'oracle-dev' en cours d'exécution")
            return True
        else:
            print(f"⚠️   Conteneur Docker 'oracle-dev' status : {status}")
            return False
    except FileNotFoundError:
        print("⚠️   Docker non installé ou conteneur non trouvé — tentative de connexion directe")
        return False


def get_connection():
    host = os.environ.get("ORACLE_HOST", "localhost")
    port = os.environ.get("ORACLE_PORT", "1521")
    service = os.environ.get("ORACLE_SERVICE", "XEPDB1")
    user = os.environ.get("ORACLE_USER", "migration_reader")
    password = os.environ["ORACLE_PASSWORD"]

    jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
    print(f"  → URL JDBC : {jdbc_url}")

    return jaydebeapi.connect(JDBC_DRIVER_CLASS, jdbc_url, [user, password], JDBC_DRIVER_PATH)


def test_oracle_connection():
    print("\n" + "=" * 60)
    print("  TEST CONNEXION — Oracle")
    print("=" * 60)

    if not os.environ.get("ORACLE_PASSWORD"):
        print("❌  ORACLE_PASSWORD non défini dans .env")
        return False

    if not check_driver():
        return False

    check_docker_oracle()

    # ── 1. Connexion ─────────────────────────────────────────────
    try:
        conn = get_connection()
        cur = conn.cursor()
        print("✅  Connexion JDBC Oracle établie")
    except Exception as e:
        print(f"❌  Connexion échouée : {e}")
        print("\n  Debug — vérifie que Oracle tourne :")
        print("    docker logs oracle-dev | tail -5")
        print("    docker ps | grep oracle")
        return False

    # ── 2. Version Oracle ─────────────────────────────────────────
    cur.execute("SELECT * FROM V$VERSION WHERE BANNER LIKE 'Oracle%'")
    row = cur.fetchone()
    if row:
        print(f"✅  Version Oracle : {row[0]}")

    # ── 3. Vérifier les tables sources ────────────────────────────
    expected_tables = ["LKP_CONTINENT", "LKP_COUNTRY", "LKP_REGION", "LKP_CITY", "FACT_TXN"]
    cur.execute("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")
    existing = {row[0] for row in cur.fetchall()}

    print(f"\n  Tables Oracle présentes ({len(existing)}) :")
    all_present = True
    for t in expected_tables:
        found = t in existing
        status = "✅" if found else "❌"
        print(f"    {status}  {t}")
        if not found:
            all_present = False

    if not all_present:
        print("\n  ⚠️  Tables manquantes → exécute le SQL de setup :")
        print(
            "    docker exec -it oracle-dev sqlplus migration_reader/ReaderPass#2024@//localhost:1521/XEPDB1"
        )
        print("    (puis colle le contenu de scripts/oracle_setup.sql)")

    # ── 4. Vérifier les données de test ───────────────────────────
    if "FACT_TXN" in existing:
        cur.execute("SELECT COUNT(*) FROM FACT_TXN")
        count = cur.fetchone()[0]
        print(f"\n✅  FACT_TXN contient {count} ligne(s)")

        if count == 0:
            print("  ⚠️  Aucune donnée — exécute les INSERT dans scripts/oracle_setup.sql")

        cur.execute("SELECT TXN_ID, TXN_TYPE_CD, NET_AMT, CURRENCY_CD FROM FACT_TXN")
        rows = cur.fetchall()
        print("  Aperçu des transactions :")
        for row in rows:
            print(f"    → {row[0]} | {row[1]} | {row[2]:>12.2f} {row[3]}")

    if "LKP_CONTINENT" in existing:
        cur.execute("SELECT COUNT(*) FROM LKP_CONTINENT")
        count = cur.fetchone()[0]
        print(f"✅  LKP_CONTINENT : {count} continents")

    # ── 5. Test performance / partitionnement ─────────────────────
    cur.execute(
        """
        SELECT ORA_HASH(ROWID, 20) AS PARTITION_ID, COUNT(*) AS CNT
        FROM FACT_TXN
        GROUP BY ORA_HASH(ROWID, 20)
        ORDER BY 1
    """
    )
    partitions = cur.fetchall()
    print(f"\n✅  Partitionnement ROWID simulé : {len(partitions)} partition(s)")
    for p in partitions:
        print(f"    Partition {p[0]} → {p[1]} ligne(s)")

    conn.close()
    print("\n🎉  Oracle : tous les tests passés avec succès !")
    return True


if __name__ == "__main__":
    try:
        import jaydebeapi
    except ImportError:
        print("❌  jaydebeapi non installé → pip install jaydebeapi JPype1")
        sys.exit(1)

    success = test_oracle_connection()
    exit(0 if success else 1)
