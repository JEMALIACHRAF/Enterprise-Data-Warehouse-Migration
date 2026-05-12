#!/usr/bin/env python3
"""
run_all_tests.py
----------------
Lance tous les tests dans l'ordre recommandé :
  1. Tests unitaires (Spark local, zéro dépendance externe)
  2. Test connexion Oracle
  3. Test connexion Snowflake
  4. Test connexion GCS
  5. Test intégration Spark end-to-end

Usage:
    python run_all_tests.py              # tous les tests
    python run_all_tests.py --unit       # unitaires uniquement
    python run_all_tests.py --conn       # connexions uniquement
    python run_all_tests.py --all        # tout
"""

import argparse
import subprocess
import sys
import os
from datetime import datetime


GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def header(title: str):
    print(f"\n{BOLD}{BLUE}{'='*60}{RESET}")
    print(f"{BOLD}{BLUE}  {title}{RESET}")
    print(f"{BOLD}{BLUE}{'='*60}{RESET}")


def run(label: str, command: list[str]) -> bool:
    print(f"\n{BOLD}▶  {label}{RESET}")
    print(f"   {' '.join(command)}\n")
    result = subprocess.run(command)
    if result.returncode == 0:
        print(f"\n{GREEN}✅  {label} : SUCCÈS{RESET}")
        return True
    else:
        print(f"\n{RED}❌  {label} : ÉCHEC (code {result.returncode}){RESET}")
        return False


def check_env():
    """Vérifie que les variables d'environnement essentielles sont définies."""
    required_for_connections = [
        "ORACLE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_PASSWORD",
        "GCS_BUCKET",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]
    missing = [v for v in required_for_connections if not os.environ.get(v)]
    if missing:
        print(f"{YELLOW}⚠️   Variables d'env manquantes pour les tests de connexion :{RESET}")
        for v in missing:
            print(f"    → {v}")
        print(f"\n{YELLOW}   Lance d'abord : export $(cat .env | xargs){RESET}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Lance tous les tests du pipeline")
    parser.add_argument("--unit",        action="store_true", help="Tests unitaires uniquement")
    parser.add_argument("--conn",        action="store_true", help="Tests de connexion uniquement")
    parser.add_argument("--integration", action="store_true", help="Test intégration Spark uniquement")
    parser.add_argument("--all",         action="store_true", help="Tous les tests (défaut)")
    args = parser.parse_args()

    # Par défaut : tout
    run_unit        = args.unit  or args.all or not any([args.unit, args.conn, args.integration])
    run_conn        = args.conn  or args.all or not any([args.unit, args.conn, args.integration])
    run_integration = args.integration or args.all or not any([args.unit, args.conn, args.integration])

    start   = datetime.utcnow()
    results = {}

    header("ORACLE → GCS → SNOWFLAKE — Suite de tests")
    print(f"  Démarrage : {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # ── 1. Tests unitaires ────────────────────────────────────────────────────
    if run_unit:
        header("1. Tests unitaires (Spark local)")
        results["Unit Tests"] = run(
            "pytest tests/unit/",
            ["python", "-m", "pytest", "tests/unit/", "-v", "--tb=short", "--color=yes"]
        )

    # ── 2. Tests de connexion ─────────────────────────────────────────────────
    if run_conn:
        header("2. Tests de connexion")

        has_env = check_env()

        if has_env:
            results["Oracle"] = run(
                "Connexion Oracle",
                ["python", "tests/connections/test_oracle.py"]
            )
            results["Snowflake"] = run(
                "Connexion Snowflake",
                ["python", "tests/connections/test_snowflake.py"]
            )
            results["GCS"] = run(
                "Connexion GCS",
                ["python", "tests/connections/test_gcs.py"]
            )
        else:
            print(f"{YELLOW}  Tests de connexion ignorés (variables manquantes){RESET}")
            results["Oracle"]    = None
            results["Snowflake"] = None
            results["GCS"]       = None

    # ── 3. Test intégration Spark ─────────────────────────────────────────────
    if run_integration:
        header("3. Test intégration Spark — Oracle → GCS → Snowflake")
        has_env = check_env()
        if has_env:
            results["Spark Integration"] = run(
                "Intégration complète PySpark",
                ["python", "tests/connections/test_spark_integration.py"]
            )
        else:
            print(f"{YELLOW}  Test d'intégration ignoré (variables manquantes){RESET}")
            results["Spark Integration"] = None

    # ── Rapport final ─────────────────────────────────────────────────────────
    duration = (datetime.utcnow() - start).total_seconds()

    header("RAPPORT FINAL")
    print(f"  Durée totale : {duration:.1f}s\n")

    all_passed = True
    for test, passed in results.items():
        if passed is True:
            print(f"  {GREEN}✅  {test}{RESET}")
        elif passed is False:
            print(f"  {RED}❌  {test}{RESET}")
            all_passed = False
        else:
            print(f"  {YELLOW}⏭   {test} (ignoré){RESET}")

    print()
    if all_passed:
        print(f"{BOLD}{GREEN}🎉  Tous les tests passés — prêt pour GitHub !{RESET}")
        print()
        print("  Prochaine étape :")
        print("    git init && git add .")
        print("    git commit -m 'feat: Oracle to Snowflake migration pipeline'")
        print("    git push -u origin main")
        sys.exit(0)
    else:
        print(f"{BOLD}{RED}❌  Certains tests ont échoué — corrige les erreurs avant de push{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
