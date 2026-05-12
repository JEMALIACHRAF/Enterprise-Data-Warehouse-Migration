"""
test_gcs.py
-----------
Teste la connexion réelle à Google Cloud Storage.
Vérifie : authentification, lecture, écriture, listage.

Usage:
    python tests/connections/test_gcs.py
"""

import os
from pathlib import Path

# Charge .env depuis la racine du projet
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

import json
from datetime import datetime

from google.cloud import storage
from google.cloud.exceptions import NotFound


def test_gcs_connection():
    print("\n" + "="*60)
    print("  TEST CONNEXION — Google Cloud Storage")
    print("="*60)

    # ── 1. Vérification credentials ──────────────────────────────
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        print("❌  GOOGLE_APPLICATION_CREDENTIALS non défini.")
        print("    → export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/spark-migration-key.json")
        return False

    print(f"✅  Credentials trouvés : {creds_path}")

    bucket_name = os.environ.get("GCS_BUCKET", "ton-bucket-migration-dev")

    # ── 2. Connexion client ───────────────────────────────────────
    try:
        client = storage.Client()
        print(f"✅  Client GCS initialisé (projet: {client.project})")
    except Exception as e:
        print(f"❌  Impossible d'initialiser le client GCS : {e}")
        return False

    # ── 3. Vérifier que le bucket existe ─────────────────────────
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"✅  Bucket trouvé : gs://{bucket_name}")
    except NotFound:
        print(f"❌  Bucket introuvable : gs://{bucket_name}")
        print(f"    → gcloud storage buckets create gs://{bucket_name} --location=europe-west1")
        return False

    # ── 4. Écriture d'un fichier test ─────────────────────────────
    test_content = json.dumps({
        "test": True,
        "timestamp": datetime.utcnow().isoformat(),
        "message": "oracle-snowflake-migration local test"
    })
    blob_name = "tests/connection_test.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(test_content, content_type="application/json")
    print(f"✅  Écriture OK : gs://{bucket_name}/{blob_name}")

    # ── 5. Lecture du fichier ─────────────────────────────────────
    downloaded = blob.download_as_text()
    assert downloaded == test_content, "Contenu lu ≠ contenu écrit !"
    print("✅  Lecture OK : contenu identique")

    # ── 6. Listage des objets ─────────────────────────────────────
    blobs = list(client.list_blobs(bucket_name, prefix="tests/"))
    print(f"✅  Listage OK : {len(blobs)} objet(s) dans gs://{bucket_name}/tests/")

    # ── 7. Nettoyage ──────────────────────────────────────────────
    blob.delete()
    print("✅  Suppression OK : fichier test nettoyé")

    print("\n🎉  GCS : tous les tests passés avec succès !")
    return True


if __name__ == "__main__":
    success = test_gcs_connection()
    exit(0 if success else 1)
