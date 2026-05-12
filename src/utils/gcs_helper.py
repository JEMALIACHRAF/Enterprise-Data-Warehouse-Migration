"""
gcs_helper.py
-------------
Helper pour les opérations Google Cloud Storage.
Utilisé par l'extracteur Oracle pour écrire les fichiers Parquet
dans la landing zone GCS.
"""

import logging
from pathlib import Path

from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSHelper:
    """Wrapper autour du client GCS pour les opérations courantes du pipeline."""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        logger.info(f"GCSHelper initialisé — bucket: gs://{bucket_name}")

    # ── Upload ────────────────────────────────────────────────────────────────

    def upload_file(self, local_path: str, gcs_path: str) -> str:
        """Upload un fichier local vers GCS. Retourne l'URI gs://..."""
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        uri = f"gs://{self.bucket_name}/{gcs_path}"
        logger.info(f"Uploaded: {local_path} → {uri}")
        return uri

    def upload_string(self, content: str, gcs_path: str, content_type: str = "text/plain") -> str:
        """Upload du contenu texte directement vers GCS."""
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_string(content, content_type=content_type)
        uri = f"gs://{self.bucket_name}/{gcs_path}"
        logger.info(f"Uploaded string → {uri}")
        return uri

    # ── Download ──────────────────────────────────────────────────────────────

    def download_file(self, gcs_path: str, local_path: str) -> None:
        """Télécharge un fichier GCS vers un chemin local."""
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        blob = self.bucket.blob(gcs_path)
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded: gs://{self.bucket_name}/{gcs_path} → {local_path}")

    def download_string(self, gcs_path: str) -> str:
        """Télécharge et retourne le contenu d'un fichier GCS en string."""
        blob = self.bucket.blob(gcs_path)
        return blob.download_as_text()

    # ── List ──────────────────────────────────────────────────────────────────

    def list_blobs(self, prefix: str = "") -> list[str]:
        """Liste les objets GCS avec un préfixe donné."""
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [b.name for b in blobs]

    def exists(self, gcs_path: str) -> bool:
        """Vérifie si un objet GCS existe."""
        blob = self.bucket.blob(gcs_path)
        return blob.exists()

    # ── Delete ────────────────────────────────────────────────────────────────

    def delete(self, gcs_path: str) -> None:
        """Supprime un objet GCS."""
        blob = self.bucket.blob(gcs_path)
        blob.delete()
        logger.info(f"Deleted: gs://{self.bucket_name}/{gcs_path}")

    def delete_prefix(self, prefix: str) -> int:
        """Supprime tous les objets GCS avec un préfixe donné."""
        blobs = list(self.client.list_blobs(self.bucket_name, prefix=prefix))
        for blob in blobs:
            blob.delete()
        logger.info(f"Deleted {len(blobs)} objects with prefix: {prefix}")
        return len(blobs)

    # ── Metadata ──────────────────────────────────────────────────────────────

    def get_size_bytes(self, prefix: str) -> int:
        """Retourne la taille totale (bytes) de tous les objets sous un préfixe."""
        total = 0
        for blob in self.client.list_blobs(self.bucket_name, prefix=prefix):
            total += blob.size or 0
        return total

    def get_size_human(self, prefix: str) -> str:
        """Retourne la taille totale en format lisible (KB, MB, GB)."""
        size = self.get_size_bytes(prefix)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"
