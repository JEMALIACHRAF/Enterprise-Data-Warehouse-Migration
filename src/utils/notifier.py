"""
notifier.py
-----------
Envoie des notifications Slack à la fin du pipeline (succès ou échec).
Si SLACK_WEBHOOK_URL n'est pas défini, les notifications sont ignorées silencieusement.
"""

import json
import logging
import os
from datetime import date
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class PipelineNotifier:

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
        if not self.webhook_url:
            logger.info("SLACK_WEBHOOK_URL non défini — notifications désactivées")

    def _send(self, payload: dict) -> None:
        if not self.webhook_url:
            return
        try:
            resp = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Notification Slack échouée (non bloquant) : {e}")

    def send_success(
        self,
        batch_id: str,
        batch_date: date,
        duration_seconds: float,
        rows_loaded: int,
        rows_rejected: int,
    ) -> None:
        duration_min = duration_seconds / 60
        self._send({
            "text": (
                f":white_check_mark: *Pipeline terminé avec succès*\n"
                f">*Batch* : `{batch_id}`\n"
                f">*Date*  : `{batch_date}`\n"
                f">*Durée* : `{duration_min:.1f} min`\n"
                f">*Lignes chargées* : `{rows_loaded:,}`\n"
                f">*Lignes rejetées* : `{rows_rejected:,}`"
            )
        })

    def send_failure(self, batch_id: str, error: str) -> None:
        self._send({
            "text": (
                f":x: *Pipeline ÉCHOUÉ*\n"
                f">*Batch* : `{batch_id}`\n"
                f">*Erreur* : ```{error[:500]}```"
            )
        })
