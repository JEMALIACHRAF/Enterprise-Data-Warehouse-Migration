"""
config.py — Pipeline configuration loader
retry.py  — Retry decorator with exponential backoff
"""

# ─── config.py ───────────────────────────────────────────────────────────────

import functools
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class OracleConfig:
    host: str
    port: int
    service_name: str
    user: str
    password: str


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    database: str
    schema: str
    warehouse: str
    role: str
    private_key_pem: str  # Injected from GCP Secret Manager at runtime


@dataclass
class PipelineConfig:
    env: str
    gcs_bucket: str
    oracle: OracleConfig
    snowflake: SnowflakeConfig
    slack_webhook_url: Optional[str] = None


def load_config(env: str) -> PipelineConfig:
    """
    Load pipeline config from YAML + override with env vars.
    Secrets (passwords, keys) are always sourced from environment variables
    or GCP Secret Manager — never from the config file.
    """
    config_path = Path(__file__).parents[2] / "config" / "pipeline_config.yaml"

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    env_cfg = raw["environments"][env]

    oracle_cfg = OracleConfig(
        host=env_cfg["oracle"]["host"],
        port=int(env_cfg["oracle"].get("port", 1521)),
        service_name=env_cfg["oracle"]["service_name"],
        user=env_cfg["oracle"]["user"],
        # Secret: sourced exclusively from env var
        password=os.environ["ORACLE_PASSWORD"],
    )

    snowflake_cfg = SnowflakeConfig(
        account=env_cfg["snowflake"]["account"],
        user=env_cfg["snowflake"]["user"],
        database=env_cfg["snowflake"]["database"],
        schema=env_cfg["snowflake"]["schema"],
        warehouse=env_cfg["snowflake"]["warehouse"],
        role=env_cfg["snowflake"]["role"],
        # Private key injected from GCP Secret Manager by the Kubernetes pod
        private_key_pem=os.environ["SNOWFLAKE_PRIVATE_KEY_PEM"],
    )

    return PipelineConfig(
        env=env,
        gcs_bucket=env_cfg["gcs_bucket"],
        oracle=oracle_cfg,
        snowflake=snowflake_cfg,
        slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL"),
    )


# ─── retry.py ────────────────────────────────────────────────────────────────

def retry_with_backoff(max_retries: int = 3, base_delay: float = 10.0, backoff: float = 2.0):
    """
    Decorator: retries the decorated function up to `max_retries` times
    using exponential backoff on any Exception.

    Args:
        max_retries: Maximum number of attempts (total = 1 + max_retries)
        base_delay:  Initial wait in seconds before first retry
        backoff:     Multiplier applied to delay after each failure

    Example:
        @retry_with_backoff(max_retries=3, base_delay=30)
        def my_flaky_function(): ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exc = None
            for attempt in range(1, max_retries + 2):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt > max_retries:
                        logger.error(
                            f"[retry] {func.__name__} failed after {attempt} attempts. Giving up."
                        )
                        raise
                    logger.warning(
                        f"[retry] {func.__name__} attempt {attempt}/{max_retries} failed: {exc}. "
                        f"Retrying in {delay:.0f}s..."
                    )
                    time.sleep(delay)
                    delay *= backoff
            raise last_exc
        return wrapper
    return decorator
