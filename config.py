"""
Collector configuration.

Reads from environment variables (with COLLECTOR_ prefix) or a JSON config file.
"""
from __future__ import annotations

import json
from pathlib import Path
from dataclasses import dataclass


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class CollectorConfig:
    # Binance credentials (loaded from config/secrets.json or collector.json)
    api_key: str = ""
    api_secret: str = ""

    # Paths
    db_root: str = str(_PROJECT_ROOT / "db_files")

    # Internal WS server (for chart-ui-server connection)
    ws_host: str = "localhost"
    ws_port: int = 8001

    # Collection parameters
    account_sync_interval_min: int = 60
    symbol_inactive_prune_days: int = 7

    # When False, skip aggTrades REST backfill entirely (no historical
    # trade scroll-back on the chart). Account sync (orders/fills) still runs.
    # Set via collector.json: {"trades_enabled": false}
    trades_enabled: bool = True


def load_config(path: str | None = None) -> CollectorConfig:
    """
    Load config from a JSON file.

    Precedence:
    1. JSON file at `path` (explicit argument)
    2. config/collector.json in the project root
    3. Credentials from config/secrets.json if not set in the config file
    """
    cfg = CollectorConfig()

    config_path = Path(path) if path else _PROJECT_ROOT / "config" / "collector.json"
    if config_path.exists():
        with open(config_path) as f:
            data = json.load(f)
        for k, v in data.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)

    if not cfg.api_key:
        secrets_path = _PROJECT_ROOT / "ShotCatcherWorker" / "config" / "secrets.json"
        if secrets_path.exists():
            with open(secrets_path) as f:
                creds = json.load(f).get("credentials", [{}])[0]
            cfg.api_key = creds.get("api_key", "")
            cfg.api_secret = creds.get("api_secret", "")

    return cfg
