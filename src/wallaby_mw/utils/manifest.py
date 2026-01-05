from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

def utc_now_iso() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_manifest(path: str, manifest: Dict[str, Any]) -> None:
    """
    Write a manifest JSON file, creating parent directories if needed.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
