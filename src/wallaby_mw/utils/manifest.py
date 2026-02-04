from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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


def load_manifest(path: str) -> Optional[Dict[str, Any]]:
    """
    Load a manifest JSON file if it exists; return None if missing.
    """
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def manifest_checksum_ok(manifest: Dict[str, Any], stage: str, filename: str) -> bool:
    """
    Return True if manifest records a successful checksum for filename in stage.
    """
    checksums = (
        manifest
        .get("stages", {})
        .get(stage, {})
        .get("checksums", [])
    )
    for entry in checksums:
        if entry.get("filename") == filename and entry.get("ok") is True:
            return True
    return False
