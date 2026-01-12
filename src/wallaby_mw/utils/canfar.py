from __future__ import annotations

import time
import os

from typing import Any, Dict, List, Mapping, Sequence
from datetime import datetime

from canfar.sessions import Session
from canfar.images import Images
from canfar.models.registry import ContainerRegistry
from canfar.models.config import Configuration

def start_session() -> Session:
    """Build a Session using Harbor creds from env vars."""
    user = os.environ.get("CANFAR_REGISTRY_USERNAME")
    secret = os.environ.get("CANFAR_REGISTRY_SECRET")

    if not user or not secret:
        raise RuntimeError(
            "Missing Harbor creds; set CANFAR_REGISTRY_USERNAME and CANFAR_REGISTRY_SECRET"
        )

    cfg = Configuration(
        registry=ContainerRegistry(username=user, secret=secret)
    )
    return Session(config=cfg)

def list_container_images() -> list[dict]:
    images_client = Images()
    return images_client.fetch()

def describe_first_n_images(n: int = 5) -> None:
    images = list_container_images()
    print(f"Found {len(images)} images total")
    for img in images[:n]:
        print(img)

def submit_test_job() -> str:
    session = Session()

    image = "images.canfar.net/skaha/astroml:latest" 

    job_params = {
        "name": "test-hello",
        "image": image,
        "kind": "headless",
        "cmd": "bash",
        "args": "-c 'echo hello from Skaha'",
        "cores": 1,
        "ram": 1,
        "env": {},
    }

    session_ids = session.create(**job_params)
    return session_ids[0]

def get_session_by_id(session_obj, session_id):
    """Return a single session dict by ID using fetch()."""
    sessions = session_obj.fetch()
    for s in sessions:
        if s["id"] == session_id:
            return s
    return None

def wait_for_session(
    session: Session,
    session_id: str,
    poll_interval: float = 30.0,
    terminal_statuses: tuple[str, ...] = ("Succeeded", "Failed", "Terminated", "Error", "Completed"),
) -> Dict:
    """
    Poll a single session until it reaches a terminal status.
    Returns the final session info dict.
    """
    while True:
        info_list = session.info(ids=session_id)
        if not info_list:
            print(f"[{session_id}] no info returned yet")
            time.sleep(poll_interval)
            continue

        info = info_list[0]
        status = info.get("status", "unknown")
        print(f"[{session_id}] status = {status}")

        if status in terminal_statuses:
            return info

        time.sleep(poll_interval)

def fetch_session_logs(session: Session, session_id: str) -> str:
    """
    Return the logs for a (finished) Skaha session as a string.
    """
    logs_dict = session.logs(ids=session_id) or {}
    return logs_dict.get(session_id, "")

def submit_job(
    *,
    session: Session,
    name: str,
    image: str,
    cmd: str,
    args: str,
    cores: int = 2,
    ram: int = 8,
    env: Mapping[str, str] | None = None,
) -> str:
    """Submit a headless Skaha job and return the session ID."""
    ids: Sequence[str] = session.create(
        name=name,
        image=image,
        kind="headless",
        cmd=cmd,
        args=args,
        cores=cores,
        ram=ram,
        env=dict(env or {}),
    )
    return ids[0]

def live_logs(
    session: Session,
    session_id: str,
    poll_interval: float = 30.0,
    max_new_lines: int = 20,
    terminal_statuses: tuple[str, ...] = ("Succeeded", "Failed", "Terminated", "Error", "Completed"),
    ):

    start_time = time.time()
    prev_logs_text = ""  # full logs from previous poll

    def format_elapsed(seconds: float) -> str:
        seconds = int(seconds)
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    
    def get_status(
        session: Session,
        session_id: str,
        ):
        info_list = session.info(ids=session_id)
        info = info_list[0]
        status = info.get("status", "unknown")
        return status

    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed = format_elapsed(time.time() - start_time)
        status = get_status(session, session_id)

        try:
            logs_dict = session.logs(ids=session_id) or {}
            full_text = logs_dict.get(session_id, "")
        except Exception as e:
            print(f"[{now} | elapsed {elapsed}] Error fetching logs: {e}")
            time.sleep(poll_interval)
            continue

        print("\n" + "=" * 20 + f" {now} | elapsed {elapsed} | session {session_id} | status {status} " + "=" * 20)

        if not full_text:
            print("No logs returned yet (job may still be initializing or has no output).")
        else:
            # Tail mode: only print NEW content since last poll
            if full_text == prev_logs_text:
                print("No new log output since last poll.")
            else:
                if full_text.startswith(prev_logs_text):
                    new_text = full_text[len(prev_logs_text):]
                else:
                    # Logs rotated or truncated; treat whole text as new
                    new_text = full_text

                new_lines = new_text.splitlines()

                if not new_lines:
                    print("No new log lines found.")
                else:
                    # Only show the last max_new_lines new lines
                    if len(new_lines) > max_new_lines:
                        new_lines = new_lines[-max_new_lines:]

                    print("\n".join(new_lines))

                prev_logs_text = full_text

        if status in terminal_statuses:
            return status
        
        time.sleep(poll_interval)