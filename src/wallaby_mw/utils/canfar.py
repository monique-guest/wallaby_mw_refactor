from __future__ import annotations

import time

from typing import Any, Dict, List

from canfar.sessions import Session
from canfar.images import Images

def get_session(*, timeout: int = 120, loglevel: int = 30) -> Session:
    return Session(timeout=timeout, loglevel=loglevel) 

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
    session_id: str,
    poll_interval: float = 5.0,
    terminal_statuses: tuple[str, ...] = ("Succeeded", "Failed", "Terminated"),
) -> dict:
    """
    Poll a single session until it reaches a terminal status.
    Returns the final session info dict.
    """
    session = Session()

    while True:
        info_list = session.info(ids=session_id)  # returns a list
        if not info_list:
            print(f"[{session_id}] no info returned yet")
            time.sleep(poll_interval)
            continue

        info = info_list[0]
        status = info.get("status", "unknown")  # <- this is the real field name
        print(f"[{session_id}] status = {status}")

        if status in terminal_statuses:
            return info

        time.sleep(poll_interval)

def fetch_session_logs(session_id: str) -> str:
    """
    Return the logs for a completed Skaha session as a string.
    """
    session = Session()
    logs_dict = session.logs(ids=session_id) or {}
    return logs_dict.get(session_id, "")