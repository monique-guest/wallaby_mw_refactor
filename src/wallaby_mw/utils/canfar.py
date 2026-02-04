from __future__ import annotations

import time
import os

from typing import Any, Dict, List, Mapping, Sequence
from datetime import datetime

from canfar.sessions import Session
from canfar.images import Images
from canfar.models.registry import ContainerRegistry
from canfar.models.config import Configuration

def start_session(timeout: int = 180) -> Session:
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
    return Session(timeout=timeout, config=cfg)

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

    try:
        session_ids = session.create(**job_params)
        if not session_ids:
            raise RuntimeError("session.create returned no session IDs")
        return session_ids[0]
    except Exception as e:
        print("\n[submit_test_job] create() failed")
        print("exception type:", type(e).__name__)
        print("exception:", e)
        # Sometimes canfar exceptions carry response fields:
        for attr in ("status_code", "response", "text", "content", "body"):
            if hasattr(e, attr):
                print(f"{attr}:", getattr(e, attr))
        raise
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
    try:
        ids = session.create(
            name=name,
            image=image,
            kind="headless",
            cmd=cmd,
            args=args,
            cores=cores,
            ram=ram,
            env=env if env else None,
        )
    except Exception as e:
        # This is the key: surface the *actual* error from Skaha/canfar
        print("\n[submit_job] EXCEPTION while creating Skaha session")
        print(f"  name={name}")
        print(f"  image={image}")
        print(f"  kind=headless cmd={cmd}")
        print(f"  cores={cores} ram={ram}")
        print(f"  args={args!r}")
        print(f"  env={env}")
        print(f"  exception={type(e).__name__}: {e}")
        raise

    if not ids:
        print("\n[submit_job] Skaha returned no session IDs.")
        print(f"  name={name}")
        print(f"  image={image}")
        print(f"  kind=headless cmd={cmd}")
        print(f"  cores={cores} ram={ram}")
        print(f"  args={args!r}")
        print(f"  env={env}")
        raise RuntimeError(
            "Skaha session creation failed: no session IDs returned."
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
    prev_status = None

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

        status_changed = status != prev_status
        logs_changed = full_text != prev_logs_text

        if logs_changed:
            if full_text.startswith(prev_logs_text):
                new_text = full_text[len(prev_logs_text):]
            else:
                # Logs rotated or truncated; treat whole text as new
                new_text = full_text

            new_lines = new_text.splitlines()
            if len(new_lines) > max_new_lines:
                new_lines = new_lines[-max_new_lines:]

            header = f"[ session {session_id} |{now} | elapsed {elapsed} | status {status} ]"
            print("\n" + header)
            for line in new_lines:
                print(f"{session_id} {elapsed} | {line}")

            prev_logs_text = full_text

        elif status_changed:
            print(f"\n[ session {session_id} |{now} | elapsed {elapsed} | status {status} ]")

        prev_status = status

        if status in terminal_statuses:
            return status
        
        time.sleep(poll_interval)

def poll_sessions(
    session_ids: Sequence[str],
    poll_interval: float = 30.0,
    max_new_lines: int = 20,
    terminal_statuses: tuple[str, ...] = ("Succeeded", "Failed", "Terminated", "Error", "Completed"),
):
    """
    Poll multiple sessions and print updates only on status/log changes.
    Returns a dict of session_id -> final status.
    """
    session = start_session()
    state: Dict[str, Dict[str, Any]] = {}
    for session_id in session_ids:
        state[session_id] = {
            "prev_logs_text": "",
            "prev_status": None,
            "start_time": time.time(),
        }

    def format_elapsed(seconds: float) -> str:
        seconds = int(seconds)
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def get_status(session: Session, session_id: str) -> str:
        info_list = session.info(ids=session_id)
        info = info_list[0] if info_list else {}
        return info.get("status", "unknown")

    final_statuses: Dict[str, str] = {}

    while True:
        all_terminal = True
        for session_id in session_ids:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            elapsed = format_elapsed(time.time() - state[session_id]["start_time"])
            status = get_status(session, session_id)

            try:
                logs_dict = session.logs(ids=session_id) or {}
                full_text = logs_dict.get(session_id, "")
            except Exception as e:
                print(f"[{now} | elapsed {elapsed}] Error fetching logs for {session_id}: {e}")
                time.sleep(poll_interval)
                continue

            status_changed = status != state[session_id]["prev_status"]
            logs_changed = full_text != state[session_id]["prev_logs_text"]

            if logs_changed:
                if full_text.startswith(state[session_id]["prev_logs_text"]):
                    new_text = full_text[len(state[session_id]["prev_logs_text"]):]
                else:
                    # Logs rotated or truncated; treat whole text as new
                    new_text = full_text

                new_lines = new_text.splitlines()
                if len(new_lines) > max_new_lines:
                    new_lines = new_lines[-max_new_lines:]

                header = f"[{now} | elapsed {elapsed} | session {session_id} | status {status}]"
                print("\n" + header)
                for line in new_lines:
                    print(f"{session_id} {elapsed} | {line}")

                state[session_id]["prev_logs_text"] = full_text

            elif status_changed:
                print(f"\n[{now} | elapsed {elapsed} | session {session_id} | status {status}]")

            state[session_id]["prev_status"] = status

            if status in terminal_statuses:
                final_statuses[session_id] = status
            else:
                all_terminal = False

        if all_terminal:
            return final_statuses

        time.sleep(poll_interval)
