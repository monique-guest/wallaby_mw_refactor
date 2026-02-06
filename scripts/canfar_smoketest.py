#!/usr/bin/env python3
"""
CANFAR / Skaha smoke test: submit a tiny headless job and poll until it finishes.

Put this file at:
  wallaby_mw_refactor/scripts/canfar_smoketest.py

Examples:

  # 1) Basic sanity check with known public image, smallest resources
  python scripts/canfar_smoketest.py \
    --image images.canfar.net/skaha/astroml:latest \
    --cores 1 --ram 1 \
    --poll-timeout 600

  # 2) Compare public image vs your custom image
  python scripts/canfar_smoketest.py \
    --image images.canfar.net/skaha/astroml:latest \
    --image images.canfar.net/<your_project>/<your_image>:<tag> \
    --cores 1 --ram 1 \
    --poll-timeout 600

  # 3) Try multiple sizes (cross-product with images)
  python scripts/canfar_smoketest.py \
    --image images.canfar.net/skaha/astroml:latest \
    --cores 1 --ram 1 \
    --cores 1 --ram 4 \
    --poll-timeout 600

Notes:
- Uses Harbor creds from env vars:
    CANFAR_REGISTRY_USERNAME
    CANFAR_REGISTRY_SECRET
- Keeps args formatting "python -m ... --flag value" (no shell quoting).
- Polling tries several method names because client libs vary.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ---- Import your CANFAR/Skaha client types here ----
# Adjust these imports if your environment uses different module paths.
try:
    from canfar.sessions import Session
    from canfar.images import Images
    from canfar.models.registry import ContainerRegistry
    from canfar.models.config import Configuration
except Exception:
    Session = None  # type: ignore
    Configuration = None  # type: ignore
    ContainerRegistry = None  # type: ignore


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def dump_exception(e: Exception, prefix: str = "") -> None:
    print(f"{prefix}exception type: {type(e).__name__}")
    print(f"{prefix}exception: {e}")
    for attr in ("status_code", "response", "text", "content", "body"):
        if hasattr(e, attr):
            val = getattr(e, attr)
            if isinstance(val, (bytes, bytearray)) and len(val) > 2000:
                print(f"{prefix}{attr}: <{len(val)} bytes>")
            else:
                print(f"{prefix}{attr}: {val}")


def require_client_imports() -> None:
    if Session is None or Configuration is None or ContainerRegistry is None:
        raise RuntimeError(
            "Could not import CANFAR/Skaha client classes.\n"
            "Update the imports at the top of this file to match your environment."
        )


def build_session(timeout: int) -> "Session":
    """Build a Session using Harbor creds from env vars."""
    user = os.environ.get("CANFAR_REGISTRY_USERNAME")
    secret = os.environ.get("CANFAR_REGISTRY_SECRET")

    if not user or not secret:
        raise RuntimeError(
            "Missing Harbor creds; set CANFAR_REGISTRY_USERNAME and CANFAR_REGISTRY_SECRET"
        )

    cfg = Configuration(registry=ContainerRegistry(username=user, secret=secret))
    return Session(timeout=timeout, config=cfg)


@dataclass(frozen=True)
class Variant:
    image: str
    cores: int
    ram: int  # GB
    kind: str = "headless"


def make_job_params(name: str, variant: Variant, smoke_mode: str) -> Dict[str, Any]:
    """
    smoke_mode:
      - "env": cmd=env (simplest; avoids args edge cases)
      - "py":  cmd=python args="-c <script>"
    """
    if smoke_mode == "env":
        # simplest possible command - avoids arg parsing issues entirely
        cmd = "env"
        args = None
    else:
        # Keep args formatting like you said: a plain space-separated string.
        # Use python -c to print a few basics and exit.
        cmd = "python"
        args = (
            "-c "
            "import os,platform,socket,datetime; "
            "print('hello from Skaha'); "
            "print('host', socket.gethostname()); "
            "print('utc', datetime.datetime.utcnow().isoformat()); "
            "print('python', platform.python_version()); "
            "print('pwd', os.getcwd()); "
            "print('done')"
        )

    params: Dict[str, Any] = {
        "name": name,
        "image": variant.image,
        "kind": variant.kind,
        "cmd": cmd,
        "cores": int(variant.cores),
        "ram": int(variant.ram),
        "env": {},
    }
    if args is not None:
        params["args"] = args
    return params


# -------------------------- Skaha API wrappers --------------------------


def session_create(session: "Session", job_params: Dict[str, Any]) -> str:
    try:
        session_ids = session.create(**job_params)
        if not session_ids:
            raise RuntimeError("session.create returned no session IDs")
        if isinstance(session_ids, (list, tuple)):
            return str(session_ids[0])
        return str(session_ids)
    except Exception as e:
        print("\n[create] create() failed")
        dump_exception(e, prefix="  ")
        raise


def session_get(session: "Session", session_id: str) -> Any:
    for m in ("get", "read", "info", "describe"):
        if hasattr(session, m):
            try:
                return getattr(session, m)(session_id)
            except Exception:
                pass
    for m in ("session", "sessions"):
        if hasattr(session, m):
            try:
                return getattr(session, m)(session_id)
            except Exception:
                pass
    raise RuntimeError("Could not find a usable session info method on Session()")


def extract_status(info: Any) -> Tuple[Optional[str], Dict[str, Any]]:
    fields: Dict[str, Any] = {}
    status = None

    if isinstance(info, dict):
        fields = info
        for key in ("phase", "status", "state"):
            if key in info and info[key] is not None:
                status = str(info[key])
                break
        return status, fields

    for key in ("phase", "status", "state"):
        if hasattr(info, key):
            val = getattr(info, key)
            if val is not None:
                status = str(val)
                break

    for key in ("message", "reason", "node", "created", "started", "finished"):
        if hasattr(info, key):
            fields[key] = getattr(info, key)

    if hasattr(info, "__dict__"):
        try:
            fields.update({k: v for k, v in info.__dict__.items() if k not in fields})
        except Exception:
            pass

    return status, fields


def session_logs(session: "Session", session_id: str) -> Optional[str]:
    for m in ("logs", "log", "get_logs", "read_logs"):
        if hasattr(session, m):
            try:
                out = getattr(session, m)(session_id)
                if out is None:
                    return None
                return out if isinstance(out, str) else str(out)
            except Exception:
                pass
    return None


def session_delete(session: "Session", session_id: str) -> bool:
    for m in ("delete", "remove", "terminate", "stop"):
        if hasattr(session, m):
            try:
                getattr(session, m)(session_id)
                return True
            except Exception:
                return False
    return False


TERMINAL_OK = {"Succeeded", "SUCCEEDED", "Completed", "COMPLETED", "complete", "done"}
TERMINAL_FAIL = {
    "Failed",
    "FAILED",
    "Error",
    "ERROR",
    "Cancelled",
    "CANCELLED",
    "Terminated",
    "TERMINATED",
}


def summarise(fields: Dict[str, Any]) -> str:
    parts = []
    for k in ("reason", "message", "node"):
        if k in fields and fields[k]:
            parts.append(f"{k}={fields[k]}")
    return " ".join(parts)


def _compact(d: Dict[str, Any], max_len: int = 500) -> str:
    s = str(d)
    return s if len(s) <= max_len else (s[:max_len] + "...<truncated>")


def poll_until_done(
    session: "Session",
    session_id: str,
    poll_interval: int,
    poll_timeout: int,
    verbose: bool,
) -> Tuple[str, Any]:
    start = time.time()
    last_status = None

    while True:
        elapsed = int(time.time() - start)
        if elapsed > poll_timeout:
            try:
                info = session_get(session, session_id)
            except Exception as e:
                raise RuntimeError(
                    f"Timed out after {poll_timeout}s; also failed to fetch session info: {e}"
                )

            status, fields = extract_status(info)
            raise RuntimeError(
                f"Timed out after {poll_timeout}s waiting for session {session_id}. "
                f"Last status={status}. Fields snapshot={_compact(fields)}"
            )

        info = session_get(session, session_id)
        status, fields = extract_status(info)

        if status != last_status or verbose:
            print(
                f"[{_now()}] session={session_id} status={status} elapsed={elapsed}s {summarise(fields)}"
            )
            last_status = status

        if status in TERMINAL_OK:
            return status, info
        if status in TERMINAL_FAIL:
            return status, info

        time.sleep(poll_interval)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CANFAR/Skaha smoke test: submit + poll a tiny job."
    )
    p.add_argument(
        "--image",
        action="append",
        required=True,
        help="Container image ref (repeatable).",
    )
    # IMPORTANT: default=None so we don't accidentally duplicate values when user passes --cores/--ram
    p.add_argument(
        "--cores",
        action="append",
        type=int,
        default=None,
        help="Cores (repeatable). If omitted, defaults to 1.",
    )
    p.add_argument(
        "--ram",
        action="append",
        type=int,
        default=None,
        help="RAM in GB (repeatable). If omitted, defaults to 1.",
    )
    p.add_argument("--kind", default="headless", help="Skaha kind (default: headless).")
    p.add_argument(
        "--smoke-mode",
        choices=("env", "py"),
        default="py",
        help="Command style: 'py' uses cmd=python args='-c ...' (default). "
        "'env' uses cmd=env with no args (most minimal).",
    )

    p.add_argument(
        "--timeout", type=int, default=180, help="Session() client timeout seconds."
    )
    p.add_argument("--poll-interval", type=int, default=5, help="Poll interval seconds.")
    p.add_argument(
        "--poll-timeout",
        type=int,
        default=600,
        help="Poll timeout seconds (fail if still not done).",
    )

    p.add_argument("--no-cleanup", action="store_true", help="Do not delete session after run.")
    p.add_argument("--verbose", action="store_true", help="Print every poll, not just status changes.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    require_client_imports()

    cores_list = args.cores or [1]
    ram_list = args.ram or [1]

    variants: List[Variant] = []
    for img in args.image:
        for c in cores_list:
            for r in ram_list:
                variants.append(Variant(image=img, cores=c, ram=r, kind=args.kind))

    print(f"[{_now()}] variants to test: {len(variants)} (smoke-mode={args.smoke_mode})")
    for v in variants:
        print(f"  - image={v.image} cores={v.cores} ram={v.ram} kind={v.kind}")

    session = build_session(timeout=args.timeout)
    overall_ok = True

    for i, v in enumerate(variants, start=1):
        test_id = uuid.uuid4().hex[:8]
        name = f"smoke-{test_id}-{i}"
        print("\n" + "=" * 80)
        print(
            f"[{_now()}] RUN {i}/{len(variants)}: name={name} image={v.image} cores={v.cores} ram={v.ram}"
        )

        job_params = make_job_params(name=name, variant=v, smoke_mode=args.smoke_mode)

        session_id: Optional[str] = None
        try:
            session_id = session_create(session, job_params)
            print(f"[{_now()}] created session_id={session_id}")

            status, info = poll_until_done(
                session=session,
                session_id=session_id,
                poll_interval=args.poll_interval,
                poll_timeout=args.poll_timeout,
                verbose=args.verbose,
            )

            if status in TERMINAL_OK:
                print(f"[{_now()}] ✅ SUCCESS: {session_id} status={status}")
                logs = session_logs(session, session_id)
                if logs:
                    print("----- logs (tail) -----")
                    print("\n".join(logs.splitlines()[-80:]))
                else:
                    print("[logs] no logs available (or logs method not present).")
            else:
                overall_ok = False
                print(f"[{_now()}] ❌ FAILURE: {session_id} status={status}")
                logs = session_logs(session, session_id)
                if logs:
                    print("----- logs (tail) -----")
                    print("\n".join(logs.splitlines()[-120:]))
                else:
                    print("[logs] no logs available (or logs method not present).")

                st, fields = extract_status(info)
                print(f"[debug] status={st} fields={_compact(fields, max_len=2000)}")

        except Exception as e:
            overall_ok = False
            print(f"[{_now()}] ❌ EXCEPTION during run: image={v.image} cores={v.cores} ram={v.ram}")
            dump_exception(e, prefix="  ")

        finally:
            if session_id and not args.no_cleanup:
                ok = session_delete(session, session_id)
                print(f"[{_now()}] cleanup delete({session_id}) -> {ok}")

    print("\n" + "=" * 80)
    if overall_ok:
        print(f"[{_now()}] ALL SMOKE TESTS PASSED ✅")
        return 0
    else:
        print(f"[{_now()}] SOME SMOKE TESTS FAILED ❌ (see output above)")
        return 2


if __name__ == "__main__":
    sys.exit(main())
