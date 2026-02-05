from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from wallaby_mw.utils.ssh import ssh_run, SSHError


@dataclass(frozen=True)
class SetonixConnection:
    host: str
    user: str
    key_path: str
    passphrase: Optional[str] = None
    port: int = 22
    timeout_s: int = 30


class SetonixError(RuntimeError):
    pass


def run_remote(conn: SetonixConnection, cmd: str) -> str:
    """
    Run a remote command and return stdout. Raises SetonixError on non-zero rc.
    """
    res = ssh_run(
        host=conn.host,
        user=conn.user,
        key_path=conn.key_path,
        passphrase=conn.passphrase,
        port=conn.port,
        timeout_s=conn.timeout_s,
        cmd=cmd,
    )

    if res.returncode != 0:
        raise SetonixError(
            f"Remote command failed (rc={res.returncode}).\n"
            f"cmd={cmd}\n"
            f"stderr:\n{res.stderr.strip()}"
        )

    return res.stdout


def check_slurm_access(conn: SetonixConnection) -> dict[str, str]:
    """
    Sanity checks: ssh works, sbatch exists, and squeue/sacct are callable.
    Returns small informational strings useful for logging.
    """
    info: dict[str, str] = {}

    info["identity"] = run_remote(conn, "hostname && whoami && pwd").strip()

    info["sbatch_version"] = run_remote(
        conn,
        "command -v sbatch >/dev/null 2>&1 && sbatch --version || (echo 'sbatch not found' && exit 2)",
    ).strip()

    # These are informational; don't fail the whole stage if accounting is restricted.
    try:
        info["squeue_head"] = run_remote(conn, "squeue -u $USER | head -n 5").strip()
    except Exception as e:
        info["squeue_head"] = f"(squeue unavailable: {e})"

    try:
        info["sacct_head"] = run_remote(
            conn,
            "sacct -u $USER --format=JobID,JobName%30,State,ExitCode -n | head -n 5",
        ).strip()
    except Exception as e:
        info["sacct_head"] = f"(sacct unavailable: {e})"

    return info
