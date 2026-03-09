from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import shlex

from wallaby_mw.utils.ssh import ssh_run, SSHError


@dataclass(frozen=True) # Make instances of the class immutable after creation
class SetonixConnection:
    host: str
    user: str
    key_path: str
    passphrase: Optional[str] = None
    port: int = 22
    timeout_s: int = 30

    def __post_init__(self):
        if self.passphrase is not None:
            object.__setattr__(self, "passphrase", self.passphrase.strip() or None)


class SetonixError(RuntimeError):
    pass


@dataclass(frozen=True)
class RemoteRepoStatus:
    path: str
    exists: bool
    cloned: bool
    head_commit: str
    head_date: str
    branch: str
    remote_url: str


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


def _shell_quote(value: str) -> str:
    return shlex.quote(value)


def ensure_remote_repo(
    conn: SetonixConnection,
    *,
    repo_url: str,
    repo_dir: str,
    branch: Optional[str] = None,
) -> RemoteRepoStatus:
    """
    Ensure a git repository exists on Setonix.

    If ``repo_dir`` does not exist, clone ``repo_url`` there.
    If it already exists, validate that it looks like a git repository and
    report its current state without modifying it.
    """
    repo_dir_q = _shell_quote(repo_dir)
    repo_url_q = _shell_quote(repo_url)
    branch_arg = f"--branch {_shell_quote(branch)} " if branch else ""

    exists_cmd = f"if [ -d {repo_dir_q} ]; then printf 'yes'; else printf 'no'; fi"
    exists = run_remote(conn, exists_cmd).strip() == "yes"

    cloned = False
    if not exists:
        parent_dir_q = _shell_quote(str(Path(repo_dir).parent).replace("\\", "/"))
        clone_cmd = (
            f"mkdir -p {parent_dir_q} && "
            f"git clone {branch_arg}{repo_url_q} {repo_dir_q}"
        )
        run_remote(conn, clone_cmd)
        cloned = True

    inspect_cmd = (
        f"cd {repo_dir_q} && "
        "git rev-parse --is-inside-work-tree >/dev/null 2>&1 || "
        "(echo 'Not a git repository' >&2; exit 2)\n"
        "git remote get-url origin\n"
        "git rev-parse --abbrev-ref HEAD\n"
        "git rev-parse HEAD\n"
        "git log -1 --format=%cI"
    )

    try:
        out = run_remote(conn, inspect_cmd)
    except (SetonixError, SSHError) as e:
        raise SetonixError(f"Failed to inspect remote repo at {repo_dir}: {e}") from e

    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if len(lines) < 4:
        raise SetonixError(
            f"Unexpected git inspection output for remote repo at {repo_dir}: {out!r}"
        )

    remote_url_actual, branch_name, head_commit, head_date = lines[:4]

    return RemoteRepoStatus(
        path=repo_dir,
        exists=True,
        cloned=cloned,
        head_commit=head_commit,
        head_date=head_date,
        branch=branch_name,
        remote_url=remote_url_actual,
    )


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

def submit_sbatch_inline(
    conn: SetonixConnection,
    script_text: str,
) -> str:
    """
    Submit a Slurm job using an inline script.
    Returns the Slurm job ID as a string.
    """
    # Use --parsable so sbatch prints just the jobid
    cmd = (
        "sbatch --parsable <<'EOF'\n"
        f"{script_text}\n"
        "EOF"
    )

    out = run_remote(conn, cmd).strip()

    if not out.isdigit():
        raise SetonixError(f"Unexpected sbatch output: {out}")

    return out
