from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True)
class RemoteEnvStatus:
    venv_dir: str
    created: bool
    python_path: str
    python_version: str
    pip_version: str


@dataclass(frozen=True)
class SetonixTaskSubmission:
    job_id: str
    repo: RemoteRepoStatus
    env: RemoteEnvStatus
    log_dir: str
    output_path_template: str
    error_path_template: str
    output_path: str
    error_path: str


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


def run_remote_streaming(conn: SetonixConnection, cmd: str) -> str:
    """
    Run a remote command and stream stdout/stderr while it executes.
    """
    res = ssh_run(
        host=conn.host,
        user=conn.user,
        key_path=conn.key_path,
        passphrase=conn.passphrase,
        port=conn.port,
        timeout_s=conn.timeout_s,
        cmd=cmd,
        stream=True,
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


def _module_init_lines(module_loads: Optional[list[str]] = None) -> list[str]:
    lines: list[str] = []
    if module_loads:
        lines.extend(
            [
                "if ! command -v module >/dev/null 2>&1; then",
                "  if [ -f /usr/share/lmod/lmod/init/bash ]; then",
                "    source /usr/share/lmod/lmod/init/bash",
                "  elif [ -f /etc/profile.d/lmod.sh ]; then",
                "    source /etc/profile.d/lmod.sh",
                "  elif [ -f /etc/profile.d/modules.sh ]; then",
                "    source /etc/profile.d/modules.sh",
                "  else",
                "    echo 'module command not available' >&2",
                "    exit 2",
                "  fi",
                "fi",
            ]
        )
        for module_name in module_loads:
            lines.append(f"module load {_shell_quote(module_name)}")
    return lines


def _bash_login_cmd(cmd: str, module_loads: Optional[list[str]] = None) -> str:
    setup_lines = _module_init_lines(module_loads)
    setup_lines.append(cmd)
    script = "\n".join(setup_lines)
    return f"bash -lc {_shell_quote(script)}"


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


def ensure_remote_env(
    conn: SetonixConnection,
    *,
    repo_dir: str,
    venv_dir: str,
    python_cmd: str = "python3",
    editable: bool = False,
    module_loads: Optional[list[str]] = None,
    stream: bool = False,
) -> RemoteEnvStatus:
    """
    Ensure a Python virtual environment exists on Setonix and install the repo.

    The environment is created if missing, then the project at ``repo_dir`` is
    installed into it using pip.
    """
    repo_dir_q = _shell_quote(repo_dir)
    venv_dir_q = _shell_quote(venv_dir)
    python_cmd_q = _shell_quote(python_cmd)
    venv_python = f"{venv_dir}/bin/python"
    venv_python_q = _shell_quote(venv_python)
    install_target = "-e ." if editable else "."
    remote_runner = run_remote_streaming if stream else run_remote

    exists_cmd = f"if [ -x {venv_python_q} ]; then printf 'yes'; else printf 'no'; fi"
    created = remote_runner(conn, _bash_login_cmd(exists_cmd, module_loads)).strip() != "yes"

    if created:
        create_cmd = (
            f"mkdir -p {venv_dir_q} && "
            f"{python_cmd_q} -m venv {venv_dir_q}"
        )
        remote_runner(conn, _bash_login_cmd(create_cmd, module_loads))

    install_cmd = (
        f"cd {repo_dir_q} && "
        f"{venv_python_q} -m pip install --upgrade pip setuptools wheel && "
        f"{venv_python_q} -m pip install {install_target}"
    )
    remote_runner(conn, _bash_login_cmd(install_cmd, module_loads))

    inspect_cmd = (
        f"{venv_python_q} -c "
        "\"import sys; print(sys.executable); print(sys.version.splitlines()[0])\"\n"
        f"{venv_python_q} -m pip --version"
    )
    out = remote_runner(conn, _bash_login_cmd(inspect_cmd, module_loads))
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if len(lines) < 3:
        raise SetonixError(
            f"Unexpected virtualenv inspection output for {venv_dir}: {out!r}"
        )

    python_path, python_version, pip_version = lines[:3]
    return RemoteEnvStatus(
        venv_dir=venv_dir,
        created=created,
        python_path=python_path,
        python_version=python_version,
        pip_version=pip_version,
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


def submit_setonix_task(
    conn: SetonixConnection,
    *,
    repo_url: str,
    repo_dir: str,
    venv_dir: str,
    command: str,
    branch: Optional[str] = None,
    python_cmd: str = "python3",
    module_loads: Optional[list[str]] = None,
    env: Optional[dict[str, str]] = None,
    job_name: str = "wallaby-mw",
    walltime: str = "01:00:00",
    nodes: int = 1,
    ntasks: int = 1,
    cpus_per_task: int = 1,
    mem: Optional[str] = None,
    log_dir: Optional[str] = None,
    output_path: Optional[str] = None,
    error_path: Optional[str] = None,
    setup_stream: bool = False,
    debug: bool = False,
) -> SetonixTaskSubmission:
    """
    Bootstrap the remote repo/env if needed and submit a Slurm job on Setonix.
    """
    repo_status = ensure_remote_repo(
        conn,
        repo_url=repo_url,
        repo_dir=repo_dir,
        branch=branch,
    )
    env_status = ensure_remote_env(
        conn,
        repo_dir=repo_dir,
        venv_dir=venv_dir,
        python_cmd=python_cmd,
        module_loads=module_loads,
        stream=setup_stream,
    )

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    effective_log_dir = log_dir or f"{repo_dir}/logs"
    output_template = output_path or f"{effective_log_dir}/{job_name}-{timestamp}-%j.out"
    error_template = error_path or f"{effective_log_dir}/{job_name}-{timestamp}-%j.err"

    mkdir_cmd = f"mkdir -p {_shell_quote(effective_log_dir)}"
    run_remote(conn, mkdir_cmd)

    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={job_name}",
        f"#SBATCH --time={walltime}",
        f"#SBATCH --nodes={nodes}",
        f"#SBATCH --ntasks={ntasks}",
        f"#SBATCH --cpus-per-task={cpus_per_task}",
        f"#SBATCH --output={output_template}",
        f"#SBATCH --error={error_template}",
    ]
    if mem:
        script_lines.append(f"#SBATCH --mem={mem}")

    script_lines.extend(
        [
            "",
            "set -euo pipefail" if not debug else "set -euxo pipefail",
        ]
    )

    script_lines.extend(_module_init_lines(module_loads))

    if env:
        for key, value in env.items():
            script_lines.append(f"export {key}={_shell_quote(value)}")

    script_lines.extend(
        [
            f"cd {_shell_quote(repo_dir)}",
            f"source {_shell_quote(venv_dir)}/bin/activate",
        ]
    )
    if debug:
        script_lines.extend(
            [
                "pwd",
                "which python",
                "python --version",
            ]
        )
    script_lines.extend(
        [
            command,
        ]
    )

    job_id = submit_sbatch_inline(conn, "\n".join(script_lines))
    resolved_output_path = output_template.replace("%j", job_id)
    resolved_error_path = error_template.replace("%j", job_id)
    return SetonixTaskSubmission(
        job_id=job_id,
        repo=repo_status,
        env=env_status,
        log_dir=effective_log_dir,
        output_path_template=output_template,
        error_path_template=error_template,
        output_path=resolved_output_path,
        error_path=resolved_error_path,
    )
