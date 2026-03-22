from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import time

import paramiko


@dataclass(frozen=True)
class SSHResult:
    """
    Result of a remote SSH command.
    """
    returncode: int
    stdout: str
    stderr: str


class SSHError(RuntimeError):
    """
    Raised when an SSH connection or command fails.
    """
    pass


def _load_private_key(
    key_path: str,
    passphrase: Optional[str],
) -> paramiko.PKey:
    """
    Try to load a private key file using supported Paramiko key types.
    """
    errors: list[str] = []

    for key_cls in (
        paramiko.Ed25519Key,
        paramiko.RSAKey,
        paramiko.ECDSAKey,
    ):
        try:
            return key_cls.from_private_key_file(
                key_path,
                password=passphrase,
            )
        except Exception as e:
            errors.append(f"{key_cls.__name__}: {e}")

    raise SSHError(
        f"Failed to load SSH key from {key_path}. Tried:\n"
        + "\n".join(errors)
    )


def ssh_run(
    *,
    host: str,
    user: str,
    cmd: str,
    key_path: str,
    passphrase: Optional[str] = None,
    port: int = 22,
    timeout_s: int = 30,
    stream: bool = False,
) -> SSHResult:
    """
    Run a command on a remote host via Paramiko SSH.

    - Fully non-interactive
    - Does NOT require ssh-agent
    - Supports passphrase-protected keys

    Parameters
    ----------
    host : str
        Remote hostname (e.g. setonix.pawsey.org.au)
    user : str
        SSH username
    cmd : str
        Command to execute remotely
    key_path : str
        Path to private SSH key
    passphrase : str | None
        Passphrase for the private key (if encrypted)
    port : int
        SSH port (default 22)
    timeout_s : int
        Connection + command timeout

    Returns
    -------
    SSHResult
    """
    client = paramiko.SSHClient()

    # Accept new host keys automatically (similar to:
    # StrictHostKeyChecking=accept-new)
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        pkey = _load_private_key(key_path, passphrase)

        client.connect(
            hostname=host,
            port=port,
            username=user,
            pkey=pkey,
            timeout=timeout_s,
            banner_timeout=timeout_s,
            auth_timeout=timeout_s,
            look_for_keys=False,
            allow_agent=False,
        )

        stdin, stdout, stderr = client.exec_command(cmd)

        if stream:
            out_chunks: list[str] = []
            err_chunks: list[str] = []
            channel = stdout.channel

            while True:
                if channel.recv_ready():
                    chunk = channel.recv(4096).decode("utf-8", errors="replace")
                    if chunk:
                        print(chunk, end="", flush=True)
                        out_chunks.append(chunk)

                if channel.recv_stderr_ready():
                    chunk = channel.recv_stderr(4096).decode("utf-8", errors="replace")
                    if chunk:
                        print(chunk, end="", flush=True)
                        err_chunks.append(chunk)

                if channel.exit_status_ready():
                    while channel.recv_ready():
                        chunk = channel.recv(4096).decode("utf-8", errors="replace")
                        if chunk:
                            print(chunk, end="", flush=True)
                            out_chunks.append(chunk)
                    while channel.recv_stderr_ready():
                        chunk = channel.recv_stderr(4096).decode("utf-8", errors="replace")
                        if chunk:
                            print(chunk, end="", flush=True)
                            err_chunks.append(chunk)
                    break

                time.sleep(0.1)

            rc = channel.recv_exit_status()
            out = "".join(out_chunks)
            err = "".join(err_chunks)
        else:
            rc = stdout.channel.recv_exit_status()
            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")

        return SSHResult(
            returncode=rc,
            stdout=out,
            stderr=err,
        )

    except Exception as e:
        raise SSHError(str(e)) from e

    finally:
        try:
            client.close()
        except Exception:
            pass
