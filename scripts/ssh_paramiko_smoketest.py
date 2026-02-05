from __future__ import annotations

import getpass
import os
from pathlib import Path

from wallaby_mw.utils.ssh import ssh_run, SSHError


def main() -> None:
    # ---- MANUAL INPUTS (for now) ----
    host = "setonix.pawsey.org.au"
    user = "mguest"
    key_path = str(Path.home() / ".ssh" / "id_nimbus.pem")

    # Prompt so nothing sensitive is hard-coded
    passphrase = getpass.getpass(
        "SSH key passphrase (leave blank if none): "
    ).strip() or None

    print("\nConnecting to Setonix via Paramiko...\n")

    try:
        res = ssh_run(
            host=host,
            user=user,
            key_path=key_path,
            passphrase=passphrase,
            cmd="hostname && whoami && pwd",
        )
    except SSHError as e:
        print("SSH FAILED")
        print(e)
        raise SystemExit(1)

    print("=== STDOUT ===")
    print(res.stdout.rstrip())

    print("\n=== STDERR ===")
    print(res.stderr.rstrip())

    print(f"\nReturn code: {res.returncode}")


if __name__ == "__main__":
    main()
