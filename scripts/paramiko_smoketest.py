from __future__ import annotations

import argparse
import getpass
import sys

import paramiko


def load_pkey(key_path: str, passphrase: str | None):
    errors = []
    for cls in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
        try:
            return cls.from_private_key_file(key_path, password=passphrase)
        except Exception as e:
            errors.append((cls.__name__, str(e)))
    msg = "Could not load key. Tried:\n" + "\n".join(f"- {n}: {e}" for n, e in errors)
    raise RuntimeError(msg)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--port", type=int, default=22)
    ap.add_argument("--cmd", default="hostname && whoami && pwd")
    ap.add_argument("--passphrase", default=None, help="If omitted, you'll be prompted (won't echo).")
    args = ap.parse_args()

    passphrase = args.passphrase
    if passphrase is None:
        # Prompt (safe-ish for testing; later you can read from credentials.ini)
        passphrase = getpass.getpass("Key passphrase (leave blank if none): ").strip() or None

    pkey = load_pkey(args.key, passphrase)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(
            hostname=args.host,
            port=args.port,
            username=args.user,
            pkey=pkey,
            timeout=20,
            banner_timeout=20,
            auth_timeout=20,
            look_for_keys=False,
            allow_agent=False,
        )

        stdin, stdout, stderr = client.exec_command(args.cmd)
        rc = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")

        print("=== STDOUT ===")
        print(out.rstrip())
        print("\n=== STDERR ===")
        print(err.rstrip())
        print(f"\nrc={rc}")
        return rc

    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
