import logging
import os
import configparser
from typing import Optional, Tuple

KEYRING_SERVICE = "astroquery:casda.csiro.au"

# Authentication failure detection
class AuthFailureHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.failed = False
        self.msg = None

    def emit(self, record):
        msg = record.getMessage()
        if "Authentication failed" in msg:
            self.failed = True
            self.msg = msg 

def install_auth_failure_handler():
    handler = AuthFailureHandler()
    logging.getLogger().addHandler(handler)
    return handler

def setup_plaintext_keyring():
    """
    Configure keyring to use a plaintext backend (suitable for headless/HPC).
    """
    try:
        import keyring
        from keyrings.alt.file import PlaintextKeyring
    except ImportError as e:
        raise RuntimeError(
            "Plaintext keyring requested but dependencies are missing. "
            "Install with: pip install keyring keyrings.alt"
        ) from e

    keyring.set_keyring(PlaintextKeyring())
    return keyring


def read_casda_credentials_ini(path: str) -> tuple[str, str | None]:
    """
    Read CASDA username (and optionally password) from an ini file:
      [CASDA]
      username = ...
      password = ...   (optional for normal runs)
    """
    cfg = configparser.ConfigParser()
    read_ok = cfg.read(path)
    if not read_ok:
        raise FileNotFoundError(f"Could not read credentials file: {path}")

    if "CASDA" not in cfg or "username" not in cfg["CASDA"]:
        raise ValueError("credentials.ini must contain [CASDA] username")

    username = cfg["CASDA"]["username"].strip()
    password = cfg["CASDA"].get("password")
    password = password.strip() if password is not None else None
    return username, password


def store_casda_password_in_keyring(username: str, password: str, service: str = KEYRING_SERVICE) -> None:
    """
    Store CASDA password for a username in the (plaintext) keyring.
    """
    keyring = setup_plaintext_keyring()
    keyring.set_password(service, username, password)


def ensure_casda_password_in_keyring(credentials_ini_path: str, *, service: str = KEYRING_SERVICE) -> str:
    """
    Convenience helper: read username/password from ini and store password in keyring.
    Returns username.
    """
    username, password = read_casda_credentials_ini(credentials_ini_path)
    if not password:
        raise ValueError("credentials.ini must contain [CASDA] password to setup keyring")
    store_casda_password_in_keyring(username, password, service=service)
    return username

def login_casda(
    username: str | None = None,
    password: str | None = None,
):
    """
    Log into CASDA and return (casda_instance, username).

    If username/password are not provided, they are taken from
    CASDA_USERNAME and CASDA_PASSWORD environment variables.

    If a password is provided, it is stored in the plaintext keyring.
    """
    from astroquery.casda import Casda  # local import

    # Prefer explicit args, fall back to env
    if username is None:
        username = os.environ.get("CASDA_USERNAME")

    if password is None:
        password = os.environ.get("CASDA_PASSWORD")

    if not username:
        raise RuntimeError("CASDA username not provided and CASDA_USERNAME not set")

    if password:
        store_casda_password_in_keyring(username, password)
        # Drop it from env if it came from there
        if os.environ.get("CASDA_PASSWORD") == password:
            os.environ.pop("CASDA_PASSWORD", None)

    casda = Casda()
    casda.login(username=username)
    return casda, username