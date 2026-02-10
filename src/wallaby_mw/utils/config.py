from __future__ import annotations

import configparser
import os

def load_credentials(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not cfg.read(path):
        raise SystemExit(f"Could not read credentials file: {path}")
    return cfg


def load_pipeline_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not cfg.read(path):
        raise SystemExit(f"Could not read pipeline config: {path}")
    return cfg


def export_env_from_creds(creds: configparser.ConfigParser) -> None:
    # CASDA
    if "CASDA" in creds:
        os.environ["CASDA_USERNAME"] = creds["CASDA"]["username"]
        os.environ["CASDA_PASSWORD"] = creds["CASDA"]["password"]

    # Harbor (images.canfar.net)
    if "Harbor" in creds:
        os.environ["CANFAR_REGISTRY_USERNAME"] = creds["Harbor"]["username"]
        os.environ["CANFAR_REGISTRY_SECRET"] = creds["Harbor"]["secret"]

    # CANFAR cert path
    if "CANFAR" in creds and "cadc_loc" in creds["CANFAR"]:
        os.environ["CADC_CERT"] = creds["CANFAR"]["cadc_loc"]

    # Setonix SSH credentials
    if "Setonix" in creds and "key" in creds["Setonix"]:
        os.environ["SETONIX_USERNAME"] = creds["Setonix"]["username"]
        os.environ["SSH_KEY"] = creds["Setonix"]["ssh_key"].strip()

        if "passphrase" in creds["Setonix"]:
            val = creds["Setonix"]["passphrase"].strip()
            if val:
                os.environ["SSH_PASSPHRASE"] = val
            else:
                # Explicitly ensure empty values do not linger
                os.environ.pop("SSH_PASSPHRASE", None)