#!/usr/bin/env python3
from pathlib import Path
from astroquery.utils.tap.core import TapPlus
from astroquery.casda import Casda
from urllib.parse import urlparse
from astropy.utils.exceptions import AstropyWarning
from datetime import datetime, timezone
from wallaby_mw.utils.auth import (
    install_auth_failure_handler,
    setup_plaintext_keyring,
    read_casda_credentials_ini,
    ensure_casda_password_in_keyring,
    login_casda,
)
from astropy.utils import iers
from wallaby_mw.utils.files import filename_from_url, create_symlinks_from_patterns
from wallaby_mw.utils.checksums import md5sum, read_checksum_file
from wallaby_mw.utils.errors import WallabyPipelineError, CasdaError, CasdaAuthError, CasdaStagingError, CasdaTapJobError
from wallaby_mw.utils.manifest import utc_now_iso, write_manifest
import time 
import logging 
import socket 
import os
import hashlib
import argparse
import sys
import warnings
import configparser
import json

iers.conf.auto_download = False

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("astroquery").setLevel(logging.WARNING)

socket.setdefaulttimeout(30)

# Silence VOTable unit warnings from CASDA datalink ("pixel" unit)
warnings.filterwarnings(
    "ignore",
    message=r".*Invalid unit string 'pixel'.*",
    category=AstropyWarning,
)

# Function to parse arguments 
def parse_args(argv=None):

    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Download WALLABY Milky Way data from CASDA"
    )

    parser.add_argument(
        "--sbids", 
        type=int, 
        nargs="+", 
        required=False, 
        help="One or more SBIDS (e.g. 66866 67022)"
    )

    parser.add_argument(
        "--rootdir",
        type=str, 
        required=False, 
        help="Root directory for pipeline outputs"
    ) 

    return parser.parse_args(argv)

def run(sbids, rootdir):
    """
    Run the CASDA download stage for one or more SBIDs.

    All real work lives here so this can be called from:
      - CLI (main)
      - Prefect tasks
      - tests
    """
    # Install the auth failure handler and attach it to the root logger
    auth_handler = install_auth_failure_handler()
    logging.getLogger().addHandler(auth_handler) 

    # CASDA auth (env or explicit, but stage doesn't care which)
    casda, username = login_casda()
    logging.info("CASDA login attempt for %s", username)

    if auth_handler.failed:
        raise CasdaAuthError(f"CASDA login failed (detected from logs): {auth_handler.msg}")

    logging.info("CASDA login OK for %s", username)

    # Assign TAP URL
    CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"

    # Start CASDA download process 

    # Create query to extract download URLs
    obs_ids = ",".join(f"'ASKAP-{s}'" for s in sbids)
    adql = f"""
    SELECT obs_id, dataproduct_type, filename, access_format, access_url
    FROM ivoa.obscore
    WHERE obs_id IN ({obs_ids}) 
        AND dataproduct_type = 'cube'
        AND (
            filename LIKE 'weights.i.%.cube.MilkyWay.fits' 
            OR filename LIKE 'image.restored.i.%.cube.MilkyWay.contsub.fits')
    """

    print("ADQL query:\n", adql, flush=True)

    # Assign TAP URL to CASDAs TAP service endpoint
    tap = TapPlus(url=CASDA_TAP_URL)

    # Submit the query to the TAP URL
    print("Submitting async job...", flush=True)
    job = tap.launch_job_async(adql, background=True)

    print("Submitted. Job phase (initial):", job.get_phase(), flush=True)

    # Set timeout (will this still be needed in prod?)
    t0 = time.time()
    timeout_s = 15 * 60
    poll_every = 2

    # Set executing to True and get the jobs current phase
    executing = True
    phase = job.get_phase()

    # Poll the job to monitor phase
    while executing:
        time.sleep(poll_every)
        phase = job.get_phase(update=True)
        elapsed = int(time.time() - t0)
        print(f"[{elapsed:>4}s] phase={phase}", flush=True)

        if phase in ("COMPLETED", "ERROR", "ABORTED"):
            executing = False
        if elapsed >= timeout_s:
            raise CasdaTapJobError(f"TAP job timed out after {timeout_s}s; last phase={phase}")

    if phase == "COMPLETED":
        print("Final phase:", phase, flush=True)
        table = job.get_results() 

        logging.info(f"Rows returned: {len(table)}")
        if len(table) > 0:
            logging.debug(table["obs_id", "dataproduct_type", "filename", "access_format", "access_url"][:10]) 

        # Process one SBID at a time
        for sbid in sbids:
            sbid = sbid.strip("[]").strip()
            obs_id = f"ASKAP-{sbid}"
            logging.info(f"Processing SBID {sbid}")

            sbid_dir = os.path.join(rootdir, str(sbid))
            casda_dir = os.path.join(sbid_dir, "casda")
            os.makedirs(sbid_dir, exist_ok=True)
            os.makedirs(casda_dir, exist_ok=True)
            sbid_manifest_path = os.path.join(sbid_dir, "manifest.json")

            stage_manifest = {
                "stage": "casda_download",
                "started_utc": utc_now_iso(),
                "sbid": int(sbid),
                "obs_id": obs_id,
                "outputs": {
                    "casda_dir": casda_dir,
                },
                "tap": {
                    "url": CASDA_TAP_URL,
                    "rows": None,
                },
                "files": [],
                "checksums": [],
            }

            # Filter the mixed TAP results table down to just this SBID
            sbid_table = table[table["obs_id"] == obs_id]
            stage_manifest["tap"]["rows"] = int(len(sbid_table))

            if len(sbid_table) == 0:
                logging.warning("No rows returned for %s (skipping)", obs_id)
                continue

            # Stage URLs only for this SBID
            try:
                url_list = casda.stage_data(sbid_table, verbose=True)
            except Exception as e:
                raise CasdaStagingError(
                    f"CASDA stage_data failed for {obs_id} (auth/permissions?): {e}"
                ) from e

            logging.debug("Staged %d URLs for %s:\n%s", len(url_list), obs_id, "\n".join(url_list))

            # Check if files from URLs already exist before downloading 
            urls_to_download = []

            for url in url_list:
                filename = filename_from_url(url)
                local_path = os.path.join(casda_dir, filename) 

                if os.path.exists(local_path):
                    logging.info(f"File already exists, skipping: {filename}")
                    
                    stage_manifest["files"].append({
                        "filename": filename,
                        "url": url,
                        "path": local_path,
                        "status": "skipped_exists",
                    })
                else:
                    urls_to_download.append(url)
                    stage_manifest["files"].append({
                        "filename": filename,
                        "url": url,
                        "path": local_path,
                        "status": "scheduled_download",
                    })

            # Download the required files
            if urls_to_download:
                logging.info("Starting to download %d URLs for %s", len(urls_to_download), obs_id)
                t_dl0 = time.time()
                files = casda.download_files(urls_to_download, savedir=casda_dir)
                logging.info("Download finished for %s in %.1fs (%d returned)", obs_id, time.time() - t_dl0, len(files))

                downloaded = {os.path.basename(str(p)) for p in files}
                for f in stage_manifest["files"]:
                    if f["status"] == "scheduled_download" and f["filename"] in downloaded:
                        f["status"] = "downloaded"

            # Check if expected files have been downloaded
            expected_files = [filename_from_url(url) for url in url_list]
            missing = [file for file in expected_files if not os.path.exists(os.path.join(casda_dir, file))]
            if missing:
                raise CasdaError(f"Download incomplete for {obs_id}; missing files: {missing}")

            # Checksum verification
            expected_fits = [f for f in expected_files if f.endswith(".fits")]
            for file in expected_fits:
                if not file.endswith(".fits"):
                    continue 

                data_path = os.path.join(casda_dir, file)
                checksum_path = data_path + ".checksum"

                if not os.path.exists(checksum_path):
                    logging.warning(f"No checksum file found for {file}")
                    stage_manifest["checksums"].append({
                        "filename": file,
                        "data_path": data_path,
                        "checksum_path": checksum_path,
                        "ok": None,
                        "note": "missing_checksum_file",
                    })
                    continue 

                expected = read_checksum_file(checksum_path)
                actual = md5sum(data_path)

                ok = (actual == expected)

                stage_manifest["checksums"].append({
                    "filename": file,
                    "data_path": data_path,
                    "checksum_path": checksum_path,
                    "expected_md5": expected,
                    "actual_md5": actual,
                    "ok": ok,
                })

                if not ok:
                    raise CasdaError(
                        f"Checksum mismatch for {file}: expected {expected}, got {actual}"
                    )

                logging.info(f"Checksum OK for file: {file}")

            # Create symlinks
            patterns = [
                {
                    "startswith": "image.restored.i.",
                    "endswith": ".cube.MilkyWay.contsub.fits",
                    "link": "cube.fits",
                    "key": "cube_fits",
                    "required": True,
                },
                {
                    "startswith": "weights.i.",
                    "endswith": ".cube.MilkyWay.fits",
                    "link": "cube_weights.fits",
                    "key": "cube_weights_fits",
                    "required": False,
                },
            ]

            canonical_outputs = create_symlinks_from_patterns(
                base_dir=casda_dir,
                filenames=expected_fits,
                patterns=patterns,
            )

            # Add canonical paths into manifest outputs
            stage_manifest["outputs"].update(canonical_outputs)

            sbid_manifest = {
                "sbid": int(sbid),
                "obs_id": obs_id,
                "updated_utc": utc_now_iso(),
                "stages": {
                    "casda_download": stage_manifest
                }
            }

            write_manifest(sbid_manifest_path, sbid_manifest)
            logging.info("Wrote SBID manifest: %s", sbid_manifest_path)

    else: 
        msg = f"TAP job finished but not COMPLETED (phase={phase})"
        if phase == "ERROR":
            try:
                msg += f"; error={job.get_error()}"
            except Exception as e:
                msg += f"; (could not fetch job error: {e})"
        raise CasdaTapJobError(msg)

def main(argv=None):

    args = parse_args(argv)

    # Normal run mode requires these:
    if not args.sbids:
        raise SystemExit("--sbids is required")
    if not args.rootdir:
        raise SystemExit("--rootdir is required")

    try:
        run(sbids=args.sbids, rootdir=args.rootdir)
    except WallabyPipelineError as e:
        logging.error(str(e))
        raise SystemExit(1) from e

if __name__ == "__main__":

    main()