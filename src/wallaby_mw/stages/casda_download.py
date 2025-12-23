#!/usr/bin/env python3
from astroquery.utils.tap.core import TapPlus
from astroquery.casda import Casda
from urllib.parse import urlparse
from astropy.utils.exceptions import AstropyWarning
import time 
import logging 
import socket 
import os
import hashlib
import argparse
import sys
import warnings

logging.basicConfig(level=logging.INFO)
logging.getLogger("astroquery").setLevel(logging.WARNING)

socket.setdefaulttimeout(30)

# Silence VOTable unit warnings from CASDA datalink ("pixel" unit)
warnings.filterwarnings(
    "ignore",
    message=r".*Invalid unit string 'pixel'.*",
    category=AstropyWarning,
)

CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"

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

auth_handler = AuthFailureHandler()
logging.getLogger().addHandler(auth_handler)
# logging.getLogger("astroquery.casda.core").setLevel(logging.INFO)

def file_exists(filepath):
    return os.path.exists(filepath) and os.path.getsize(filepath) > 0 

def filename_from_url(url):
    return os.path.basename(urlparse(url).path) 

def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda:f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_checksum_file(checksum_path):
    # Expects "<md5> <filename>"
    with open(checksum_path, "r") as f:
        return f.read().split()[0].strip()

def main():

    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Download WALLABY Milky Way data from CASDA"
    )

    parser.add_argument(
        "--sbids", 
        type=int, 
        nargs="+", 
        required=True, 
        help="One or more SBIDS (e.g. 66866 67022)"
    )
    parser.add_argument(
        "--rootdir",
        type=str, 
        required=True, 
        help="Root directory for pipeline outputs"
    ) 

    args = parser.parse_args()

    sbids = args.sbids
    rootdir = args.rootdir 

    # CASDA auth 
    username = "monique.guest@csiro.au" # move to env/CLI 
    casda = Casda()

    casda.login(username=username)

    if auth_handler.failed:
        logging.error("CASDA login failed (detected from logs): %s", auth_handler.msg)
        sys.exit(1)

    logging.info("CASDA login OK for %s", username)

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
            logging.warning(f"Timed out after {timeout_s}s; last phase={phase}", flush=True)
            return

    if phase == "COMPLETED":
        print("Final phase:", phase, flush=True)
        table = job.get_results() 

        logging.info(f"Rows returned: {len(table)}")
        if len(table) > 0:
            logging.debug(table["obs_id", "dataproduct_type", "filename", "access_format", "access_url"][:10]) 

        # Process one SBID at a time
        for sbid in sbids:
            obs_id = f"ASKAP-{sbid}"
            logging.info(f"Processing SBID {sbid}")

            sbid_dir = os.path.join(rootdir, str(sbid))
            casda_dir = os.path.join(sbid_dir, "casda")
            os.makedirs(sbid_dir, exist_ok=True)
            os.makedirs(casda_dir, exist_ok=True)

            # Filter the mixed TAP results table down to just this SBID
            sbid_table = table[table["obs_id"] == obs_id]

            if len(sbid_table) == 0:
                logging.warning("No rows returned for %s (skipping)", obs_id)
                continue

            # Stage URLs only for this SBID
            # url_list = casda.stage_data(sbid_table, verbose=True)
            try:
                url_list = casda.stage_data(sbid_table, verbose=True)
            except Exception as e:
                logging.error("CASDA stage_data failed for %s (auth/permissions?): %s", obs_id, e)
                sys.exit(1)

            logging.debug("Staged %d URLs for %s:\n%s", len(url_list), obs_id, "\n".join(url_list))

            # Check if files from URLs already exist before downloading 
            urls_to_download = []

            for url in url_list:
                filename = filename_from_url(url)
                local_path = os.path.join(casda_dir, filename) 

                if file_exists(local_path):
                    logging.info(f"File already exists, skipping: {filename}")
                else:
                    urls_to_download.append(url)

            # Download the required files
            if urls_to_download:
                files = casda.download_files(urls_to_download, savedir=casda_dir)
                logging.info("Downloaded %d files", len(files))

            # Checksum verification
            for file in os.listdir(casda_dir):
                if not file.endswith(".fits"):
                    continue 

                data_path = os.path.join(casda_dir, file)
                checksum_path = data_path + ".checksum"

                if not os.path.exists(checksum_path):
                    logging.warning(f"No checksum file found for {file}")
                    continue 

                expected = read_checksum_file(checksum_path)
                actual = md5sum(data_path)

                if actual != expected:
                    raise RuntimeError(
                        f"Checksum mismatch for {file}: expected {expected}, got {actual}"
                    )

                logging.info(f"Checksum OK for file: {file}")
    else: 
        print("Job finished but not COMPLETED. Phase:", phase, flush=True)
        if phase == "ERROR":
            try:
                print("Error:", job.get_error(), flush=True)
            except Exception as e:
                print("Could not fetch error:", e, flush=True)

if __name__ == "__main__":
    main()