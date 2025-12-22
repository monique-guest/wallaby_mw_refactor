#!/usr/bin/env python3
from astroquery.utils.tap.core import TapPlus
from astroquery.casda import Casda
from urllib.parse import urlparse
import time 
import logging 
import socket 
import os
import hashlib

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("astroquery").setLevel(logging.DEBUG)

socket.setdefaulttimeout(30)

casda = Casda()

# Credentials (moved to env vars / CLI later)
username = "monique.guest@csiro.au"

casda.login(username=username)

CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"

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
    sbid = 66866 
    filename_like = "image.restored.i.SB66866%.cube.MilkyWay%.fits" 
    rootdir = f"C:/Users/gue034/Code/Work/sandpit/data"
    outdir = f"{rootdir}/{sbid}"

    # Create directory
    os.makedirs(outdir, exist_ok=True)
    os.makedirs(os.path.join(outdir, "casda"), exist_ok=True)

    # Assign query to extract download URLs
    # adql = f"""
    # SELECT TOP 50
    #     obs_id, filename, access_url, access_format
    # FROM ivoa.obscore
    # WHERE obs_id = '{sbid}'
    #     AND dataproduct_type = 'cube'
    #     AND filename LIKE '{filename_like}'
    # """

    adql = f"""
    SELECT TOP 10 obs_id, dataproduct_type, filename, access_format, access_url
    FROM ivoa.obscore
    WHERE obs_id = 'ASKAP-{sbid}' 
        AND filename = 'image.restored.i.SB{sbid}.cube.MilkyWay.contsub.fits'
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

        # Extract URLs for data to be downloaded
        url_list = casda.stage_data(table, verbose=True)
        logging.debug("Staged %d URLs:\n%s", len(url_list), "\n".join(url_list))

        # Check if files from URLs already exist before downloading 
        urls_to_download = []

        for url in url_list:
            filename = filename_from_url(url)
            local_path = os.path.join(outdir, filename) 

            if file_exists(local_path):
                logging.info(f"File already exists, skipping: {filename}")
                continue

            urls_to_download.append(url)

        # Download the required files
        if urls_to_download:
            files = casda.download_files(urls_to_download, savedir=outdir)
            logging.info("Downloaded files:\n", files) 

        # Checksum verification
        for file in os.listdir(outdir):
            if not file.endswith(".fits"):
                continue 

            data_path = os.path.join(outdir, file)
            checksum_path = data_path + ".checksum"

            if not os.path.exists(checksum_path):
                logging.warning(f"No checkseum file found for {file}")
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