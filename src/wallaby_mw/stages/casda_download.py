#!/usr/bin/env python3
from astroquery.utils.tap.core import TapPlus
from wallaby_mw.utils.auth import ensure_casda_login
from astropy.utils import iers
from wallaby_mw.utils.files import filename_from_url, create_symlinks_from_patterns
from wallaby_mw.utils.checksums import md5sum, read_checksum_file
from wallaby_mw.utils.errors import WallabyPipelineError, CasdaError, CasdaStagingError, CasdaTapJobError
from wallaby_mw.utils.manifest import utc_now_iso, write_manifest, load_manifest, manifest_checksum_ok
from wallaby_mw.utils.logging import setup_logging
import time 
import logging 
import socket 
import os
import argparse

iers.conf.auto_download = False

socket.setdefaulttimeout(30)

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
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity (default: INFO).",
    )
    parser.add_argument(
        "--tap-timeout",
        type=int,
        default=15 * 60,
        help="Timeout in seconds for the TAP query job (default: 900).",
    )
    parser.add_argument(
        "--tap-retries",
        type=int,
        default=2,
        help="Number of retries for TAP query failures/timeouts (default: 2).",
    )
    parser.add_argument(
        "--tap-retry-wait",
        type=int,
        default=10,
        help="Seconds to wait between TAP retries (default: 10).",
    )
    parser.add_argument(
        "--download-retries",
        type=int,
        default=1,
        help="Number of retries for CASDA downloads (default: 1).",
    )
    parser.add_argument(
        "--download-retry-wait",
        type=int,
        default=10,
        help="Seconds to wait between download retries (default: 10).",
    )

    return parser.parse_args(argv)

def run(
    sbid,
    rootdir,
    casda=None,
    username=None,
    tap_timeout_s: int = 15 * 60,
    tap_retries: int = 2,
    tap_retry_wait_s: int = 10,
    download_retries: int = 1,
    download_retry_wait_s: int = 10,
):
    """
    Run the CASDA download stage for a single SBID.

    All real work lives here so this can be called from:
      - CLI (main)
      - Prefect tasks
      - tests
    """
    if casda is None:
        casda, username = ensure_casda_login()
    else:
        logging.info("Using existing CASDA login for %s", username or "unknown-user")

    # Assign TAP URL
    CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"

    # Start CASDA download process 

    sbid = str(sbid).strip("[]").strip()
    # Create query to extract download URLs
    obs_id = f"ASKAP-{sbid}"
    adql = f"""
    SELECT obs_id, dataproduct_type, filename, access_format, access_url
    FROM ivoa.obscore
    WHERE obs_id = '{obs_id}'
        AND dataproduct_type = 'cube'
        AND (
            filename LIKE 'weights.i.%.cube.MilkyWay.fits' 
            OR filename LIKE 'image.restored.i.%.cube.MilkyWay.contsub.fits')
    """

    print("ADQL query:\n", adql, flush=True)

    # Assign TAP URL to CASDAs TAP service endpoint
    tap = TapPlus(url=CASDA_TAP_URL)

    # Submit the query to the TAP URL (with retry)
    poll_every = 2
    job = None
    phase = None
    tap_success = False
    attempts = tap_retries + 1

    for attempt in range(1, attempts + 1):
        print(f"Submitting async job (attempt {attempt}/{attempts})...", flush=True)
        try:
            job = tap.launch_job_async(adql, background=True)
            print("Submitted. Job phase (initial):", job.get_phase(), flush=True)

            t0 = time.time()
            executing = True
            phase = job.get_phase()

            while executing:
                time.sleep(poll_every)
                phase = job.get_phase(update=True)
                elapsed = int(time.time() - t0)
                print(f"[{elapsed:>4}s] phase={phase}", flush=True)

                if phase in ("COMPLETED", "ERROR", "ABORTED"):
                    executing = False
                if elapsed >= tap_timeout_s:
                    raise CasdaTapJobError(
                        f"TAP job timed out after {tap_timeout_s}s; last phase={phase}"
                    )

            if phase == "COMPLETED":
                tap_success = True
                break
            raise CasdaTapJobError(f"TAP job finished but not COMPLETED (phase={phase})")
        except CasdaTapJobError as e:
            if attempt >= attempts:
                raise
            logging.warning(
                "TAP query failed (attempt %d/%d): %s", attempt, attempts, e
            )
            time.sleep(tap_retry_wait_s)

    if not tap_success:
        raise CasdaTapJobError("TAP query failed after retries")

    if phase == "COMPLETED":
        print("Final phase:", phase, flush=True)
        table = job.get_results() 

        logging.info(f"Rows returned: {len(table)}")
        if len(table) > 0:
            logging.debug(table["obs_id", "dataproduct_type", "filename", "access_format", "access_url"][:10]) 

        logging.info(f"Processing SBID {sbid}")

        sbid_dir = os.path.join(rootdir, str(sbid))
        casda_dir = os.path.join(sbid_dir, "casda")
        os.makedirs(sbid_dir, exist_ok=True)
        os.makedirs(casda_dir, exist_ok=True)
        sbid_manifest_path = os.path.join(sbid_dir, "manifest.json")
        existing_manifest = load_manifest(sbid_manifest_path)
        if existing_manifest is None:
            logging.debug("No existing manifest found at %s", sbid_manifest_path)
            existing_manifest = {}
        else:
            logging.info("Loaded existing manifest: %s", sbid_manifest_path)

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

        # Filter the TAP results table down to just this SBID
        sbid_table = table[table["obs_id"] == obs_id]
        stage_manifest["tap"]["rows"] = int(len(sbid_table))

        if len(sbid_table) == 0:
            logging.warning("No rows returned for %s (skipping)", obs_id)
            return

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
        url_by_filename = {}

        for url in url_list:
            filename = filename_from_url(url)
            local_path = os.path.join(casda_dir, filename) 
            url_by_filename[filename] = url

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

        # Download the required files (with retry)
        if urls_to_download:
            logging.info("Starting to download %d URLs for %s", len(urls_to_download), obs_id)
            files = []
            download_success = False
            attempts = download_retries + 1
            for attempt in range(1, attempts + 1):
                try:
                    t_dl0 = time.time()
                    files = casda.download_files(urls_to_download, savedir=casda_dir)
                    logging.info(
                        "Download finished for %s in %.1fs (%d returned)",
                        obs_id,
                        time.time() - t_dl0,
                        len(files),
                    )
                    download_success = True
                    break
                except Exception as e:
                    if attempt >= attempts:
                        raise CasdaError(f"Download failed after retries for {obs_id}: {e}") from e
                    logging.warning(
                        "Download failed (attempt %d/%d) for %s: %s",
                        attempt,
                        attempts,
                        obs_id,
                        e,
                    )
                    time.sleep(download_retry_wait_s)

            if not download_success:
                raise CasdaError(f"Download failed after retries for {obs_id}")

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
        logging.info("Checksum verification for %d FITS files", len(expected_fits))
        for file in expected_fits:
            if not file.endswith(".fits"):
                continue 

            if manifest_checksum_ok(existing_manifest, "casda_download", file):
                logging.info("Checksum already OK in manifest for %s; skipping", file)
                stage_manifest["checksums"].append({
                    "filename": file,
                    "data_path": os.path.join(casda_dir, file),
                    "checksum_path": os.path.join(casda_dir, file) + ".checksum",
                    "ok": True,
                    "note": "skipped_checksum_manifest_ok",
                })
                continue

            data_path = os.path.join(casda_dir, file)
            checksum_path = data_path + ".checksum"

            if not os.path.exists(checksum_path):
                logging.warning("No checksum file found for %s", file)
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
                logging.warning(
                    "Checksum mismatch for %s: expected %s, got %s; re-downloading",
                    file,
                    expected,
                    actual,
                )
                url = url_by_filename.get(file)
                if not url:
                    raise CasdaError(
                        f"Checksum mismatch for {file} but no download URL found."
                    )

                redownload_attempts = download_retries + 1
                redownload_ok = False
                for attempt in range(1, redownload_attempts + 1):
                    try:
                        if os.path.exists(data_path):
                            os.remove(data_path)
                        casda.download_files([url], savedir=casda_dir)
                        actual = md5sum(data_path)
                        redownload_ok = (actual == expected)
                        stage_manifest["checksums"].append({
                            "filename": file,
                            "data_path": data_path,
                            "checksum_path": checksum_path,
                            "expected_md5": expected,
                            "actual_md5": actual,
                            "ok": redownload_ok,
                            "note": "redownload_after_checksum_mismatch",
                            "attempt": attempt,
                        })
                        if redownload_ok:
                            break
                    except Exception as e:
                        if attempt >= redownload_attempts:
                            raise CasdaError(
                                f"Redownload failed for {file}: {e}"
                            ) from e
                        logging.warning(
                            "Redownload failed (attempt %d/%d) for %s: %s",
                            attempt,
                            redownload_attempts,
                            file,
                            e,
                        )
                        time.sleep(download_retry_wait_s)

                if not redownload_ok:
                    raise CasdaError(
                        f"Checksum mismatch for {file} after redownload attempts"
                    )

            logging.info("Checksum OK for file: %s", file)

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

    setup_logging(args.log_level)

    # CASDA auth once for all SBIDs
    casda, username = ensure_casda_login()

    try:
        for sbid in args.sbids:
            run(
                sbid=sbid,
                rootdir=args.rootdir,
                casda=casda,
                username=username,
                tap_timeout_s=args.tap_timeout,
                tap_retries=args.tap_retries,
                tap_retry_wait_s=args.tap_retry_wait,
                download_retries=args.download_retries,
                download_retry_wait_s=args.download_retry_wait,
            )
    except WallabyPipelineError as e:
        logging.error(str(e))
        raise SystemExit(1) from e

if __name__ == "__main__":

    main()
