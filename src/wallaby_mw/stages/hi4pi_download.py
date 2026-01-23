#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path
import sys
import requests
import warnings
import urllib3
from tqdm import tqdm

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astroquery.vizier import Vizier

from wallaby_mw.utils.astro import get_centre_from_header
from astropy.wcs import FITSFixedWarning

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = 'https://cdsarc.u-strasbg.fr/ftp/J/A+A/594/A116/CUBES/EQ2000/SIN/'
CATALOG = 'J/A+A/594/A116/cubes_eq'

# Suppress FITSFixedWarning
warnings.filterwarnings('ignore', category=FITSFixedWarning)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Download HI4PI image for WALLABY SBID"
    )
    parser.add_argument(
        "--rootdir", 
        required=True, 
        help="Pipeline root directory"
    )
    parser.add_argument(
        "--sbid", 
        type=int, 
        nargs="+", 
        required=True, 
        help="One or more SBIDS (e.g. 66866)"
    )
    parser.add_argument(
        "--width", 
        type=float, 
        default=20.0, 
        help="Width (degrees) of region to query"
    )
    parser.add_argument(
        "--url", 
        default=URL, 
        help="Base URL for HI4PI images"
    )
    parser.add_argument(
        "--catalog", 
        default=CATALOG, 
        help="Vizier catalog identifier"
    )
    parser.add_argument(
        "--vizier-server",
        default="vizier.cds.unistra.fr",
        help="Vizier server to query (e.g. vizier.cfa.harvard.edu)"
    )
    # OPTIONAL last-resort. Ignore this unless you’re totally stuck
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable HTTPS certificate verification (NOT recommended)."
    )
    return parser.parse_args(argv)


def make_session(insecure: bool = False) -> requests.Session:
    """
    Create a requests session that does NOT use env/system proxy settings.
    """
    s = requests.Session()
    s.trust_env = False
    if insecure:
        logger.warning("SSL verification disabled (--insecure used).")
        s.verify = False
        # avoid noisy warnings when using --insecure
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return s


def download_hi4pi(ra, dec, width, output_file, url=URL, catalog=CATALOG, vizier_server="vizier.cds.unistra.fr", insecure: bool = False):
    centre = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)

    # Pin Vizier server (avoid mirrors)
    Vizier.VIZIER_SERVER = vizier_server
    vizier = Vizier(columns=["*"], catalog=catalog)

    logger.info("Querying VizieR %s for catalog %s", vizier_server, catalog)
    tables = vizier.query_region(centre, width=width * u.deg)
    if not tables:
        raise RuntimeError(f"Vizier returned no tables for centre {ra}, {dec} (width={width} deg).")

    res = tables[0]
    logger.debug("VizieR query returned %d rows.", len(res))
    mask = res["WCSproj"] == "SIN"
    sin_res = res[mask]
    logger.debug("Filtered to %d rows with SIN projection.", len(sin_res))

    if len(sin_res) > 1:
        raise RuntimeError(f"More than 1 HI4PI image matched for centre {ra}, {dec}.")
    if len(sin_res) == 0:
        raise RuntimeError(f"No HI4PI image matched for centre {ra}, {dec}.")

    logger.info("Found matching HI4PI image entry.")

    session = make_session(insecure=insecure)

    for row in sin_res:
        filename = row["FileName"]
        download_url = f"{url.rstrip('/')}/{filename}"
        logger.info("Downloading HI4PI image %s", download_url)

        with session.get(download_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))

            with open(output_file, "wb") as out, tqdm(
                total=total_size, unit='iB', unit_scale=True, desc=filename, ncols=80
            ) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        out.write(chunk)
                        pbar.update(len(chunk))

            if total_size != 0 and pbar.n != total_size:
                logger.warning(
                    "Download size mismatch for %s: expected %d, got %d. "
                    "The file may be incomplete.",
                    filename, total_size, pbar.n
                )
        
        logger.info("Successfully downloaded to %s (%.2f MB)",
                    output_file, output_file.stat().st_size / 1024**2)


def run(rootdir: Path, sbid: str, width: float, url: str, catalog: str,
        vizier_server: str, insecure: bool):
    logger.debug("run() called with: rootdir=%s, sbid=%s, width=%s, url=%s, catalog=%s, vizier_server=%s, insecure=%s",
                 rootdir, sbid, width, url, catalog, vizier_server, insecure)

    sbid_dir = rootdir / sbid
    casda_dir = sbid_dir / "casda"
    hi4pi_dir = sbid_dir / "hi4pi"
    hi4pi_dir.mkdir(parents=True, exist_ok=True)

    input_image = casda_dir / "cube.fits"
    if not input_image.exists():
        raise FileNotFoundError(f"Input cube not found for SBID {sbid}: {input_image}")

    output_image = hi4pi_dir / "hi4pi.fits"
    if output_image.exists():
        logger.info("HI4PI image %s already exists. Skipping.", output_image)
        return

    logger.debug("Opening %s to read header.", input_image)
    with fits.open(input_image) as hdul:
        header = hdul[0].header

    centre = get_centre_from_header(header)
    logger.info("Centre coordinate for SBID %s: %s", sbid, centre)

    download_hi4pi(
        centre.ra.value, centre.dec.value, width, output_image,
        url=url, catalog=catalog, vizier_server=vizier_server, insecure=insecure
    )
    logger.info("HI4PI download stage complete for SBID %s.", sbid)


def main(argv=None):
    args = parse_args(argv)
    logger.info("Python executable: %s", sys.executable)

    rootdir = Path(args.rootdir)
    for sbid in args.sbid:
        logger.info("--- Starting HI4PI download for SBID %s ---", sbid)
        run(rootdir, str(sbid), args.width, args.url, args.catalog, args.vizier_server, args.insecure)


if __name__ == "__main__":
    main()