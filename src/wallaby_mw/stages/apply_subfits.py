# src/wallaby_mw/stages/apply_subfits.py

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import logging
import os


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run subfits on CASDA cubes for one or more SBIDs."
    )
    p.add_argument(
        "--rootdir",
        required=True,
        help="Pipeline root directory (e.g. /arc/.../mw_pipeline_outputs)",
    )
    p.add_argument(
        "--sbids",
        required=True,
        help="Space-separated list of SBIDs (e.g. '66866 67022')",
    )
    return p.parse_args()


def run_subfits_for_sbid(rootdir: Path, sbid: str) -> None:
    sbid_root = rootdir / sbid
    casda_dir = sbid_root / "casda"
    subfits_dir = sbid_root / "subfits"
    subfits_dir.mkdir(parents=True, exist_ok=True)

    input_fits = casda_dir / "cube.fits"
    if not input_fits.exists():
        raise FileNotFoundError(
            f"[subfits] Expected input cube not found for SBID {sbid}: {input_fits}"
        )

    output_fits = subfits_dir / f"subfits.fits"
    if output_fits.exists():
        logger.info(
            "[subfits] Output already exists for SBID %s (%s); skipping",
            sbid,
            output_fits,
        )
        return

    subfits_script = os.environ.get(
        "SUBFITS_SCRIPT",
        "/opt/subfits/subfits.py",  # default for the container
    )

    cmd = [
        "python",
        subfits_script,
        "-i",
        str(input_fits),
        "-o",
        str(output_fits),
        "-r",  # remove dummy axes 
    ]

    logger.info("[subfits] SBID %s: running %s", sbid, " ".join(cmd))
    subprocess.check_call(cmd)
    logger.info("[subfits] SBID %s: wrote %s", sbid, output_fits)


def main() -> None:
    args = parse_args()
    rootdir = Path(args.rootdir)
    sbid_list = args.sbids.split()

    logger.info("[subfits] rootdir=%s, sbids=%s", rootdir, sbid_list)

    for sbid in sbid_list:
        run_subfits_for_sbid(rootdir=rootdir, sbid=sbid)


if __name__ == "__main__":
    main()