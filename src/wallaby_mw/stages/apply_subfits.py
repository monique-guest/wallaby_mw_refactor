# src/wallaby_mw/stages/apply_subfits.py

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import logging
import os
import shutil

from wallaby_mw.utils.files import file_status_by_size


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run subfits on CASDA cubes for one SBID."
    )
    p.add_argument(
        "--rootdir",
        required=True,
        help="Pipeline root directory (e.g. /arc/.../mw_pipeline_outputs)",
    )
    p.add_argument(
        "--sbid",
        type=int, 
        nargs="+", 
        required=True,
        help="Single SBID (e.g. 66866)",
    )
    return p.parse_args(argv)


def run_subfits_for_sbid(rootdir: Path, sbid: str) -> None:
    sbid_str = sbid.strip("[]").strip()
    sbid_root = rootdir / sbid_str
    casda_dir = sbid_root / "casda"
    subfits_dir = sbid_root / "subfits"
    subfits_dir.mkdir(parents=True, exist_ok=True)

    input_fits = casda_dir / "cube.fits"
    if not input_fits.exists():
        raise FileNotFoundError(
            f"[subfits] Expected input cube not found for SBID {sbid_str}: {input_fits}"
        )

    output_fits = subfits_dir / f"subfits.fits"
    if output_fits.exists():
        logger.info(
            "[subfits] Output already exists for SBID %s (%s); skipping",
            sbid_str,
            output_fits,
        )
        return

    # Check for existing/partial output
    status, size = file_status_by_size(output_fits, min_bytes=1_000_000_000)

    if status == "ok":
        logger.info(
            "[subfits] Output already exists for SBID %s (%s, %.2f GiB); skipping",
            sbid,
            output_fits,
            size / 2**30,
        )
        return
    elif status == "partial":
        logger.warning(
            "[subfits] Existing output for SBID %s looks partial (%s, %.2f GiB); "
            "deleting and re-running",
            sbid,
            output_fits,
            size / 2**30,
        )
        output_fits.unlink()

    # Log disk usage before writing
    usage = shutil.disk_usage(subfits_dir)
    logger.info(
        "[subfits] Disk usage at %s: total=%.1f GiB, used=%.1f GiB, free=%.1f GiB",
        subfits_dir,
        usage.total / 2**30,
        usage.used / 2**30,
        usage.free / 2**30,
    )

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

    logger.info("[subfits] SBID %s: running %s", sbid_str, " ".join(cmd))
    subprocess.check_call(cmd)
    logger.info("[subfits] SBID %s: wrote %s", sbid_str, output_fits)


def main(argv=None):
    args = parse_args(argv)
    rootdir = Path(args.rootdir)
    sbid = str(args.sbid)

    logger.info("[subfits] Starting single-SBID job: sbid=%s rootdir=%s", sbid, rootdir)

    run_subfits_for_sbid(rootdir=rootdir, sbid=sbid)


if __name__ == "__main__":
    main()