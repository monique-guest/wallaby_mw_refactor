#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from astropy.io import fits

from wallaby_mw.utils.astro import wallaby_pixel_region

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEFAULT_WALLABY_RELPATH = Path("subfits") / "subfits.fits"
DEFAULT_SINGLEDISH_RELPATH = Path("hi4pi") / "hi4pi.fits"

DEFAULT_SCRIPT_DIRNAME = "miriad_script"
DEFAULT_MIRIAD_DIRNAME = "miriad"
DEFAULT_SCRIPT_NAME = "miriad_script.sh"
DEFAULT_OUTPUT_NAME = "combined.fits"


@dataclass(frozen=True)
class Inputs:
    rootdir: Path
    sbid: str

    # Derived inputs (absolute paths)
    wallaby_fits: Path
    singledish_fits: Path

    # Output layout
    script_dir: Path
    miriad_dir: Path
    script_path: Path
    output_fits: Path

    # Optional processing args
    imsub_region: Optional[str]
    imsub_wallaby_channels: str
    imsub_hi4pi_channels: str
    immerge_uvrange: str
    size_arcmin: int


@dataclass(frozen=True)
class Outputs:
    script_path: Path
    output_fits: Path
    miriad_dir: Path


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate a MIRIAD csh script for combining WALLABY + HI4PI."
    )
    p.add_argument("--rootdir", required=True, help="Pipeline root directory")
    p.add_argument("--sbid", required=True, help="SBID (e.g. 66866)")

    # Optional overrides (default to your invariant layout)
    p.add_argument(
        "--wallaby-relpath",
        default=str(DEFAULT_WALLABY_RELPATH),
        help="Path to WALLABY FITS relative to rootdir/sbid (default: subfits/subfits.fits)",
    )
    p.add_argument(
        "--singledish-relpath",
        default=str(DEFAULT_SINGLEDISH_RELPATH),
        help="Path to HI4PI FITS relative to rootdir/sbid (default: hi4pi/hi4pi.fits)",
    )

    # Processing parameters
    p.add_argument("-r", "--imsub_region", default=None)
    p.add_argument("-cw", "--imsub_wallaby_channels", default="141,394")
    p.add_argument("-cs", "--imsub_hi4pi_channels", default="42,426")
    p.add_argument("-uv", "--immerge_uvrange", default="25,35,meters")
    p.add_argument("-sz", "--size", type=int, default=320, help="Spatial subcube size in arcmin (square)")

    # Layout (optional)
    p.add_argument("--script-dirname", default=DEFAULT_SCRIPT_DIRNAME)
    p.add_argument("--miriad-dirname", default=DEFAULT_MIRIAD_DIRNAME)
    p.add_argument("--script-name", default=DEFAULT_SCRIPT_NAME)
    p.add_argument("--output-name", default=DEFAULT_OUTPUT_NAME)

    return p.parse_args(argv)


def _compute_region_string(wallaby_fits: Path, size_arcmin: int) -> str:
    logger.info("Reading WALLABY FITS header to compute spatial region (size=%d arcmin)", size_arcmin)
    with fits.open(wallaby_fits) as hdul:
        header = hdul[0].header
        region = wallaby_pixel_region(header, size_arcmin)

    width = region[2] - region[0] + 1
    height = region[3] - region[1] + 1
    logger.info(
        "Computed WALLABY spatial region: %s width=%d height=%d ratio=%.3f",
        region, width, height, width / height,
    )
    return str(region).strip("()").replace(" ", "")


def build_inputs(
    *,
    rootdir: Path,
    sbid: str,
    wallaby_relpath: Path,
    singledish_relpath: Path,
    script_dirname: str,
    miriad_dirname: str,
    script_name: str,
    output_name: str,
    imsub_region: Optional[str],
    imsub_wallaby_channels: str,
    imsub_hi4pi_channels: str,
    immerge_uvrange: str,
    size_arcmin: int,
) -> Inputs:
    rootdir = rootdir.expanduser().resolve()
    sbid_dir = rootdir / str(sbid)

    if not sbid_dir.exists():
        raise FileNotFoundError(f"SBID directory does not exist: {sbid_dir}")

    wallaby_fits = (sbid_dir / wallaby_relpath).resolve()
    singledish_fits = (sbid_dir / singledish_relpath).resolve()

    if not wallaby_fits.exists():
        raise FileNotFoundError(f"WALLABY FITS does not exist: {wallaby_fits}")
    if not singledish_fits.exists():
        raise FileNotFoundError(f"HI4PI FITS does not exist: {singledish_fits}")

    script_dir = sbid_dir / script_dirname
    miriad_dir = sbid_dir / miriad_dirname

    script_path = script_dir / script_name
    output_fits = miriad_dir / output_name

    return Inputs(
        rootdir=rootdir,
        sbid=str(sbid),
        wallaby_fits=wallaby_fits,
        singledish_fits=singledish_fits,
        script_dir=script_dir,
        miriad_dir=miriad_dir,
        script_path=script_path,
        output_fits=output_fits,
        imsub_region=imsub_region,
        imsub_wallaby_channels=imsub_wallaby_channels,
        imsub_hi4pi_channels=imsub_hi4pi_channels,
        immerge_uvrange=immerge_uvrange,
        size_arcmin=size_arcmin,
    )


def generate_script(inp: Inputs) -> Outputs:
    inp.script_dir.mkdir(parents=True, exist_ok=True)
    inp.miriad_dir.mkdir(parents=True, exist_ok=True)

    if inp.script_path.exists():
        logger.warning("MIRIAD script already exists at %s (overwriting).", inp.script_path)
    if inp.output_fits.exists():
        logger.warning("Output FITS already exists (will be overwritten by MIRIAD run): %s", inp.output_fits)

    region_str = inp.imsub_region
    if region_str is None:
        region_str = _compute_region_string(inp.wallaby_fits, inp.size_arcmin)

    workdir = inp.miriad_dir

    lines = [
        "#!/bin/csh\n",
        "miriad\n",
        "\n",
        "# Read FITS into MIRIAD format\n",
        f"fits in={inp.singledish_fits} op=xyin out={workdir / 'sd'}\n",
        f"fits in={inp.wallaby_fits} op=xyin out={workdir / 'wallaby'}\n",
        "\n",
        "# Preprocess single-dish data\n",
        f"hanning in={workdir / 'sd'} out={workdir / 'sd_hann'}\n",
        f"imsub in={workdir / 'sd_hann'} out={workdir / 'sd_imsub_incr'} incr=1,1,2\n",
        (
            f"imsub in={workdir / 'sd_imsub_incr'} out={workdir / 'sd_imsub'} "
            f"\"region=images({inp.imsub_hi4pi_channels})\"\n"
        ),
        "\n",
        "# Preprocess WALLABY data\n",
        f"velsw in={workdir / 'wallaby'} axis=freq options=altspc\n",
        f"velsw in={workdir / 'wallaby'} axis=freq,lsrk\n",
        (
            f"imsub in={workdir / 'wallaby'} out={workdir / 'wallaby_trim'} "
            f"\"region=boxes({region_str})({inp.imsub_wallaby_channels})\"\n"
        ),
        "\n",
        "# Regrid and merge\n",
        f"regrid in={workdir / 'sd_imsub'} tin={workdir / 'wallaby_trim'} out={workdir / 'sd_regrid'}\n",
        (
            f"immerge in={workdir / 'wallaby_trim'},{workdir / 'sd_regrid'} "
            f"out={workdir / 'combined'} uvrange={inp.immerge_uvrange} options=notaper\n"
        ),
        f"fits in={workdir / 'combined'} op=xyout out={inp.output_fits}\n",
        "\n",
        "exit\n",
    ]

    logger.info("Writing MIRIAD script: %s", inp.script_path)
    inp.script_path.write_text("".join(lines), encoding="utf-8")
    inp.script_path.chmod(0o700)

    logger.info("Done. Script=%s Output will be=%s", inp.script_path, inp.output_fits)
    return Outputs(script_path=inp.script_path, output_fits=inp.output_fits, miriad_dir=inp.miriad_dir)


def main(argv=None):
    args = parse_args(argv)

    inp = build_inputs(
        rootdir=Path(args.rootdir),
        sbid=str(args.sbid),
        wallaby_relpath=Path(args.wallaby_relpath),
        singledish_relpath=Path(args.singledish_relpath),
        script_dirname=args.script_dirname,
        miriad_dirname=args.miriad_dirname,
        script_name=args.script_name,
        output_name=args.output_name,
        imsub_region=args.imsub_region,
        imsub_wallaby_channels=args.imsub_wallaby_channels,
        imsub_hi4pi_channels=args.imsub_hi4pi_channels,
        immerge_uvrange=args.immerge_uvrange,
        size_arcmin=int(args.size),
    )
    generate_script(inp)


if __name__ == "__main__":
    main()
