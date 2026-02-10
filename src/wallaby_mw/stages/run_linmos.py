from __future__ import annotations

import argparse
import logging
import os
import textwrap
from pathlib import Path
from typing import List

from wallaby_mw.utils.parse import parse_sbid_groups
from wallaby_mw.utils.setonix import SetonixConnection, check_slurm_access, submit_sbatch_inline


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run linmos stage via Setonix (initially: verify SSH + Slurm access)."
    )

    p.add_argument(
        "--rootdir",
        required=True,
        help="Pipeline root directory (e.g. /arc/.../mw_pipeline_outputs).",
    )

    p.add_argument(
        "--sbid-groups",
        required=True,
        type=parse_sbid_groups,
        help='SBID groups, e.g. "[66866 67022] [68759 64378 60000]" or "[[66866,67022],[68759,64378,60000]]"',
    )

    p.add_argument(
        "--submit-test-job",
        action="store_true",
        help="Submit a minimal Slurm smoketest job that writes a sentinel file.",
        )

    p.add_argument(
        "--testdir",
        help="Directory to output Slurm tests.",
    )

    # Setonix connection info (defaults can come from env vars exported by config.py)
    p.add_argument("--setonix-host", default=os.environ.get("SETONIX_HOST", "setonix.pawsey.org.au"))
    p.add_argument("--setonix-user", default=os.environ.get("SETONIX_USER"))
    p.add_argument("--ssh-key", default=os.environ.get("SSH_KEY"))
    p.add_argument("--ssh-passphrase", default=os.environ.get("SSH_PASSPHRASE"))
    p.add_argument("--ssh-port", type=int, default=int(os.environ.get("SSH_PORT", "22")))
    p.add_argument("--timeout-s", type=int, default=int(os.environ.get("SSH_TIMEOUT_S", "30")))

    return p.parse_args(argv)

def submit_test_job(conn, testdir):
    if not testdir:
        raise SystemExit("[linmos] --testdir must be provided when --submit-test-job is used")

    logger.info("[linmos] Submitting minimal Slurm smoketest job")
    logger.info("[linmos] Test output directory: %s", testdir)

    slurm_script = textwrap.dedent(f"""\
    #!/bin/bash
    #SBATCH --job-name=linmos-smoketest
    #SBATCH --output=linmos-smoketest.out
    #SBATCH --error=linmos-smoketest.err
    #SBATCH --time=00:02:00
    #SBATCH --nodes=1
    #SBATCH --ntasks=1
    #SBATCH --cpus-per-task=1

    set -euo pipefail

    OUTDIR="{testdir}"
    mkdir -p "$OUTDIR"

    echo "OUTDIR=$OUTDIR"

    mkdir -p "$OUTDIR"
    ls -ld "$OUTDIR"

    echo "Hello from Slurm on $(hostname) at $(date)" > "$OUTDIR/hello.txt"
    ls -l "$OUTDIR/hello.txt"
    """)

    jobid = submit_sbatch_inline(conn, slurm_script)
    logger.info("[linmos] Submitted smoketest Slurm job: jobid=%s", jobid)


def main(argv=None) -> None:
    args = parse_args(argv)
    rootdir = Path(args.rootdir)

    # args.sbid_groups is already parsed into list[list[int]]
    sbid_groups: List[List[int]] = args.sbid_groups

    if not args.setonix_user:
        raise SystemExit("[linmos] Missing Setonix username. Provide --setonix-user or set SETONIX_USER.")
    if not args.ssh_key:
        raise SystemExit("[linmos] Missing SSH key path. Provide --ssh-key or set SSH_KEY.")

    logger.info(
        "[linmos] Starting linmos stage (checks only): rootdir=%s groups=%s",
        rootdir,
        sbid_groups,
    )

    passphrase = args.ssh_passphrase.strip() if args.ssh_passphrase else None

    conn = SetonixConnection(
        host=args.setonix_host,
        user=args.setonix_user,
        key_path=args.ssh_key,
        passphrase=passphrase,
        port=args.ssh_port,
        timeout_s=args.timeout_s,
    )

    info = check_slurm_access(conn)

    logger.info("[linmos] Remote identity:\n%s", info.get("identity", ""))
    logger.info("[linmos] sbatch:\n%s", info.get("sbatch_version", ""))

    squeue_head = info.get("squeue_head", "")
    if squeue_head:
        logger.info("[linmos] squeue (head):\n%s", squeue_head)

    sacct_head = info.get("sacct_head", "")
    if sacct_head:
        logger.info("[linmos] sacct (head):\n%s", sacct_head)

    logger.info("[linmos] Done.")

    if args.submit_test_job:
        submit_test_job(conn=conn, testdir=args.testdir)


if __name__ == "__main__":
    main()
