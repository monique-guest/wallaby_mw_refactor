WALLABY Milky Way Pipeline (wallaby_mw_refactor) — AGENT Context

This document provides high-level, durable context for humans and AI agents working on the WALLABY Milky Way pipeline. It describes intent, structure, execution model, and non-obvious constraints.

============================================================
1. What this project is
============================================================

This repository implements a stage-based, containerised radio astronomy pipeline for processing WALLABY Milky Way data products.

Key goals:
- Process very large WALLABY 4D FITS cubes reproducibly
- Run remotely on CANFAR/Skaha (local runs only for dev/testing on small cutouts)
- Be driven by a single config.ini
- Be modular, restartable, and extendable
- Surface failures clearly (job IDs, logs, error context)

Non-goals:
- Running the full pipeline locally on a normal machine
- Hiding infrastructure details (Skaha jobs and logs are first-class artifacts)

============================================================
2. How to run the pipeline (current)
============================================================

From the repo root:

  python -m flows.wallaby_flow --config configs/config.ini

Notes:
- This submits remote Skaha jobs and blocks until they complete
- Full pipeline execution assumes CANFAR/Skaha
- Local runs are intended only for individual stages on small test data

============================================================
3. Repository layout
============================================================

configs/
  config.ini           Main pipeline configuration
  credentials.ini      Credentials referenced by config.ini
  example.ini          Example config

containers/
  casda_download/      Dockerfile for CASDA download stage
  hi4pi_download/      Dockerfile for HI4PI download stage
  subfits/             Dockerfile for subfits stage
  miriad_script/       Dockerfile for MIRIAD script generation stage

flows/
  wallaby_flow.py      Main Prefect flow

src/wallaby_mw/
  __main__.py          Stage-dispatch CLI (python -m wallaby_mw ...)
  stages/              Stage implementations
  utils/               CANFAR helpers, config, FITS/WCS helpers, errors, etc.

prefect.yaml           Prefect configuration
pyproject.toml         Python package configuration

============================================================
4. Stage CLI entrypoints
============================================================

Stages can be run individually using:

  python -m wallaby_mw <command> [args]

Registered commands:
- casda-download   (alias: casda_download)
- apply-subfits    (alias: apply_subfits)
- hi4pi-download   (alias: hi4pi_download)
- miriad-script    (alias: miriad_script)

Each command maps to wallaby_mw.stages.<stage>.main(argv)

============================================================
5. Data model and output contracts
============================================================

All outputs live under a shared ARC root directory, one directory per SBID:

  {rootdir}/{SBID}/
    casda/cube.fits
    subfits/subfits.fits
    hi4pi/hi4pi.fits
    miriad_script/miriad_script.sh
    miriad/combined.fits

Principles:
- One SBID per directory
- Each stage writes only to its own subdirectory
- Later stages consume outputs from earlier stages

============================================================
6. Stage input/output dependencies
============================================================

CASDA download:
- Inputs: SBID list
- Outputs: casda/cube.fits

Apply subfits:
- Inputs: casda/cube.fits
- Outputs: subfits/subfits.fits

HI4PI download:
- Inputs: casda/cube.fits (for centre/size metadata)
- Outputs: hi4pi/hi4pi.fits

Generate MIRIAD script:
- Inputs:
  - subfits/subfits.fits
  - hi4pi/hi4pi.fits
- Outputs:
  - miriad_script/miriad_script.sh

Run MIRIAD combine:
- Inputs:
  - subfits/subfits.fits
  - hi4pi/hi4pi.fits
  - miriad_script/miriad_script.sh
- Outputs:
  - miriad/combined.fits

============================================================
7. Configuration (config.ini)
============================================================

[pipeline]
- credentials : path to credentials.ini
- sbids       : whitespace-separated SBIDs
- rootdir     : ARC output root directory

Each stage section defines:
- run     : True/False
- timeout : Skaha session timeout
- image   : CANFAR container image
- cmd     : command inside container (usually python)
- args    : CLI args with placeholders {rootdir}, {sbids}, {sbid}
- cores   : CPU cores
- ram     : RAM in GB

============================================================
8. Credentials (credentials.ini)
============================================================

credentials.ini contains all required credentials and paths:

[CASDA]
username = <email>
password = <password>

[Harbor]
username = <registry username>
secret  = <registry secret>

[CANFAR]
cadc_cert = path\to\.ssl\cadcproxy.pem

These values are loaded and exported as environment variables before
submitting Skaha jobs.

Future note:
- These may eventually be migrated to Prefect Secrets or Blocks.
- Storing secrets in plain-text ini files is not ideal long-term.

============================================================
9. Orchestration model (Prefect + Skaha)
============================================================

The Prefect flow:
- Loads config and credentials
- Exports required env vars
- Loops over SBIDs at the flow level
- Submits Skaha headless jobs per stage
- Streams logs and waits for terminal status

Current logical ordering:
1. CASDA download
2. Apply subfits (per-SBID parallel)
3. HI4PI download (per-SBID parallel)
4. Generate MIRIAD script (depends on subfits + hi4pi)
5. Run MIRIAD combine (depends on generated script)

Planned improvement:
- Run subfits and HI4PI concurrently once CASDA completes
- Represent per-SBID DAG explicitly in Prefect

============================================================
10. CANFAR / Skaha integration
============================================================

Key helpers live in wallaby_mw.utils.canfar:

- start_session(timeout)
- submit_job(...)
- live_logs(session, session_id)

Jobs are submitted as headless Skaha sessions.
Job ID and logs are first-class debugging artifacts.

============================================================
11. MIRIAD scratch-path requirement (CRITICAL)
============================================================

MIRIAD uses fixed-length Fortran strings and fails on long paths.

Symptoms:
- GETFIELD: string too long
- XYOPEN error

Required workaround (implemented):
- All MIRIAD datasets and intermediates live under:

    /scratch/wallaby_mw/{SBID}/

- Inputs are symlinked into scratch
- Only final FITS is written back to ARC

Scratch behavior:
- /scratch is available in Skaha jobs
- Directory structure is created as needed
- Scratch is wiped automatically when the job ends

============================================================
12. Invariants and assumptions
============================================================

- Full pipeline runs on CANFAR/Skaha
- Local runs are for development only
- Stages should be independently runnable and restartable
- Scratch is mandatory for MIRIAD
- Large ARC paths must be treated carefully

============================================================
13. Known gaps and planned upgrades
============================================================

- Map remote Skaha job state to Prefect task state
- Split submit-job vs wait-job tasks
- Add retries and backoff for submission and polling
- Add resume/force semantics with per-stage success markers
- Add FITS-aware validation of outputs
- Add concurrency controls (max concurrent SBIDs)
- Improve structured logging and metrics
