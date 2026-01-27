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

from prefect import flow, task

from wallaby_mw.utils.config import (
    load_credentials,
    load_pipeline_config,
    export_env_from_creds,
)

from wallaby_mw.utils.canfar import (
    start_session,
    submit_job,
    live_logs,
)

# Function to parse arguments 
def parse_args(argv=None):

    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Run WALLABY Milky Way Single Dish Combination Pipeline"
    )

    parser.add_argument(
        "--config", 
        type=str, 
        required=True, 
        help="Path to the config.ini file"
    )

    return parser.parse_args(argv)

# Function to run general setup
def _setup(config_path: str):
    config = load_pipeline_config(path=config_path) 
    creds = load_credentials(path=config['pipeline']['credentials']) 
    export_env_from_creds(creds=creds)
    return config

# Generic function for submitting prefect tasks
def _submit_task(
    pipeline_cfg: configparser.ConfigParser,
    section: str,
    env: dict[str, str] | None = None,
    fmt: dict[str, str] | None = None,
    ) -> str:
    """
    Generic function to submit CANFAR as Prefect tasks. 

    Parameters
    ----------
    pipeline_cfg (ConfigParser):
    section (str): The name of the section in config.ini, e.g. "casda", "hi4pi"
    env (dict): 
    """
    rootdir = pipeline_cfg["pipeline"]["rootdir"]
    sbids = pipeline_cfg["pipeline"]["sbids"]
    stage = pipeline_cfg[section]

    timeout = stage.getint("timeout", fallback=120)
    image = stage["image"]
    cmd = stage.get("cmd", "python")

    # Base placeholders
    format_values = dict(stage)
    format_values.update({
        "rootdir": rootdir,
        "sbids": sbids,
    })

    # Add/override placeholders for this specific call
    if fmt:
        format_values.update(fmt)

    args = stage["args"].format(**format_values)
    cores = stage.getint("cores", 2)
    ram = stage.getint("ram", 8)

    name = f"{section}-{format_values.get('sbid', sbids).replace(' ', '-')}"

    session = start_session(timeout=timeout)

    session_id = submit_job(
        session=session,
        name=name,
        image=image,
        cmd=cmd,
        args=args,
        cores=cores,
        ram=ram,
        env=env if env else None
    )

    print(f"Launched session for [{section}]", session_id)
    final_status = live_logs(session=session, session_id=session_id)
    print(f"[{section}] step finished with final status: {final_status}")

    return session_id

def _run_casda(
    pipeline_cfg: configparser.ConfigParser
    ) -> str:
    env = {
        "CASDA_USERNAME": os.environ["CASDA_USERNAME"],
        "CASDA_PASSWORD": os.environ["CASDA_PASSWORD"],
        } 
    casda_session = _submit_task(pipeline_cfg=pipeline_cfg, section="casda", env=env)
    return casda_session

def _run_subfits(
    pipeline_cfg: configparser.ConfigParser,
    sbid: str
    ) -> str:
    subfits_session = _submit_task(pipeline_cfg=pipeline_cfg, section="subfits", env=None, fmt={"sbid": sbid})
    return subfits_session

def _run_hi4pi(
    pipeline_cfg: configparser.ConfigParser,
    sbid: str
    ) -> str:
    hi4pi_session = _submit_task(pipeline_cfg=pipeline_cfg, section="hi4pi", env=None, fmt={"sbid": sbid})
    return hi4pi_session

@task(name="casda")
def casda_task(pipeline_cfg: configparser.ConfigParser) -> str:
    casda_session = _run_casda(pipeline_cfg=pipeline_cfg)
    return casda_session

@task(name="subfits")
def subfits_task(pipeline_cfg: configparser.ConfigParser, sbid: str) -> str:
    subfits_session = _run_subfits(pipeline_cfg=pipeline_cfg, sbid=sbid)
    return subfits_session

@task(name="hi4pi")
def hi4pi_task(pipeline_cfg: configparser.ConfigParser, sbid: str) -> str:
    hi4pi_session = _run_hi4pi(pipeline_cfg=pipeline_cfg, sbid=sbid)
    return hi4pi_session

@flow(name="wallaby-mw-pipeline")
def wallaby_flow(config_path: str) -> str:
    """
    Prefect task wrapper.
    """
    config = _setup(config_path=config_path)

    # Extract sbids
    sbids = config["pipeline"]["sbids"].split()

    # CASDA Step
    run_casda = config.getboolean("casda", "run", fallback=True)

    if run_casda:
        casda_future = casda_task.submit(pipeline_cfg=config)
        casda_future.result()  # wait for CASDA to finish for all SBIDs
    else:
        print(f"[casda] Skipped because config['casda']['run']={config['casda']['run']}.")

    # Subfits Step (runs sbids in parallel)
    run_subfits = config.getboolean("subfits", "run", fallback=True)
    if run_subfits:
        subfits_futures = []
        for sbid in sbids:
            print(f"[subfits] Submitted for sbid={sbid}")
            fut = subfits_task.submit(pipeline_cfg=config, sbid=sbid)
            subfits_futures.append(fut)

        # Wait for all subfits tasks to finish
        for fut in subfits_futures:
            fut.result()   # blocks until the task (i.e. Skaha job) is complete
    else:
        print(f"[subfits] Skipped because config['subfits']['run']={config.get('subfits', 'run', fallback='True')}.")

    # HI4PI Step
    run_hi4pi = config.getboolean("hi4pi", "run", fallback=True)
    if run_hi4pi:
        hi4pi_futures = []
        for sbid in sbids:
            print(f"[hi4pi] Submitted for sbid={sbid}")
            fut = hi4pi_task.submit(pipeline_cfg=config, sbid=sbid)
            hi4pi_futures.append(fut)
        
        for fut in hi4pi_futures:
            fut.result()
    else:
        print(f"[hi4pi] Skipped because config['hi4pi']['run']={config.get('hi4pi', 'run', fallback='True')}.")

def main(argv=None):

    args = parse_args(argv)

    wallaby_flow(args.config)

if __name__ == "__main__":

    main()
