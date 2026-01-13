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

    image = stage["image"]
    cmd = stage.get("cmd", "python")
    args = stage["args"].format(sbids=sbids, rootdir=rootdir)
    cores = stage.getint("cores", 2)
    ram = stage.getint("ram", 8)

    name = f"{section}-{sbids.replace(' ', '-')}"

    session = start_session()

    session_id = submit_job(
        session=session,
        name=name,
        image=image,
        cmd=cmd,
        args=args,
        cores=cores,
        ram=ram,
        env=env
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

@task(name="casda")
def casda_task(pipeline_cfg: configparser.ConfigParser) -> str:
    casda_session = _run_casda(pipeline_cfg=pipeline_cfg)
    return casda_session

@flow(name="wallaby-mw-pipeline")
def wallaby_flow(config_path: str) -> str:
    """
    Prefect task wrapper.
    """
    config = _setup(config_path=config_path)
    casda_session = casda_task(pipeline_cfg=config)

def main(argv=None):

    args = parse_args(argv)

    wallaby_flow(args.config)

if __name__ == "__main__":

    main()
