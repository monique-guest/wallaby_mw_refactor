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

# Function to run the CASDA download step
def _run_casda(
    pipeline_cfg: configparser.ConfigParser
) -> str:

    rootdir = pipeline_cfg["pipeline"]["rootdir"]
    sbids = pipeline_cfg["pipeline"]["sbids"]
    stage = pipeline_cfg["casda"]

    image = stage["image"]
    cmd = "python"
    args = (
            "-m wallaby_mw casda-download "
            f"--sbids {sbids} "
            f"--rootdir {rootdir}"
        )
    cores = stage.getint("cores", 2)
    ram = stage.getint("ram", 8)

    session = start_session()

    name = f"casda-{sbids.replace(' ', '-')}"

    session_id = submit_job(
        session=session,
        name=name,
        image=image,
        cmd=cmd,
        args=args,
        cores=cores,
        ram=ram,
        env={
            "CASDA_USERNAME": os.environ["CASDA_USERNAME"],
            "CASDA_PASSWORD": os.environ["CASDA_PASSWORD"],
        },
    )

    print("Launched CASDA session:", session_id)
    final_status = live_logs(session=session, session_id=session_id)
    print(f"CASDA stage finished with final status: {final_status}")

    return session_id

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
