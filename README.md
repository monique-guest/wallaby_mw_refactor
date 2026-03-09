# 🛰️ WALLABY Milky Way Pipeline

**A Python (Prefect) pipeline for combining WALLABY Milky Way observations with Parkes HI4PI single dish observations, and performing source finding, for generating a source catalog of Milky Way data products. This has been written to run on the [CANFAR science platform](https://www.canfar.net/science-portal/).**

This repository contains:

*   Stage-by-stage processing code (`src/wallaby_mw/stages`)
    
*   Utility modules for CASDA, CANFAR/Skaha, manifests, checksums, and config handling
    
*   Prefect-based orchestration (`flows/`)
    
*   Container definitions for each stage
    
*   Configuration files (`configs/`)    

The pipeline is intended to be run on CANFAR, but the ability to runs scripts and build containers locally is supported.

See AGENT.md for authoritative pipeline architecture, execution model, and constraints.

## 📦 Features
    
*   **Containerized execution:** Each stage runs inside a dedicated Docker/OCI container for reproducibility.
    
*   **Skaha/CANFAR integration:** Pipeline stages can be submitted as remote compute jobs using the Skaha API.
    
*   **Prefect orchestration:** End-to-end workflow execution with UI visualisation, live remote logs, and easy future expansion.
    
*   **Modular stages:** Additional processing steps (HI4PI merge, sub-FITS generation, reprojection, etc.) can be added without modifying core framework code. 

## ➡️ Stages

*   **CASDA Download** 

## 🚀 Running the Pipeline on CANFAR

### 1. Clone the repository

```bash
git clone https://github.com/monique-guest/wallaby_mw_refactor.git
cd wallaby_mw_refactor
```
### 2. Create a Python virtual environment

```bash
python -m venv .venv
```
Then activate the virtual environment.

```bash
source .venv/bin/activate
```

### 3. Install the package (editable mode)

Ensure you're using Python 3.10+ and your virtual environment is activated.

```python
python -m pip install -e .
```

### 4. Install Prefect (if needed)

```bash
pip install prefect
```
$${\color{red}INSERT \space PREFECT \space SERVER \space INFO}$$

### 5. Create the `credentials.ini` file

The pipeline uses `configs/credentials.ini` to read and inject the credentials for CASDA, Harbor and CANFAR 
to environment variables and pass them to pipeline stages as needed. This allows the authentication and use 
of these tools, and is essential for the pipeline to run. 

Example `credentials.ini`

```ini
[CASDA]
username = name@email.com
password = password

[Harbor]
username = user.name
secret = secret 

[CANFAR]
cadc_cert = path\to\.ssl\cadcproxy.pem 

[Setonix]
host = setonix.pawsey.org.au
username = username
ssh_key = ~/path/to/.ssh/key.pem
passphrase = passphrase
```

### 6. Create the `config.ini` file 

The pipeline uses `configs/config.ini` to determine pipeline settings and feed the necessary parameters
to each stage. 

Example `config.ini`

```ini 
[pipeline]
credentials = path\to\wallaby_mw\configs\credentials.ini
sbids = 67022 66866
rootdir = /arc/projects/WALLABY_test/mw/mw_pipeline_outputs
canfar_log_level = WARNING

[casda]
run = False
timeout = 300
image = images.canfar.net/srcnet/wallaby-mw-casda
logging = INFO
cmd = python
args = -m wallaby_mw casda-download --sbids {sbids} --rootdir {rootdir} --log-level {logging}
cores = 2
ram = 8

[subfits]
run = True
timeout = 300
image = images.canfar.net/srcnet/wallaby-mw-subfits:latest
cmd = python
args = -m wallaby_mw apply-subfits --sbid {sbid} --rootdir {rootdir}
cores = 4
ram = 32

[hi4pi]
run = True
timeout = 300
image = images.canfar.net/srcnet/wallaby-mw-hi4pi:latest
logging = INFO
url = https://cdsarc.u-strasbg.fr/ftp/J/A+A/594/A116/CUBES/EQ2000/SIN/
catalog = J/A+A/594/A116/cubes_eq
vizier_query_width = 20.0
cmd = python
args = -m wallaby_mw hi4pi-download --rootdir {rootdir} --sbid {sbid} --width {vizier_query_width} --url {url} --catalog {catalog} --log-level {logging} --insecure
cores = 1
ram = 4

[miriad_script]
run = True
timeout = 300
image = images.canfar.net/srcnet/wallaby-mw-miriad-script:latest
imsub_region = 630,630,3960,3940
cmd = python
args = -m wallaby_mw miriad-script --rootdir {rootdir} --sbid {sbid} --imsub_region {imsub_region}
cores = 1
ram = 4

[miriad]
run = True
timeout = 300
image = images.canfar.net/srcnet/miriad:dev
cmd = /bin/csh
args = {rootdir}/{sbid}/miriad_script/miriad_script.sh
cores = 4
ram = 32
```

The only parameters that should require updating are those in the `[pipeline]` section. The pipeline 
checks if $${\color{red}INSERT \space FILES}$$ already exist, and if they do it skips 
downloading them. If you want files to be re-downloaded, move or delete them in CANFAR.

<details>
  <summary>Parameter Descriptions</summary>

  | Parameter   | Description | Section/s  |
  |-------------|-------------|------------|
  | credentials |             | [pipeline] |
  | sbids       |             | [pipeline] |
  | rootdir     |             | [pipeline] |
  | image       |             |            |
  | cmd         |             |            |
  | args        |             |            |
  | cores       |             |            |
</details>

### 7. Run the pipeline

Use the following command to run the pipeline. The only input parameter required is the path to 
your `config.ini` file.

```bash
python -m flows.wallaby_flow --config configs/config.ini
```

This will:

1.  Load configs
    
2.  Load credentials
    
3.  Export env variables for CASDA/Skaha auth
    
4.  Submit the CASDA stage to Skaha
    
5.  Stream live logs in your terminal
    
6.  Display flow + task execution in the Prefect UI

## 🐳 Building and Pushing Containers (Makefile)

The repo includes a `Makefile` to simplify building, tagging, and pushing the stage containers.

### Single image

```powershell
make casda
make subfits
make hi4pi
make miriad
```

### Multiple images

```powershell
make casda hi4pi
```

### All images

```powershell
make all
```

### Override registry and tags

```powershell
make casda REGISTRY=images.canfar.net/yourproject DEV_TAG=dev PUSH_TAG=latest
```

## 🧠 Directory Structure

```graphql
wallaby_mw_refactor/
│
├── configs/                # Pipeline + credentials config
│   ├── config.ini
│   ├── credentials.ini
│   └── example.ini
│
├── containers/
│   └── casda_download/     # Dockerfile for CASDA stage container
│
├── flows/                  # Prefect orchestration layer
│   ├── __init__.py
│   └── wallaby_mw.py
│
├── src/                    # Main Python package
│   └── wallaby_mw/
│       ├── stages/         # Individual processing stages
│       ├── utils/          # Shared helper modules
│       ├── __init__.py
│       └── __main__.py
│
├── prefect.yaml            # Prefect configuration
├── pyproject.toml          # Package metadata and dependencies
└── README.md               # This file
```

## 🚀 Testing the Pipeline Locally

$${\color{red}INSERT \space INSTRUCTIONS}$$
