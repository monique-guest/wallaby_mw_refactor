# 🛰️ WALLABY Milky Way Pipeline

**A Python (Prefect) pipeline for combining WALLABY Milky Way observations with Parkes HI4PI single dish observations, and performing source finding, for generating a source catalog of Milky Way data products. This has been written to run on the [CANFAR science platform](https://www.canfar.net/science-portal/).**

This repository contains:

*   Stage-by-stage processing code (`src/wallaby_mw/stages`)
    
*   Utility modules for CASDA, CANFAR/Skaha, manifests, checksums, and config handling
    
*   Prefect-based orchestration (`flows/`)
    
*   Container definitions for each stage
    
*   Configuration files (`configs/`)    

The pipeline is intended to be run on CANFAR, but the ability to runs scripts and build containers locally is supported.

## 📦 Features
    
*   **Containerized execution:** Each stage runs inside a dedicated Docker/OCI container for reproducibility.
    
*   **Skaha/CANFAR integration:** Pipeline stages can be submitted as remote compute jobs using the Skaha API.
    
*   **Prefect orchestration:** End-to-end workflow execution with UI visualisation, live remote logs, and easy future expansion.
    
*   **Modular stages:** Additional processing steps (HI4PI merge, sub-FITS generation, reprojection, etc.) can be added without modifying core framework code. 

## ➡️ Stages

*   **CASDA Download** 

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/monique-guest/wallaby_mw_refactor.git
cd wallaby_mw_refactor
```

### 2. Install the package (editable mode)

Ensure you're using Python 3.10+.

```python
python -m pip install -e .
```

### 3. Install Prefect (if needed)

```bash
pip install prefect
```

### 4. Create the `credentials.ini` file

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
```

### 5. Create the `config.ini` file 

The pipeline uses `configs/config.ini` to determine pipeline settings and feed the necessary parameters
to each stage. 

Example `config.ini`

```ini 
[pipeline]
credentials = path\to\credentials.ini
sbids = 67022 66866
rootdir = /arc/projects/WALLABY_test/mw/mw_pipeline_outputs

[casda]
image = images.canfar.net/srcnet/wallaby-mw-casda
cmd = python
args = -m wallaby_mw casda-download --sbids {sbids} --rootdir {rootdir}
cores = 2
ram = 8
```

The only parameters that should require updating are those in the `[pipeline]` section.
