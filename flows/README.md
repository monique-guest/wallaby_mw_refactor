# 🚦 Prefect Flows — WALLABY Milky Way Pipeline

This directory contains the **Prefect orchestration layer** for the WALLABY Milky Way pipeline.

Prefect is used to:

- Coordinate multi-stage pipeline execution
- Manage dependencies between stages
- Stream logs from remote Skaha jobs
- Visualize execution in the Prefect UI
- Provide future support for scheduling and fault tolerance

The flow layer does **not** contain scientific logic or data processing code.  
Those belong in `src/wallaby_mw/stages/`.

Instead, this layer:

- Loads configuration files
- Sets up environment variables
- Wraps stage calls as Prefect tasks
- Submits containerized jobs to CANFAR Skaha
- Tracks execution status and logs

## 📁 Files in This Directory

### `wallaby_mw.py`

This is the main orchestration entrypoint. It defines three key concepts:

### 1️⃣ `_setup(config_path)`

Responsible for:

- Loading `config.ini`
- Loading `credentials.ini` referenced in config
- Exporting environment variables:
  - CASDA auth
  - Harbor registry credentials
  - CADC certificate path (if present)

This prepares the environment for all downstream tasks.

### 2️⃣ `_run_<stage>()` functions

Example: 

```python
def _run_casda(cfg):
    env = {
        "CASDA_USERNAME": os.environ["CASDA_USERNAME"],
        "CASDA_PASSWORD": os.environ["CASDA_PASSWORD"],
    }
    return _submit_task(cfg, section="casda", env=env)
```

Each stage:
    
*   Prepares stage-specific environment variables
    
*   Delegates execution to \_submit\_task()
    
*   Returns the Skaha session ID or result
    
These functions **do not** contain scientific logic — they only prepare orchestration parameters. 

### 3️⃣ @task wrappers (Prefect tasks)

Each stage has a Prefect task wrapper: 

```python
@task(name="casda")
def casda_task(cfg):
    return _run_casda(cfg)
```

The wrapper gives Prefect:

*   A named node in the UI

*   Separate logging and runtime tracking

*   Potential retry/backoff policies in the future

Adding a new pipeline stage is as simple as:

*   Creating _run_<stage>()

*   Creating <stage>_task()

*   Adding it in the flow’s sequence

### 4️⃣ wallaby_flow (top-level Prefect flow)

This is the main workflow:

```python
@flow(name="wallaby-mw-pipeline")
def wallaby_flow(config_path: str):
    config = _setup(config_path)
    casda_session = casda_task(config)
    # future steps go here...
```

## 📡 Remote Execution via Skaha

Stages run remotely using helper functions in utils.canfar:

*   start_session()

*   submit_job()

*   live_logs()

These functions:

*   Launch a remote job using a container stored in images.canfar.net

*   Stream logs back to the Prefect task console

*   Wait for terminal states: Succeeded, Failed, or Terminated

The orchestration layer never deals directly with:

*   TAP queries

*   FITS files

*   CASDA downloads

*   Checksum logic

Those belong entirely inside containerized stage modules.

## Running the Flow Locally

Run locally:

```bash
python -m flows.wallaby_mw --config configs/config.ini
```

Prefect UI:

```bash
prefect server start
```

View at:

```cpp
http://127.0.0.1:4200
```

You will see:

*   Flow: wallaby-mw-pipeline

*   Task: casda

*   Real-time Skaha logs

*   Status & runtime metrics
