# 🧰 `wallaby_mw.utils` — Utility Modules

This package contains shared helper modules used across the WALLABY Milky Way pipeline.  
They provide common functionality for:

- Authentication
- CANFAR / Skaha integration
- Configuration handling
- File and checksum operations
- Error types
- Manifest creation

These modules are intentionally **Prefect-free** and **stage-agnostic**, so they can be reused by any stage or flow.

## 📁 Modules Overview

### `auth.py`

Helpers for CASDA authentication and keyring setup.

Typical responsibilities:

- Reading CASDA credentials from config / environment
- Setting up a simple keyring backend for non-interactive use
- Logging in to CASDA via `astroquery.casda.Casda`

Example usage:

```python
INSERT CODE EXAMPLE
```
This keeps CASDA-specific auth logic out of the pipeline orchestration code.

### `canfar.py`

Thin wrappers around the CANFAR / Skaha Python client.
    
Responsibilities:
    
*   Creating a configured Session client using registry credentials
    
*   Submitting jobs to Skaha
    
*   Polling for session status
    
*   Fetching logs from a session
    
*   Convenience helpers used by Prefect tasks
    
*   Example usage:

```python
from wallaby_mw.utils.canfar import start_session, submit_job, live_logs

session = start_session()
session_id = submit_job(
    session=session,
    name="casda-66866",
    image="images.canfar.net/srcnet/wallaby-mw-casda",
    cmd="python",
    args="-m wallaby_mw casda-download --sbids 66866 --rootdir /arc/...",
    cores=2,
    ram=8,
    env={},
)
final_status = live_logs(session=session, session_id=session_id)
```

The goal is to keep all Skaha interaction in this module, not scattered throughout the codebase.

### `checksums.py`

Utilities for computing and validating file checksums (currently MD5).
    
Responsibilities:
    
*   Calculating MD5 checksum of a file on disk
    
*   Reading expected checksum values from .checksum files
    
*   Helper logic used by stages to verify data integrity
    
Example usage:

```python
from wallaby_mw.utils.checksums import md5sum, read_checksum_file

expected = read_checksum_file("/path/image.fits.checksum")
actual = md5sum("/path/image.fits")

if expected != actual:
    raise ValueError("Checksum mismatch")
```

### `config.py`

Helpers for working with pipeline and credentials configuration files.

Responsibilities:

*   Loading `config.ini` (pipeline configuration)

*   Loading `credentials.ini` (auth details)

*   Exporting relevant values to environment variables for downstream code

Example usage:

```python
from wallaby_mw.utils.config import (
    load_pipeline_config,
    load_credentials,
    export_env_from_creds,
)

cfg = load_pipeline_config("configs/config.ini")
creds = load_credentials(cfg["pipeline"]["credentials"])
export_env_from_creds(creds)
```

This keeps configuration loading consistent across flows and scripts.

### `errors.py`

Custom exception types used throughout the pipeline.

Typical classes:

*   WallabyPipelineError (base class)

*   CasdaError

*   CasdaAuthError

*   CasdaStagingError

*   CasdaTapJobError

*   Any other stage-specific or system-level error types

These allow stages and flows to:

*   Distinguish between different failure modes

*   Catch and handle known error types more cleanly

*   Provide clearer log messages and exit codes

Example usage:

```python
from wallaby_mw.utils.errors import WallabyPipelineError, CasdaTapJobError

try:
    # run stage
except CasdaTapJobError as e:
    logging.error(f"TAP job failed: {e}")
    raise
except WallabyPipelineError as e:
    logging.error(f"Pipeline error: {e}")
    raise
```

### `files.py`

General filesystem helpers.

Example usage:

```python
from wallaby_mw.utils.files import filename_from_url

fname = filename_from_url("https://example/path/image.fits")
```

This module is focused on generic file operations, not stage-specific logic.

### `manifest.py`

Helpers for writing and updating manifest JSON files that track stage outputs and metadata.

Responsibilities:

*   Providing a standard schema for stage manifests

*   Writing JSON to disk with safe overwrites

*   Basic timestamp utilities (e.g., utc_now_iso())

Typically, each stage builds a Python dict describing:

*   stage name

*   SBID(s)

*   inputs / outputs

*   checksums

*   timestamps

Then calls write_manifest(path, data)

Example usage:

```python
from wallaby_mw.utils.manifest import write_manifest, utc_now_iso

manifest = {
    "sbid": 66866,
    "stage": "casda_download",
    "updated_utc": utc_now_iso(),
    "outputs": {...},
    "checksums": [...],
}
write_manifest("/arc/.../66866/manifest.json", manifest)
```
