# 🧩 WALLABY Milky Way Pipeline - /stages

This folder contains the individual processing stages that make up the WALLABY Milky Way data pipeline.

Each stage is implemented as a standalone Python module and is designed to:

*   Run independently in a container
    
*   Receive input via CLI arguments or environment variables
    
*   Write outputs and a manifest JSON
    
*   Be orchestrated by Prefect
    
*   Support execution either locally or on CANFAR Skaha

## Stages

### `casda_download.py`

The first stage of the pipeline:

*   Authenticates to CASDA using environment variables

*   Submits an ADQL query to the CASDA TAP service

*   Stages and downloads FITS cubes for the configured SBIDs

*   Verifies MD5 checksums

*   Writes a manifest (`manifest.json`) under each SBID directory

*   Designed to run in a container (`wallaby-mw-casda`)

This stage is triggered via Prefect by the `_run_casda()` wrapper in `flows/wallaby_mw.py`. 

### `run_linmos.py`

Test job - git bash and filepath issue

```bash
python -m wallaby_mw.stages.run_linmos   --rootdir /scratch/ja3/mguest/wallaby_mw/pipeline/outputs   --sbid-groups "[66866 67022]"   --submit-test-job   --testdir "//scratch//ja3//mguest//wallaby_mw//pipeline//outputs//paramiko"
```

#### Windows / Git Bash users
When passing POSIX paths (e.g. /scratch/...) to remote jobs,
disable MSYS path conversion:

    MSYS2_ARG_CONV_EXCL="*" python -m wallaby_mw.stages.run_linmos ...

<details>
  <summary>Outputs</summary> 

  $${\color{red}COMING \space SOON}$$
</details>

<details>
  <summary>🧪 Testing</summary> 

  $${\color{red}COMING \space SOON}$$
</details>
