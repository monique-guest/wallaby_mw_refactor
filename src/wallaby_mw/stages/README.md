# 🧩 WALLABY Milky Way Pipeline - /stages

This folder contains the individual processing stages that make up the WALLABY Milky Way data pipeline.

Each stage is implemented as a standalone Python module and is designed to:

## Stages

<details>
  <summary>### casda_download.py</summary> 
  
  #### Overview
  
  The first stage of the pipeline:
  
  *   Authenticates to CASDA using environment variables
  
  *   Submits an ADQL query to the CASDA TAP service
  
  *   Stages and downloads FITS cubes for the configured SBIDs
  
  *   Verifies MD5 checksums
  
  *   Writes a manifest (`manifest.json`) under each SBID directory
  
  *   Designed to run in a container (`wallaby-mw-casda`)
  
  This stage is triggered via Prefect by the `_run_casda()` wrapper in `flows/wallaby_mw.py`. 
  
  #### Outputs
  $${\color{red}COMING SOON}$$
  
  #### 🧪 Testing
  $${\color{red}COMING SOON}$$
