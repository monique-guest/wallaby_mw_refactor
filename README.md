🛰️ WALLABY Milky Way Pipeline
==============================

**A modular, containerized, Prefect-orchestrated workflow for downloading, processing, and combining single-dish and interferometric data products for the WALLABY Milky Way survey.**

This repository contains:

*   Stage-by-stage processing code (src/wallaby\_mw/stages)
    
*   Utility modules for CASDA, CANFAR/Skaha, manifests, checksums, and config handling
    
*   Prefect-based orchestration (flows/)
    
*   Container definitions for each stage
    
*   Configuration files (configs/)
    

The pipeline supports both **local execution** and **remote execution on CANFAR Skaha**, enabling scalable data processing for ASKAP/WALLABY Milky Way observations.

📦 Features
-----------

*   **CASDA download stage**Queries CASDA TAP services, stages data, downloads FITS cubes, verifies MD5 checksums, and writes manifest files.
    
*   **Containerized execution**Each stage runs inside a dedicated Docker/OCI container for reproducibility.
    
*   **Skaha/CANFAR integration**Pipeline stages can be submitted as remote compute jobs using the Skaha API.
    
*   **Prefect orchestration**End-to-end workflow execution with UI visualisation, live remote logs, and easy future expansion.
    
*   **Modular stages**Additional processing steps (HI4PI merge, sub-FITS generation, reprojection, etc.) can be added without modifying core framework code.
    

🚀 Getting Started
==================

1\. Clone the repository
------------------------