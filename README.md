# 🛰️ WALLABY Milky Way Pipeline
==============================

**A Python (Prefect) pipeline for combining WALLABY Milky Way observations with Parkes HI4PI single dish observations, and performing source finding, for generating a source catalog of Milky Way data products. This has been written to run on the [CANFAR science platform](https://www.canfar.net/science-portal/).**

This repository contains:

*   Stage-by-stage processing code (`src/wallaby_mw/stages`)
    
*   Utility modules for CASDA, CANFAR/Skaha, manifests, checksums, and config handling
    
*   Prefect-based orchestration (`flows/`)
    
*   Container definitions for each stage
    
*   Configuration files (`configs/`)    

The pipeline is intended to be run on CANFAR, but the ability to runs scripts and build containers locally is supported.

## 📦 Features
-----------
    
*   **Containerized execution:** Each stage runs inside a dedicated Docker/OCI container for reproducibility.
    
*   **Skaha/CANFAR integration:** Pipeline stages can be submitted as remote compute jobs using the Skaha API.
    
*   **Prefect orchestration:** End-to-end workflow execution with UI visualisation, live remote logs, and easy future expansion.
    
*   **Modular stages:** Additional processing steps (HI4PI merge, sub-FITS generation, reprojection, etc.) can be added without modifying core framework code. 

## ➡️ Stages
-----------
*   **CASDA Download** 

## 🚀 Getting Started
==================

### 1\. Clone the repository
------------------------