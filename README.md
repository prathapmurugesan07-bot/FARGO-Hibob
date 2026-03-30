# HiBob Repo

This folder contains the HiBob-specific code and assets extracted from the main workspace.

## Contents

- `src/hibob_client.py`
- `src/hibob_test.py`
- `src/azure_ingest_hibob.py`
- `src/test_all_endpoints.py`
- `Hibob_Ingestion.ipynb`
- `hibob_api_reference.csv`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### Environment variables
The ingestion flow now uploads both the raw and normalized HiBob snapshots. Make sure the following variables are defined in `.env` (matching `.env.example`):

- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_KEY`
- `AZURE_RAW_CONTAINER_NAME` (where raw snapshots live; default `raw`)
- `AZURE_HIBOB_CONTAINER_NAME` (where normalized snapshots go; default `hibob`)

## Run

```bash
python src/hibob_test.py
python src/azure_ingest_hibob.py
python src/test_all_endpoints.py
```
