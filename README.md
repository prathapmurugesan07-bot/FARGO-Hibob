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

## Run

```bash
python src/hibob_test.py
python src/azure_ingest_hibob.py
python src/test_all_endpoints.py
```
