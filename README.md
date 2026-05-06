# NASA NeoWs PySpark Pipeline

This project extracts Near Earth Objects data from NASA's NeoWs API and transforms the nested JSON structure into a flat analytical dataset using PySpark.

## Pipeline Steps
1. Extract raw JSON from the NeoWs API
2. Save the raw data locally
3. Flatten nested asteroid fields with PySpark
4. Generate processed Parquet output
5. Generate curated monthly metrics

## Tech Stack
- Python
- Requests
- PySpark
- Parquet

## How to Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python -m src.jobs.batch_job