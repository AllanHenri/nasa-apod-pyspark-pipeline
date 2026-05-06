import os 
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

NASA_API_KEY = os.getenv("NASA_API_KEY", 'DEMO_KEY')
START_DATE = os.getenv("START_DATE", '2026-04-01')
END_DATE = os.getenv("END_DATE", '2026-04-10')

RAW_OUTPUT = BASE_DIR / os.getenv("RAW_OUTPUT","data/raw/NeoWs.json")
PROCESSED_OUTPUT = BASE_DIR / os.getenv("PROCESSED_OUTPUT","data/processed/NeoWs_cleaned")
CURATED_OUTPUT = BASE_DIR / os.getenv("CURATED_OUTPUT","data/curated/NeoWs_metrics")