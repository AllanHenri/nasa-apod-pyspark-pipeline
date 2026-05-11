import json 
from pathlib import Path
import requests

from src.config import NASA_API_KEY, START_DATE, END_DATE, RAW_OUTPUT

def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def run_extraction() -> None:
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "api_key": NASA_API_KEY,
        "start_date": START_DATE,
        "end_date": END_DATE,
              }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    ensure_parent(RAW_OUTPUT)
    with open(RAW_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Raw data saved to {RAW_OUTPUT}")