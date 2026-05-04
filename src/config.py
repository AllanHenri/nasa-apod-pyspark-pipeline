import os 
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

NASA_API_KEY = os.getenv("NASA_API_KEY", 'DEMO_KEY')
NASA_API_KEY = os.getenv("NASA_API_KEY", 'DEMO_KEY')
NASA_API_KEY = os.getenv("NASA_API_KEY", 'DEMO_KEY')