from src.extract import run_extraction
from src.transform import run_transformation


def run() -> None:
    """
    Executes the full batch pipeline:
    1. Extract raw data from NASA NeoWs API
    2. Transform raw JSON into analytical Parquet datasets
    """
    print("Starting extraction...")
    run_extraction()

    print("Starting transformation...")
    run_transformation()

    print("Batch pipeline finished successfully.")


if __name__ == "__main__":
    run()