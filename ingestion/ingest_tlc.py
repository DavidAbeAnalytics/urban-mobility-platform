"""
NYC TLC Yellow Taxi Ingestion
Source: NYC Taxi & Limousine Commission
URL Pattern: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
Coverage: November 2025, December 2025, January 2026
"""

import requests
import pandas as pd
from pathlib import Path
from loguru import logger
from ingestion.base_ingestion import BaseIngestion


# Why this URL? The TLC CloudFront CDN is the official distribution endpoint.
# It's faster and more stable than the NYC Open Data portal for bulk downloads.
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


class TLCIngestion(BaseIngestion):

    def __init__(self):
        super().__init__(source_name="trips")

    def fetch(self, year: str, month: str) -> Path:
        """
        Download the monthly TLC Parquet file.
        Files are typically 40-60MB each — manageable for local download.
        """
        url = f"{TLC_BASE_URL}/yellow_tripdata_{year}-{month}.parquet"
        local_path = self.local_dir / f"yellow_tripdata_{year}-{month}.parquet"

        logger.info(f"Fetching TLC data from: {url}")

        response = requests.get(url, stream=True, timeout=180)

        # Raise immediately if URL returns 404 — data not published yet
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)

        size_mb = downloaded / (1024 * 1024)
        logger.success(f"Downloaded {size_mb:.1f} MB → {local_path.name}")
        return local_path

    def process(self, local_path: Path, year: str, month: str) -> dict:
        """
        Read the Parquet file and extract basic stats for metadata tagging.
        We do NOT transform data here — raw means raw.
        The only goal is to confirm the file is readable and log what arrived.
        """
        logger.info(f"Validating TLC parquet file...")

        df = pd.read_parquet(local_path)

        # Basic sanity checks
        row_count = len(df)
        column_count = len(df.columns)
        columns = list(df.columns)

        # Confirm critical columns exist — if these are missing, pipeline should fail loudly
        required_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "trip_distance"
        ]

        missing = [col for col in required_columns if col not in columns]
        if missing:
            raise ValueError(f"Schema validation failed — missing columns: {missing}")

        logger.success(f"TLC validation passed: {row_count:,} rows | {column_count} columns")

        return {
            "row_count": str(row_count),
            "column_count": str(column_count),
            "data_year": year,
            "data_month": month,
            "schema_validated": "true"
        }


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    # Run for all three months in your project window
    months = [("2025", "11"), ("2025", "12"), ("2026", "01")]

    ingestion = TLCIngestion()
    for year, month in months:
        ingestion.run(year, month)