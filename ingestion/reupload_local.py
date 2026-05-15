"""
Re-upload locally cached parquet files to GCS.
Used when download succeeded but upload failed due to network issues.
Run this BEFORE running the full ingestion again to avoid re-downloading.
"""

import os
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
from google.cloud import storage

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
LOCAL_DIR = Path("ingestion/raw_downloads")
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def reupload_file(local_path: Path, gcs_path: str) -> bool:
    """Upload a single file with chunked resumable upload."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    # 8MB chunks for stability on slow/unstable connections
    blob.chunk_size = 8 * 1024 * 1024

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"Uploading {local_path.name} (attempt {attempt}/{max_attempts})")
            blob.upload_from_filename(str(local_path), timeout=600)

            size_mb = local_path.stat().st_size / (1024 * 1024)
            logger.success(f"✅ Uploaded {size_mb:.1f}MB → gs://{BUCKET_NAME}/{gcs_path}")
            return True

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                wait = 15 * attempt
                logger.info(f"Waiting {wait}s before retry...")
                time.sleep(wait)

    logger.error(f"❌ Failed to upload {local_path.name} after {max_attempts} attempts")
    return False


def reupload_all():
    """
    Re-upload all parquet files found in raw_downloads/.
    Used when download succeeded but upload failed.
    """
    upload_map = {
        "yellow_tripdata_2025-11.parquet": "raw/trips/year=2025/month=11/trips_2025-11.parquet",
        "yellow_tripdata_2025-12.parquet": "raw/trips/year=2025/month=12/trips_2025-12.parquet",
        "yellow_tripdata_2026-01.parquet": "raw/trips/year=2026/month=01/trips_2026-01.parquet",
    }

    results = {"success": [], "failed": []}

    for filename, gcs_path in upload_map.items():
        local_path = LOCAL_DIR / filename

        if not local_path.exists():
            logger.warning(f"File not found locally, skipping: {filename}")
            continue

        success = reupload_file(local_path, gcs_path)

        if success:
            results["success"].append(filename)
            local_path.unlink()
            logger.info(f"Local file cleaned up: {filename}")
        else:
            results["failed"].append(filename)

    logger.info(f"\n{'='*50}")
    logger.info("RE-UPLOAD SUMMARY")
    logger.info(f"{'='*50}")
    for f in results["success"]:
        logger.info(f"✅ {f}")
    for f in results["failed"]:
        logger.info(f"❌ {f}")


def download_and_upload_single(year: str, month: str):
    """
    Re-download and upload a single TLC month.
    Used for targeted recovery when one month fails in the full pipeline run.
    """
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    local_path = LOCAL_DIR / filename

    logger.info(f"Downloading {filename}...")

    response = requests.get(url, stream=True, timeout=600)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = local_path.stat().st_size / (1024 * 1024)
    logger.success(f"Downloaded {size_mb:.1f}MB → {local_path.name}")

    gcs_path = f"raw/trips/year={year}/month={month}/trips_{year}-{month}.parquet"
    success = reupload_file(local_path, gcs_path)

    if success:
        local_path.unlink()
        logger.success(f"TLC {year}-{month} fully ingested and cleaned up.")
    else:
        logger.error("Upload failed — file retained locally for manual retry.")


if __name__ == "__main__":
    # Targeting January 2026 specifically — the only failed month
    download_and_upload_single("2026", "01")