"""
Base Ingestion Class
All three data source ingestion scripts inherit from this class.
Provides shared GCS upload, metadata tagging, and logging behaviour.
"""

import os
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
from google.cloud import storage
from google.api_core.retry import Retry
from google.api_core.exceptions import ServiceUnavailable, InternalServerError

load_dotenv()


class BaseIngestion(ABC):
    """
    Abstract base class for all ingestion sources.
    Enforces a consistent interface: every source must implement fetch() and process().
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.bucket_name = os.getenv("GCS_BUCKET_NAME")
        self.pipeline_version = os.getenv("PIPELINE_VERSION", "1.0.0")
        self.environment = os.getenv("ENVIRONMENT", "development")

        # Local staging directory for temporary downloads
        self.local_dir = Path("ingestion/raw_downloads")
        self.local_dir.mkdir(parents=True, exist_ok=True)

        # GCS client — authenticated via GOOGLE_APPLICATION_CREDENTIALS
        self.gcs_client = storage.Client(project=self.project_id)
        self.bucket = self.gcs_client.bucket(self.bucket_name)

        logger.info(
            f"Initialised {self.source_name} ingestion | "
            f"env={self.environment} | version={self.pipeline_version}"
        )

    @abstractmethod
    def fetch(self, year: str, month: str) -> Path:
        """
        Download or fetch data for the given year/month.
        Must return the local Path where data was saved.
        Every subclass MUST implement this method.
        """
        pass

    @abstractmethod
    def process(self, local_path: Path, year: str, month: str) -> dict:
        """
        Perform any lightweight processing before GCS upload.
        Returns a dict of stats (row counts, file size, etc.) for metadata.
        Every subclass MUST implement this method.
        """
        pass

    def upload_to_gcs(self, local_path: Path, gcs_path: str) -> str:
        """
        Upload a local file to GCS using resumable upload with retry logic.

        Why resumable? Files are 60-70MB each. On unstable connections, a simple
        upload_from_filename() will fail mid-transfer and lose all progress.
        Resumable uploads checkpoint progress so a dropped connection resumes
        from where it stopped — not from zero.
        """
        blob = self.bucket.blob(gcs_path)

        # Configure retry policy — retry on transient network errors
        retry_policy = Retry(
            initial=2.0,      # Wait 2 seconds before first retry
            maximum=60.0,     # Never wait more than 60 seconds between retries
            multiplier=2.0,   # Double the wait time each retry (exponential backoff)
            deadline=600.0,   # Give up after 10 minutes total
            predicate=lambda e: isinstance(e, (
                ServiceUnavailable,
                InternalServerError,
                ConnectionError,
                TimeoutError,
            ))
        )

        max_attempts = 5
        attempt = 0

        while attempt < max_attempts:
            attempt += 1
            try:
                logger.info(
                    f"Uploading {local_path.name} → "
                    f"gs://{self.bucket_name}/{gcs_path} "
                    f"(attempt {attempt}/{max_attempts})"
                )

                # 8MB chunks = more checkpoints = safer on unstable connections
                blob.chunk_size = 8 * 1024 * 1024

                blob.upload_from_filename(
                    str(local_path),
                    timeout=300,
                    retry=retry_policy,
                    checksum="md5",  # Verify file integrity after upload
                )

                gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
                logger.success(f"Upload complete: {gcs_uri}")
                return gcs_uri

            except Exception as e:
                logger.warning(f"Upload attempt {attempt} failed: {e}")

                if attempt < max_attempts:
                    wait_seconds = 10 * attempt  # 10s, 20s, 30s, 40s
                    logger.info(f"Waiting {wait_seconds}s before retry...")
                    time.sleep(wait_seconds)
                else:
                    logger.error(
                        f"All {max_attempts} upload attempts failed "
                        f"for {local_path.name}"
                    )
                    raise

    def tag_metadata(self, gcs_path: str, extra_metadata: dict = None):
        """
        Tag GCS object with ingestion metadata.
        This creates an audit trail — every file knows when it arrived,
        from where, and which pipeline version produced it.
        """
        blob = self.bucket.blob(gcs_path)

        metadata = {
            "ingested_at": datetime.utcnow().isoformat(),
            "source": self.source_name,
            "pipeline_version": self.pipeline_version,
            "environment": self.environment,
        }

        # Merge any source-specific metadata
        if extra_metadata:
            metadata.update(extra_metadata)

        blob.metadata = metadata
        blob.patch()
        logger.info(f"Metadata tagged on {gcs_path}")

    def cleanup_local(self, local_path: Path):
        """
        Delete local temporary file after successful GCS upload.
        GCS is the source of truth — local files are just transit buffers.
        """
        if local_path.exists():
            local_path.unlink()
            logger.info(f"Local temp file cleaned up: {local_path.name}")

    def run(self, year: str, month: str):
        """
        Main orchestration method — calls fetch → process → upload → tag → cleanup.
        This is the single entry point every Airflow DAG task will call.
        """
        logger.info(f"{'='*50}")
        logger.info(f"Starting {self.source_name} ingestion for {year}-{month}")
        logger.info(f"{'='*50}")

        try:
            # Step 1: Fetch data
            local_path = self.fetch(year, month)

            # Step 2: Process and get stats
            stats = self.process(local_path, year, month)

            # Step 3: Upload to GCS
            gcs_path = self.build_gcs_path(year, month, local_path.suffix)
            gcs_uri = self.upload_to_gcs(local_path, gcs_path)

            # Step 4: Tag metadata
            self.tag_metadata(gcs_path, extra_metadata=stats)

            # Step 5: Cleanup local file
            self.cleanup_local(local_path)

            logger.success(f"Ingestion complete: {self.source_name} {year}-{month}")
            return {"status": "success", "gcs_uri": gcs_uri, "stats": stats}

        except Exception as e:
            logger.error(
                f"Ingestion failed: {self.source_name} {year}-{month} | Error: {e}"
            )
            raise

    def build_gcs_path(self, year: str, month: str, extension: str) -> str:
        """
        Build the standardised GCS destination path.
        Pattern: raw/{source}/year={year}/month={month}/{source}_{year}-{month}{ext}
        """
        return (
            f"raw/{self.source_name}/"
            f"year={year}/month={month}/"
            f"{self.source_name}_{year}-{month}{extension}"
        )