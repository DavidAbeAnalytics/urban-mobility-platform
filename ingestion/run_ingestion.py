"""
Master Ingestion Runner
Triggers all three data sources for a given month range.
In production this is replaced by the Airflow DAG — but this script
is useful for backfills, manual reruns, and local testing.
"""

import os
from dotenv import load_dotenv
from loguru import logger
from ingestion.ingest_tlc import TLCIngestion
from ingestion.ingest_weather import WeatherIngestion
from ingestion.ingest_mta import MTAIngestion

load_dotenv()


def run_all_sources(months: list):
    """
    Run ingestion for all three sources across all specified months.
    months: list of (year, month) tuples e.g. [("2025", "11"), ("2025", "12")]
    """
    results = []

    sources = [
        ("NYC TLC Trips",  TLCIngestion()),
        ("Weather",        WeatherIngestion()),
        ("MTA Alerts",     MTAIngestion()),
    ]

    for source_name, ingestion in sources:
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting source: {source_name}")
        logger.info(f"{'='*60}")

        for year, month in months:
            try:
                result = ingestion.run(year, month)
                results.append({
                    "source": source_name,
                    "year": year,
                    "month": month,
                    "status": "success",
                    "gcs_uri": result.get("gcs_uri")
                })
            except Exception as e:
                logger.error(f"FAILED: {source_name} {year}-{month} | {e}")
                results.append({
                    "source": source_name,
                    "year": year,
                    "month": month,
                    "status": "failed",
                    "error": str(e)
                })

    # Print summary table
    logger.info(f"\n{'='*60}")
    logger.info("INGESTION SUMMARY")
    logger.info(f"{'='*60}")

    for r in results:
        status_icon = "✅" if r["status"] == "success" else "❌"
        logger.info(f"{status_icon} {r['source']} | {r['year']}-{r['month']} | {r['status']}")

    success_count = sum(1 for r in results if r["status"] == "success")
    total = len(results)
    logger.info(f"\nCompleted: {success_count}/{total} successful")


if __name__ == "__main__":
    # Project window: November 2025 → January 2026
    project_months = [
        ("2025", "11"),
        ("2025", "12"),
        ("2026", "01"),
    ]

    run_all_sources(project_months)