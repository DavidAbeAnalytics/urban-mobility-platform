"""
BigQuery Loader
Loads all three raw GCS sources into BigQuery raw dataset.
Each source gets its own partitioned and clustered table.

Architecture:
    GCS raw/ → BigQuery raw dataset
    No transformations — dbt handles all transformation in Phase 6.

Tables created:
    raw.yellow_taxi_raw     — partitioned by pickup date, clustered by location
    raw.weather_raw         — partitioned by date
    raw.mta_alerts_raw      — partitioned by alert start date
"""

import os
from dotenv import load_dotenv
from loguru import logger
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "raw")


def get_bq_client() -> bigquery.Client:
    """Return an authenticated BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


# ─────────────────────────────────────────────
# TLC TRIPS LOADER
# ─────────────────────────────────────────────

def load_tlc_to_bigquery(year: str, month: str):
    """
    Load TLC yellow taxi parquet from GCS into BigQuery raw.yellow_taxi_raw.

    Why partition by tpep_pickup_datetime?
    Every analytical query will filter by date — "show me trips in December",
    "compare weekday vs weekend demand". Partitioning means BigQuery only
    reads the relevant date partitions instead of scanning all 12M rows.
    This directly reduces cost and query time.

    Why cluster by PULocationID and DOLocationID?
    The core business questions involve zones — "which zones surge during
    subway disruptions?" Clustering physically co-locates rows with the same
    location IDs, making zone-based filters dramatically faster.
    """
    client = get_bq_client()

    gcs_uri = (
        f"gs://{BUCKET_NAME}/raw/trips/"
        f"year={year}/month={month}/"
        f"trips_{year}-{month}.parquet"
    )

    table_id = f"{PROJECT_ID}.{BQ_DATASET_RAW}.yellow_taxi_raw"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,

        # Partition by pickup datetime — enables date-based partition pruning
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="tpep_pickup_datetime",
        ),

        # Cluster by pickup and dropoff zone for fast location-based queries
        clustering_fields=["PULocationID", "DOLocationID"],

        # WRITE_APPEND — each monthly load adds to the table, not overwrites
        # This is critical for incremental loading across Nov, Dec, Jan
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

        # Autodetect schema from Parquet — TLC schema is well-defined
        autodetect=True,
    )

    logger.info(f"Loading TLC {year}-{month} → {table_id}")
    logger.info(f"Source: {gcs_uri}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for job to complete

    table = client.get_table(table_id)
    logger.success(
        f"✅ TLC {year}-{month} loaded | "
        f"Total rows in table: {table.num_rows:,}"
    )
    return table.num_rows


# ─────────────────────────────────────────────
# WEATHER LOADER
# ─────────────────────────────────────────────

def load_weather_to_bigquery(year: str, month: str):
    """
    Load Open-Meteo hourly weather parquet from GCS into BigQuery raw.weather_raw.

    Why partition by datetime_hour?
    Weather joins to taxi trips on the hour of pickup. Partitioning by date
    means queries like "show me all rainy hours in November" scan only
    November partitions — not the full weather table.
    """
    client = get_bq_client()

    gcs_uri = (
        f"gs://{BUCKET_NAME}/raw/weather/"
        f"year={year}/month={month}/"
        f"weather_{year}-{month}.parquet"
    )

    table_id = f"{PROJECT_ID}.{BQ_DATASET_RAW}.weather_raw"

    # Explicit schema for weather — more reliable than autodetect
    # for timestamp columns which can be misinterpreted
    schema = [
        bigquery.SchemaField("datetime_hour", "TIMESTAMP"),
        bigquery.SchemaField("temperature_2m", "FLOAT64"),
        bigquery.SchemaField("precipitation", "FLOAT64"),
        bigquery.SchemaField("snowfall", "FLOAT64"),
        bigquery.SchemaField("windspeed_10m", "FLOAT64"),
        bigquery.SchemaField("weathercode", "INT64"),
        bigquery.SchemaField("visibility", "FLOAT64"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("timezone", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="datetime_hour",
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,  # Use explicit schema above
    )

    logger.info(f"Loading weather {year}-{month} → {table_id}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    table = client.get_table(table_id)
    logger.success(
        f"✅ Weather {year}-{month} loaded | "
        f"Total rows in table: {table.num_rows:,}"
    )
    return table.num_rows


# ─────────────────────────────────────────────
# MTA ALERTS LOADER
# ─────────────────────────────────────────────

def load_mta_to_bigquery(year: str, month: str):
    """
    Load MTA subway alerts parquet from GCS into BigQuery raw.mta_alerts_raw.

    No partitioning by time here — MTA alert volume is small (45 records/month)
    so partitioning would add overhead without benefit. This is a deliberate
    cost optimisation decision — partitioning only helps when tables are large
    enough that partition pruning saves meaningful scan costs.
    """
    client = get_bq_client()

    gcs_uri = (
        f"gs://{BUCKET_NAME}/raw/mta_alerts/"
        f"year={year}/month={month}/"
        f"mta_alerts_{year}-{month}.parquet"
    )

    table_id = f"{PROJECT_ID}.{BQ_DATASET_RAW}.mta_alerts_raw"

    schema = [
        bigquery.SchemaField("alert_id", "STRING"),
        bigquery.SchemaField("subway_line", "STRING"),
        bigquery.SchemaField("cause", "STRING"),
        bigquery.SchemaField("effect", "STRING"),
        bigquery.SchemaField("header_text", "STRING"),
        bigquery.SchemaField("start_time", "STRING"),
        bigquery.SchemaField("end_time", "STRING"),
        bigquery.SchemaField("ingested_at", "STRING"),
        bigquery.SchemaField("is_synthetic", "BOOLEAN"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )

    logger.info(f"Loading MTA alerts {year}-{month} → {table_id}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    table = client.get_table(table_id)
    logger.success(
        f"✅ MTA alerts {year}-{month} loaded | "
        f"Total rows in table: {table.num_rows:,}"
    )
    return table.num_rows


# ─────────────────────────────────────────────
# MASTER LOADER
# ─────────────────────────────────────────────

def load_all_sources(months: list):
    """
    Load all three sources into BigQuery for all specified months.
    Runs in sequence: TLC → Weather → MTA for each month.
    """
    results = []

    loaders = [
        ("TLC Trips",  load_tlc_to_bigquery),
        ("Weather",    load_weather_to_bigquery),
        ("MTA Alerts", load_mta_to_bigquery),
    ]

    for source_name, loader_fn in loaders:
        logger.info(f"\n{'='*60}")
        logger.info(f"Loading source: {source_name}")
        logger.info(f"{'='*60}")

        for year, month in months:
            try:
                row_count = loader_fn(year, month)
                results.append({
                    "source": source_name,
                    "year": year,
                    "month": month,
                    "status": "success",
                    "rows": row_count
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

    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info("BIGQUERY LOAD SUMMARY")
    logger.info(f"{'='*60}")

    for r in results:
        if r["status"] == "success":
            logger.info(
                f"✅ {r['source']} | {r['year']}-{r['month']} | "
                f"{r.get('rows', 0):,} total rows"
            )
        else:
            logger.info(
                f"❌ {r['source']} | {r['year']}-{r['month']} | "
                f"Error: {r.get('error')}"
            )

    success_count = sum(1 for r in results if r["status"] == "success")
    logger.info(f"\nCompleted: {success_count}/{len(results)} successful")


if __name__ == "__main__":
    project_months = [
        ("2025", "11"),
        ("2025", "12"),
        ("2026", "01"),
    ]

    load_all_sources(project_months)