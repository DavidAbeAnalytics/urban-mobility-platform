# Urban Mobility Data Platform (NYC Transportation Intelligence System on Google Cloud)



[SCREENSHOT: 01_dashboard_overview.png]
<!-- Full Power BI dashboard screenshot — place here as the hero image -->

<br>
<br>

### Overview

This platform ingests, transforms, and analyses **11.17 million NYC yellow taxi trips** across the 
November 2025 - January 2026 festive and winter period, enriched with real-time weather observations and 
subway disruption events across 263 NYC zones.

<br>

The system was engineered as a production-grade ELT platform; not a demonstration pipeline, with the same 
architectural decisions, data quality standards, and operational patterns applied in enterprise data engineering environments.

<br>

**Core analytical question driving the platform:** <br>
How do weather conditions and subway disruptions influence taxi demand patterns across NYC zones during the peak festive 
and winter period?

<br>
<br>
<br>

## Architecture

[SCREENSHOT: 02_architecture_diagram.png]
<!-- dbt lineage graph from dbt docs — the full graph showing all nodes -->


The platform implements a four-layer medallion architecture on Google Cloud:

```text
NYC TLC API          Open-Meteo API       MTA Subway Alerts
     │                     │                      │
     ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────┐
│              GCS Data Lake (Raw Zone)               │
│   raw/trips/   raw/weather/   raw/mta_alerts/       │
│   year=/month= partitioned Hive structure           │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│              BigQuery Raw Dataset                   │
│   yellow_taxi_raw   weather_raw   mta_alerts_raw    │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│              dbt Transformation Pipeline            │
│                                                     │
│  Staging → Intermediate → Marts                     │
│  (views)    (tables)      (star schema)             │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│              Power BI Dashboard                     │
│   NYC Transportation Intelligence Platform          │
└─────────────────────────────────────────────────────┘
```

<br>
<br>
<br>

## Technology Stack and Data Sources

### Technology Stack

| Layer            | Technology                | Purpose                                                  |
|------------------|---------------------------|----------------------------------------------------------|
| Data Lake        | Google Cloud Storage      | Raw Parquet file storage with Hive partitioning          |
| Data Warehouse   | BigQuery                  | Columnar query engine - partitioned and clustered tables |
| Transformation   | dbt 1.9.0                 | ELT modeling, testing, documentation, lineage            |
| Orchestration    | Cloud Composer (Airflow)  | Production pipeline scheduling                           |
| CI/CD            | GitHub Actions            | Automated dbt test suite on every pull request           |
| Ingestion        | Python 3.11               | Multi-source ingestion with resumable GCS uploads        |
| Visualisation    | Power BI                  | Business intelligence dashboard                          |
| Infrastructure   | GCP IAM, Service Accounts | Least-privilege authentication                           |

<br>


## Data Sources

| Source               | Coverage            | Volume               | Refresh         |
|----------------------|---------------------|----------------------|-----------------|
| NYC TLC Yellow Taxi  | Nov 2025 - Jan 2026 | 11.17M trips         | Monthly Parquet |
| Open-Meteo Archive   | Nov 2025 - Jan 2026 | 2,208 hourly records | Monthly API     |
| MTA Subway Alerts    | Nov 2025 - Jan 2026 | 135 alert records    | Monthly API     |

#### Why this window? 
November through January captures three analytically distinct demand periods: <br>
- the pre-holiday baseline (November),
- festive surge (December),
- and winter correction (January) <br>
enabling demand pattern comparison across contrasting conditions within a single pipeline.


<br>
<br>
<br>


## Data Lake Design

[SCREENSHOT: 03_gcs_bucket_structure.png]
<!-- GCS bucket expanded showing raw/trips, raw/weather, raw/mta_alerts folders -->

Raw data lands in GCS using Hive-compatible partitioning:

urban-mobility-bucket/
├── raw/
│   ├── trips/year=2025/month=11/trips_2025-11.parquet
│   ├── trips/year=2025/month=12/trips_2025-12.parquet
│   ├── trips/year=2026/month=01/trips_2026-01.parquet
│   ├── weather/year=2025/month=11/weather_2025-11.parquet
│   └── mta_alerts/year=2025/month=11/mta_alerts_2025-11.parquet



The `year=`/`month=` naming convention enables BigQuery partition pruning on
external table queries — a cost optimisation that becomes significant at scale.
Each GCS object is tagged with ingestion metadata at upload time:

```python
blob.metadata = {
    "ingested_at": datetime.utcnow().isoformat(),
    "source": self.source_name,
    "pipeline_version": self.pipeline_version,
    "environment": self.environment,
}
```

---

## Ingestion Layer

The ingestion layer implements a base class pattern — shared upload logic,
retry handling, and metadata tagging are defined once and inherited by all
three source-specific ingestion classes.

```python
class BaseIngestion(ABC):
    """
    Abstract base class enforcing a consistent fetch → process → upload
    → tag → cleanup contract across all data sources.
    """
    @abstractmethod
    def fetch(self, year: str, month: str) -> Path: ...

    @abstractmethod
    def process(self, local_path: Path, year: str, month: str) -> dict: ...
```

**Resilience pattern:** All GCS uploads use 8MB chunked resumable uploads
with exponential backoff (5 attempts, 10-minute deadline). A dropped
connection resumes from the last successful chunk rather than restarting
the transfer — the GCP-recommended approach for files over 5MB.

---

## BigQuery Layer

[SCREENSHOT: 04_bigquery_raw_tables.png]
<!-- BigQuery showing raw dataset with yellow_taxi_raw, weather_raw, mta_alerts_raw -->

[SCREENSHOT: 05_yellow_taxi_raw_rowcount.png]
<!-- yellow_taxi_raw table details showing 11.17M rows and 1.66GB -->

Three raw tables load from GCS into BigQuery:

| Table | Rows | Size |
|---|---|---|
| `raw.yellow_taxi_raw` | 11,170,737 | 1.66 GB |
| `raw.weather_raw` | 2,208 | < 1 MB |
| `raw.mta_alerts_raw` | 135 | < 1 MB |

---

## dbt Transformation Pipeline

[SCREENSHOT: 06_dbt_lineage_graph.png]
<!-- Full dbt lineage graph from dbt docs serve — all nodes visible -->

### Staging Layer — Cleaning and Standardisation

The staging layer is the single point of truth for data cleaning.
All type casting, column renaming, deduplication, and filtering
happens here — never in intermediate or marts.

Key transformations in `stg_yellow_taxi`:

```sql
deduped as (
    select *,
        row_number() over (
            partition by
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                VendorID,
                cast(fare_amount * 100 as int64),
                cast(trip_distance * 100 as int64),
                passenger_count
            order by tpep_pickup_datetime
        ) as row_num
    from source
),

filtered as (
    select * from renamed
    where
        pickup_datetime >= '2025-11-01'
        and pickup_datetime < '2026-02-01'
        and trip_duration_minutes > 0
        and trip_duration_minutes <= 90
        and trip_distance_miles > 0
        and trip_distance_miles <= 40
        and fare_amount > 0
        and fare_amount <= 150
        and pickup_location_id is not null
        and dropoff_location_id is not null
)
```

**Why these specific filter bounds:**
- 90 minutes covers the longest realistic NYC trip (Manhattan to JFK)
- 40 miles covers all five boroughs and both major airports
- $150 covers the JFK flat rate ($70) with headroom for all legitimate fares
- Fare distribution analysis confirmed 99.92% of legitimate trips fall below $150

### Intermediate Layer — Business Logic

Four intermediate models join the three sources and compute analytical metrics:

| Model | Purpose | Output Rows |
|---|---|---|
| `int_yellow_taxi_weather` | Joins trips to hourly weather at pickup time | 11.17M |
| `int_yellow_taxi_metrics` | Adds duration buckets, distance buckets, speed | 11.17M |
| `int_demand_zone_hour` | Aggregates to zone-hour grain | 357K |
| `int_subway_impact` | Joins zone-hour demand to disruption events | 357K |

The join from 11.17M individual trips to 357K zone-hour summaries
represents a 97% data compression — pre-aggregation that eliminates
redundant computation at every downstream query.

### Marts Layer — Star Schema

[SCREENSHOT: 07_bigquery_marts_datasets.png]
<!-- BigQuery showing marts dataset with fact_trips, dim_location, dim_time, dim_weather_conditions -->

[SCREENSHOT: 08_fact_trips_schema.png]
<!-- fact_trips schema tab showing all columns -->

```text
        dim_time
           │
dim_location ── fact_trips ── dim_weather_conditions
```

| Model | Type | Rows | Description |
|---|---|---|---|
| `fact_trips` | Fact | 11.17M | One row per trip — all metrics and FK references |
| `dim_location` | Dimension | 265 | NYC taxi zones mapped to boroughs and service areas |
| `dim_time` | Dimension | 2,208 | Hour-grain time dimension with peak/holiday flags |
| `dim_weather_conditions` | Dimension | 2,208 | Hourly weather with severity scoring |

`fact_trips` uses a seven-field surrogate key with a final ROW_NUMBER()
deduplication pass — required because 564 VeriFone (vendor_id=2) records
in the TLC source contained hardware-level duplicate meter readings
with no distinguishing fields.

---

## Data Quality

[SCREENSHOT: 09_dbt_test_results.png]
<!-- Terminal showing PASS=74 WARN=2 ERROR=0 SKIP=0 TOTAL=76 -->

76 automated tests run across all pipeline layers on every execution:
