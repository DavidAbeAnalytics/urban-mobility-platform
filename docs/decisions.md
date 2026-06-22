# Architecture & Technical Decision Log

### ADR-001: Python Version Downgrade
**Date:** 2026 <br>

**Decision:** Downgraded from Python 3.13 - Python 3.11.9 <br>

**Reason:** Python 3.13 incompatible with core GCP packages (protobuf, dbt-bigquery).
            Python 3.11.x is the current production-stable version for data engineering. <br>

**Trade-off:** No relevant Python 3.13 features lost for this project.

<br>
<br>

### ADR-002: Apache Airflow Excluded from Local Environment
**Date:** 2026 <br>

**Decision:** Removed apache-airflow from requirements.txt. <br>

**Reason:** Airflow has no native Windows support. Production orchestration
            targets Google Cloud Composer (managed Airflow on GCP). <br>

**Trade-off:** DAGs cannot be tested locally; deployed directly to Composer. <br>

<br>
<br>

### ADR-003: Exact Package Version Pinning
**Date:** 2026 <br>

**Decision:** All packages pinned to exact versions confirmed working together. <br>

**Reason:** Protobuf conflicts between google-cloud packages require precise alignment. <br>

**Confirmed versions:** <br>
- google-cloud-storage==2.18.2
- google-cloud-bigquery==3.25.0
- google-cloud-bigquery-storage==2.27.0
- dbt-bigquery==1.9.0 / dbt-core==1.9.0
- great-expectations==0.18.19

<br>
<br>

### ADR-004: Binary-Only Package Installation
**Date:** 2026 <br>

**Decision:** All packages installed via `pip install --only-binary=:all:`. <br>

**Reason:** Avoids C compiler dependency failures on Windows for packages
            like pyarrow and grpcio. <br>

**Trade-off:** Negligible; all required packages have Windows wheels available.

<br>
<br>

### ADR-005: MTA Synthetic Data Fallback
**Date:** 2026 <br>

**Decision:** MTA ingestion generates synthetic alert data when the live API
              is unavailable. <br>

**Reason:** MTA GTFS-RT endpoint returns 403 Forbidden without an API key.
            Historical backfill is not supported via this endpoint.
            Synthetic data enables full pipeline and dbt model validation. <br>

**Trade-off:** Subway-to-taxi correlation analysis uses estimated disruption
               patterns, not verified historical events. All synthetic records
               flagged with is_synthetic=TRUE for downstream filtering. <br>
               
**Production path:** Replace with MTA historical GTFS archive files from
                     the MTA developer portal when available. <br>

<br>
<br>

### ADR-006: Resumable GCS Uploads with Exponential Backoff
**Date:** 2026 <br>

**Decision:** Switched to chunked resumable uploads (8MB chunks) with
              exponential backoff retry (max 5 attempts, 10-min deadline). <br>

**Reason:** Initial upload_from_filename() calls failed mid-transfer on
            60-70MB files due to connection instability. GCS resumable
            uploads checkpoint at each chunk boundary; a dropped connection
            resumes from the last successful chunk, not from zero. <br>

**Trade-off:** Marginally more complex upload code; eliminates re-download
               cost on connection failure.

<br>
<br>

### ADR-007: BigQuery and GCS Co-located in us-central1
**Date:** 2026 <br>

**Decision:** All BigQuery datasets and the GCS bucket created in us-central1. <br>

**Reason:** BigQuery load jobs from GCS are free within the same region.
            Cross-region transfers incur data egress costs that compound
            at production data volumes. <br>

**Trade-off:** Slightly higher latency for non-US users; acceptable for a
               batch analytics platform where queries are not latency-sensitive. 

<br>
<br>

### ADR-008: Raw Table Loaded Without Explicit Partitioning
**Date:** 2026 <br>

**Decision:** yellow_taxi_raw loaded with schema auto-detected from Parquet.
              No partitioning applied at the raw layer. <br>

**Reason:** BigQuery console requires explicit schema definition before
            enabling partition-by-field. Defining schema at load time
            duplicates logic that belongs in dbt staging. Raw is only
            read by dbt; never by analysts directly. <br>

**Recovery:** Partitioning and clustering applied at the marts layer on
              fact_trips where query cost optimisation matters. <br>

**Trade-off:** Raw table queries scan the full table. Acceptable since raw
               is only accessed by pipeline processes. 

<br>
<br>

### ADR-009: Corrupt Timestamp Handling in Raw TLC Data
**Date:** 2026 <br>

**Finding:** Raw yellow_taxi_raw contains trips with impossible timestamps; 
             3 trips dated 2008-2009 (vendor clock errors) and 22 trips
             outside the Nov 2025-Jan 2026 window (month boundary overflow). <br>

**Decision:** Filtered in dbt staging using:
              WHERE pickup_datetime BETWEEN '2025-11-01' AND '2026-01-31' <br>

**Reason:** Raw layer preserves source data exactly as received. Cleaning
            happens in staging so raw always reflects what TLC actually sent. <br>

**Impact:** 25 rows excluded out of 12,211,339 total (0.0002%).

<br>
<br>

### ADR-010: Intermediate Models Write to Staging Dataset
**Date:** 2026 <br>

**Decision:** Intermediate models write to the staging BigQuery dataset
              rather than a dedicated intermediate dataset. <br>

**Reason:** dbt schema routing caused dataset name doubling
            (staging_staging) in this environment. Layers are separated
            by naming convention (int_ prefix) rather than dataset boundary. <br>

**Trade-off:** Less visual separation in BigQuery console. Production would
               use a dedicated intermediate dataset with a custom schema macro.

<br>
<br>

### ADR-011: dim_location Demand Category is Static Classification
**Date:** 2026 <br>

**Decision:** demand_category uses TLC service zone definitions rather than
              measured trip volume from actual data. <br>

**Reason:** dim_location is referenced by fact_trips. Computing demand from
            trip counts would create a circular dependency in the dbt DAG. <br>

**Known limitation:** A zone classified as high_demand by TLC designation
                      may not reflect actual demand in the Nov 2025-Jan 2026
                      window. <br>

**Future improvement:** Build a separate mart aggregating trips per zone
                        and join back for a data-driven demand score.

<br>
<br>

### ADR-012: Surrogate Key Strategy for fact_trips
**Date:** 2026 <br>

**Finding:** 564 duplicate trip records in source TLC data; all vendor_id=2
             (VeriFone) hardware-level duplicates where the meter recorded
             the same trip twice with identical field values. <br>

**Decision:** Seven-field surrogate key (pickup_datetime, dropoff_datetime,
              pickup_location_id, dropoff_location_id, vendor_id, fare_amount,
              trip_distance_miles, passenger_count) plus ROW_NUMBER()
              deduplication in fact_trips as final safety net. <br>
              
**Trade-off:** If all seven fields are identical across two rows, both rows
               share a trip_id; statistically negligible at 12M row scale.

<br>
<br>

### ADR-013: Data Quality Observations — fact_trips
**Date:** 2026 <br>

**Finding 1: Negative Adjustments (10,746 trips)** <br>
total_amount < fare_amount in 10,746 trips. These are legitimate TLC vendor
adjustment records where dispute credits produce a lower total than the
metered fare. Average gap: $3.98. Min total_amount: $0.00 (voided trips).
Treatment: dbt test configured to WARN severity for ongoing monitoring.  <br>

**Finding 2: Impossible Speed Records (287 trips)** <br>
287 trips show distances over 100 miles in under 30 minutes; GPS or meter
hardware errors. Statistically negligible at 0.002% of total dataset.
Treatment: dbt test configured to WARN severity for ongoing monitoring. <br>

**Decision:** Both findings preserved in fact_trips for analytical transparency.
              Raw layer retains all original records per ADR-009.

<br>
<br>

### ADR-014: Outlier Filtering Tightened in stg_yellow_taxi
**Date:** 2026 <br>

**Finding:** Power BI dashboard review revealed physically impossible records surviving initial staging filters: <br>
- 49,126 trips over 90 minutes duration
- 3,925 trips over 40 miles distance
- 38,157 trips with fare over $100
- Total: 84,822 rows (0.76% of 11.17M trips)
- Symptom: avg_speed values of 272,000+ mph in dashboard <br>

**Root Cause:** Initial upper bound of trip_duration_minutes < 300 was too
               permissive. No upper bounds existed for distance or fare,
               allowing GPS errors and meter malfunctions through. <br>

**Decision:** Staging filters tightened to: <br>
- trip_duration_minutes <= 90 (covers JFK runs, longest realistic trip)
- trip_distance_miles <= 40 (covers all five boroughs and both airports)
- fare_amount <= 150 (covers JFK flat rate and all legitimate metered fares) <br>

**Fare distribution analysis confirming $150 cutoff:** <br>
             93.11% of trips under $50; consistent with NYC street hails
             6.55% between $50-$100; airport runs and long metered trips
             0.26% between $100-$150; retained, legitimate long trips
             0.08% above $150; removed, confirmed meter malfunctions <br>
 
**Impact:** 84,822 rows removed (0.76%). Core KPI metrics unaffected.
            All downstream models rebuilt after filter update. <br>

**Note:** Dashboard-driven data validation caught what automated schema
            tests did not. Visual review is a necessary complement to
            programmatic data quality testing in production pipelines.