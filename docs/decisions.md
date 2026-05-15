# Architecture & Technical Decision Log

## ADR-001: Python Version Downgrade
**Date:** 2026
**Decision:** Downgraded from Python 3.13 → Python 3.11.9
**Reason:** Python 3.13 was too new for most GCP packages (protobuf, dbt-bigquery).
            Python 3.11.x is the current production-stable version for data engineering stacks.
**Trade-off:** Lose access to Python 3.13 features (none relevant to this project).

---

## ADR-002: Apache Airflow Removed from Local Setup
**Date:** 2026
**Decision:** Removed apache-airflow from requirements.txt for local development.
**Reason:** Airflow is not supported on Windows natively. Installing via WSL adds complexity
            that is out of scope for this phase.
**Alternative:** Orchestration will run on Google Cloud Composer (managed Airflow on GCP).
                 DAG files will be written locally and deployed to Composer.
**Trade-off:** Cannot test DAGs locally — will use Cloud Composer dev environment instead.

---

## ADR-003: Package Version Pinning Strategy
**Date:** 2026
**Decision:** Pinned all packages to exact versions tested and confirmed working.
**Reason:** Protobuf version conflicts between google-cloud-* packages require careful alignment.
**Versions confirmed working together:**
- google-cloud-storage==2.18.2
- google-cloud-bigquery==3.25.0
- google-cloud-bigquery-storage==2.27.0
- dbt-bigquery==1.9.0
- dbt-core==1.9.0
- great-expectations==0.18.19

---

## ADR-004: Install Strategy — Binary-Only
**Date:** 2026
**Decision:** Used `pip install --only-binary=:all:` for all package installations.
**Reason:** Avoids C compiler dependency issues on Windows for packages like pyarrow and grpcio.
**Trade-off:** Slightly less flexibility if a pre-built wheel is unavailable for a package,
              but all required packages have Windows wheels available.

---

## ADR-005: MTA Synthetic Data Fallback
**Date:** 2026
**Decision:** MTA ingestion script generates synthetic alert data when
              the live API is unavailable or returns no historical data.
**Reason:** The MTA real-time API is designed for live feeds, not historical
            backfills. Historical alert archives are inconsistently available.
            Synthetic data allows full pipeline testing and dbt model validation
            without blocking progress on downstream layers.
**Trade-off:** Synthetic MTA data means subway-to-taxi correlation analysis
               uses estimated disruption patterns, not verified historical events.
               All synthetic records are flagged with is_synthetic=True so they
               can be filtered or replaced when real data becomes available.
**Production path:** Replace with MTA historical GTFS archive files when
                     available from the MTA developer portal.
**Live API finding:** The MTA GTFS-RT endpoint returns 403 Forbidden without
                      an API key. Free developer keys are available at https://api.mta.info 
                      but historical backfill via this endpoint is not supported. 
                      Synthetic data remains the correct approach for the Nov 2025 - Jan 2026 window.             

---

## ADR-006: Resumable GCS Uploads with Exponential Backoff
**Date:** 2026
**Problem:** Initial upload_from_filename() calls failed mid-transfer on
             60-70MB files due to residential connection instability.
             Two failure modes observed: WriteTimeout and SSLEOFError.
**Decision:** Switched to chunked resumable uploads (8MB chunks) with
              exponential backoff retry policy (max 5 attempts, 10-min deadline).
**Reason:** GCS resumable uploads checkpoint progress at each chunk boundary.
            A dropped connection resumes from the last successful chunk,
            not from zero. This is the GCP-recommended approach for files
            over 5MB in production pipelines.
**Trade-off:** Slightly more complex upload code, but eliminates re-download
               cost on connection failure.


---


## ADR-007: BigQuery and GCS in Same Region (us-central1)
**Date:** 2026
**Decision:** All BigQuery datasets and GCS bucket created in us-central1.
**Reason:** BigQuery load jobs from GCS are free when source and destination
            are in the same region. Cross-region transfers incur data egress
            costs that compound significantly at production data volumes.
**Trade-off:** Slightly higher latency for users outside the US — acceptable
               for a batch analytics platform where queries are not
               latency-sensitive.

---

## ADR-011: dim_location Demand Category is Static Classification
**Date:** 2026
**Decision:** demand_category in dim_location uses TLC service zone
              definitions rather than measured trip volume from actual data.
**Reason:** dim_location is a dimension table referenced by fact_trips.
            Computing demand from trip counts would create a circular
            dependency in the dbt DAG.
**Known limitation:** A zone classified as high_demand by TLC designation
                      may have low actual demand in the Nov 2025-Jan 2026
                      window due to seasonal or external factors.
**Future improvement:** Build a separate mart aggregating actual trips per
                        zone and join back to dim_location for a
                        data-driven demand score.

---

## ADR-012: Surrogate Key Strategy for fact_trips
**Date:** 2026
**Finding:** 564 duplicate trip records exist in the source TLC data,
             all from vendor_id=2 (VeriFone). These are hardware-level
             duplicates where the meter recorded the same trip twice
             with identical timestamps, locations, and fare amounts.
**Decision:** Extended surrogate key to include trip_distance_miles and
              passenger_count in addition to pickup_datetime,
              pickup_location_id, dropoff_location_id, vendor_id,
              and fare_amount.
**Trade-off:** Surrogate key now uses 7 fields. If all 7 fields are
               identical across two rows, both rows will still share
               a trip_id — but this scenario is statistically negligible
               at 12M row scale.


---

## ADR-013: Data Quality Observations in fact_trips
**Date:** 2026
**Finding 1 — Negative Adjustments (10,746 trips)**
total_amount < fare_amount in 10,746 trips. Investigation shows these
are legitimate TLC vendor adjustment records where dispute credits
produce a lower total than the metered fare. Not a pipeline error.
Treatment: converted test to WARN severity for ongoing monitoring.

**Finding 2 — Impossible Speed Records (287 trips)**
287 trips show distances over 100 miles completed in under 30 minutes —
physically impossible. Source: GPS or meter hardware errors in vendor
systems. Statistically negligible at 0.002% of 12M trip dataset.
Treatment: converted test to WARN severity for ongoing monitoring.

**Decision:** Both findings preserved in fact_trips for analytical
transparency. Analysts can filter using trip_distance_miles and
total_amount if needed. Raw layer retains all original records
per ADR-009 raw preservation policy.
