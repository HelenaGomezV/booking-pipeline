# Booking Analytics Pipeline

End-to-end data pipeline using **Medallion Architecture** (Bronze → Silver → Gold) for hotel booking analytics.

## Architecture
```
CSV (119K rows)
    │
    ▼
┌─────────┐     ┌──────────┐     ┌──────────┐
│ BRONZE  │ ──▶ │  SILVER  │ ──▶ │   GOLD   │
│ Raw     │     │  Clean   │     │ Analytics│
│ TEXT    │     │  Typed   │     │ Ready    │
└─────────┘     └──────────┘     └──────────┘
    │                │                │
    │                │                ├── popular_countries
    │                │                └── mom_growth
    │                │
    │                └── Dedup, NULLIF, type casting,
    │                    revenue calc, date reconstruction
    │
    └── SHA-256 hash, ON CONFLICT DO NOTHING,
        schema validation, batch insert (5K)
```

## Key Features

- **Idempotent ingestion** with SHA-256 deduplication (119K rows → 87K unique)
- **Medallion Architecture** with schema drift tolerance (all TEXT in Bronze)
- **dbt transformations** with 10+ automated tests
- **Airflow orchestration** with retry strategy and SLA monitoring
- **CI/CD pipeline** with 6 validation stages via GitHub Actions
- **Fully containerized** with Docker Compose (one command setup)

## Tech Stack

| Layer         | Technology                    |
|---------------|-------------------------------|
| Ingestion     | Python 3.11, psycopg2        |
| Storage       | PostgreSQL 15                 |
| Transformation| dbt-postgres                  |
| Orchestration | Apache Airflow 2.8            |
| Testing       | pytest, dbt tests             |
| CI/CD         | GitHub Actions (6 stages)     |
| Infrastructure| Docker Compose                |

## Assumptions

1. **CSV as S3 proxy:** The local CSV simulates a daily extract from S3. In production, the ingestion script would read from S3 using boto3.
2. **Exact duplicates are errors:** Rows with identical values across all 32 columns (31,994 found) are treated as ingestion duplicates, not intentional records. This would be validated with the business team in production.
3. **Canceled bookings generate zero revenue:** A canceled reservation did not materialize, so revenue = 0 regardless of ADR. Cancellation data is preserved in Silver for behavioral analysis.
4. **Zero-night stays are valid edge cases:** 715 bookings with 0 nights (revenue = €0) are kept in Silver. They may represent day-use rooms or data entry errors.
5. **Country codes may be anonymized:** Codes like SOM, TON, GMB likely represent anonymized European countries in the original dataset (ISO 3166-1 alpha-3).
6. **NULL and NA strings represent missing data:** Both literal strings are converted to real SQL NULLs in Silver using NULLIF chains.
7. **ADR outliers are preserved:** One negative ADR (-€6.38) and 1,959 zero-ADR records are kept as-is. In production, these would be flagged for review.
8. **Single-run batch processing:** The pipeline is designed for daily batch execution, not real-time streaming. Scaling to streaming would require architectural changes.

## Quick Start
```bash
# 1. Clone the repo
git clone https://github.com/HelenaGomezV/booking-pipeline
cd booking-pipeline

# 2. Copy environment file
cp .env.example .env

# 3. Start everything
docker compose up -d --build

# 4. Open Airflow UI
# http://localhost:8080 (user: airflow / pass: airflow)

# 5. Trigger the DAG: booking_medallion_pipeline
```

## Verify Results
```sql
-- Connect to the database
docker compose exec booking-postgres psql -U booking_user -d booking_db

-- Check Bronze (raw data)
SELECT COUNT(*) FROM bronze.raw_bookings;
-- 87,396

-- Check Silver (cleaned data)
SELECT COUNT(*) FROM public_silver.stg_bookings;
-- 87,396

-- Check Gold: Top 10 countries
SELECT popularity_rank, country, total_bookings, booking_share_pct
FROM public_gold.popular_countries
WHERE popularity_rank <= 10;

-- Check Gold: MoM growth
SELECT hotel_type, year_month, total_booking_amount, mom_pct_change
FROM public_gold.mom_growth
WHERE hotel_type = 'Hotel'
ORDER BY year_month;
```

## Key Findings

| Metric                | Value      |
|-----------------------|------------|
| Total reservations    | 119,390    |
| Unique (after dedup)  | 87,396     |
| Cancellation rate     | 37.0%      |
| Hotel cancellation    | 41.7%      |
| Apartment cancellation| 27.8%      |
| Total revenue         | ~€26M      |
| Countries represented | 165        |
| Average stay          | 3.4 nights |
| Average ADR           | €101.83    |

## Testing Strategy

- **dbt tests (10+):** unique, not_null, accepted_values, volume check, positive values
- **Python tests (13):** hash determinism, schema validation, data quality, distributions
- **CI/CD (6 stages):** lint → unit tests → dbt compile → DAG check → docker build → integration

## Design Decisions

1. **All TEXT in Bronze:** Tolerates schema drift. If the CSV changes, Bronze doesn't break.
2. **NULLIF for 'NULL' and 'NA':** The CSV uses text strings instead of real NULLs.
3. **SHA-256 dedup:** The CSV had 31,994 exact duplicates. Hash-based dedup removed them.
4. **Revenue = 0 for canceled:** Canceled bookings don't generate real income.
5. **PARTITION BY hotel_type in LAG():** Hotel and Apartment have independent MoM timelines.

## Scaling Strategy

| Component     | Current (Local)        | Production              |
|---------------|------------------------|-------------------------|
| Storage       | CSV local              | S3 + Parquet            |
| Warehouse     | PostgreSQL             | Readshift/Snowflake  |
| Orchestration | Docker Airflow         | MWAA / Cloud Composer   |
| dbt models    | Full refresh           | Incremental + snapshots |
| Monitoring    | Airflow logs           | DataDog + PagerDuty     |
| CI/CD         | GitHub Actions         | + Terraform + multi-env |

## Project Structure
```
booking-pipeline/
├── dags/
│   └── booking_pipeline.py        # Airflow DAG
├── scripts/
│   ├── ingestion.py               # Bronze ingestion (CSV → PostgreSQL)
│   ├── init_db.sql                # Database initialization
│   └── run_dbt.sh                 # dbt wrapper for Airflow
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_bookings.sql   # Silver model
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── popular_countries.sql  # Gold model 1
│   │       ├── mom_growth.sql         # Gold model 2
│   │       └── schema.yml
│   ├── tests/
│   │   └── volume_check.sql
│   ├── macros/
│   │   └── test_positive_value.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/
│   ├── test_ingestion.py
│   └── test_data_quality.py
├── data/raw/
│   └── bookings.csv
├── .github/workflows/
│   └── ci.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## Author

Helena Gómez Villegas — Data Engineer
- Email: helena.villegas90@gmail.com
- LinkedIn: linkedin.com/in/yhgomez
