# Doctor Data

This crap is running on a $20 lenovo

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    MacBook Pro (Development)                 │
│  - Code editing via VS Code/Cursor                          │
│  - Git repository management                                 │
│  - SSH into Lenovo for execution                             │
└──────────────────────┬──────────────────────────────────────┘
                       │ SSH
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Lenovo ThinkCentre (Execution)                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Orchestration Layer (Apache Airflow)                │  │
│  │  - DAG scheduling and execution                       │  │
│  │  - Task dependency management                         │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
│  ┌──────────────▼───────────────────────────────────────┐  │
│  │  Data Ingestion Layer (dlt)                           │  │
│  │  - dlt pipelines for API data extraction              │  │
│  │  - Orchestrated via Airflow DAGs                      │  │
│  │  - Raw data storage (local SSD: 500GB)                 │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
│  ┌──────────────▼───────────────────────────────────────┐  │
│  │  Storage Layer                                        │  │
│  │  - Apache Iceberg (Parquet files)                    │  │
│  │  - ClickHouse (columnar warehouse)                   │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
│  ┌──────────────▼───────────────────────────────────────┐  │
│  │  Transformation Layer (dbt Core)                     │  │
│  │  - dbt-duckdb (Iceberg transformations)             │  │
│  │  - dbt-clickhouse (ClickHouse transformations)       │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
│  ┌──────────────▼───────────────────────────────────────┐  │
│  │  Analytics Layer                                      │  │
│  │  - DuckDB (query engine)                             │  │
│  │  - ClickHouse (OLAP queries)                         │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  NAS (~30TB) - Long-term storage                             │
│  - Archive old data                                          │
│  - Backup storage                                            │
└─────────────────────────────────────────────────────────────┘
```

### Current Setup (500GB SSD)
- **Orchestration**: Apache Airflow (Docker) - DAG scheduling and task management
- **Data Ingestion**: dlt (data load tool) pipelines
  - HackerNews API pipeline (runs every 6 hours)
  - Loads directly into ClickHouse
- **Raw data**: `~/data/raw/` - Temporary staging
- **Iceberg tables**: `~/data/iceberg/` - Parquet files organized by table
- **ClickHouse data**: Managed by ClickHouse in Docker volumes
- **dbt artifacts**: `~/data/dbt/` - Compiled models, logs

### Future: NAS Integration
- Use NAS for:
  - **Archive**: Move data older than X days/months
  - **Backup**: Regular snapshots of critical data
  - **Cold storage**: Historical data rarely accessed
- Consider **S3-compatible storage** (MinIO) on NAS for Spark compatibility
- Use **symbolic links** or **mount points** to seamlessly extend storage

Add folder structure later
Probably going to try fusion at some point against a temp databricks or snowflake instance? not sure.