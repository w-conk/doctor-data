# Data Engineering Playground

A comprehensive data engineering setup running on a Lenovo ThinkCentre, designed as a local "cloud" environment for data ingestion, transformation, and analysis.

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
│  │  Data Ingestion Layer (Python)                       │  │
│  │  - API data pulls (scheduled via cron)                │  │
│  │  - Raw data storage (local SSD: 500GB)                │  │
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

## Storage Strategy Recommendations

### Current Setup (500GB SSD)
- **Raw data**: `/data/raw/` - Temporary staging
- **Iceberg tables**: `/data/iceberg/` - Parquet files organized by table
- **ClickHouse data**: Managed by ClickHouse in Docker volumes
- **dbt artifacts**: `/data/dbt/` - Compiled models, logs

### Future: NAS Integration
- Use NAS for:
  - **Archive**: Move data older than X days/months
  - **Backup**: Regular snapshots of critical data
  - **Cold storage**: Historical data rarely accessed
- Consider **S3-compatible storage** (MinIO) on NAS for Spark compatibility
- Use **symbolic links** or **mount points** to seamlessly extend storage

### S3 vs Local Storage
**Recommendation: Start local, add S3 later**
- **Local storage** is faster for development/learning
- **S3/MinIO** becomes valuable when:
  - You need Spark (S3 is Spark-native)
  - You want to simulate cloud workflows
  - You need object storage semantics
  - You're ready to scale beyond single machine

## Workflow: MacBook → Lenovo

### Development Workflow
1. **Edit code on MacBook** using VS Code/Cursor
2. **Commit to GitHub** from MacBook
3. **Pull on Lenovo** via SSH: `git pull origin main`
4. **Run scripts on Lenovo** via SSH
5. **View results** via:
   - SSH terminal output
   - ClickHouse web UI (port 8123)
   - Jupyter notebooks (optional, port 8888)

### Recommended Setup
```bash
# On MacBook: Clone repo
git clone <your-repo> ~/projects/data-engineering

# On Lenovo: Clone same repo
git clone <your-repo> ~/data-engineering

# On MacBook: Edit, commit, push
# On Lenovo: Pull and run
cd ~/data-engineering
git pull origin main
python scripts/ingest/reddit_api.py
```

## Data Sources

### Recommended APIs for Regular Pulls

1. **Reddit API** (`scripts/ingest/reddit_api.py`)
   - Free, no API key needed (with rate limits)
   - Rich data: posts, comments, subreddits
   - Good for text analysis, sentiment, trends

2. **OpenWeatherMap API** (`scripts/ingest/weather_api.py`)
   - Free tier: 1,000 calls/day
   - Historical and current weather data
   - Great for time-series analysis

3. **GitHub API** (`scripts/ingest/github_api.py`)
   - Free, no auth needed for public repos
   - Repository stats, commits, issues
   - Good for software metrics

4. **NewsAPI** (`scripts/ingest/news_api.py`)
   - Free tier: 100 requests/day
   - News articles from various sources
   - Good for NLP and trend analysis

5. **CoinGecko API** (`scripts/ingest/crypto_api.py`)
   - Free, no API key needed
   - Cryptocurrency prices and market data
   - Great for financial time-series

6. **Hacker News API** (`scripts/ingest/hackernews_api.py`)
   - Free, no auth needed
   - Stories, comments, user data
   - Simple JSON API

## Project Structure

```
data_engineering/
├── README.md                 # This file
├── docker-compose.yml        # ClickHouse and supporting services
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── scripts/
│   ├── ingest/               # Data ingestion scripts
│   │   ├── reddit_api.py
│   │   ├── weather_api.py
│   │   ├── github_api.py
│   │   └── base_ingester.py  # Base class for all ingestors
│   └── utils/                # Utility scripts
│       ├── setup_directories.py
│       └── archive_to_nas.py
├── dbt/
│   ├── duckdb/               # dbt project for DuckDB/Iceberg
│   │   ├── dbt_project.yml
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   ├── intermediate/
│   │   │   └── marts/
│   │   └── profiles.yml
│   └── clickhouse/           # dbt project for ClickHouse
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       └── profiles.yml
├── data/
│   ├── raw/                  # Raw ingested data (JSON, CSV)
│   ├── iceberg/              # Iceberg table storage
│   └── dbt/                  # dbt artifacts
└── notebooks/                # Jupyter notebooks for exploration
```

## Quick Start

### 1. Initial Setup (on Lenovo)

```bash
# Clone repository
git clone <your-repo> ~/data-engineering
cd ~/data-engineering/data_engineering

# Create data directories
python scripts/utils/setup_directories.py

# Copy and configure environment
cp .env.example .env
nano .env  # Add API keys if needed

# Install Python dependencies
pip install -r requirements.txt

# Start ClickHouse
docker-compose up -d

# Initialize dbt projects
cd dbt/duckdb && dbt deps && cd ../..
cd dbt/clickhouse && dbt deps && cd ../..
```

### 2. Run Data Ingestion

```bash
# Test a single ingestion
python scripts/ingest/reddit_api.py

# Or run all ingestions
python scripts/ingest/run_all.py
```

### 3. Transform Data with dbt

```bash
# DuckDB/Iceberg transformations
cd dbt/duckdb
dbt run
dbt test

# ClickHouse transformations
cd ../clickhouse
dbt run
dbt test
```

### 4. Query Data

```bash
# Using DuckDB CLI
duckdb data/iceberg/my_table.parquet

# Using ClickHouse client
clickhouse-client --query "SELECT * FROM my_table LIMIT 10"
```

## Visualization Strategy

### Option 1: Hosted Website (Recommended for Production)
- **Pros**: Accessible anywhere, professional presentation
- **Cons**: Need to expose data or build API layer
- **Tools**: Plotly Dash, Streamlit, or static HTML with Chart.js
- **Architecture**: 
  - Build API endpoints on Lenovo (Flask/FastAPI)
  - Host frontend on your website
  - API calls from website to Lenovo (consider VPN/tunnel for security)

### Option 2: Local Dashboards
- **Pros**: Fast, no network concerns
- **Cons**: Only accessible on local network
- **Tools**: 
  - **Grafana** (connects to ClickHouse)
  - **Metabase** (connects to ClickHouse/DuckDB)
  - **Jupyter Dashboards**

### Option 3: Hybrid Approach
- **Development**: Local dashboards (Grafana/Metabase)
- **Production**: Export visualizations to static HTML, host on website
- **Real-time**: API endpoints for live data

## Future: Spark Integration

When ready for Spark:

1. **Setup**: Install Spark on Lenovo or use Docker
2. **Storage**: Move to S3-compatible storage (MinIO on NAS)
3. **Workflow**: 
   - Spark for large-scale transformations
   - DuckDB for interactive queries
   - ClickHouse for real-time analytics
4. **dbt-spark**: Use dbt-spark adapter for Spark transformations

## Scheduling

Use **cron** for regular data pulls:

```bash
# Edit crontab
crontab -e

# Example: Pull Reddit data every 6 hours
0 */6 * * * cd ~/data-engineering/data_engineering && python scripts/ingest/reddit_api.py >> logs/reddit.log 2>&1
```

## Monitoring

- **ClickHouse**: Built-in system tables for monitoring
- **dbt**: Use `dbt docs generate` and `dbt docs serve` for lineage
- **Logs**: All scripts log to `logs/` directory
- **Health checks**: Simple Python scripts to verify data freshness

## Next Steps

1. Set up the project structure
2. Configure your first data source
3. Run initial ingestion
4. Create your first dbt models
5. Build a simple visualization
6. Set up scheduling for regular pulls
