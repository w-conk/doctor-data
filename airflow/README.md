# Airflow Setup

This directory contains Airflow configuration and DAGs for orchestrating data pipelines.

## Initial Setup

1. **Set Airflow UID** (Linux only - prevents permission issues):
   ```bash
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

2. **Create required directories** (already done, but for reference):
   ```bash
   mkdir -p airflow/dags airflow/logs airflow/plugins airflow/config
   ```

3. **Start Airflow services**:
   ```bash
   docker compose up -d
   ```

4. **Create Airflow admin user** (first time only):
   ```bash
   docker compose exec airflow-webserver airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password admin
   ```

5. **Access Airflow UI**:
   - From ThinkCentre: http://localhost:8080
   - From MacBook: http://192.168.1.69:8080
   - Username: `admin`
   - Password: `admin` (change this!)

## Important Notes

- **ClickHouse Connection**: The dlt pipeline needs to connect to ClickHouse. Since both are on the same Docker network, you can use:
  - Host: `clickhouse` (service name) for connections from Airflow containers
  - Host: `localhost` or `192.168.1.69` for connections from your MacBook
  - Update `dlt/.dlt/secrets.toml` accordingly

- **DAGs Location**: Place your DAG files in `airflow/dags/`

- **Logs**: Check logs in `airflow/logs/` or via the Airflow UI

- **Requirements**: Make sure `requirements.txt` includes all dependencies needed by your dlt pipelines

## Useful Commands

```bash
# View logs
docker compose logs airflow-webserver
docker compose logs airflow-scheduler

# Restart services
docker compose restart airflow-webserver airflow-scheduler

# Stop all services
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v
```
