"""
Prefect flow for HackerNews dlt pipeline.

This flow runs the HackerNews data extraction and loads it into ClickHouse.
"""

from prefect import flow, task
from pathlib import Path
import sys
import subprocess


@task(name="run_hackernews_pipeline", log_prints=True)
def run_hackernews_dlt_pipeline():
    """Run the HackerNews dlt pipeline."""
    # Get paths - run from dlt/ directory so dlt finds .dlt/secrets.toml
    project_root = Path("/home/will-data/repos/doctor-data")
    dlt_dir = project_root / "dlt"
    pipeline_script = dlt_dir / "hacker-news" / "hackernews-load.py"
    
    # Run the pipeline script from dlt/ directory so .dlt/secrets.toml is found
    result = subprocess.run(
        [sys.executable, str(pipeline_script)],
        cwd=str(dlt_dir),
        capture_output=True,
        text=True,
    )
    
    # Raise exception if pipeline failed (Prefect UI will show the error)
    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline failed with return code {result.returncode}\n"
            f"Error output: {result.stderr}"
        )
    
    return result.returncode


@flow(name="hackernews_ingestion", log_prints=True)
def hackernews_ingestion_flow():
    """Main flow for HackerNews data ingestion."""
    return run_hackernews_dlt_pipeline()


if __name__ == "__main__":
    # Use from_source with local directory path - this works better with process workers
    # The flow will run every 4 hours with smaller batches (2,000 items per run)
    from prefect import flow
    
    flow.from_source(
        source="/home/will-data/repos/doctor-data",
        entrypoint="prefect/flows/hackernews_flow.py:hackernews_ingestion_flow"
    ).serve(
        name="hackernews-daily",
        cron="0 */4 * * *",  # Every 4 hours
        tags=["hackernews", "daily"],
    )
