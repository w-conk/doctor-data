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
    # Use absolute path to the actual repo location (not temp Git clone)
    # This ensures dlt finds .dlt/secrets.toml which isn't in Git
    actual_repo_path = Path("/home/will-data/repos/doctor-data")
    dlt_dir = actual_repo_path / "dlt"
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
    # Deploy using Git storage - works better with process workers
    # The flow will run every 12 hours
    # Git storage preserves directory structure when Prefect clones the repo
    # Job variables point dlt to the original secrets location (not in Git)
    from prefect import flow
    
    flow.from_source(
        source="https://github.com/w-conk/doctor-data.git",
        entrypoint="prefect/flows/hackernews_flow.py:hackernews_ingestion_flow"
    ).deploy(
        name="hackernews-daily",
        work_pool_name="default",
        cron="0 */12 * * *",
        tags=["hackernews", "daily"],
    )
