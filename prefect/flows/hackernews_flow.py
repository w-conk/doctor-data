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
    project_root = Path(__file__).parent.parent.parent
    dlt_dir = project_root / "dlt"
    pipeline_script = dlt_dir / "hacker-news" / "hackernews-load.py"
    
    print(f"Running HackerNews pipeline from: {dlt_dir}")
    
    # Run the pipeline script from dlt/ directory so .dlt/secrets.toml is found
    result = subprocess.run(
        [sys.executable, str(pipeline_script)],
        cwd=str(dlt_dir),
        capture_output=True,
        text=True,
    )
    
    # Print output
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    
    # Raise exception if pipeline failed
    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline failed with return code {result.returncode}\n"
            f"Error output: {result.stderr}"
        )
    
    print("Pipeline completed successfully!")
    return result.returncode


@flow(name="hackernews_ingestion", log_prints=True)
def hackernews_ingestion_flow():
    """Main flow for HackerNews data ingestion."""
    print("Starting HackerNews ingestion flow...")
    
    result = run_hackernews_dlt_pipeline()
    
    print(f"HackerNews ingestion completed successfully!")
    return result


if __name__ == "__main__":
    # Deploy with scheduling - creates a deployment that runs on a schedule
    # The flow will run every 12 hours
    # Using deploy() with work_pool_name for process workers
    from pathlib import Path
    
    project_root = Path(__file__).parent.parent.parent
    
    hackernews_ingestion_flow.deploy(
        name="hackernews-daily",
        work_pool_name="default",
        cron="0 */12 * * *",
        tags=["hackernews", "daily"],
        # Use local file system storage with absolute path
        path=str(project_root),
    )
