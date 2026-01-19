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
    # Get the path to the dlt pipeline script
    project_root = Path(__file__).parent.parent
    pipeline_script = project_root / "dlt" / "hacker-news" / "hackernews-load.py"
    script_dir = pipeline_script.parent
    
    print(f"Running HackerNews pipeline from: {script_dir}")
    
    # Run the pipeline script
    result = subprocess.run(
        [sys.executable, str(pipeline_script)],
        cwd=str(script_dir),
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
    hackernews_ingestion_flow()
