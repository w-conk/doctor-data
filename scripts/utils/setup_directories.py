#!/usr/bin/env python3
"""
Setup script to create necessary directories for the data engineering project.
Run this once after cloning the repository.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_directories():
    """Create all necessary directories for data storage."""
    
    # Get base paths from environment or use defaults
    # Expand user home directory (~ or ${HOME}) if present
    data_root = os.path.expanduser(os.getenv('DATA_ROOT', './data'))
    raw_path = os.path.expanduser(os.getenv('RAW_DATA_PATH', f'{data_root}/raw'))
    iceberg_path = os.path.expanduser(os.getenv('ICEBERG_DATA_PATH', f'{data_root}/iceberg'))
    dbt_path = os.path.expanduser(os.getenv('DBT_ARTIFACTS_PATH', f'{data_root}/dbt'))
    log_path = os.path.expanduser(os.getenv('LOG_PATH', './logs'))
    
    directories = [
        data_root,
        raw_path,
        f'{raw_path}/hackernews',
        iceberg_path,
        dbt_path,
        log_path,
        './notebooks',
        './config/clickhouse',
    ]
    
    print("Creating directories...")
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ {directory}")
    
    print("\nDirectory setup complete!")
    print(f"\nData will be stored in: {data_root}")
    print(f"Raw data: {raw_path}")
    print(f"Iceberg tables: {iceberg_path}")
    print(f"dbt artifacts: {dbt_path}")

if __name__ == "__main__":
    create_directories()
