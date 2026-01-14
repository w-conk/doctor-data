#!/usr/bin/env python3
"""
Base class for all data ingestion scripts.
Provides common functionality for API calls, error handling, and data storage.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
log_path = os.getenv('LOG_PATH', './logs')
Path(log_path).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{log_path}/ingestion.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class BaseIngester(ABC):
    """Base class for data ingestion from APIs."""
    
    def __init__(self, source_name: str, raw_data_path: Optional[str] = None):
        """
        Initialize the ingester.
        
        Args:
            source_name: Name of the data source (e.g., 'reddit', 'weather')
            raw_data_path: Base path for raw data storage
        """
        self.source_name = source_name
        self.raw_data_path = raw_data_path or os.getenv(
            'RAW_DATA_PATH', 
            './data/raw'
        )
        self.source_dir = Path(self.raw_data_path) / source_name
        self.source_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataEngineeringBot/1.0'
        })
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch data from the API.
        
        Returns:
            List of dictionaries containing the fetched data
        """
        pass
    
    def save_raw_data(self, data: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
        """
        Save raw data to JSON file.
        
        Args:
            data: List of data records to save
            filename: Optional filename (defaults to timestamp)
            
        Returns:
            Path to saved file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'{self.source_name}_{timestamp}.json'
        
        filepath = self.source_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Saved {len(data)} records to {filepath}")
        return str(filepath)
    
    def load_latest_data(self) -> Optional[List[Dict[str, Any]]]:
        """Load the most recent data file for this source."""
        files = sorted(self.source_dir.glob(f'{self.source_name}_*.json'))
        if not files:
            return None
        
        latest_file = files[-1]
        with open(latest_file, 'r') as f:
            return json.load(f)
    
    def ingest(self, save: bool = True, **kwargs) -> List[Dict[str, Any]]:
        """
        Main ingestion method.
        
        Args:
            save: Whether to save data to disk
            **kwargs: Additional arguments passed to fetch_data
            
        Returns:
            List of fetched data records
        """
        try:
            logger.info(f"Starting ingestion for {self.source_name}")
            data = self.fetch_data(**kwargs)
            
            if save and data:
                self.save_raw_data(data)
            
            logger.info(f"Successfully ingested {len(data)} records from {self.source_name}")
            return data
            
        except Exception as e:
            logger.error(f"Error ingesting {self.source_name}: {str(e)}", exc_info=True)
            raise
    
    def get_api_key(self, key_name: str) -> Optional[str]:
        """Get API key from environment variables."""
        return os.getenv(key_name)
    
    def make_request(self, url: str, params: Optional[Dict] = None, 
                    headers: Optional[Dict] = None, **kwargs) -> requests.Response:
        """
        Make HTTP request with error handling.
        
        Args:
            url: URL to request
            params: Query parameters
            headers: Additional headers
            **kwargs: Additional arguments for requests.get/post
            
        Returns:
            Response object
        """
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        try:
            response = self.session.get(
                url, 
                params=params, 
                headers=request_headers,
                timeout=30,
                **kwargs
            )
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise
