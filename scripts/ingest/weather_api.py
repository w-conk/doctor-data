#!/usr/bin/env python3
"""
OpenWeatherMap API data ingestion.
Requires API key (free tier: 1,000 calls/day).
Fetches current weather and forecasts for specified cities.
"""

import os
from typing import List, Dict, Any, Optional
from base_ingester import BaseIngester
import logging

logger = logging.getLogger(__name__)


class WeatherIngester(BaseIngester):
    """Ingester for OpenWeatherMap API data."""
    
    def __init__(self):
        super().__init__('weather')
        self.api_key = self.get_api_key('OPENWEATHER_API_KEY')
        if not self.api_key:
            logger.warning("OPENWEATHER_API_KEY not set. Weather ingestion will fail.")
        self.base_url = 'https://api.openweathermap.org/data/2.5'
    
    def fetch_data(self, cities: List[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        Fetch current weather data.
        
        Args:
            cities: List of dicts with 'name' and optionally 'country_code'
                   Default: Major US cities
            
        Returns:
            List of weather data dictionaries
        """
        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY environment variable not set")
        
        if cities is None:
            cities = [
                {'name': 'Chicago', 'country_code': 'US'},
                {'name': 'New York', 'country_code': 'US'},
                {'name': 'Los Angeles', 'country_code': 'US'},
                {'name': 'San Francisco', 'country_code': 'US'},
                {'name': 'Seattle', 'country_code': 'US'},
            ]
        
        all_weather = []
        
        for city in cities:
            try:
                city_name = city['name']
                country = city.get('country_code', 'US')
                
                url = f"{self.base_url}/weather"
                params = {
                    'q': f"{city_name},{country}",
                    'appid': self.api_key,
                    'units': 'metric'
                }
                
                response = self.make_request(url, params=params)
                data = response.json()
                
                weather_record = {
                    'city': city_name,
                    'country': country,
                    'timestamp': data.get('dt'),
                    'temperature': data.get('main', {}).get('temp'),
                    'feels_like': data.get('main', {}).get('feels_like'),
                    'humidity': data.get('main', {}).get('humidity'),
                    'pressure': data.get('main', {}).get('pressure'),
                    'wind_speed': data.get('wind', {}).get('speed'),
                    'wind_direction': data.get('wind', {}).get('deg'),
                    'weather_main': data.get('weather', [{}])[0].get('main'),
                    'weather_description': data.get('weather', [{}])[0].get('description'),
                    'clouds': data.get('clouds', {}).get('all'),
                    'visibility': data.get('visibility'),
                    'sunrise': data.get('sys', {}).get('sunrise'),
                    'sunset': data.get('sys', {}).get('sunset'),
                    'ingested_at': self._get_timestamp()
                }
                
                all_weather.append(weather_record)
                logger.info(f"Fetched weather for {city_name}, {country}")
                
            except Exception as e:
                logger.error(f"Error fetching weather for {city['name']}: {str(e)}")
                continue
        
        return all_weather
    
    @staticmethod
    def _get_timestamp():
        """Get current timestamp."""
        from datetime import datetime
        return datetime.utcnow().isoformat()


def main():
    """Main execution function."""
    ingester = WeatherIngester()
    data = ingester.ingest()
    print(f"Successfully ingested weather data for {len(data)} cities")


if __name__ == "__main__":
    main()
