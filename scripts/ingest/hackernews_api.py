#!/usr/bin/env python3
"""
Hacker News API data ingestion.
Free, no API key required.
Fetches top stories, new stories, or specific items.
"""

from typing import List, Dict, Any
from base_ingester import BaseIngester
import logging

logger = logging.getLogger(__name__)


class HackerNewsIngester(BaseIngester):
    """Ingester for Hacker News API data."""
    
    def __init__(self):
        super().__init__('hackernews')
        self.base_url = 'https://hacker-news.firebaseio.com/v0'
    
    def fetch_data(self, story_type: str = 'top', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch stories from Hacker News.
        
        Args:
            story_type: Type of stories ('top', 'new', 'best', 'ask', 'show', 'job')
            limit: Maximum number of stories to fetch
            
        Returns:
            List of story dictionaries
        """
        try:
            # Get story IDs
            url = f"{self.base_url}/{story_type}stories.json"
            response = self.make_request(url)
            story_ids = response.json()[:limit]
            
            all_stories = []
            
            # Fetch details for each story
            for story_id in story_ids:
                try:
                    item_url = f"{self.base_url}/item/{story_id}.json"
                    item_response = self.make_request(item_url)
                    item_data = item_response.json()
                    
                    # Only include stories (not comments)
                    if item_data.get('type') == 'story':
                        story = {
                            'id': item_data.get('id'),
                            'title': item_data.get('title'),
                            'by': item_data.get('by'),
                            'time': item_data.get('time'),
                            'score': item_data.get('score'),
                            'descendants': item_data.get('descendants'),  # number of comments
                            'url': item_data.get('url'),
                            'text': item_data.get('text', ''),
                            'type': item_data.get('type'),
                            'ingested_at': self._get_timestamp()
                        }
                        all_stories.append(story)
                
                except Exception as e:
                    logger.warning(f"Error fetching story {story_id}: {str(e)}")
                    continue
            
            logger.info(f"Fetched {len(all_stories)} {story_type} stories from Hacker News")
            return all_stories
            
        except Exception as e:
            logger.error(f"Error fetching {story_type} stories: {str(e)}")
            raise
    
    @staticmethod
    def _get_timestamp():
        """Get current timestamp."""
        from datetime import datetime
        return datetime.utcnow().isoformat()


def main():
    """Main execution function."""
    ingester = HackerNewsIngester()
    data = ingester.ingest(story_type='top', limit=50)
    print(f"Successfully ingested {len(data)} Hacker News stories")


if __name__ == "__main__":
    main()
