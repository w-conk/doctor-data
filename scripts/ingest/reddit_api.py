#!/usr/bin/env python3
"""
Reddit API data ingestion.
Fetches posts from specified subreddits.
No API key required, but rate limits apply (60 requests per minute).
"""

import os
from typing import List, Dict, Any
from base_ingester import BaseIngester
import logging

logger = logging.getLogger(__name__)


class RedditIngester(BaseIngester):
    """Ingester for Reddit API data."""
    
    def __init__(self):
        super().__init__('reddit')
        self.base_url = 'https://www.reddit.com'
    
    def fetch_data(self, subreddits: List[str] = None, limit: int = 25, 
                   sort: str = 'hot') -> List[Dict[str, Any]]:
        """
        Fetch posts from Reddit.
        
        Args:
            subreddits: List of subreddit names (default: popular subreddits)
            limit: Number of posts per subreddit (max 100)
            sort: Sort method ('hot', 'new', 'top', 'rising')
            
        Returns:
            List of post dictionaries
        """
        if subreddits is None:
            # Default interesting subreddits for data analysis
            subreddits = [
                'dataisbeautiful',
                'MachineLearning',
                'datascience',
                'programming',
                'technology',
                'science',
                'worldnews',
                'todayilearned'
            ]
        
        all_posts = []
        
        for subreddit in subreddits:
            try:
                url = f"{self.base_url}/r/{subreddit}/{sort}.json"
                params = {'limit': min(limit, 100)}
                
                response = self.make_request(url, params=params)
                data = response.json()
                
                posts = data.get('data', {}).get('children', [])
                
                for post in posts:
                    post_data = post.get('data', {})
                    all_posts.append({
                        'id': post_data.get('id'),
                        'subreddit': post_data.get('subreddit'),
                        'title': post_data.get('title'),
                        'author': post_data.get('author'),
                        'created_utc': post_data.get('created_utc'),
                        'score': post_data.get('score'),
                        'upvote_ratio': post_data.get('upvote_ratio'),
                        'num_comments': post_data.get('num_comments'),
                        'url': post_data.get('url'),
                        'selftext': post_data.get('selftext', ''),
                        'is_self': post_data.get('is_self'),
                        'domain': post_data.get('domain'),
                        'ingested_at': self._get_timestamp()
                    })
                
                logger.info(f"Fetched {len(posts)} posts from r/{subreddit}")
                
            except Exception as e:
                logger.error(f"Error fetching from r/{subreddit}: {str(e)}")
                continue
        
        return all_posts
    
    @staticmethod
    def _get_timestamp():
        """Get current timestamp."""
        from datetime import datetime
        return datetime.utcnow().isoformat()


def main():
    """Main execution function."""
    ingester = RedditIngester()
    data = ingester.ingest(limit=50)
    print(f"Successfully ingested {len(data)} Reddit posts")


if __name__ == "__main__":
    main()
