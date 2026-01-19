"""HackerNews API pipeline using dlt."""

import dlt
import requests
from typing import Set


@dlt.source
def hacker_news_api_source(lookback_items: int = 10000):
    """
    HackerNews API source that fetches items in a range from the current maxitem.
    
    Args:
        lookback_items: Number of items to fetch back from the current maxitem.
                        10,000 is roughly 1-2 days of data.
    """
    base_url = "https://hacker-news.firebaseio.com/v0/"
    
    # Track usernames encountered to fetch profiles later
    usernames: Set[str] = set()

    @dlt.resource(name="items", write_disposition="merge", primary_key="id")
    def items_resource():
        """Fetch a range of items (stories, comments, etc.) starting from maxitem."""
        # Get the current max item ID
        max_item_response = requests.get(f"{base_url}maxitem.json")
        if max_item_response.status_code != 200:
            print("Failed to fetch maxitem.json")
            return
        
        max_id = max_item_response.json()
        start_id = max_id - lookback_items
        
        print(f"Fetching items from ID {start_id} to {max_id} ({lookback_items} items)...")
        
        count = 0
        for item_id in range(start_id, max_id + 1):
            response = requests.get(f"{base_url}item/{item_id}.json")
            if response.status_code == 200:
                item = response.json()
                if item:
                    yield item
                    count += 1
                    if count % 100 == 0:
                        print(f"  Fetched {count} items...")
        
        print(f"Finished fetching {count} items.")

    @dlt.transformer(data_from=items_resource, name="users", write_disposition="merge", primary_key="id")
    def users_resource(item):
        """
        Fetch user profile for the author of an item.
        The transformer receives each 'item' yielded by items_resource.
        """
        username = item.get("by")
        if username and username not in usernames:
            usernames.add(username)
            response = requests.get(f"{base_url}user/{username}.json")
            if response.status_code == 200:
                user = response.json()
                if user:
                    yield user


    return [items_resource, users_resource]


if __name__ == "__main__":
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name='hackernews_pipeline',
        destination='clickhouse',
        dataset_name='hackernews',
    )

    # Run the source
    # Fetch 10,000 items (~1-2 days of data) to ensure we don't miss items between runs
    source = hacker_news_api_source(lookback_items=10000)
    
    print("Running pipeline...")
    load_info = pipeline.run(source)
    print(load_info)
