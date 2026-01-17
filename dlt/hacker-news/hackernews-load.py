"""HackerNews API pipeline using dlt REST API source."""

import dlt
import requests
from dlt.sources.rest_api import rest_api_source


@dlt.source
def hacker_news_api_load():
    """HackerNews API source with story lists, items, and users."""
    base_url = "https://hacker-news.firebaseio.com/v0/"
    
    # Story ID lists using REST API source
    story_source = rest_api_source({
        "client": {
            "base_url": base_url,
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "topstories",
                "primary_key": None,
                "endpoint": {
                    "path": "topstories.json",
                },
            },
            {
                "name": "newstories",
                "primary_key": None,
                "endpoint": {
                    "path": "newstories.json",
                },
            },
            {
                "name": "beststories",
                "primary_key": None,
                "endpoint": {
                    "path": "beststories.json",
                },
            },
        ],
    })
    
    # Yield story resources
    for resource in story_source.resources.values():
        yield resource
    
    # Items endpoint - fetch item details for all story IDs
    @dlt.resource(name="items", write_disposition="merge", primary_key="id")
    def items_resource():
        """Fetch item details for all story IDs from topstories, newstories, and beststories."""
        # Get all story IDs from the story lists
        story_ids = set()
        
        print("Fetching story ID lists...")
        # Fetch story ID lists
        for story_type in ["topstories", "newstories", "beststories"]:
            response = requests.get(f"{base_url}{story_type}.json")
            if response.status_code == 200:
                ids = response.json()
                story_ids.update(ids)
                print(f"  {story_type}: {len(ids)} IDs")
        
        print(f"Total unique story IDs: {len(story_ids)}")
        print("Fetching item details...")
        
        # Fetch item details for each ID
        count = 0
        for item_id in story_ids:
            response = requests.get(f"{base_url}item/{item_id}.json")
            if response.status_code == 200:
                item = response.json()
                if item:  # Some items might be None (deleted)
                    yield item
                    count += 1
                    if count % 50 == 0:
                        print(f"  Fetched {count} items...")
        
        print(f"Finished fetching {count} items")
    
    yield items_resource
    
    # Users endpoint - fetch user profiles from items
    @dlt.resource(name="users", write_disposition="merge", primary_key="id")
    def users_resource():
        """Fetch user profiles for authors from items."""
        print("Collecting usernames from items...")
        usernames = set()
        
        # Get all story IDs and fetch items to collect usernames
        story_ids = set()
        for story_type in ["topstories", "newstories", "beststories"]:
            response = requests.get(f"{base_url}{story_type}.json")
            if response.status_code == 200:
                ids = response.json()
                story_ids.update(ids)
        
        # Fetch items to get usernames
        count = 0
        for item_id in story_ids:
            response = requests.get(f"{base_url}item/{item_id}.json")
            if response.status_code == 200:
                item = response.json()
                if item and isinstance(item, dict) and "by" in item:
                    usernames.add(item["by"])
                count += 1
                if count % 50 == 0:
                    print(f"  Processed {count} items, found {len(usernames)} unique users...")
        
        print(f"Found {len(usernames)} unique users")
        print("Fetching user profiles...")
        
        # Fetch user details
        user_count = 0
        for username in usernames:
            response = requests.get(f"{base_url}user/{username}.json")
            if response.status_code == 200:
                user = response.json()
                if user:
                    yield user
                    user_count += 1
                    if user_count % 25 == 0:
                        print(f"  Fetched {user_count} users...")
        
        print(f"Finished fetching {user_count} users")
    
    yield users_resource


pipeline = dlt.pipeline(
    pipeline_name='unknown_source_migration_pipeline',
    destination='clickhouse',
    dataset_name='hackernews',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
)


if __name__ == "__main__":
    source = hacker_news_api_load()
    load_info = pipeline.run(source)
    print(load_info) 
