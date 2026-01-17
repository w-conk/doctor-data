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
        
        # Fetch story ID lists
        for story_type in ["topstories", "newstories", "beststories"]:
            response = requests.get(f"{base_url}{story_type}.json")
            if response.status_code == 200:
                ids = response.json()
                story_ids.update(ids)
        
        # Fetch item details for each ID
        for item_id in story_ids:
            response = requests.get(f"{base_url}item/{item_id}.json")
            if response.status_code == 200:
                item = response.json()
                if item:  # Some items might be None (deleted)
                    yield item
    
    yield items_resource
    
    # Users endpoint - fetch user profiles from items
    @dlt.transformer(name="users", write_disposition="merge", primary_key="id", data_from=items_resource)
    def users_resource(items):
        """Fetch user profiles for authors from items."""
        usernames = set()
        
        # Collect unique usernames from items
        for item in items:
            if item and "by" in item:
                usernames.add(item["by"])
        
        # Fetch user details
        for username in usernames:
            response = requests.get(f"{base_url}user/{username}.json")
            if response.status_code == 200:
                user = response.json()
                if user:
                    yield user
    
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
