"""HackerNews API pipeline using dlt."""

import dlt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from typing import Set


@dlt.source
def hacker_news_api_source(lookback_items: int = 10000):
    """
    HackerNews API source that fetches items in a range from the current maxitem.
    
    Args:
        lookback_items: Number of items to fetch back from the current maxitem.
                        2,000 is roughly 4-6 hours of data.
    """
    base_url = "https://hacker-news.firebaseio.com/v0/"
    
    # Create a session with retry logic to handle transient SSL/network errors
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Wait 1, 2, 4, 8, 16 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"],  # Only retry GET requests
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Track usernames encountered to fetch profiles later
    usernames: Set[str] = set()

    @dlt.resource(name="items", write_disposition="merge", primary_key="id")
    def items_resource():
        """Fetch a range of items (stories, comments, etc.) starting from maxitem."""
        # Get the current max item ID with retry logic
        max_item_response = None
        for attempt in range(5):
            try:
                max_item_response = session.get(f"{base_url}maxitem.json", timeout=10)
                if max_item_response.status_code == 200:
                    break
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                if attempt < 4:
                    wait_time = 2 ** attempt
                    print(f"SSL/Connection error fetching maxitem, retrying in {wait_time}s... (attempt {attempt + 1}/5)")
                    time.sleep(wait_time)
                else:
                    print(f"Failed to fetch maxitem.json after 5 attempts: {e}")
                    return
            except Exception as e:
                print(f"Unexpected error fetching maxitem: {e}")
                return
        
        if not max_item_response or max_item_response.status_code != 200:
            print("Failed to fetch maxitem.json")
            return
        
        max_id = max_item_response.json()
        start_id = max_id - lookback_items
        
        print(f"Fetching items from ID {start_id} to {max_id} ({lookback_items} items)...")
        
        count = 0
        failed_items = 0
        for item_id in range(start_id, max_id + 1):
            # Add small delay to avoid rate limiting
            if item_id > start_id:
                time.sleep(0.01)  # 10ms delay between requests
            
            # Retry logic for individual item requests
            item = None
            for attempt in range(3):  # 3 attempts per item
                try:
                    response = session.get(f"{base_url}item/{item_id}.json", timeout=10)
                    if response.status_code == 200:
                        item = response.json()
                        break
                    elif response.status_code == 404:
                        # Item doesn't exist, skip it
                        break
                except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                    if attempt < 2:
                        wait_time = 0.5 * (attempt + 1)
                        time.sleep(wait_time)
                    else:
                        failed_items += 1
                        if failed_items % 100 == 0:
                            print(f"  Warning: {failed_items} items failed to fetch (SSL/connection errors)")
                        break
                except Exception as e:
                    failed_items += 1
                    break
            
            if item:
                yield item
                count += 1
                if count % 100 == 0:
                    print(f"  Fetched {count} items... (failed: {failed_items})")
        
        print(f"Finished fetching {count} items. ({failed_items} items failed due to network errors)")

    @dlt.transformer(data_from=items_resource, name="users", write_disposition="merge", primary_key="id")
    def users_resource(item):
        """
        Fetch user profile for the author of an item.
        The transformer receives each 'item' yielded by items_resource.
        """
        username = item.get("by")
        if username and username not in usernames:
            usernames.add(username)
            # Retry logic for user requests
            for attempt in range(3):
                try:
                    response = session.get(f"{base_url}user/{username}.json", timeout=10)
                    if response.status_code == 200:
                        user = response.json()
                        if user:
                            yield user
                        break
                    elif response.status_code == 404:
                        # User doesn't exist, skip it
                        break
                except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                    if attempt < 2:
                        time.sleep(0.5 * (attempt + 1))
                    else:
                        # Skip this user if we can't fetch after retries
                        break
                except Exception:
                    # Skip this user on other errors
                    break


    return [items_resource, users_resource]


if __name__ == "__main__":
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name='hackernews_pipeline',
        destination='clickhouse',
        dataset_name='hackernews',
    )

    # Run the source
    # Fetch 2,000 items (~4-6 hours of data) - smaller batches for reliability
    source = hacker_news_api_source(lookback_items=2000)
    
    print("Running pipeline...")
    load_info = pipeline.run(source)
    print(load_info)
