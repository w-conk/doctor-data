-- Staging model for Reddit data
-- Reads from raw JSON files and creates a clean table

{{ config(materialized='view') }}

with raw_reddit as (
    select
        value:id::string as post_id,
        value:subreddit::string as subreddit,
        value:title::string as title,
        value:author::string as author,
        value:created_utc::timestamp as created_at,
        value:score::int as score,
        value:upvote_ratio::float as upvote_ratio,
        value:num_comments::int as num_comments,
        value:url::string as url,
        value:selftext::string as selftext,
        value:is_self::boolean as is_self_post,
        value:domain::string as domain,
        value:ingested_at::timestamp as ingested_at
    from read_json_auto('{{ var("raw_data_path") }}/reddit/*.json')
)

select
    post_id,
    subreddit,
    title,
    author,
    created_at,
    score,
    upvote_ratio,
    num_comments,
    url,
    selftext,
    is_self_post,
    domain,
    ingested_at,
    current_timestamp() as transformed_at
from raw_reddit
