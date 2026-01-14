-- Mart model: Daily summary of Reddit activity by subreddit

{{ config(materialized='table') }}

with daily_metrics as (
    select
        date(created_at) as date,
        subreddit,
        count(*) as post_count,
        sum(score) as total_score,
        avg(score) as avg_score,
        sum(num_comments) as total_comments,
        avg(num_comments) as avg_comments,
        avg(upvote_ratio) as avg_upvote_ratio,
        count(distinct author) as unique_authors
    from {{ ref('stg_reddit') }}
    where created_at is not null
    group by date(created_at), subreddit
)

select
    date,
    subreddit,
    post_count,
    total_score,
    avg_score,
    total_comments,
    avg_comments,
    avg_upvote_ratio,
    unique_authors,
    current_timestamp() as updated_at
from daily_metrics
order by date desc, total_score desc
