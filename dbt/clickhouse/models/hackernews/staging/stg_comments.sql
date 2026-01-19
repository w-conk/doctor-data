select * from {{source('hackernews', 'hackernews___items')}}
where type = 'comment'