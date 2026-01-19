select * from {{source('hackernews', 'hackernews__items')}}
where type = 'comment'