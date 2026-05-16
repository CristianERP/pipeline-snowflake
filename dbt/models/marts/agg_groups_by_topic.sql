select
    gt.topic_id,
    coalesce(t.topic_name, gt.topic_name, 'UNKNOWN') as topic_name,
    count(distinct gt.group_id) as total_groups,
    sum(coalesce(g.group_members, 0)) as total_group_members,
    avg(coalesce(g.group_rating, 0)) as avg_group_rating,
    current_timestamp() as processed_at
from {{ ref('stg_groups_topics') }} gt
left join {{ ref('stg_topics') }} t
    on gt.topic_id = t.topic_id
left join {{ ref('stg_groups') }} g
    on gt.group_id = g.group_id
group by
    1, 2