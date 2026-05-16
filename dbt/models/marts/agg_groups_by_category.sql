select
    g.category_id,
    coalesce(g.category_name, c.category_name, 'UNKNOWN') as category_name,
    coalesce(g.category_shortname, c.shortname, 'UNKNOWN') as category_shortname,
    count(distinct g.group_id) as total_groups,
    sum(coalesce(g.group_members, 0)) as total_members,
    avg(coalesce(g.group_rating, 0)) as avg_group_rating,
    current_timestamp() as processed_at
from {{ ref('stg_groups') }} g
left join {{ ref('stg_categories') }} c
    on g.category_id = c.category_id
group by
    1, 2, 3