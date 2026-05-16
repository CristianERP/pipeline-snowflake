select
    group_id,
    event_group_name as group_name,
    category_id,
    coalesce(category_name, 'UNKNOWN') as category_name,
    count(*) as total_events,
    sum(coalesce(yes_rsvp_count, 0)) as total_yes_rsvp,
    avg(coalesce(event_rating_average, 0)) as avg_event_rating,
    avg(coalesce(group_rating, 0)) as avg_group_rating,
    current_timestamp() as processed_at
from {{ ref('events_enriched') }}
group by
    1, 2, 3, 4