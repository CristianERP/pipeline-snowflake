select
    coalesce(venue_city, 'UNKNOWN') as venue_city,
    coalesce(venue_state, 'UNKNOWN') as venue_state,
    coalesce(venue_country, 'UNKNOWN') as venue_country,
    count(*) as total_events,
    sum(coalesce(yes_rsvp_count, 0)) as total_yes_rsvp,
    sum(coalesce(maybe_rsvp_count, 0)) as total_maybe_rsvp,
    sum(coalesce(waitlist_count, 0)) as total_waitlist,
    avg(coalesce(event_rating_average, 0)) as avg_event_rating,
    avg(coalesce(group_rating, 0)) as avg_group_rating,
    avg(coalesce(fee_amount, 0)) as avg_fee_amount,
    current_timestamp() as processed_at
from {{ ref('events_enriched') }}
group by
    1, 2, 3