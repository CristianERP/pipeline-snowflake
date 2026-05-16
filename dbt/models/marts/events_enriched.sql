select
    e.event_id,
    e.event_name,
    e.event_description,
    e.event_url,
    e.event_status,
    e.event_visibility,
    e.event_created,
    e.event_updated,
    e.event_time,
    e.duration,
    e.utc_offset,
    e.headcount,
    e.rsvp_limit,
    e.yes_rsvp_count,
    e.maybe_rsvp_count,
    e.waitlist_count,
    e.photo_url,
    e.why,
    e.how_to_find_us,

    e.fee_accepts,
    e.fee_amount,
    e.fee_currency,
    e.fee_description,
    e.fee_label,
    e.fee_required,

    e.event_rating_average,
    e.event_rating_count,

    e.group_id,
    e.event_group_name,
    e.event_group_urlname,
    e.event_group_join_mode,
    e.event_group_who,
    e.event_group_created,
    e.event_group_lat,
    e.event_group_lon,

    g.category_id,
    g.category_name,
    g.category_shortname,
    g.city_id,
    g.group_city,
    g.group_state,
    g.group_country,
    g.group_members,
    g.group_rating,
    g.group_visibility,
    g.group_timezone,
    g.group_urlname,
    g.group_who,
    g.group_link,
    g.group_lat,
    g.group_lon,
    g.organizer_member_id,
    g.organizer_name,

    e.venue_id,
    coalesce(e.venue_name, v.venue_name) as venue_name,
    coalesce(e.venue_city, v.city) as venue_city,
    coalesce(e.venue_state, v.state) as venue_state,
    coalesce(e.venue_country, v.country) as venue_country,
    coalesce(e.venue_address_1, v.address_1) as venue_address_1,
    e.venue_address_2,
    coalesce(e.venue_lat, v.lat) as venue_lat,
    coalesce(e.venue_lon, v.lon) as venue_lon,
    coalesce(
        e.venue_localized_country_name,
        v.localized_country_name
    ) as venue_localized_country_name,
    e.venue_phone,
    e.venue_repinned,
    e.venue_zip,

    v.rating as venue_rating,
    v.rating_count as venue_rating_count,
    v.normalised_rating as venue_normalised_rating,

    current_timestamp() as processed_at

from {{ ref('stg_events') }} e
left join {{ ref('stg_groups') }} g
    on e.group_id = g.group_id
left join {{ ref('stg_venues') }} v
    on e.venue_id = v.venue_id