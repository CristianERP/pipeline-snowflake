CREATE OR REPLACE TABLE MEETUP_DE.ANALYTICS.EVENTS_ENRICHED AS
SELECT
    e."event_id",
    e."event_name",
    e."description" AS event_description,
    e."event_url",
    e."event_status",
    e."visibility" AS event_visibility,
    e."created" AS event_created,
    e."updated" AS event_updated,
    e."event_time",
    e."duration",
    e."utc_offset",
    e."headcount",
    e."rsvp_limit",
    e."yes_rsvp_count",
    e."maybe_rsvp_count",
    e."waitlist_count",
    e."photo_url",
    e."why",
    e."how_to_find_us",

    e."fee.accepts"      AS fee_accepts,
    e."fee.amount"       AS fee_amount,
    e."fee.currency"     AS fee_currency,
    e."fee.description"  AS fee_description,
    e."fee.label"        AS fee_label,
    e."fee.required"     AS fee_required,

    e."rating.average"   AS event_rating_average,
    e."rating.count"     AS event_rating_count,

    e."group_id",
    e."group.name"       AS event_group_name,
    e."group.urlname"    AS event_group_urlname,
    e."group.join_mode"  AS event_group_join_mode,
    e."group.who"        AS event_group_who,
    e."group.created"    AS event_group_created,
    e."group.group_lat"  AS event_group_lat,
    e."group.group_lon"  AS event_group_lon,
    -- CATEGORY
    g."category_id",
    g."category.name"       AS category_name,
    g."category.shortname"  AS category_shortname,
    g."city_id",
    g."city"                 AS group_city,
    g."state"                AS group_state,
    g."country"              AS group_country,
    g."members"              AS group_members,
    g."rating"               AS group_rating,
    g."visibility"           AS group_visibility,
    g."timezone"             AS group_timezone,
    g."urlname"              AS group_urlname,
    g."who"                  AS group_who,
    g."link"                 AS group_link,
    g."lat"                  AS group_lat,
    g."lon"                  AS group_lon,
    g."organizer.member_id"  AS organizer_member_id,
    g."organizer.name"     AS organizer_name,
    -- VENUE
    e."venue_id",
    COALESCE(e."venue.name", v."venue_name") AS venue_name,
    COALESCE(e."venue.city", v."city") AS venue_city,
    COALESCE(e."venue.state", v."state") AS venue_state,
    COALESCE(e."venue.country", v."country") AS venue_country,
    COALESCE(e."venue.address_1", v."address_1") AS venue_address_1,
    e."venue.address_2" AS venue_address_2,
    COALESCE(e."venue.lat", v."lat") AS venue_lat,
    COALESCE(e."venue.lon", v."lon") AS venue_lon,
    COALESCE(e."venue.localized_country_name", v."localized_country_name") AS venue_localized_country_name,
    e."venue.phone" AS venue_phone,
    e."venue.repinned" AS venue_repinned,
    e."venue.zip" AS venue_zip,
    v."rating" AS venue_rating,
    v."rating_count" AS venue_rating_count,
    v."normalised_rating" AS venue_normalised_rating,
    
    CURRENT_TIMESTAMP() AS processed_at
FROM MEETUP_DE.RAW.EVENTS e
LEFT JOIN MEETUP_DE.RAW.GROUPS g
    ON e."group_id" = g."group_id"
LEFT JOIN MEETUP_DE.RAW.VENUES v
    ON e."venue_id" = v."venue_id";


CREATE OR REPLACE TABLE MEETUP_DE.ANALYTICS.AGG_EVENTS_BY_CITY AS
SELECT
    COALESCE(venue_city, 'UNKNOWN') AS venue_city,
    COALESCE(venue_state, 'UNKNOWN') AS venue_state,
    COALESCE(venue_country, 'UNKNOWN') AS venue_country,
    COUNT(*) AS total_events,
    SUM(COALESCE("yes_rsvp_count", 0)) AS total_yes_rsvp,
    SUM(COALESCE("maybe_rsvp_count", 0)) AS total_maybe_rsvp,
    SUM(COALESCE("waitlist_count", 0)) AS total_waitlist,
    AVG(COALESCE(EVENT_RATING_AVERAGE, 0)) AS avg_event_rating,
    AVG(COALESCE(GROUP_RATING, 0)) AS avg_group_rating,
    AVG(COALESCE(FEE_AMOUNT, 0)) AS avg_fee_amount,
    CURRENT_TIMESTAMP() AS processed_at
FROM MEETUP_DE.ANALYTICS.EVENTS_ENRICHED
GROUP BY 1,2,3;

CREATE OR REPLACE TABLE MEETUP_DE.ANALYTICS.AGG_GROUPS_BY_CATEGORY AS
SELECT
    g."category_id",
    COALESCE(g."category.name", c."category_name", 'UNKNOWN') AS category_name,
    COALESCE(g."category.shortname", c."shortname", 'UNKNOWN') AS category_shortname,
    COUNT(DISTINCT g."group_id") AS total_groups,
    SUM(COALESCE(g."members", 0)) AS total_members,
    AVG(COALESCE(g."rating", 0)) AS avg_group_rating,
    CURRENT_TIMESTAMP() AS processed_at
FROM MEETUP_DE.RAW.GROUPS g
LEFT JOIN MEETUP_DE.RAW.CATEGORIES c
    ON g."category_id" = c."category_id"
GROUP BY 1,2,3;


CREATE OR REPLACE TABLE MEETUP_DE.ANALYTICS.AGG_GROUPS_BY_TOPIC AS
SELECT
    gt."topic_id",
    COALESCE(t."topic_name", gt."topic_name", 'UNKNOWN') AS topic_name,
    COUNT(DISTINCT gt."group_id") AS total_groups,
    SUM(COALESCE(g."members", 0)) AS total_group_members,
    AVG(COALESCE(g."rating", 0)) AS avg_group_rating,
    CURRENT_TIMESTAMP() AS processed_at
FROM MEETUP_DE.RAW.GROUPS_TOPICS gt
LEFT JOIN MEETUP_DE.RAW.TOPICS t
    ON gt."topic_id" = t."topic_id"
LEFT JOIN MEETUP_DE.RAW.GROUPS g
    ON gt."group_id" = g."group_id"
GROUP BY 1,2;


CREATE OR REPLACE TABLE MEETUP_DE.ANALYTICS.AGG_EVENTS_BY_GROUP AS
SELECT
    "group_id",
    event_group_name AS group_name,
    "category_id",
    COALESCE(category_name, 'UNKNOWN') AS category_name,
    COUNT(*) AS total_events,
    SUM(COALESCE("yes_rsvp_count", 0)) AS total_yes_rsvp,
    AVG(COALESCE(event_rating_average, 0)) AS avg_event_rating,
    AVG(COALESCE(group_rating, 0)) AS avg_group_rating,
    CURRENT_TIMESTAMP() AS processed_at
FROM MEETUP_DE.ANALYTICS.EVENTS_ENRICHED
GROUP BY 1,2,3,4;