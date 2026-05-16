select
    "venue_id" as venue_id,
    "venue_name" as venue_name,
    "city" as city,
    "state" as state,
    "country" as country,
    "address_1" as address_1,
    "lat" as lat,
    "lon" as lon,
    "localized_country_name" as localized_country_name,
    "rating" as rating,
    "rating_count" as rating_count,
    "normalised_rating" as normalised_rating
from {{ source('raw', 'venues') }}