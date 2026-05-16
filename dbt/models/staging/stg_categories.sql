select
    "category_id" as category_id,
    "category_name" as category_name,
    "shortname" as shortname
from {{ source('raw', 'categories') }}