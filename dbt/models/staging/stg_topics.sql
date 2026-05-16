select
    "topic_id" as topic_id,
    "topic_name" as topic_name
from {{ source('raw', 'topics') }}