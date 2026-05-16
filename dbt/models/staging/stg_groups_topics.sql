select
    "group_id" as group_id,
    "topic_id" as topic_id,
    "topic_name" as topic_name
from {{ source('raw', 'groups_topics') }}