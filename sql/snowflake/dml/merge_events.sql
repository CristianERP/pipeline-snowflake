MERGE INTO MEETUP_DE.RAW.EVENTS AS tgt
USING (
    SELECT *
    FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
) AS src
ON tgt."event_id" = src."event_id"

WHEN MATCHED THEN UPDATE SET
    tgt."yes_rsvp_count" = src."yes_rsvp_count",
    tgt."maybe_rsvp_count" = src."maybe_rsvp_count",
    tgt."waitlist_count" = src."waitlist_count",
    tgt."updated" = src."updated",
    tgt."event_status" = src."event_status"

WHEN NOT MATCHED THEN INSERT ALL BY NAME;