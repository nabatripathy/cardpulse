select * from {{ source('decision', 'trigger_event') }}
