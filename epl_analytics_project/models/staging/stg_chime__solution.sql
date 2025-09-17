with source as (
    select * from {{ source('raw_data', 'SOLUTION') }}
)

select 
    *,
    REGEXP_SUBSTR(
        source_file, 
        '[0-9]{4}-[0-9]{2}-[0-9]{2}'
    ) AS file_date
from source