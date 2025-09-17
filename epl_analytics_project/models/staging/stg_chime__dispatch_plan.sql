with source as (
    select * from {{ source('raw_data', 'DISPATCH_PLAN') }}
)

select 
    *,
    
    REGEXP_SUBSTR(
        source_file, 
        '[0-9]{4}-[0-9]{2}-[0-9]{2}'
    ) AS file_date,
    last_value(dispatch_wave ignore nulls) over(partition by file_date order by source_row_number rows between unbounded preceding and current row) as dispatch_wave_adjusted
from source