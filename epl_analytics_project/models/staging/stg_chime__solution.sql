with source as (
    select * from {{ source('raw_data', 'SOLUTION') }}
),

add_derived_fields as (
    select 
        *,
        REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}-[0-9]{2}') AS file_date
    from source
),

deduplicated as (
    select 
        * exclude rn
    from (
        select
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    file_date,
                    UPPER(TRIM(dsp)),
                    TRIM(route_code)
                ORDER BY 
                    COALESCE(loaded_at, CURRENT_TIMESTAMP()) DESC,
                    source_file ASC
            ) as rn
        from add_derived_fields
    )
    qualify rn = 1
)

select 
    -- Primary Key (composite)
    file_date || '_' || TRIM(route_code) as route_pk,
    
    -- Date fields
    file_date,
    
    -- Identifiers (standardized)
    UPPER(TRIM(dsp)) as dsp,
    TRIM(route_code) as route_code,
    
    -- Route attributes
    TRIM(service_type) as service_type,
    TRIM(wave) as wave,
    TRIM(staging_location) as staging_location,
    route_duration,
    
    -- Metrics
    num_zones,
    num_packages,
    num_commercial_pkgs,
    
    -- Metadata
    source_file,
    source_row_number,
    loaded_at
    
from deduplicated