with source as (
    select * from {{ source('raw_data', 'ROUTES') }}
),

add_file_date as (
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
                    TRIM(route_code),
                    TRIM(driver_name)
                ORDER BY 
                    COALESCE(loaded_at, CURRENT_TIMESTAMP()) DESC,
                    source_file ASC
            ) as rn
        from add_file_date
    )
    qualify rn = 1
),

cleaned as (
    select 
        -- Primary Key (composite) - matches solution table
        file_date || '_' || TRIM(route_code) as route_pk,
        
        -- Date fields
        file_date,
        
        -- Identifiers (standardized)
        'EPLC' as dsp,  -- Hard-coded because routes data doesn't have DSP
        TRIM(route_code) as route_code,
        TRIM(transporter_id) as transporter_id,
        
        -- Driver information
        TRIM(driver_name) as driver_name,
        
        -- Route metrics
        TRIM(route_progress) as route_progress,
        TRIM(delivery_service_type) as delivery_service_type,
        route_duration,
        
        -- Stop counts (convert to integers)
        TRY_CAST(all_stops AS INTEGER) AS all_stops,
        TRY_CAST(stops_complete AS INTEGER) AS stops_complete,
        TRY_CAST(not_started_stops AS INTEGER) AS not_started_stops,
        
        -- Metadata
        source_file,
        source_row_number,
        loaded_at
        
    from deduplicated
)

select * from cleaned