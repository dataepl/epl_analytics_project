with source as (
    select * from {{ source('raw_data', 'ROUTES') }}
),

cleaned as (
    select 
        -- Metadata
        source_file,
        source_row_number,
        loaded_at,
        
        -- Extract file date for partitioning
        REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}-[0-9]{2}') AS file_date,
        
        -- Route identifiers
        route_code,
        dsp,
        transporter_id,
        
        -- Driver information
        driver_name,
        
        -- Route metrics
        route_progress,
        delivery_service_type,
        route_duration,
        
        -- Stop counts (convert to integers)
        TRY_CAST(all_stops AS INTEGER) AS all_stops,
        TRY_CAST(stops_complete AS INTEGER) AS stops_complete,
        TRY_CAST(not_started_stops AS INTEGER) AS not_started_stops,
        
        -- -- Calculated metrics
        -- CASE 
        --     WHEN TRY_CAST(all_stops AS INTEGER) > 0 
        --     THEN ROUND(TRY_CAST(stops_complete AS INTEGER) * 100.0 / TRY_CAST(all_stops AS INTEGER), 2)
        --     ELSE NULL 
        -- END AS completion_rate_pct
        
    from source
)

select * from cleaned