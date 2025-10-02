{{
    config(
        materialized='table',
        schema='marts'
    )
}}

select * from {{ ref('stg_logistics__routes') }}