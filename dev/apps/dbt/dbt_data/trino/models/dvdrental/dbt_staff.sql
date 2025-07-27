{{ config(
    materialized='view'
) }}

select * from {{ source('dvdrental', 'staff') }} limit 1