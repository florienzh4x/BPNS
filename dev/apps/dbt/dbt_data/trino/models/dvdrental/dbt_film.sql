{{ config(
    materialized='table'
) }}

select * from {{ source('dvdrental', 'film') }} limit 10