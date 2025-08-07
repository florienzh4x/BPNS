{{
    config(
        materialized = 'incremental',
        unique_key = 'actor_id',
    )
}}

SELECT
  *
FROM {{ source('dvdrental_hive', 'actor') }}

{% if is_incremental() %}
  -- Only fetch rows updated after the last run
  WHERE last_update > (SELECT MAX(last_update) FROM {{ this }})
{% endif %}