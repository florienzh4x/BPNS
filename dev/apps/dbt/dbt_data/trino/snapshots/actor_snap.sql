{% snapshot actor_snap %}

{{
    config(
        unique_key='actor_id',
        strategy='check',
        check_cols=['first_name', 'last_name'],
        target_schema='dvdrental',
        column_types={
            'dbt_valid_from': 'timestamp',
            'dbt_valid_to': 'timestamp',
            'dbt_updated_at': 'timestamp',
            'dbt_scd_id': 'varchar'
        }
    )
}}

SELECT
    actor_id,
    first_name,
    last_name,
    CAST(last_update AS timestamp) AS last_update
FROM {{ source('dvdrental', 'actor') }}

{% endsnapshot %}
