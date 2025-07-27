CREATE SCHEMA minio.dvdrental
WITH (
  location = 's3a://landing-zones/dvdrental'
);

---

CREATE TABLE minio.dvdrental.film (
    film_id BIGINT,
    title VARCHAR,
    description VARCHAR,
    release_year BIGINT,
    language_id BIGINT,
    rental_duration BIGINT,
    rental_rate DOUBLE,
    length BIGINT,
    replacement_cost DOUBLE,
    rating VARCHAR,
    last_update TIMESTAMP,
    special_features ARRAY(VARCHAR),
    fulltext VARCHAR,
    updated_at TIMESTAMP,
    created_at TIMESTAMP,
    deleted_at TIMESTAMP

)
WITH (
    external_location = 's3a://landing-zones/dvdrental/public/film/2025-07-27',
    format = 'PARQUET'
);

-----

CREATE TABLE minio.dvdrental.staff (
    staff_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    address_id BIGINT,
    email VARCHAR,
    store_id BIGINT,
    active BOOLEAN,
    username VARCHAR,
    password VARCHAR,
    last_update TIMESTAMP,
    picture VARBINARY
)
WITH (
    external_location = 's3a://landing-zones/dvdrental/public/staff/2025-07-27',
    format = 'PARQUET'
);

----

CREATE TABLE minio.dvdrental.customer (
    customer_id BIGINT,
    store_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    address_id BIGINT,
    activebool BOOLEAN,
    create_date DATE,
    last_update TIMESTAMP,
    active BIGINT,
    updated_at TIMESTAMP,
    created_at TIMESTAMP,
    deleted_at TIMESTAMP
)
WITH (
    external_location = 's3a://landing-zones/dvdrental/public/customer/2025-07-27',
    format = 'PARQUET'
);