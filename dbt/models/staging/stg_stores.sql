SELECT
    STORE_ID,
    STORE_NAME,
    CITY,
    REGION
FROM {{ source('retail_staging', 'stores') }}
