SELECT
    HASH(STORE_ID) AS STORE_KEY,
    STORE_ID,
    STORE_NAME,
    CITY,
    REGION
FROM {{ ref('stg_stores') }}
