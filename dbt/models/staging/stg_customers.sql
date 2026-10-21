SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    SIGNUP_DATE
FROM {{ source('retail_staging', 'customers') }}
