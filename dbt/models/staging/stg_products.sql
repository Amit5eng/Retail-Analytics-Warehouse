SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    UNIT_COST,
    UNIT_PRICE
FROM {{ source('retail_staging', 'products') }}
