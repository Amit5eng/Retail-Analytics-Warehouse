SELECT
    s.TRANSACTION_ID,
    d.DATE_KEY,
    p.PRODUCT_KEY,
    c.CUSTOMER_KEY,
    st.STORE_KEY,
    s.QUANTITY,
    s.UNIT_PRICE,
    s.UNIT_COST,
    s.SALES_AMOUNT,
    s.COST_AMOUNT,
    s.PROFIT_AMOUNT,
    s.PAYMENT_METHOD,
    s.ORDER_STATUS,
    s.TRANSACTION_DATE
FROM {{ ref('int_sales_enriched') }} AS s
INNER JOIN {{ ref('dim_date') }} AS d
    ON s.TRANSACTION_DATE = d.DATE_DAY
INNER JOIN {{ ref('dim_products') }} AS p
    ON s.PRODUCT_ID = p.PRODUCT_ID
INNER JOIN {{ ref('dim_customers') }} AS c
    ON s.CUSTOMER_ID = c.CUSTOMER_ID
INNER JOIN {{ ref('dim_stores') }} AS st
    ON s.STORE_ID = st.STORE_ID
