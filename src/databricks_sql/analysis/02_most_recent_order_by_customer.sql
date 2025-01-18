CREATE TABLE IF NOT EXISTS metrics_most_recent_order_by_customer AS
WITH RankedOrders AS (
    SELECT 
        c.customer_name,
        f.order_id,
        d.full_date AS order_date,
        p.product_name,
        f.price,
        ROW_NUMBER() OVER (PARTITION BY c.customer_name ORDER BY d.full_date DESC) AS rn
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_products p ON f.product_id = p.product_id
    JOIN dim_dates d ON f.date_id = d.date_id
)
SELECT 
    customer_name,
    order_id,
    order_date,
    product_name,
    price
FROM RankedOrders
WHERE rn = 1
ORDER BY order_date DESC;