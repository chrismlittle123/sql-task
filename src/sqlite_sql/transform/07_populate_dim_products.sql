INSERT OR IGNORE INTO dim_products (product_name) 
SELECT DISTINCT product_name 
FROM cleaned_orders; 