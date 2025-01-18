-- Populate products dimension
INSERT INTO dim_products (product_name)
SELECT DISTINCT product_name FROM cleaned_orders; 