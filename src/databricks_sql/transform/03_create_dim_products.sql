-- Create products dimension table
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products (
    product_id INT GENERATED ALWAYS AS IDENTITY,
    product_name STRING
) USING DELTA; 