-- Create products dimension table
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name STRING
) USING DELTA; 