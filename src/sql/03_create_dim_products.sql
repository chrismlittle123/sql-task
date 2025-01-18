CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT UNIQUE
); 