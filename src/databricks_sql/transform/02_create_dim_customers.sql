CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT UNIQUE
); 