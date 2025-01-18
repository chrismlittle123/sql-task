import sqlite3


def create_star_schema():
    conn = sqlite3.connect("data/application.db")
    cursor = conn.cursor()

    # Create dimension tables
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_name TEXT UNIQUE
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id TEXT PRIMARY KEY,
        passenger_capacity INTEGER
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_dates (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        full_date DATETIME,
        year INTEGER,
        month INTEGER,
        day INTEGER
    )
    """
    )

    # Create fact table
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id TEXT PRIMARY KEY,
        customer_id INTEGER,
        product_id TEXT,
        date_id INTEGER,
        price DECIMAL(10,2),
        passenger_count INTEGER,
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
        FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
    )
    """
    )

    # Populate dimension tables from raw data
    cursor.execute(
        "INSERT OR IGNORE INTO dim_customers (customer_name) SELECT DISTINCT customer_name FROM raw_orders"
    )

    cursor.execute(
        """
    INSERT OR IGNORE INTO dim_products (product_id, passenger_capacity) 
    SELECT DISTINCT product_id, MAX(passenger_count) 
    FROM raw_orders 
    GROUP BY product_id
    """
    )

    cursor.execute(
        """
    INSERT OR IGNORE INTO dim_dates (full_date, year, month, day)
    SELECT DISTINCT 
        order_date,
        CAST(strftime('%Y', order_date) AS INTEGER),
        CAST(strftime('%m', order_date) AS INTEGER),
        CAST(strftime('%d', order_date) AS INTEGER)
    FROM raw_orders
    """
    )

    # Populate fact table
    cursor.execute(
        """
    INSERT OR IGNORE INTO fact_orders (
        order_id, customer_id, product_id, date_id, price, passenger_count
    )
    SELECT 
        r.order_id,
        c.customer_id,
        r.product_id,
        d.date_id,
        r.price,
        r.passenger_count
    FROM raw_orders r
    JOIN dim_customers c ON r.customer_name = c.customer_name
    JOIN dim_dates d ON r.order_date = d.full_date
    """
    )

    conn.commit()

    # Print some statistics
    print("Data transformation completed!")
    for table in ["dim_customers", "dim_products", "dim_dates", "fact_orders"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")

    conn.close()


if __name__ == "__main__":
    create_star_schema()
