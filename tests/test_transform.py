import pytest
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sqlite_transform import DataTransformer


def get_sql_path(filename):
    """Helper to get the full path to SQL files"""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "src", "sql", "transform", filename
    )


def test_create_cleaned_orders(setup_raw_data, execute_sql_file, db_cursor, sql_dir):
    """Test creation of cleaned_orders table"""
    execute_sql_file(os.path.join(sql_dir, "transform", "01_create_cleaned_orders.sql"))

    # Verify the structure and data
    db_cursor.execute("SELECT * FROM cleaned_orders")
    rows = db_cursor.fetchall()

    assert len(rows) == 4  # Four records in test data
    # Verify fast-track transformation for both records
    db_cursor.execute(
        "SELECT product_name FROM cleaned_orders WHERE order_id IN ('ORD002', 'ORD004')"
    )
    product_names = db_cursor.fetchall()
    assert len(product_names) == 2
    assert all(name[0] == "fast_track" for name in product_names)


def test_create_dimension_tables(setup_raw_data, execute_sql_file, db_cursor, sql_dir):
    """Test creation of dimension tables"""
    # Set up cleaned_orders first
    execute_sql_file(os.path.join(sql_dir, "transform", "01_create_cleaned_orders.sql"))

    # Create dimension tables
    execute_sql_file(os.path.join(sql_dir, "transform", "02_create_dim_customers.sql"))
    execute_sql_file(os.path.join(sql_dir, "transform", "03_create_dim_products.sql"))
    execute_sql_file(os.path.join(sql_dir, "transform", "04_create_dim_dates.sql"))

    # Verify table structures
    tables = [
        ("dim_customers", ["customer_id", "customer_name"]),
        ("dim_products", ["product_id", "product_name"]),
        ("dim_dates", ["date_id", "full_date", "year", "month", "day"]),
    ]

    for table_name, expected_columns in tables:
        db_cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in db_cursor.fetchall()]
        assert set(columns) == set(expected_columns)  # Order doesn't matter


def test_populate_dimension_tables(
    setup_raw_data, execute_sql_file, db_cursor, sql_dir
):
    """Test population of dimension tables"""
    # Set up all required tables
    execute_sql_file(os.path.join(sql_dir, "transform", "01_create_cleaned_orders.sql"))
    execute_sql_file(os.path.join(sql_dir, "transform", "02_create_dim_customers.sql"))
    execute_sql_file(os.path.join(sql_dir, "transform", "03_create_dim_products.sql"))
    execute_sql_file(os.path.join(sql_dir, "transform", "04_create_dim_dates.sql"))

    # Populate dimension tables
    execute_sql_file(
        os.path.join(sql_dir, "transform", "06_populate_dim_customers.sql")
    )
    execute_sql_file(os.path.join(sql_dir, "transform", "07_populate_dim_products.sql"))
    execute_sql_file(os.path.join(sql_dir, "transform", "08_populate_dim_dates.sql"))

    # Verify customer data
    db_cursor.execute("SELECT customer_name FROM dim_customers ORDER BY customer_name")
    customers = [row[0] for row in db_cursor.fetchall()]
    assert sorted(customers) == ["Jane Smith", "John Doe", "John Smith"]

    # Verify product data
    db_cursor.execute("SELECT product_name FROM dim_products ORDER BY product_name")
    products = [row[0] for row in db_cursor.fetchall()]
    assert "fast_track" in products
    assert "lounge" in products
    assert "parking" in products
    assert len(products) == 3  # lounge, parking, fast_track


def test_create_and_populate_fact_table(
    setup_raw_data, execute_sql_file, db_cursor, sql_dir
):
    """Test creation and population of fact table"""
    # Set up all required tables and data
    sql_files = [
        "01_create_cleaned_orders.sql",
        "02_create_dim_customers.sql",
        "03_create_dim_products.sql",
        "04_create_dim_dates.sql",
        "05_create_fact_orders.sql",
        "06_populate_dim_customers.sql",
        "07_populate_dim_products.sql",
        "08_populate_dim_dates.sql",
        "09_populate_fact_orders.sql",
    ]

    for sql_file in sql_files:
        execute_sql_file(os.path.join(sql_dir, "transform", sql_file))

    # Verify fact table data
    db_cursor.execute("SELECT COUNT(*) FROM fact_orders")
    assert db_cursor.fetchone()[0] == 4  # Four orders in test data

    # Verify relationships
    db_cursor.execute(
        """
        SELECT f.order_id, c.customer_name, p.product_name, d.full_date, f.price
        FROM fact_orders f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        JOIN dim_products p ON f.product_id = p.product_id
        JOIN dim_dates d ON f.date_id = d.date_id
        ORDER BY f.order_id
    """
    )
    rows = db_cursor.fetchall()

    assert len(rows) == 4
    assert rows[0][0] == 101  # First order is lounge
    assert rows[3][0] == 103  # Last order is fast_track

    # Check all customers exist
    customer_names = [row[1] for row in rows]
    assert "Jane Smith" in customer_names
    assert "John Doe" in customer_names
    assert "John Smith" in customer_names

    # Check products
    product_names = [row[2] for row in rows]
    assert "lounge" in product_names
    assert "parking" in product_names
    assert sum(name == "fast_track" for name in product_names) == 2
