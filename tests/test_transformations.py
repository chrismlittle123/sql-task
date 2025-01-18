import pytest
import os
from datetime import datetime


def get_sql_path(filename):
    """Helper to get the full path to SQL files"""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "src", "sql", filename
    )


def test_create_cleaned_orders(setup_raw_data, execute_sql_file, db_cursor):
    """Test creation of cleaned_orders table"""
    execute_sql_file(get_sql_path("01_create_cleaned_orders.sql"))

    # Verify the structure and data
    db_cursor.execute("SELECT * FROM cleaned_orders")
    rows = db_cursor.fetchall()

    assert len(rows) == 3
    # Verify fast-track transformation
    db_cursor.execute(
        "SELECT product_name FROM cleaned_orders WHERE order_id = 'ORD002'"
    )
    assert db_cursor.fetchone()[0] == "fast_track"


def test_create_dimension_tables(setup_raw_data, execute_sql_file, db_cursor):
    """Test creation of dimension tables"""
    # Set up cleaned_orders first
    execute_sql_file(get_sql_path("01_create_cleaned_orders.sql"))

    # Create dimension tables
    execute_sql_file(get_sql_path("02_create_dim_customers.sql"))
    execute_sql_file(get_sql_path("03_create_dim_products.sql"))
    execute_sql_file(get_sql_path("04_create_dim_dates.sql"))

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


def test_populate_dimension_tables(setup_raw_data, execute_sql_file, db_cursor):
    """Test population of dimension tables"""
    # Set up all required tables
    execute_sql_file(get_sql_path("01_create_cleaned_orders.sql"))
    execute_sql_file(get_sql_path("02_create_dim_customers.sql"))
    execute_sql_file(get_sql_path("03_create_dim_products.sql"))
    execute_sql_file(get_sql_path("04_create_dim_dates.sql"))

    # Populate dimension tables
    execute_sql_file(get_sql_path("06_populate_dim_customers.sql"))
    execute_sql_file(get_sql_path("07_populate_dim_products.sql"))
    execute_sql_file(get_sql_path("08_populate_dim_dates.sql"))

    # Verify customer data
    db_cursor.execute("SELECT customer_name FROM dim_customers ORDER BY customer_name")
    customers = [row[0] for row in db_cursor.fetchall()]
    assert sorted(customers) == ["Jane Smith", "John Doe"]

    # Verify product data
    db_cursor.execute("SELECT product_name FROM dim_products ORDER BY product_name")
    products = [row[0] for row in db_cursor.fetchall()]
    assert "fast_track" in products

    # Verify date data
    db_cursor.execute("SELECT COUNT(*) FROM dim_dates")
    assert db_cursor.fetchone()[0] == 3  # Three unique dates in test data


def test_create_and_populate_fact_table(setup_raw_data, execute_sql_file, db_cursor):
    """Test creation and population of fact table"""
    # Set up all required tables and data
    execute_sql_file(get_sql_path("01_create_cleaned_orders.sql"))
    execute_sql_file(get_sql_path("02_create_dim_customers.sql"))
    execute_sql_file(get_sql_path("03_create_dim_products.sql"))
    execute_sql_file(get_sql_path("04_create_dim_dates.sql"))
    execute_sql_file(get_sql_path("05_create_fact_orders.sql"))
    execute_sql_file(get_sql_path("06_populate_dim_customers.sql"))
    execute_sql_file(get_sql_path("07_populate_dim_products.sql"))
    execute_sql_file(get_sql_path("08_populate_dim_dates.sql"))
    execute_sql_file(get_sql_path("09_populate_fact_orders.sql"))

    # Verify fact table data
    db_cursor.execute("SELECT COUNT(*) FROM fact_orders")
    assert db_cursor.fetchone()[0] == 3  # All orders present

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

    assert len(rows) == 3
    assert rows[0][0] == "ORD001"  # First order
    assert any(row[1] == "Jane Smith" for row in rows)  # Jane Smith exists
    assert any(row[2] == "fast_track" for row in rows)  # fast_track product exists
