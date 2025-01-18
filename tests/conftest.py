import pytest
import sqlite3
import os
from datetime import datetime, timedelta


@pytest.fixture
def db_connection():
    """Create a temporary in-memory database for testing"""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def db_cursor(db_connection):
    """Create a database cursor"""
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture
def setup_raw_data(db_connection):
    """Set up raw_orders table with test data"""
    cursor = db_connection.cursor()

    # Create raw_orders table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_orders (
            customer_name TEXT,
            order_id TEXT PRIMARY KEY,
            price DECIMAL(10,2),
            order_date DATETIME,
            product_id TEXT,
            passenger_count INTEGER
        )
    """
    )

    # Insert test data
    test_data = [
        ("John Doe", "ORD001", 100.00, "2024-03-01 10:00:00", "PROD001", 2),
        ("Jane Smith", "ORD002", 150.00, "2024-03-02 11:00:00", "fast-track", 1),
        ("John Doe", "ORD003", 200.00, "2024-03-03 12:00:00", "PROD002", 3),
    ]

    cursor.executemany(
        """
        INSERT INTO raw_orders 
        (customer_name, order_id, price, order_date, product_id, passenger_count)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        test_data,
    )

    db_connection.commit()
    return db_connection


@pytest.fixture
def execute_sql_file(db_connection):
    """Helper function to execute SQL files"""

    def _execute_sql_file(file_path):
        cursor = db_connection.cursor()
        with open(file_path, "r") as f:
            sql = f.read()
            cursor.executescript(sql)  # Use executescript to handle multiple statements
        db_connection.commit()

    return _execute_sql_file
