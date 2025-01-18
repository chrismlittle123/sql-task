import pytest
import sqlite3
import os
import sys
from datetime import datetime, timedelta

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

from sqlite_analysis import OrderAnalysis
from sqlite_transform import DataTransformer


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
        ("John Doe", 101, 100.00, "2023-12-12", "lounge", 2),
        ("Jane Smith", 102, 150.00, "2023-12-10", "fast-track", 1),
        ("John Doe", 103, 200.00, "2023-12-08", "parking", 3),
        ("John Smith", 104, 250.00, "2023-12-06", "fast_track", 6),
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
def sql_dir():
    """Return the path to the SQL files directory"""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "src", "sql")


@pytest.fixture
def execute_sql_file(db_connection, sql_dir):
    """Helper function to execute SQL files"""

    def _execute_sql_file(file_path):
        cursor = db_connection.cursor()
        with open(file_path, "r") as f:
            sql = f.read()
            cursor.executescript(sql)
        db_connection.commit()

    return _execute_sql_file


@pytest.fixture
def analysis(db_connection):
    """Create analysis instance with test database"""
    analysis = OrderAnalysis(":memory:")
    analysis.set_connection(db_connection)
    return analysis


@pytest.fixture
def transformer(db_connection):
    """Create transformer instance with test database"""
    transformer = DataTransformer(":memory:")
    transformer.set_connection(db_connection)
    return transformer
