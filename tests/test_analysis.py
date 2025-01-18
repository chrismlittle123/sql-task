import pytest
from datetime import datetime
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.analysis import OrderAnalysis
from src.transform import DataTransformer


def test_total_orders_by_customer(setup_raw_data, transformer, analysis, db_cursor):
    """Test total orders by customer calculation"""
    # Transform the data
    transformer.transform_data()

    results = analysis.get_total_orders_by_customer()

    # Verify results
    assert len(results) == 3  # Three unique customers

    # Verify table creation
    db_cursor.execute("SELECT COUNT(*) FROM metrics_total_orders_by_customer")
    assert db_cursor.fetchone()[0] == 3

    # Find John Doe's orders
    john_doe_orders = next(r for r in results if r["customer_name"] == "John Doe")
    assert john_doe_orders["total_orders"] == 2  # John Doe has 2 orders


def test_most_recent_orders(setup_raw_data, transformer, analysis, db_cursor):
    """Test most recent order by customer"""
    # Transform the data
    transformer.transform_data()

    results = analysis.get_most_recent_orders()

    # Verify results
    assert len(results) == 3  # One result per customer

    # Verify table creation
    db_cursor.execute("SELECT COUNT(*) FROM metrics_most_recent_order_by_customer")
    assert db_cursor.fetchone()[0] == 3

    # Most recent order should be John Smith's
    most_recent = results[0]  # Results are ordered by date DESC
    assert most_recent["customer_name"] == "John Smith"
    assert most_recent["order_id"] == "ORD004"


def test_top_customers_last_week(setup_raw_data, transformer, analysis, db_cursor):
    """Test top customers by value in last week"""
    # Transform the data
    transformer.transform_data()

    # First drop the metrics table if it exists
    db_cursor.execute("DROP TABLE IF EXISTS metrics_top_customers_last_week")

    # Create metrics table with our test data's date range
    db_cursor.execute(
        """
    CREATE TABLE metrics_top_customers_last_week AS
    SELECT 
        c.customer_name,
        SUM(f.price) as total_value,
        COUNT(f.order_id) as number_of_orders
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_dates d ON f.date_id = d.date_id
    WHERE d.full_date >= '2024-03-01' AND d.full_date <= '2024-03-05'
    GROUP BY c.customer_name
    ORDER BY total_value DESC
    """
    )

    results = db_cursor.execute(
        """
        SELECT customer_name, total_value, number_of_orders
        FROM metrics_top_customers_last_week
        ORDER BY total_value DESC
    """
    ).fetchall()

    # Verify table creation
    db_cursor.execute("SELECT COUNT(*) FROM metrics_top_customers_last_week")
    assert db_cursor.fetchone()[0] == 3

    # All orders in test data are within our date range
    assert len(results) == 3  # Three customers

    # John Doe should be top customer (300.00)
    top_customer = results[0]
    assert top_customer[0] == "John Doe"  # customer_name
    assert float(top_customer[1]) == 300.00  # total_value

    # John Smith should have value of 250.00
    john_smith = next(r for r in results if r[0] == "John Smith")
    assert float(john_smith[1]) == 250.00  # total_value
