import pytest
from datetime import datetime
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sqlite_analysis import OrderAnalysis
from sqlite_transform import DataTransformer


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

    # Most recent order should be John Doe's (2023-12-12)
    most_recent = results[0]  # Results are ordered by date DESC
    assert most_recent["customer_name"] == "John Doe"
    assert most_recent["order_id"] == 101
    assert most_recent["order_date"].startswith("2023-12-12")


def test_top_customers_last_week(setup_raw_data, transformer, analysis, db_cursor):
    """Test top customers by value in last week"""
    # Transform the data
    transformer.transform_data()

    # Use a reference date that includes our test data
    reference_date = datetime(2023, 12, 12)  # Date of most recent order
    results = analysis.get_top_customers_last_week(reference_date)

    # Verify results
    assert len(results) == 2  # Two customers in the last week

    # John Smith should be top customer in the last week (250.00)
    top_customer = results[0]
    assert top_customer["customer_name"] == "John Smith"
    assert float(top_customer["total_value"]) == 250.00
    assert top_customer["number_of_orders"] == 1

    # John Doe should have one order of 200.00 in the last week
    john_doe = results[1]
    assert john_doe["customer_name"] == "John Doe"
    assert float(john_doe["total_value"]) == 200.00
    assert john_doe["number_of_orders"] == 1
