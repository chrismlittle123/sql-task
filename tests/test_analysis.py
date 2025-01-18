import pytest
from datetime import datetime
import os
import sys

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis import OrderAnalysis
from src.transform import DataTransformer


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


def test_total_orders_by_customer(setup_raw_data, transformer, analysis):
    """Test total orders by customer calculation"""
    # Transform the data
    transformer.transform_data()

    results = analysis.get_total_orders_by_customer()

    # Verify results
    assert len(results) == 3  # Three unique customers
    john_doe_orders = next(r for r in results if r["customer_name"] == "John Doe")
    assert john_doe_orders["total_orders"] == 2  # John Doe has 2 orders


def test_most_recent_orders(setup_raw_data, transformer, analysis):
    """Test most recent order by customer"""
    # Transform the data
    transformer.transform_data()

    results = analysis.get_most_recent_orders()

    # Verify results
    assert len(results) == 3  # One result per customer
    most_recent = results[0]  # Results are ordered by date DESC
    assert most_recent["customer_name"] == "John Smith"
    assert most_recent["order_id"] == "ORD004"


def test_top_customers_last_week(setup_raw_data, transformer, analysis):
    """Test top customers by value in last week"""
    # Transform the data
    transformer.transform_data()

    # Use a reference date that includes our test data
    reference_date = datetime(2024, 3, 5)  # One day after our last test order
    results = analysis.get_top_customers_last_week(reference_date)

    # All orders in test data are within last week
    assert len(results) == 3  # Three customers

    # John Doe should be top customer (300.00)
    top_customer = results[0]
    assert top_customer["customer_name"] == "John Doe"
    assert float(top_customer["total_value"]) == 300.00

    # John Smith should have value of 250.00
    john_smith = next(r for r in results if r["customer_name"] == "John Smith")
    assert float(john_smith["total_value"]) == 250.00
