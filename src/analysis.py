from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
import sys

# Add project root to Python path when running directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.database import Database


class OrderAnalysis:
    def __init__(self, db_path: str = "data/application.db"):
        self.db = Database(db_path)
        self.sql_dir = os.path.join(os.path.dirname(__file__), "sql", "analysis")

    def set_connection(self, connection):
        """Set a specific connection (used for testing)"""
        self.db.set_connection(connection)

    def get_total_orders_by_customer(self):
        """Get total number of orders for each customer"""
        # Create metrics table
        self.db.execute_sql_file(self.sql_dir, "01_total_orders_by_customer.sql")
        # Execute the query without returning data
        self.db.execute_sql(
            """
            SELECT customer_name, total_orders 
            FROM metrics_total_orders_by_customer
            ORDER BY total_orders DESC
            """
        )

    def get_most_recent_orders(self):
        """Get most recent order for each customer"""
        # Create metrics table
        self.db.execute_sql_file(self.sql_dir, "02_most_recent_order_by_customer.sql")
        # Execute the query without returning data
        self.db.execute_sql(
            """
            SELECT customer_name, order_id, order_date, product_name, price
            FROM metrics_most_recent_order_by_customer
            ORDER BY order_date DESC
            """
        )

    def get_top_customers_last_week(self, reference_date: datetime = None):
        """Get customers who provided most value in the past week"""
        if reference_date is None:
            reference_date = datetime.now()

        start_date = (reference_date - timedelta(days=7)).strftime("%Y-%m-%d")
        # Create metrics table
        self.db.execute_sql_file(
            self.sql_dir, "03_top_customers_last_week.sql", {"start_date": start_date}
        )
        # Execute the query without returning data
        self.db.execute_sql(
            """
            SELECT customer_name, total_value, number_of_orders
            FROM metrics_top_customers_last_week
            ORDER BY total_value DESC
            """
        )


if __name__ == "__main__":
    analysis = OrderAnalysis()
    analysis.get_total_orders_by_customer()
    analysis.get_most_recent_orders()
    analysis.get_top_customers_last_week()
