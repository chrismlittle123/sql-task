from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
import sys

# Add project root to Python path when running directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.sqlite_database import Database


class OrderAnalysis:
    def __init__(self, db_path: str = "data/application.db"):
        self.db = Database(db_path)
        self.sql_dir = os.path.join(os.path.dirname(__file__), "sqlite_sql", "analysis")

    def set_connection(self, connection):
        """Set a specific connection (used for testing)"""
        self.db.set_connection(connection)

    def get_total_orders_by_customer(self) -> List[Dict[str, Any]]:
        """Get total number of orders for each customer"""
        # Create metrics table
        self.db.execute_sql_file(self.sql_dir, "01_total_orders_by_customer.sql")

        # Return results
        cursor = self.db.connection.cursor()
        cursor.execute(
            "SELECT customer_name, total_orders FROM metrics_total_orders_by_customer"
        )
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_most_recent_orders(self) -> List[Dict[str, Any]]:
        """Get most recent order for each customer"""
        # Create metrics table
        self.db.execute_sql_file(self.sql_dir, "02_most_recent_order_by_customer.sql")

        # Return results
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT * FROM metrics_most_recent_order_by_customer")
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_top_customers_last_week(
        self, reference_date: datetime = None
    ) -> List[Dict[str, Any]]:
        """Get customers who provided most value in the past week"""
        if reference_date is None:
            reference_date = datetime.now()

        start_date = (reference_date - timedelta(days=7)).strftime("%Y-%m-%d")
        # Create metrics table
        self.db.execute_sql_file(
            self.sql_dir, "03_top_customers_last_week.sql", {"start_date": start_date}
        )

        # Return results
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT * FROM metrics_top_customers_last_week")
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


if __name__ == "__main__":
    analysis = OrderAnalysis()
    analysis.get_total_orders_by_customer()
    analysis.get_most_recent_orders()
    analysis.get_top_customers_last_week()
