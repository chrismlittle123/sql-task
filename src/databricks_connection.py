from databricks.sql import connect
from databricks.sql.client import Connection
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
import pandas as pd


class DatabricksConnection:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Get connection parameters from environment variables
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError(
                "Missing required environment variables. Please ensure DATABRICKS_SERVER_HOSTNAME, "
                "DATABRICKS_HTTP_PATH, and DATABRICKS_ACCESS_TOKEN are set in your .env file"
            )

        self._connection = None

    def get_connection(self) -> Connection:
        """Get or create a Databricks SQL connection"""
        if self._connection is None:
            self._connection = connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
            )
        return self._connection

    def execute_sql(self, sql: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a pandas DataFrame"""
        connection = self.get_connection()
        with connection.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Get column names
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                # Fetch results and convert to DataFrame
                results = cursor.fetchall()
                return pd.DataFrame(results, columns=columns)
            return pd.DataFrame()

    def close(self):
        """Close the connection"""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __del__(self):
        """Ensure connection is closed when object is deleted"""
        self.close()
