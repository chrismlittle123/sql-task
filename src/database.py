import sqlite3
import os
from typing import List, Dict, Any, Optional
from contextlib import contextmanager


class Database:
    def __init__(self, db_path: str = "data/application.db"):
        self.db_path = db_path
        self._conn = None  # For test database connection

    def set_connection(self, connection):
        """Set a specific connection (used for testing)"""
        self._conn = connection

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if self._conn is not None:
            # Use existing connection (for testing)
            yield self._conn
        else:
            # Create new connection
            conn = sqlite3.connect(self.db_path)
            try:
                yield conn
            finally:
                conn.close()

    def execute_sql_file(
        self, sql_dir: str, filename: str, params: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """Execute a SQL file and return results as a list of dictionaries"""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            file_path = os.path.join(sql_dir, filename)
            with open(file_path, "r") as f:
                sql = f.read()

            if params:
                # If we have parameters, use normal execute
                cursor.execute(sql, params)
            else:
                # For CREATE TABLE AS statements, use executescript
                cursor.executescript(sql)

            conn.commit()

            try:
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            except sqlite3.OperationalError:
                # No results to fetch (e.g., for CREATE/INSERT statements)
                return []

    def execute_sql(
        self, sql: str, params: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """Execute a SQL string and return results as a list of dictionaries"""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, params or {})
            conn.commit()

            try:
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            except sqlite3.OperationalError:
                # No results to fetch
                return []

    def execute_sql_script(self, sql: str) -> None:
        """Execute a SQL script that may contain multiple statements"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executescript(sql)
            conn.commit()
