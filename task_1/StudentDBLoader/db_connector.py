# db_connector.py
import psycopg2
import psycopg2.extras
import sys
from sql_queries import CREATE_TABLES, CREATE_INDEXES


class DatabaseConnector:
    """
    Handles connection to PostgreSQL database
    and execution of SQL queries.
    """

    def __init__(self, dbname, user, password, host, port):
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.conn.autocommit = False
        except psycopg2.OperationalError as e:
            print(f"Database connection error: {e}", file=sys.stderr)
            sys.exit(1)

    def create_tables(self):
        """Creates tables and indexes."""
        try:
            with self.conn.cursor() as cursor:
                # Create tables
                cursor.execute(CREATE_TABLES)
                # Create indexes
                cursor.execute(CREATE_INDEXES)
                self.conn.commit()
                print("Tables and indexes created successfully.")

        except Exception as e:
            print(f"Error creating tables: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def execute_query(self, query, params=None, fetch_many=False):
        """Executes an SQL query and returns the result."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if fetch_many:
                    return cursor.fetchall()
                return cursor.fetchone()
        except Exception as e:
            print(f"Error executing query: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def execute_many(self, query, data_list):
        """Performs batch data insertion."""
        try:
            with self.conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, data_list)
                self.conn.commit()
        except Exception as e:
            print(f"Batch insert error: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def close(self):
        """Closes the database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()