import mysql.connector
import os
import sys
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Function.utils import load_config

class DatabaseConnector:
    """
    Handles connection and operations with the MySQL/MariaDB database.
    This version will establish and close connection for each query to ensure fresh data.
    """
    _config = None

    def __init__(self):
        if not DatabaseConnector._config:
            config_path = os.path.join(os.path.dirname(__file__), '..', 'Config', 'config.json')
            DatabaseConnector._config = load_config(config_path)
            if not DatabaseConnector._config or "DATABASE_CONFIG" not in DatabaseConnector._config:
                logger.error("Database configuration not found or invalid in config.json. Exiting.")
                sys.exit(1)

    # Removed global _connection. Connection will be local to execute_query.

    def _get_new_connection(self):
        """Internal helper to get a fresh database connection."""
        try:
            db_conf = DatabaseConnector._config["DATABASE_CONFIG"]
            conn = mysql.connector.connect(
                host=db_conf["HOST"],
                user=db_conf["USER"],
                password=db_conf["PASSWORD"],
                database=db_conf["DATABASE"]
            )
            logger.debug("New database connection established.")
            return conn
        except mysql.connector.Error as err:
            logger.error(f"Error establishing new database connection: {err}")
            raise err

    def execute_query(self, query, params=None, fetch_one=False, fetch_all=False, commit=False):
        """
        Executes a SQL query. Establishes a new connection and closes it after execution.
        This ensures fresh data and handles connections explicitly.
        """
        connection = None
        cursor = None
        try:
            connection = self._get_new_connection() # Get a fresh connection
            cursor = connection.cursor(buffered=True)
            logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)

            if commit:
                connection.commit()
                logger.debug("Database transaction committed.")

            if fetch_one:
                return cursor.fetchone()
            elif fetch_all:
                return cursor.fetchall()
            return None

        except mysql.connector.Error as err:
            logger.error(f"Database query error: {err} | Query: {query} | Params: {params}")
            if connection and connection.is_connected() and commit:
                connection.rollback()
                logger.warning("Database transaction rolled back due to error.")
            raise err
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close() # <--- Close the connection after each execution
                logger.debug("Database connection closed after query.")

    # Removed connect() and disconnect() methods as they are now handled per query
    def connect(self): # Keep for compatibility with existing calls in bot main.py for initial test
        logger.debug("DatabaseConnector.connect() called. Forcing a new connection check.")
        conn = self._get_new_connection()
        conn.close() # Close immediately after test connection
        logger.info("Bot successfully tested database connection.")
        return True # Return True if test connection succeeded

    def disconnect(self): # Keep for compatibility, but no-op now
        logger.debug("DatabaseConnector.disconnect() called (no-op as connections are per query).")
        pass

# Example Usage (for testing db_connector.py individually)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    db = DatabaseConnector()
    try:
        # This initial connect call will now just test connection, not keep it open
        db.connect()

        # Insert some data
        test_user_id = 999999999
        test_id_stamp_in = "TEST-IN-20250531-ABC"
        db.execute_query(
            """INSERT INTO inventory (sku, lot, location, quantity, inbound_date, created_by, last_updated_by, id_stamp)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity), last_updated_by = VALUES(last_updated_by), id_stamp = VALUES(id_stamp)""",
            ("TESTSKU1", "TESTLOT1", "TESTLOC1", 50, "2025-05-30", test_user_id, test_user_id, test_id_stamp_in),
            commit=True
        )
        logger.info("Inserted/Updated sample inventory data.")

        # Select data (should be fresh)
        select_inv_query = "SELECT * FROM inventory WHERE sku = %s"
        inv_data = db.execute_query(select_inv_query, ("TESTSKU1",), fetch_all=True)
        if inv_data:
            logger.info("Inventory Data after insert:")
            for row in inv_data:
                logger.info(f"  {row}")

        # Update data (should be committed immediately)
        db.execute_query("UPDATE inventory SET quantity = %s WHERE sku = %s", (55, "TESTSKU1"), commit=True)
        logger.info("Updated TESTSKU1 quantity.")

        # Select data again (should reflect update)
        inv_data_after_update = db.execute_query(select_inv_query, ("TESTSKU1",), fetch_all=True)
        if inv_data_after_update:
            logger.info("Inventory Data after update:")
            for row in inv_data_after_update:
                logger.info(f"  {row}")

    except mysql.connector.Error as err:
        logger.error(f"An error occurred during database operations: {err}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")