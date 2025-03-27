import mysql.connector
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Database configuration - update with your credentials
DB_CONFIG = {
    'host': 'localhost',
    'user': 'metrika_user',
    'password': 'your_secure_password',
    'database': 'yandex_metrika',
    'port': 3306
}


def test_connection():
    """Test the database connection and print table information"""
    try:
        # Connect to the database
        logger.info("Attempting database connection...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        logger.info("Connection successful!")

        # Get list of tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        if not tables:
            logger.info("No tables found in the database.")
            return

        logger.info(f"Tables in database: {', '.join([t[0] for t in tables])}")

        # Check each table's structure and row count
        for table in tables:
            table_name = table[0]

            # Get column information
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            logger.info(f"\nTable: {table_name}")
            logger.info("Columns:")
            for col in columns:
                logger.info(f"  - {col[0]} ({col[1]})")

            # Count rows
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"Row count: {count}")

            # Show sample data if table has rows
            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                logger.info("Sample data:")
                column_names = [column[0] for column in cursor.description]
                for i, row in enumerate(rows):
                    logger.info(f"  Row {i + 1}: {dict(zip(column_names, row))}")

        # Close connection
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    test_connection()