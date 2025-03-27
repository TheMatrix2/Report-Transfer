import mysql.connector
from config import DB_CONFIG, logger

def init_database():
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        conn.commit()

        cursor.execute(f"USE {DB_CONFIG['database']}")

        tables = {
            'visits_by_day': '''
                CREATE TABLE IF NOT EXISTS visits_by_day (
                    stat_date DATE,
                    visits INT,
                    users INT,
                    pageviews INT,
                    bounce_rate FLOAT,
                    page_depth FLOAT,
                    avg_visit_duration TIME,
                    PRIMARY KEY (stat_date)
                )
            ''',
            'traffic_sources': '''
                CREATE TABLE IF NOT EXISTS traffic_sources (
                    stat_date DATE,
                    traffic_source VARCHAR(255),
                    visits INT,
                    users INT,
                    bounce_rate FLOAT,
                    page_depth FLOAT,
                    avg_visit_duration TIME,
                    PRIMARY KEY (stat_date, traffic_source)
                )
            ''',
            'devices': '''
                CREATE TABLE IF NOT EXISTS devices (
                    stat_date DATE,
                    device_category VARCHAR(50),
                    visits INT,
                    users INT,
                    bounce_rate FLOAT,
                    PRIMARY KEY (stat_date, device_category)
                )
            ''',
            'geography': '''
                CREATE TABLE IF NOT EXISTS geography (
                    stat_date DATE,
                    country VARCHAR(100),
                    city VARCHAR(100),
                    visits INT,
                    users INT,
                    PRIMARY KEY (stat_date, country, city)
                )
            ''',
            'search_phrases': '''
                CREATE TABLE IF NOT EXISTS search_phrases (
                    stat_date DATE,
                    search_phrase VARCHAR(1024),
                    visits INT,
                    PRIMARY KEY (stat_date, search_phrase(255))
                )
            '''
        }

        for table_name, query in tables.items():
            logger.info(f"Creating table {table_name} if it doesn't exist")
            cursor.execute(query)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Database and tables initialization complete")
        return True
    except mysql.connector.Error as e:
        logger.error(f"Database initialization error: {e}")
        return False