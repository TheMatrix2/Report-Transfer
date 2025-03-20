import requests
import datetime
import pandas as pd
from urllib.parse import urlencode
import mysql.connector
from io import StringIO
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("metrika_etl.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("metrika_etl")

url = 'https://api-metrika.yandex.ru/stat/v1/data.csv'
token = 'y0__xDc3N9hGMXaNSDZo9K3ElW6wEIGmXlh21UQ7jVH75YjpLi8'
headers = {'Authorization': f'OAuth {token}'}

counter_id = '45158844'

DB_CONFIG = {
    'host': 'localhost',
    'user': 'metrika_user',
    'password': 'your_secure_password',
    'database': 'yandex_metrika',
    'port': 3306
}


def get_date_range(days=30):
    today = datetime.datetime.now()
    date_to = today.strftime('%Y-%m-%d')
    date_from = (today - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    return date_from, date_to


def get_metrika_report(params):
    full_url = f"{url}?{urlencode(params)}"
    try:
        response = requests.get(full_url, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Yandex Metrika: {e}")
        return None


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
                    date DATE,
                    visits INT,
                    users INT,
                    pageviews INT,
                    bounce_rate FLOAT,
                    page_depth FLOAT,
                    avg_visit_duration INT,
                    report_date DATE,
                    PRIMARY KEY (date, report_date)
                )
            ''',
            'traffic_sources': '''
                CREATE TABLE IF NOT EXISTS traffic_sources (
                    date DATE,
                    traffic_source VARCHAR(255),
                    visits INT,
                    users INT,
                    bounce_rate FLOAT,
                    page_depth FLOAT,
                    avg_visit_duration INT,
                    report_date DATE,
                    PRIMARY KEY (date, traffic_source, report_date)
                )
            ''',
            'devices': '''
                CREATE TABLE IF NOT EXISTS devices (
                    date DATE,
                    device_category VARCHAR(50),
                    visits INT,
                    users INT,
                    bounce_rate FLOAT,
                    report_date DATE,
                    PRIMARY KEY (date, device_category, report_date)
                )
            ''',
            'popular_pages': '''
                CREATE TABLE IF NOT EXISTS popular_pages (
                    date DATE,
                    url VARCHAR(2048),
                    title VARCHAR(1024),
                    pageviews INT,
                    users INT,
                    avg_visit_duration INT,
                    report_date DATE,
                    PRIMARY KEY (date, url(255), report_date)
                )
            ''',
            'geography': '''
                CREATE TABLE IF NOT EXISTS geography (
                    date DATE,
                    country VARCHAR(100),
                    city VARCHAR(100),
                    visits INT,
                    users INT,
                    report_date DATE,
                    PRIMARY KEY (date, country, city, report_date)
                )
            ''',
            'search_phrases': '''
                CREATE TABLE IF NOT EXISTS search_phrases (
                    date DATE,
                    search_phrase VARCHAR(1024),
                    visits INT,
                    report_date DATE,
                    PRIMARY KEY (date, search_phrase(255), report_date)
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


def load_data_to_db(data, table_name, report_date):
    if not data:
        logger.warning(f"No data to load for {table_name}")
        return False

    try:
        df = pd.read_csv(StringIO(data), sep=',')

        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace(':', '_') for col in df.columns]

        df['report_date'] = report_date

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        temp_table = f"temp_{table_name}"
        column_definitions = ", ".join([f"`{col}` TEXT" for col in df.columns])
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cursor.execute(f"CREATE TABLE {temp_table} ({column_definitions})")

        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        insert_query = f"INSERT INTO {temp_table} ({columns}) VALUES ({placeholders})"

        data_tuples = [tuple(row) for row in df.values]
        cursor.executemany(insert_query, data_tuples)

        cursor.execute(f"DELETE FROM {table_name} WHERE report_date = %s", (report_date,))

        column_mappings = {
            'visits_by_day': '''
                date, 
                visits, 
                users, 
                pageviews, 
                bounce_rate, 
                page_depth, 
                avg_visit_duration_seconds AS avg_visit_duration,
                report_date
            ''',
            'traffic_sources': '''
                date, 
                traffic_source, 
                visits, 
                users, 
                bounce_rate, 
                page_depth, 
                avg_visit_duration_seconds AS avg_visit_duration,
                report_date
            ''',
            'devices': '''
                date, 
                device_category, 
                visits, 
                users, 
                bounce_rate,
                report_date
            ''',
            'popular_pages': '''
                date, 
                url, 
                title, 
                pageviews, 
                users, 
                avg_visit_duration_seconds AS avg_visit_duration,
                report_date
            ''',
            'geography': '''
                date, 
                region_country AS country, 
                region_city AS city, 
                visits, 
                users,
                report_date
            ''',
            'search_phrases': '''
                date, 
                search_phrase, 
                visits,
                report_date
            '''
        }

        mapping = column_mappings.get(table_name)
        if mapping:
            cursor.execute(f"INSERT INTO {table_name} SELECT {mapping} FROM {temp_table}")

        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
        return True

    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        return False


def fetch_and_load_reports(days=30):
    date_from, date_to = get_date_range(days)
    report_date = datetime.datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Collecting data from {date_from} to {date_to}")

    if not init_database():
        logger.error("Failed to initialize database, aborting")
        return

    reports = [
        {
            'name': 'visits_by_day',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
                'dimensions': 'ym:s:date',
                'date1': date_from,
                'date2': date_to,
                'sort': 'ym:s:date'
            }
        },
        {
            'name': 'traffic_sources',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
                'dimensions': 'ym:s:date,ym:s:trafficSource',
                'date1': date_from,
                'date2': date_to,
                'sort': 'ym:s:date'
            }
        },
        {
            'name': 'devices',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:bounceRate',
                'dimensions': 'ym:s:date,ym:s:deviceCategory',
                'date1': date_from,
                'date2': date_to,
                'sort': 'ym:s:date'
            }
        },
        {
            'name': 'popular_pages',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:pv:pageviews,ym:pv:users,ym:pv:avgVisitDurationSeconds',
                'dimensions': 'ym:pv:date,ym:pv:URL,ym:pv:title',
                'date1': date_from,
                'date2': date_to,
                'sort': 'ym:pv:date,-ym:pv:pageviews',
                'limit': 10000
            }
        },
        {
            'name': 'geography',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users',
                'dimensions': 'ym:s:date,ym:s:regionCountry,ym:s:regionCity',
                'date1': date_from,
                'date2': date_to,
                'sort': 'ym:s:date,-ym:s:visits'
            }
        },
        {
            'name': 'search_phrases',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits',
                'dimensions': 'ym:s:date,ym:s:searchPhrase',
                'date1': date_from,
                'date2': date_to,
                'sort': 'ym:s:date',
                'filters': "ym:s:trafficSource=='organic'"
            }
        }
    ]

    success_count = 0
    for report in reports:
        logger.info(f"Fetching {report['name']} report")
        data = get_metrika_report(report['params'])

        if data:
            if load_data_to_db(data, report['name'], report_date):
                success_count += 1

    logger.info(f"ETL process completed. {success_count}/{len(reports)} reports successfully loaded.")


if __name__ == "__main__":
    import sys

    days = 360

    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid days argument, using default of 30 days")

    fetch_and_load_reports(days)