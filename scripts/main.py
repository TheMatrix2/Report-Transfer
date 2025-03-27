import pandas as pd
import mysql.connector
from io import StringIO
import sys

from scripts.logger import logger
from scripts.metrika import get_date_range, get_metrika_report, counter_id
from scripts.database import init_database, DB_CONFIG


# Загрузка csv в базу данных
def load_data_to_db(data, table_name):
    if not data:
        logger.warning(f"No data to load for {table_name}")
        return False

    try:
        df = pd.read_csv(StringIO(data), sep=',')

        logger.info(f"Original columns for {table_name}: {df.columns.tolist()}")

        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace(':', '_').replace('%', 'pct') for col in
                      df.columns]

        logger.info(f"Cleaned columns for {table_name}: {df.columns.tolist()}")

        df = df.dropna(how='all')
        df = df.drop([2, 0])

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        column_mappings = {
            'visits_by_day': {
                'stat_date': 'Дата_визита',
                'visits': 'Визиты',
                'users': 'Посетители',
                'pageviews': 'Просмотры',
                'bounce_rate': 'Отказы',
                'page_depth': 'Глубина_просмотра',
                'avg_visit_duration': 'Время_на_сайте'
            },
            'traffic_sources': {
                'stat_date': 'Дата_визита',
                'traffic_source': 'Последний_источник_трафика',
                'visits': 'Визиты',
                'users': 'Посетители',
                'bounce_rate': 'Отказы',
                'page_depth': 'Глубина_просмотра',
                'avg_visit_duration': 'Время_на_сайте'
            },
            'devices': {
                'stat_date': 'Дата_визита',
                'device_category': 'Тип_устройства',
                'visits': 'Визиты',
                'users': 'Посетители',
                'bounce_rate': 'Отказы'
            },
            'geography': {
                'stat_date': 'Дата_визита',
                'country': 'Страна',
                'city': 'Город',
                'visits': 'Визиты',
                'users': 'Посетители'
            },
            'search_phrases': {
                'stat_date': 'Дата_визита',
                'search_phrase': 'Последняя_поисковая_фраза',
                'visits': 'Визиты'
            }
        }

        if table_name in column_mappings:
            mapping = {}
            for target_col, source_col in column_mappings[table_name].items():
                if source_col in df.columns:
                    mapping[target_col] = source_col
                else:
                    possible_matches = [col for col in df.columns if source_col.lower() in col.lower()]
                    if possible_matches:
                        mapping[target_col] = possible_matches[0]
                        logger.info(f"Using {possible_matches[0]} as {target_col} for {table_name}")
                    else:
                        logger.warning(f"Column {source_col} not found for {table_name}, setting to NULL")

            target_columns = list(column_mappings[table_name].keys())
            target_df = pd.DataFrame(columns=target_columns)

            for target_col, source_col in mapping.items():
                if source_col in df.columns:
                    target_df[target_col] = df[source_col]
                else:
                    target_df[target_col] = None

            df = target_df


        existing_dates_query = f"SELECT DISTINCT stat_date FROM {table_name}"
        cursor.execute(existing_dates_query)
        existing_dates = set(str(row[0]) for row in cursor.fetchall())

        # Удаление строк из датафрейма с датой, существующей в таблице
        df = df[~df['stat_date'].isin(existing_dates)]

        # Вставка новых строк
        if not df.empty:
            placeholders = ", ".join(["%s"] * len(df.columns))
            columns = ", ".join([f"`{col}`" for col in df.columns])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            data_tuples = [tuple(row) for row in df.values]

            if data_tuples:
                logger.info(f"First row sample for {table_name}: {data_tuples[0]}")
                logger.info(f"Inserting {len(data_tuples)} rows into {table_name}")

                cursor.executemany(insert_query, data_tuples)
                conn.commit()
                logger.info(f"Successfully loaded {len(data_tuples)} rows into {table_name}")
            else:
                logger.info(f"No new data to add for {table_name}")
                return True
        else:
            logger.info(f"No new unique dates to add for {table_name}")
            return True

        return True

    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def fetch_and_load_reports(days=30):
    date_from, date_to = get_date_range(days)
    logger.info(f"Collecting data from {date_from} to {date_to}")

    if not init_database():
        logger.error("Failed to initialize database, aborting")
        return

    # Параметры отчетов
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
            if load_data_to_db(data, report['name']):
                success_count += 1

    logger.info(f"ETL process completed. {success_count}/{len(reports)} reports successfully loaded.")


if __name__ == "__main__":

    days = 360

    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid days argument, using default of 30 days")

    fetch_and_load_reports(days)