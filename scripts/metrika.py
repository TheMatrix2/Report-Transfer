import requests
import datetime
from urllib.parse import urlencode
import os

from scripts.logger import logger

# API конфигурация
url = 'https://api-metrika.yandex.ru/stat/v1/data.csv'
token = str(os.getenv('TOKEN'))
headers = {'Authorization': f'OAuth {token}'}

counter_id = str(os.getenv('COUNTER_ID'))


def get_date_range(days=30):
    today = datetime.datetime.now()
    date_to = today.strftime('%Y-%m-%d')
    date_from = (today - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    return date_from, date_to


def get_metrika_report(params):
    full_url = f"{url}?{urlencode(params)}"
    try:
        logger.info(f"Requesting URL: {full_url}")
        response = requests.get(full_url, headers=headers)
        response.raise_for_status()

        response_text = response.text
        logger.info(f"Response first 500 chars: {response_text[:500]}")

        return response_text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Yandex Metrika: {e}")
        return None