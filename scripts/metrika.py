import requests
import datetime
from urllib.parse import urlencode
from config import API_URL, API_HEADERS, logger


def get_date_range(days=30):
    today = datetime.datetime.now()
    date_to = today.strftime('%Y-%m-%d')
    date_from = (today - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    return date_from, date_to


def get_metrika_report(params):
    full_url = f"{API_URL}?{urlencode(params)}"
    try:
        logger.info(f"Requesting URL: {full_url}")
        response = requests.get(full_url, headers=API_HEADERS)
        response.raise_for_status()

        response_text = response.text
        logger.info(f"Response first 500 chars: {response_text[:500]}")

        return response_text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Yandex Metrika: {e}")
        return None