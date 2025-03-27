import requests
import pandas as pd
from urllib.parse import urlencode
from io import StringIO
import logging
import sys
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# API Configuration
url = 'https://api-metrika.yandex.ru/stat/v1/data'
token = 'y0__xDc3N9hGMXaNSDZo9K3ElW6wEIGmXlh21UQ7jVH75YjpLi8'  # Replace with your token
headers = {'Authorization': f'OAuth {token}'}

# Yandex Metrika counter ID
counter_id = '45158844'  # Replace with your counter ID


def test_api_connection():
    """Test basic API connectivity"""
    try:
        # Simple request to check if the API is accessible
        params = {
            'ids': counter_id,
            'metrics': 'ym:s:visits',
            'dimensions': 'ym:s:date',
            'date1': '7daysAgo',
            'date2': 'today'
        }

        # First try JSON format for better error messages
        json_url = f"{url}.json?{urlencode(params)}"
        logger.info(f"Testing API connectivity with URL: {json_url}")

        response = requests.get(json_url, headers=headers)

        if response.status_code == 200:
            logger.info("API connection successful!")
            data = response.json()
            logger.info(f"API response contains {len(data.get('data', []))} data rows")

            # Show available dimensions and metrics
            logger.info("\nAvailable dimensions in response:")
            for dim in data.get('query', {}).get('dimensions', []):
                logger.info(f"  - {dim}")

            logger.info("\nAvailable metrics in response:")
            for metric in data.get('query', {}).get('metrics', []):
                logger.info(f"  - {metric}")

            # Show a sample of the data
            if data.get('data'):
                logger.info("\nSample data:")
                for i, row in enumerate(data.get('data')[:3]):
                    logger.info(f"  Row {i + 1}: {json.dumps(row)}")

            # Now try CSV format to check column headers
            csv_url = f"{url}.csv?{urlencode(params)}"
            csv_response = requests.get(csv_url, headers=headers)

            if csv_response.status_code == 200:
                csv_data = csv_response.text
                df = pd.read_csv(StringIO(csv_data), sep=',')
                logger.info("\nCSV column headers:")
                for col in df.columns:
                    logger.info(f"  - {col}")
            else:
                logger.error(f"CSV request failed: {csv_response.status_code} - {csv_response.text}")
        else:
            logger.error(f"API connection failed: {response.status_code} - {response.text}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def test_all_report_types():
    """Test all report types used in the main script"""

    reports = [
        {
            'name': 'visits_by_day',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
                'dimensions': 'ym:s:date',
                'date1': '30daysAgo',
                'date2': 'today',
                'sort': 'ym:s:date'
            }
        },
        {
            'name': 'traffic_sources',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
                'dimensions': 'ym:s:date,ym:s:trafficSource',
                'date1': '30daysAgo',
                'date2': 'today',
                'sort': 'ym:s:date'
            }
        },
        {
            'name': 'devices',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:bounceRate',
                'dimensions': 'ym:s:date,ym:s:deviceCategory',
                'date1': '30daysAgo',
                'date2': 'today',
                'sort': 'ym:s:date'
            }
        },
        {
            'name': 'popular_pages',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:pv:pageviews,ym:pv:users,ym:pv:avgVisitDurationSeconds',
                'dimensions': 'ym:pv:URL,ym:pv:title',  # Removed date from dimensions
                'date1': '30daysAgo',
                'date2': 'today',
                'sort': '-ym:pv:pageviews',
                'limit': 100
            }
        },
        {
            'name': 'geography',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits,ym:s:users',
                'dimensions': 'ym:s:date,ym:s:regionCountry,ym:s:regionCity',
                'date1': '30daysAgo',
                'date2': 'today',
                'sort': 'ym:s:date,-ym:s:visits'
            }
        },
        {
            'name': 'search_phrases',
            'params': {
                'ids': counter_id,
                'metrics': 'ym:s:visits',
                'dimensions': 'ym:s:date,ym:s:searchPhrase',
                'date1': '30daysAgo',
                'date2': 'today',
                'sort': 'ym:s:date',
                'filters': "ym:s:trafficSource=='organic'"
            }
        }
    ]

    for report in reports:
        logger.info(f"\n\nTesting report: {report['name']}")

        # Use JSON format for better debugging
        json_url = f"{url}.json?{urlencode(report['params'])}"

        try:
            response = requests.get(json_url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Success! Received {len(data.get('data', []))} rows")

                # Get actual column names for CSV format
                csv_url = f"{url}.csv?{urlencode(report['params'])}"
                csv_response = requests.get(csv_url, headers=headers)

                if csv_response.status_code == 200:
                    csv_data = csv_response.text
                    df = pd.read_csv(StringIO(csv_data), sep=',')
                    logger.info(f"CSV columns: {df.columns.tolist()}")

                    if not df.empty:
                        logger.info(f"First row: {df.iloc[0].to_dict()}")
                else:
                    logger.error(f"CSV request failed: {csv_response.status_code} - {csv_response.text}")
            else:
                logger.error(f"Request failed: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"Error testing {report['name']}: {e}")


if __name__ == "__main__":
    logger.info("Testing Yandex Metrika API")
    test_api_connection()
    test_all_report_types()