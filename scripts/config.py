import os
from dotenv import load_dotenv
import logging
import sys

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../metrika_etl.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("metrika_etl")

# API конфигурация
API_URL = 'https://api-metrika.yandex.ru/stat/v1/data.csv'
API_TOKEN = str(os.getenv('TOKEN'))
API_HEADERS = {'Authorization': f'OAuth {API_TOKEN}'}
COUNTER_ID = str(os.getenv('COUNTER_ID'))

# Конфигурация базы данных
DB_CONFIG = {
    'host': str(os.getenv('DB_HOST', 'localhost')),
    'user': str(os.getenv('DB_USER', 'metrika_user')),
    'password': str(os.getenv('DB_PASSWORD', 'your_secure_password')),
    'database': str(os.getenv('DB_NAME', 'yandex_metrika')),
    'port': int(os.getenv('DB_PORT', 3306))
} 