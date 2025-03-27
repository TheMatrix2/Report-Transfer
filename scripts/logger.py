import logging

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