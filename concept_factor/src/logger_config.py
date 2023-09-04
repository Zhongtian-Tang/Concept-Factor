from loguru import logger

def setup_logger():
    logger.add('logs/my_log_file.log', format="{time} {level} {message}", level="ERROR", rotation="1 year")