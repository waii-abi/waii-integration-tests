# logging_config.py
import logging
import sys
from logging.handlers import RotatingFileHandler

def init_logger(log_file="logs/test_run.log", level=logging.INFO):
    # Create or get the logger
    logger = logging.getLogger("waii_tests")
    logger.setLevel(level)
    logger.propagate = False

    # If handlers already exist, don't add them again.
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

logger = init_logger()
