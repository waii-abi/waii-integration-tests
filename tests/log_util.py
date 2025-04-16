# logging_config.py
import logging
import sys
from logging.handlers import RotatingFileHandler

from tests.docker_configs.docker_configs import get_logger_file


def init_logger(log_file="logs/test_run.log", level=logging.INFO):
    final_file = get_logger_file(log_file)
    # Create or get the logger
    this_logger = logging.getLogger("waii_tests")
    this_logger.setLevel(level)
    this_logger.propagate = False

    # If handlers already exist, don't add them again.
    if not this_logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = RotatingFileHandler(final_file, maxBytes=5*1024*1024, backupCount=3)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        this_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        this_logger.addHandler(console_handler)

    return this_logger

logger = init_logger()
