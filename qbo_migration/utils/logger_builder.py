# utils/logger_builder.py

import logging
import os
from datetime import datetime

def build_logger(module_name: str, log_dir: str = "logs", with_start_end: bool = True) -> logging.Logger:
    """
    Create a module-specific logger with a timestamped file output and optional start/end logging.

    Args:
        module_name (str): Name of the module (used in file name and log tag)
        log_dir (str): Directory for log files
        with_start_end (bool): Whether to log automatic start/end messages

    Returns:
        logging.Logger: Configured logger
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{module_name}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    # Set up logger
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Also add console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if with_start_end:
            logger.info(f"🚀 Start of {module_name} migration")

    return logger
