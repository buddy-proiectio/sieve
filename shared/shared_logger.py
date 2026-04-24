import logging
import sys


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        time_str = self.formatTime(record, "%y-%m-%d %H:%M:%S")
        level_name = f"{record.levelname:8}"

        # default Green (INFO, etc.)
        color_code = "\x1b[32m"

        if record.levelno == logging.WARNING:
            color_code = "\x1b[33m"  # Yellow
        elif record.levelno >= logging.ERROR:
            color_code = "\x1b[31m"  # Red

        reset_code = "\x1b[0m"

        return (
            f"{color_code}{time_str} - {level_name}{reset_code} | {record.getMessage()}"
        )


import os


def setup_logger(log_file: str = None, logger_name: str = None) -> logging.Logger:
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

    custom_logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()
    custom_logger.setLevel(logging.INFO)

    # Reset handlers to avoid duplicates
    if custom_logger.hasHandlers():
        custom_logger.handlers.clear()
        
    # Prevent propagation to root logger to avoid duplicates if root is configured
    if logger_name:
        custom_logger.propagate = False

    # File output formatter
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)-8s | %(message)s", datefmt="%y-%m-%d %H:%M:%S"
    )

    # Console output handler (with color)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredFormatter())

    custom_logger.addHandler(console_handler)

    # File output handler
    if log_file:
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(file_formatter)
        custom_logger.addHandler(file_handler)

    return custom_logger
