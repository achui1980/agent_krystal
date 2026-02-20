"""
Structured logging utility for Krystal V4.
Provides INFO-level logging with clear visual formatting.
Logs to both console (stdout) and file (logs/ directory).
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logger(name: str = "krystal_v4", level: int = logging.INFO) -> logging.Logger:
    """
    Setup structured logger with INFO level.
    Outputs to both console and log file.

    Args:
        name: Logger name
        level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()

    # Format: [INFO] 2025-02-09 14:30:45 - Message
    formatter = logging.Formatter(
        fmt="[%(levelname)s] %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler - logs/krystal_YYYYMMDD_HHMMSS.log
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"krystal_{timestamp}.log"

    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Store log file path on logger for external access
    logger.log_file = str(log_file)

    logger.propagate = False

    return logger


def log_step(
    logger: logging.Logger, step_num: int, total_steps: int, message: str
) -> None:
    """
    Log a pipeline step with visual formatting.

    Args:
        logger: Logger instance
        step_num: Current step number
        total_steps: Total number of steps
        message: Step description
    """
    logger.info(f"{'=' * 60}")
    logger.info(f"STEP {step_num}/{total_steps}: {message}")
    logger.info(f"{'=' * 60}")


def log_error_box(logger: logging.Logger, title: str, details: dict) -> None:
    """
    Log an error message in a formatted box.

    Args:
        logger: Logger instance
        title: Error title
        details: Dictionary of error details
    """
    box_width = 64
    logger.error(f"╔{'═' * box_width}╗")
    logger.error(f"║{title.center(box_width)}║")
    logger.error(f"╚{'═' * box_width}╝")
    logger.error("")

    for key, value in details.items():
        logger.error(f"{key}: {value}")
    logger.error("")


def log_success(logger: logging.Logger, message: str) -> None:
    """
    Log a success message with visual indicator.

    Args:
        logger: Logger instance
        message: Success message
    """
    logger.info(f"✓ {message}")


def log_warning(logger: logging.Logger, message: str) -> None:
    """
    Log a warning message with visual indicator.

    Args:
        logger: Logger instance
        message: Warning message
    """
    logger.warning(f"⚠ {message}")


# Global logger instance
logger = setup_logger()
