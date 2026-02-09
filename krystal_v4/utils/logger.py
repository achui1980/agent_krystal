"""
Structured logging utility for Krystal V4.
Provides INFO-level logging with clear visual formatting.
"""

import logging
import sys
from typing import Optional


def setup_logger(name: str = "krystal_v4", level: int = logging.INFO) -> logging.Logger:
    """
    Setup structured logger with INFO level.

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

    # Create console handler with formatting
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # Format: [INFO] 2025-02-09 14:30:45 - Message
    formatter = logging.Formatter(
        fmt="[%(levelname)s] %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
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
