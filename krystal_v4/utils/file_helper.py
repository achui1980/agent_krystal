"""
File operation utilities for Krystal V4.
"""

import os
import json
from pathlib import Path
from typing import Any, Dict
from datetime import datetime


def ensure_output_dir(case_name: str) -> Path:
    """
    Create output directory for a test case.

    Args:
        case_name: Test case identifier

    Returns:
        Path object for output directory
    """
    output_dir = Path("output") / case_name
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_json(data: Dict[str, Any], file_path: str) -> None:
    """
    Save dictionary as JSON file with pretty formatting.

    Args:
        data: Dictionary to save
        file_path: Output file path
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(file_path: str) -> Dict[str, Any]:
    """
    Load JSON file as dictionary.

    Args:
        file_path: Input file path

    Returns:
        Dictionary from JSON
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def generate_source_token() -> str:
    """
    Generate source token with timestamp.

    Returns:
        Token string in format: generated_YYYYMMDD_HHMMSS
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"generated_{timestamp}"


def read_file_lines(file_path: str, encoding: str = "utf-8") -> list:
    """
    Read file and return lines.

    Args:
        file_path: Input file path
        encoding: File encoding

    Returns:
        List of lines
    """
    with open(file_path, "r", encoding=encoding) as f:
        return f.readlines()


def write_file_lines(file_path: str, lines: list, encoding: str = "utf-8") -> None:
    """
    Write lines to file.

    Args:
        file_path: Output file path
        lines: List of lines to write
        encoding: File encoding
    """
    with open(file_path, "w", encoding=encoding) as f:
        f.writelines(lines)


def file_exists(file_path: str) -> bool:
    """
    Check if file exists.

    Args:
        file_path: File path to check

    Returns:
        True if file exists, False otherwise
    """
    return os.path.isfile(file_path)
