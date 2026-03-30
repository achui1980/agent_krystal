"""
Multi-format File Readers for ETL Data

This module provides utilities to read source data files in various formats:
- Excel files (.xlsx, .xls)
- CSV files (.csv)
- Text files (.txt) with pipe or comma delimiters

All columns are read as strings to avoid pandas auto-conversion of dates and numbers.

Usage:
    from krystal_v2.case_generator.utils.file_readers import read_source_file

    df = read_source_file('data.xlsx')
    df = read_source_file('data.csv')
    df = read_source_file('data.txt', delimiter='|')
"""

import os
from typing import Optional
import pandas as pd


def detect_delimiter(file_path: str, sample_lines: int = 5) -> str:
    """
    Detect the delimiter used in a text file.

    Args:
        file_path: Path to the text file
        sample_lines: Number of lines to sample for detection

    Returns:
        Detected delimiter ('|' or ',')
    """
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [f.readline() for _ in range(sample_lines)]

    # Count occurrences of pipe and comma
    pipe_count = sum(line.count("|") for line in lines)
    comma_count = sum(line.count(",") for line in lines)

    # Return the more common delimiter
    return "|" if pipe_count > comma_count else ","


def read_excel_file(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    Read an Excel file and return a DataFrame with all columns as strings.

    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to read (default: first sheet)

    Returns:
        DataFrame with all columns as strings
    """
    # Read Excel file with all columns as strings
    if sheet_name:
        df = pd.read_excel(
            file_path, sheet_name=sheet_name, dtype=str, engine="openpyxl"
        )
    else:
        df = pd.read_excel(file_path, dtype=str, engine="openpyxl")

    # Fill NaN values with empty strings
    df = df.fillna("")

    return df


def read_csv_file(file_path: str, delimiter: str = ",") -> pd.DataFrame:
    """
    Read a CSV file and return a DataFrame with all columns as strings.

    Args:
        file_path: Path to the CSV file
        delimiter: Field delimiter (default: ',')

    Returns:
        DataFrame with all columns as strings
    """
    # Read CSV with all columns as strings
    df = pd.read_csv(file_path, sep=delimiter, dtype=str, encoding="utf-8")

    # Fill NaN values with empty strings
    df = df.fillna("")

    return df


def read_text_file(file_path: str, delimiter: Optional[str] = None) -> pd.DataFrame:
    """
    Read a text file and return a DataFrame with all columns as strings.
    Auto-detects delimiter if not provided.

    Args:
        file_path: Path to the text file
        delimiter: Field delimiter (None = auto-detect)

    Returns:
        DataFrame with all columns as strings
    """
    # Auto-detect delimiter if not provided
    if delimiter is None:
        delimiter = detect_delimiter(file_path)

    # Read text file as CSV with specified delimiter
    return read_csv_file(file_path, delimiter=delimiter)


def read_source_file(
    file_path: str, sheet_name: Optional[str] = None, delimiter: Optional[str] = None
) -> pd.DataFrame:
    """
    Read a source data file in any supported format.
    Auto-detects format based on file extension.

    Supported formats:
    - Excel: .xlsx, .xls
    - CSV: .csv
    - Text: .txt (with auto-detected or specified delimiter)

    Args:
        file_path: Path to the source file
        sheet_name: Excel sheet name (only for Excel files)
        delimiter: Text file delimiter (None = auto-detect)

    Returns:
        DataFrame with all columns as strings

    Raises:
        FileNotFoundError: If the file does not exist
        ValueError: If the file format is not supported

    Examples:
        >>> df = read_source_file('case/humanaS10/source.csv')
        >>> df = read_source_file('case/highmark/source.txt', delimiter='|')
        >>> df = read_source_file('case/example/source.xlsx', sheet_name='Sheet1')
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get file extension
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()

    # Read file based on extension
    if ext in [".xlsx", ".xls"]:
        return read_excel_file(file_path, sheet_name=sheet_name)
    elif ext == ".csv":
        return read_csv_file(file_path, delimiter=delimiter or ",")
    elif ext == ".txt":
        return read_text_file(file_path, delimiter=delimiter)
    else:
        raise ValueError(
            f"Unsupported file format: {ext}. Supported formats: .xlsx, .xls, .csv, .txt"
        )


def get_column_names(file_path: str) -> list:
    """
    Get column names from a source file without reading all data.

    Args:
        file_path: Path to the source file

    Returns:
        List of column names
    """
    df = read_source_file(file_path)
    return df.columns.tolist()
