"""
CSV reader tool for Krystal V4.
Reads CSV files and returns data in structured format.
"""

import csv
from typing import List, Dict, Any
from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class CSVReaderInput(BaseModel):
    """Input schema for CSVReaderTool"""

    file_path: str = Field(description="Path to CSV file to read")
    max_rows: int = Field(
        default=None, description="Maximum number of rows to read (optional)"
    )


class CSVReaderTool(BaseTool):
    name: str = "CSV Reader"
    description: str = (
        "Reads a CSV file and returns the data as a list of dictionaries. "
        "Each row becomes a dictionary with column names as keys. "
        "Input: file_path (string), max_rows (optional integer)"
    )
    args_schema: type[BaseModel] = CSVReaderInput

    def _run(self, file_path: str, max_rows: int = None) -> str:
        """
        Read CSV file and return data.

        Args:
            file_path: Path to CSV file
            max_rows: Maximum rows to read (None = all)

        Returns:
            String representation of data or error message
        """
        try:
            data = []
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for i, row in enumerate(reader):
                    if max_rows and i >= max_rows:
                        break
                    data.append(dict(row))

            # Return summary
            if not data:
                return f"CSV file is empty: {file_path}"

            columns = list(data[0].keys())
            return (
                f"Successfully read {len(data)} rows from {file_path}\n"
                f"Columns ({len(columns)}): {', '.join(columns)}\n"
                f"First row sample: {data[0]}"
            )

        except FileNotFoundError:
            return f"ERROR: File not found: {file_path}"
        except Exception as e:
            return f"ERROR reading CSV: {str(e)}"

    def read_as_list(
        self, file_path: str, max_rows: int = None
    ) -> List[Dict[str, Any]]:
        """
        Read CSV and return as list of dictionaries.
        (Helper method for programmatic use)

        Args:
            file_path: Path to CSV file
            max_rows: Maximum rows to read

        Returns:
            List of row dictionaries
        """
        data = []
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if max_rows and i >= max_rows:
                    break
                data.append(dict(row))
        return data
