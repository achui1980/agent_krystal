"""
JSON writer tool for Krystal V4.
Writes JSON files with pretty formatting.
"""

import json
from typing import Dict, Any
from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class JSONWriterInput(BaseModel):
    """Input schema for JSONWriterTool"""

    file_path: str = Field(description="Path to output JSON file")
    data: str = Field(description="Data to write as JSON string")


class JSONWriterTool(BaseTool):
    name: str = "JSON Writer"
    description: str = (
        "Writes data to a JSON file with pretty formatting (indented). "
        "Input: file_path (string), data (JSON string)"
    )
    args_schema: type[BaseModel] = JSONWriterInput

    def _run(self, file_path: str, data: str) -> str:
        """
        Write JSON data to file.

        Args:
            file_path: Output file path
            data: Data as JSON string

        Returns:
            Success or error message
        """
        try:
            # Parse and re-format with indentation
            data_dict = json.loads(data)

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data_dict, f, indent=2, ensure_ascii=False)

            return f"Successfully wrote JSON to {file_path}"

        except json.JSONDecodeError as e:
            return f"ERROR: Invalid JSON data: {str(e)}"
        except Exception as e:
            return f"ERROR writing JSON file: {str(e)}"

    def write_dict(self, file_path: str, data: Dict[str, Any]) -> str:
        """
        Write dictionary directly to JSON file (programmatic interface).

        Args:
            file_path: Output file path
            data: Dictionary to write

        Returns:
            Success or error message
        """
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            return f"Successfully wrote JSON to {file_path}"

        except Exception as e:
            return f"ERROR writing JSON: {str(e)}"
