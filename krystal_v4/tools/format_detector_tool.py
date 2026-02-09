"""
Format detector tool for Krystal V4.
Auto-detects file format (CSV with quotes, pipe-delimited, or comma-separated).
"""

from typing import Optional
from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class FormatDetectorInput(BaseModel):
    """Input schema for FormatDetectorTool"""

    file_path: str = Field(description="Path to file to detect format")


class FormatDetectorTool(BaseTool):
    name: str = "Format Detector"
    description: str = (
        "Detects the format of a delimited text file. "
        "Returns 'csv_quoted' for CSV with quoted fields, "
        "'pipe' for pipe-delimited (|), or 'comma' for simple comma-separated. "
        "Input: file_path (string)"
    )
    args_schema: type[BaseModel] = FormatDetectorInput

    def _run(self, file_path: str) -> str:
        """
        Detect file format by analyzing first few lines.

        Args:
            file_path: Path to file

        Returns:
            Format string: 'csv_quoted', 'pipe', or 'comma'
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # Read first 3 lines for analysis
                lines = [f.readline() for _ in range(3)]
                lines = [line for line in lines if line.strip()]

            if not lines:
                return "comma"  # Default fallback

            first_line = lines[0]

            # Count delimiters
            pipe_count = first_line.count("|")
            comma_count = first_line.count(",")
            quote_count = first_line.count('"')

            # Decision logic
            if pipe_count > comma_count:
                return "pipe"
            elif quote_count >= 2 and comma_count > 0:
                # CSV with quoted fields
                return "csv_quoted"
            elif comma_count > 0:
                return "comma"
            else:
                return "comma"  # Default fallback

        except FileNotFoundError:
            return f"ERROR: File not found: {file_path}"
        except Exception as e:
            return f"ERROR: {str(e)}"
