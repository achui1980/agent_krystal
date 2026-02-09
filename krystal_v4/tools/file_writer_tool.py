"""
File writer tool for Krystal V4.
Writes data to files in various formats.
"""

import csv
from typing import List, Dict, Any
from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class FileWriterInput(BaseModel):
    """Input schema for FileWriterTool"""

    file_path: str = Field(description="Path to output file")
    data: str = Field(description="Data to write (as string)")
    format_type: str = Field(
        default="text", description="Format: 'text', 'csv_quoted', 'pipe'"
    )


class FileWriterTool(BaseTool):
    name: str = "File Writer"
    description: str = (
        "Writes data to a file in specified format. "
        "Supports plain text, CSV with quotes, and pipe-delimited formats. "
        "Input: file_path (string), data (string), format_type (string)"
    )
    args_schema: type[BaseModel] = FileWriterInput

    def _run(self, file_path: str, data: str, format_type: str = "text") -> str:
        """
        Write data to file.

        Args:
            file_path: Output file path
            data: Data to write
            format_type: Format type

        Returns:
            Success or error message
        """
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(data)

            return f"Successfully wrote data to {file_path} (format: {format_type})"

        except Exception as e:
            return f"ERROR writing file: {str(e)}"

    def write_csv_quoted(
        self, file_path: str, data: List[Dict[str, Any]], fieldnames: List[str]
    ) -> str:
        """
        Write data as CSV with quoted fields.

        Args:
            file_path: Output file path
            data: List of row dictionaries
            fieldnames: Column names

        Returns:
            Success or error message
        """
        try:
            with open(file_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    quoting=csv.QUOTE_ALL,  # Quote all fields
                )
                writer.writeheader()
                writer.writerows(data)

            return (
                f"Successfully wrote {len(data)} rows to {file_path} (CSV with quotes)"
            )

        except Exception as e:
            return f"ERROR writing CSV: {str(e)}"

    def write_pipe_delimited(
        self,
        file_path: str,
        data: List[Dict[str, Any]],
        fieldnames: List[str],
        header_metadata: Dict[str, str] = None,
    ) -> str:
        """
        Write data as pipe-delimited file.

        Args:
            file_path: Output file path
            data: List of row dictionaries
            fieldnames: Column names
            header_metadata: Optional metadata to write before data

        Returns:
            Success or error message
        """
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                # Write metadata header if provided
                if header_metadata:
                    for key, value in header_metadata.items():
                        f.write(f"{key}:{value}\n")
                    f.write("\n")  # Blank line after metadata

                # Write column headers
                f.write("|".join(fieldnames) + "\n")

                # Write data rows
                for row in data:
                    values = [str(row.get(field, "")) for field in fieldnames]
                    f.write("|".join(values) + "\n")

            return (
                f"Successfully wrote {len(data)} rows to {file_path} (pipe-delimited)"
            )

        except Exception as e:
            return f"ERROR writing pipe-delimited file: {str(e)}"
