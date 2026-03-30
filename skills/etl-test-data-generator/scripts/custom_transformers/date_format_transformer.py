#!/usr/bin/env python3
"""
Date Format Transformer — converts dates between formats.

This is a REFERENCE EXAMPLE showing how to write a generic, parameterized
custom transformer. Note how EVERY variable aspect is a config parameter:
- input_format: strftime pattern for parsing (e.g., "%Y-%m-%d")
- output_format: strftime pattern for output (e.g., "%m/%d/%Y")

This transformer handles ANY date format conversion, not just one specific case.

File naming convention: {type_name}_transformer.py
Registration: must call TransformerRegistry.register_transformer() at module level
"""

from datetime import datetime
from typing import Any, Dict

# These are injected by load_custom_transformers() — no import needed
# BaseTransformer = <injected>
# TransformerRegistry = <injected>


class DateFormatTransformer(BaseTransformer):
    """Converts dates from one strftime format to another."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Converts dates from one strftime format to another. Handles None/empty gracefully.",
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Source field containing the date string",
                },
                "input_format": {
                    "type": "str",
                    "required": True,
                    "description": "strftime format of the input date (e.g., '%Y-%m-%d')",
                },
                "output_format": {
                    "type": "str",
                    "required": True,
                    "description": "strftime format for the output date (e.g., '%m/%d/%Y')",
                },
            },
            "examples": [
                {
                    "config": {
                        "source_field": "DOB",
                        "input_format": "%Y-%m-%d",
                        "output_format": "%m/%d/%Y",
                    },
                    "input": {"DOB": "1960-01-15"},
                    "output": "01/15/1960",
                },
                {
                    "config": {
                        "source_field": "EventDate",
                        "input_format": "%d/%m/%Y",
                        "output_format": "%Y-%m-%d",
                    },
                    "input": {"EventDate": "25/12/2025"},
                    "output": "2025-12-25",
                },
            ],
        }

    def validate_config(self) -> None:
        for key in ("source_field", "input_format", "output_format"):
            if key not in self.config:
                raise ValueError(f"DateFormatTransformer requires '{key}' in config")
        # Validate formats are valid strftime patterns
        try:
            datetime.now().strftime(self.config["output_format"])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid output_format: {e}")

    def transform(self, source_record: Dict[str, Any]) -> str:
        value = source_record.get(self.config["source_field"])
        if value is None:
            return ""
        value = str(value).strip()
        if not value:
            return ""
        try:
            dt = datetime.strptime(value, self.config["input_format"])
            return dt.strftime(self.config["output_format"])
        except (ValueError, TypeError):
            # If parsing fails, return original value
            return value


# Register with the engine
TransformerRegistry.register_transformer("date_format", DateFormatTransformer)
