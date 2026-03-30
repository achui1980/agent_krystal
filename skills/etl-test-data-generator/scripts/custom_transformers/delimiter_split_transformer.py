#!/usr/bin/env python3
"""
Delimiter Split Transformer — splits a string by a configurable delimiter
and extracts the element at a configurable index position.

Generic string-splitting transformer. Works for any delimiter-based
extraction: comma-separated values, dash-separated codes, path segments,
email local/domain splitting, date component extraction, etc.

File naming convention: {type_name}_transformer.py
Registration: must call TransformerRegistry.register_transformer() at module level
"""

from typing import Any, Dict

# These are injected by load_custom_transformers() — no import needed
# BaseTransformer = <injected>
# TransformerRegistry = <injected>


class DelimiterSplitTransformer(BaseTransformer):
    """Splits a string by a delimiter and extracts the part at a given index."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": (
                "Splits a string by a configurable delimiter and extracts the "
                "element at a 0-based index. Strips whitespace from the input "
                "and the extracted result. Returns empty string for None/empty "
                "input. Returns empty string if the index is out of range."
            ),
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Source field containing the string to split",
                },
                "delimiter": {
                    "type": "str",
                    "required": True,
                    "description": "Character(s) to split on (e.g., ',', '-', '/', '@')",
                },
                "index": {
                    "type": "int",
                    "required": True,
                    "description": "0-based index of the element to extract after splitting",
                },
            },
            "examples": [
                {
                    "config": {
                        "source_field": "FullName",
                        "delimiter": ",",
                        "index": 0,
                    },
                    "input": {"FullName": "SMITH, JOHN"},
                    "output": "SMITH",
                },
                {
                    "config": {
                        "source_field": "FullName",
                        "delimiter": ",",
                        "index": 1,
                    },
                    "input": {"FullName": "SMITH, JOHN"},
                    "output": "JOHN",
                },
                {
                    "config": {
                        "source_field": "Code",
                        "delimiter": "-",
                        "index": 0,
                    },
                    "input": {"Code": "S5884-197"},
                    "output": "S5884",
                },
                {
                    "config": {
                        "source_field": "Code",
                        "delimiter": "-",
                        "index": 1,
                    },
                    "input": {"Code": "S5884-197"},
                    "output": "197",
                },
                {
                    "config": {
                        "source_field": "DateStr",
                        "delimiter": "/",
                        "index": 2,
                    },
                    "input": {"DateStr": "2025/01/15"},
                    "output": "15",
                },
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError(
                "DelimiterSplitTransformer requires 'source_field' in config"
            )
        if "delimiter" not in self.config:
            raise ValueError("DelimiterSplitTransformer requires 'delimiter' in config")
        if not self.config["delimiter"]:
            raise ValueError(
                "DelimiterSplitTransformer 'delimiter' must be a non-empty string"
            )
        if "index" not in self.config:
            raise ValueError("DelimiterSplitTransformer requires 'index' in config")
        if not isinstance(self.config["index"], int) or self.config["index"] < 0:
            raise ValueError(
                "DelimiterSplitTransformer 'index' must be a non-negative integer"
            )

    def transform(self, source_record: Dict[str, Any]) -> str:
        value = source_record.get(self.config["source_field"])
        if value is None:
            return ""
        value = str(value).strip()
        if not value:
            return ""

        delimiter = self.config["delimiter"]
        index = self.config["index"]

        parts = value.split(delimiter)
        if index >= len(parts):
            return ""

        return parts[index].strip()


# Register with the engine
TransformerRegistry.register_transformer("delimiter_split", DelimiterSplitTransformer)
