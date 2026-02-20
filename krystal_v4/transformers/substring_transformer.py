"""
Substring extraction transformer.
Extracts LEFT/RIGHT/MID substrings with optional character stripping.
"""

import re
from typing import Dict, Any, Optional
from krystal_v4.transformers.base import BaseTransformer


class SubstringTransformer(BaseTransformer):
    """
    Transformer that extracts substrings using LEFT/RIGHT/MID operations.

    Config:
        source_field: Name of source field
        method: "left", "right", or "mid"
        length: Number of characters to extract
        start: Start position for "mid" method (0-based, default 0)
        strip_chars: Optional characters to strip before extraction (e.g. "()-. ")

    Examples:
        # LEFT 3 chars of phone "(555) 867-5309" after stripping non-digits → "555"
        config = {"source_field": "Phone", "method": "left", "length": 3, "strip_chars": "()-. "}
        # RIGHT 7 chars → "8675309"
        config = {"source_field": "Phone", "method": "right", "length": 7, "strip_chars": "()-. "}
    """

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Extracts a substring using LEFT/RIGHT/MID. Optionally strips characters before extraction.",
            "config": {
                "source_field": {"type": "str", "required": True, "description": "Source field to extract from"},
                "method": {"type": "str", "required": True, "description": "Extraction method", "enum": ["left", "right", "mid"]},
                "length": {"type": "int", "required": True, "description": "Number of characters to extract"},
                "start": {"type": "int", "required": False, "description": "Start position for 'mid' method (0-based, default 0)"},
                "strip_chars": {"type": "str", "required": False, "description": "Characters to remove from source before extraction (e.g. '()-. ' to strip formatting from phone numbers)"},
            },
            "examples": [
                {"config": {"source_field": "Phone", "method": "left", "length": 3, "strip_chars": "()-. "}, "input": {"Phone": "(555) 867-5309"}, "output": "555"},
                {"config": {"source_field": "Phone", "method": "right", "length": 7, "strip_chars": "()-. "}, "input": {"Phone": "(555) 867-5309"}, "output": "8675309"},
                {"config": {"source_field": "Code", "method": "mid", "length": 3, "start": 2}, "input": {"Code": "ABCDEF"}, "output": "CDE"},
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("SubstringTransformer requires 'source_field' in config")
        if "method" not in self.config:
            raise ValueError("SubstringTransformer requires 'method' in config")
        if self.config["method"] not in ("left", "right", "mid"):
            raise ValueError("SubstringTransformer 'method' must be 'left', 'right', or 'mid'")
        if "length" not in self.config:
            raise ValueError("SubstringTransformer requires 'length' in config")
        if not isinstance(self.config["length"], int) or self.config["length"] <= 0:
            raise ValueError("SubstringTransformer 'length' must be a positive integer")
        if self.config["method"] == "mid" and "start" in self.config:
            if not isinstance(self.config["start"], int) or self.config["start"] < 0:
                raise ValueError("SubstringTransformer 'start' must be a non-negative integer")

    def transform(self, source_record: Dict[str, Any]) -> str:
        source_field = self.config["source_field"]
        method = self.config["method"]
        length = self.config["length"]

        value = source_record.get(source_field)
        if value is None:
            return ""

        value = str(value).strip()
        if not value:
            return ""

        # Strip specified characters before extraction
        strip_chars = self.config.get("strip_chars")
        if strip_chars:
            for ch in strip_chars:
                value = value.replace(ch, "")

        if method == "left":
            return value[:length]
        elif method == "right":
            return value[-length:] if len(value) >= length else value
        else:  # mid
            start = self.config.get("start", 0)
            return value[start:start + length]
