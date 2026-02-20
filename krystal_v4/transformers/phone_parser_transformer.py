"""
Phone number parser transformer.
Parses phone numbers to extract area code or phone number parts.
"""

import re
from typing import Dict, Any, Optional
from krystal_v4.transformers.base import BaseTransformer


class PhoneParserTransformer(BaseTransformer):
    """
    Transformer that parses phone numbers into area code and phone number.

    Strips all non-digit characters first, then assumes 10-digit US format:
    digits[0:3] = area code, digits[3:10] = phone number.

    Config:
        source_field: Name of source field containing phone number
        part: "area_code" or "phone_number"
        format: Optional output format for phone_number (e.g. "nnn-nnnn")

    Examples:
        # Input: "(555) 867-5309" → digits "5558675309"
        config = {"source_field": "Phone", "part": "area_code"}
        # → "555"

        config = {"source_field": "Phone", "part": "phone_number", "format": "nnn-nnnn"}
        # → "867-5309"
    """

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Parses phone numbers (strips non-digits, assumes 10-digit US format) and extracts area_code or phone_number.",
            "config": {
                "source_field": {"type": "str", "required": True, "description": "Source field containing phone number"},
                "part": {"type": "str", "required": True, "description": "Which part to extract", "enum": ["area_code", "phone_number"]},
                "format": {"type": "str", "required": False, "description": "Output format for phone_number part (e.g. 'nnn-nnnn' inserts '-' after 3rd digit)"},
            },
            "examples": [
                {"config": {"source_field": "Phone", "part": "area_code"}, "input": {"Phone": "(555) 867-5309"}, "output": "555"},
                {"config": {"source_field": "Phone", "part": "phone_number"}, "input": {"Phone": "(555) 867-5309"}, "output": "8675309"},
                {"config": {"source_field": "Phone", "part": "phone_number", "format": "nnn-nnnn"}, "input": {"Phone": "(555) 867-5309"}, "output": "867-5309"},
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("PhoneParserTransformer requires 'source_field' in config")
        if "part" not in self.config:
            raise ValueError("PhoneParserTransformer requires 'part' in config")
        if self.config["part"] not in ("area_code", "phone_number"):
            raise ValueError("PhoneParserTransformer 'part' must be 'area_code' or 'phone_number'")

    def transform(self, source_record: Dict[str, Any]) -> str:
        source_field = self.config["source_field"]
        part = self.config["part"]

        value = source_record.get(source_field)
        if value is None:
            return ""

        # Strip all non-digit characters
        digits = re.sub(r"\D", "", str(value))

        if not digits:
            return ""

        # Handle 11-digit numbers starting with 1 (country code)
        if len(digits) == 11 and digits[0] == "1":
            digits = digits[1:]

        if len(digits) < 10:
            # Not enough digits for full US phone; return what we can
            if part == "area_code":
                return digits[:3] if len(digits) >= 3 else digits
            else:
                return digits[3:] if len(digits) > 3 else ""

        area_code = digits[:3]
        phone_number = digits[3:10]

        if part == "area_code":
            return area_code

        # part == "phone_number"
        fmt = self.config.get("format")
        if fmt and fmt.lower() == "nnn-nnnn" and len(phone_number) >= 7:
            return f"{phone_number[:3]}-{phone_number[3:7]}"

        return phone_number
