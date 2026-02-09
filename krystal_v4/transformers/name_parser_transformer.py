"""
Name parser transformer.
Parses "LAST,FIRST" format into first name or last name.
"""

from typing import Dict, Any, Optional
from krystal_v4.transformers.base import BaseTransformer


class NameParserTransformer(BaseTransformer):
    """
    Transformer that parses "LAST,FIRST" format names.

    Config:
        source_field: Name of source field containing "LAST,FIRST"
        part: Which part to extract - "first" or "last"

    Example:
        config = {"source_field": "Member", "part": "first"}
        transformer = NameParserTransformer(config)
        result = transformer.transform({"Member": "MOUSE,MICKEY"})  # Returns "MICKEY"
    """

    def validate_config(self) -> None:
        """Validate configuration."""
        if "source_field" not in self.config:
            raise ValueError("NameParserTransformer requires 'source_field' in config")

        if "part" not in self.config:
            raise ValueError("NameParserTransformer requires 'part' in config")

        if self.config["part"] not in ["first", "last"]:
            raise ValueError("NameParserTransformer 'part' must be 'first' or 'last'")

    def transform(self, source_record: Dict[str, Any]) -> Optional[str]:
        """
        Parse name and return requested part.

        Args:
            source_record: Source record dictionary

        Returns:
            First name or last name, or None if parsing fails
        """
        source_field = self.config["source_field"]
        part = self.config["part"]

        name_value = source_record.get(source_field)
        if not name_value:
            return None

        # Handle "LAST,FIRST" format
        if "," in name_value:
            parts = name_value.split(",", 1)
            last_name = parts[0].strip()
            first_name = parts[1].strip() if len(parts) > 1 else ""

            if part == "first":
                return first_name
            else:  # part == "last"
                return last_name

        # If no comma, return as-is (fallback)
        return name_value
