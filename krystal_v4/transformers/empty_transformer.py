"""
Empty transformer.
Returns empty string for fields that should be blank.
"""

from typing import Dict, Any
from krystal_v4.transformers.base import BaseTransformer


class EmptyTransformer(BaseTransformer):
    """
    Transformer that returns empty string.

    Config:
        (none required)

    Example:
        config = {}
        transformer = EmptyTransformer(config)
        result = transformer.transform(record)  # Returns ""
    """

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Returns empty string. Use for fields that should always be blank.",
            "config": {},
            "examples": [
                {"config": {}, "output": ""},
            ],
        }

    def validate_config(self) -> None:
        """No configuration required for empty transformer."""
        pass

    def transform(self, source_record: Dict[str, Any]) -> str:
        """
        Return empty string.

        Args:
            source_record: Source record (not used)

        Returns:
            Empty string
        """
        return ""
