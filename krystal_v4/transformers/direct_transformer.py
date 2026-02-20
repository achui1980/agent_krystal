"""
Direct field mapping transformer.
Maps source field directly to target field.
"""

from typing import Dict, Any, Optional
from krystal_v4.transformers.base import BaseTransformer


class DirectTransformer(BaseTransformer):
    """
    Transformer that directly maps a source field to target.

    Config:
        source_field: Name of source field to map

    Example:
        config = {"source_field": "DOB"}
        transformer = DirectTransformer(config)
        result = transformer.transform({"DOB": "1960-01-15"})  # Returns "1960-01-15"
    """

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Directly copies a source field value to the target field.",
            "config": {
                "source_field": {"type": "str", "required": True, "description": "Name of source field to copy from"},
            },
            "examples": [
                {"config": {"source_field": "DOB"}, "input": {"DOB": "1960-01-15"}, "output": "1960-01-15"},
            ],
        }

    def validate_config(self) -> None:
        """Validate that 'source_field' is present in config."""
        if "source_field" not in self.config:
            raise ValueError("DirectTransformer requires 'source_field' in config")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        """
        Return the value of the source field.

        Args:
            source_record: Source record dictionary

        Returns:
            Value of source field, or None if not found
        """
        source_field = self.config["source_field"]
        return source_record.get(source_field)
