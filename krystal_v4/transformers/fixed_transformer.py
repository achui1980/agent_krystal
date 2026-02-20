"""
Fixed value transformer.
Returns a fixed value regardless of input.
"""

from typing import Dict, Any
from krystal_v4.transformers.base import BaseTransformer


class FixedTransformer(BaseTransformer):
    """
    Transformer that returns a fixed value.

    Config:
        value: Fixed value to return

    Example:
        config = {"value": "66,175,206"}
        transformer = FixedTransformer(config)
        result = transformer.transform(record)  # Returns "66,175,206"
    """

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Returns a constant value regardless of input.",
            "config": {
                "value": {"type": "str", "required": True, "description": "The fixed value to return"},
            },
            "examples": [
                {"config": {"value": "66,175,206"}, "output": "66,175,206"},
            ],
        }

    def validate_config(self) -> None:
        """Validate that 'value' is present in config."""
        if "value" not in self.config:
            raise ValueError("FixedTransformer requires 'value' in config")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        """
        Return the fixed value.

        Args:
            source_record: Source record (not used)

        Returns:
            Fixed value from config
        """
        return self.config["value"]
