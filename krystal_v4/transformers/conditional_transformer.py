"""
Conditional mapping transformer.
Maps source values to target values based on conditions.
"""

from typing import Dict, Any, Optional
from krystal_v4.transformers.base import BaseTransformer


class ConditionalTransformer(BaseTransformer):
    """
    Transformer that applies conditional mapping to source values.

    Config:
        source_field: Name of source field
        mappings: Dictionary mapping source values to target values
        default: Optional default value if no mapping matches

    Example:
        config = {
            "source_field": "Product",
            "mappings": {
                "PDP": "MD",
                "HAP": "MS",
                "HUM": "MS",
                "HV": "MS"
            },
            "default": "MA/MAPD"
        }
        transformer = ConditionalTransformer(config)
        result = transformer.transform({"Product": "PDP"})  # Returns "MD"
    """

    def validate_config(self) -> None:
        """Validate configuration."""
        if "source_field" not in self.config:
            raise ValueError("ConditionalTransformer requires 'source_field' in config")

        if "mappings" not in self.config:
            raise ValueError("ConditionalTransformer requires 'mappings' in config")

        if not isinstance(self.config["mappings"], dict):
            raise ValueError("ConditionalTransformer 'mappings' must be a dictionary")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        """
        Apply conditional mapping to source value.

        Args:
            source_record: Source record dictionary

        Returns:
            Mapped value or default value
        """
        source_field = self.config["source_field"]
        mappings = self.config["mappings"]
        default = self.config.get("default")

        source_value = source_record.get(source_field)
        if source_value is None:
            return default

        # Check for exact match
        if source_value in mappings:
            return mappings[source_value]

        # Check for case-insensitive match
        source_value_upper = str(source_value).upper()
        for key, value in mappings.items():
            if str(key).upper() == source_value_upper:
                return value

        # Return default if no match
        return default
