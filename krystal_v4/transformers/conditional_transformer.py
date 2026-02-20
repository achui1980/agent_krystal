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

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize and normalize mappings by trimming whitespace.

        Args:
            config: Transformer configuration
        """
        super().__init__(config)
        # CRITICAL: Trim all mapping keys and values to avoid " HUM" != "HUM" issues
        raw_mappings = self.config.get("mappings", {})
        self.normalized_mappings = {
            str(k).strip(): str(v).strip() for k, v in raw_mappings.items()
        }

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
        default = self.config.get("default")

        source_value = source_record.get(source_field)
        if source_value is None:
            return default

        # CRITICAL: Trim source value before lookup to avoid whitespace mismatch
        source_value_trimmed = str(source_value).strip()

        # Check for exact match in normalized mappings
        if source_value_trimmed in self.normalized_mappings:
            return self.normalized_mappings[source_value_trimmed]

        # Check for case-insensitive match
        source_value_upper = source_value_trimmed.upper()
        for key, value in self.normalized_mappings.items():
            if key.upper() == source_value_upper:
                return value

        # Return default if no match
        return default
