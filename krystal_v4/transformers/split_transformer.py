"""
Split extract transformer.
Splits string by delimiter and extracts element at specified index.
"""

from typing import Dict, Any, Optional
from krystal_v4.transformers.base import BaseTransformer


class SplitTransformer(BaseTransformer):
    """
    Transformer that splits string and extracts element.

    Config:
        source_field: Name of source field
        delimiter: Delimiter to split on
        index: Index of element to extract (0-based)

    Example:
        config = {"source_field": "Plan_Name", "delimiter": "-", "index": 0}
        transformer = SplitTransformer(config)
        result = transformer.transform({"Plan_Name": "S5884-197"})  # Returns "S5884"
    """

    def validate_config(self) -> None:
        """Validate configuration."""
        if "source_field" not in self.config:
            raise ValueError("SplitTransformer requires 'source_field' in config")

        if "delimiter" not in self.config:
            raise ValueError("SplitTransformer requires 'delimiter' in config")

        if "index" not in self.config:
            raise ValueError("SplitTransformer requires 'index' in config")

        if not isinstance(self.config["index"], int):
            raise ValueError("SplitTransformer 'index' must be an integer")

    def transform(self, source_record: Dict[str, Any]) -> Optional[str]:
        """
        Split source value and extract element at index.

        Args:
            source_record: Source record dictionary

        Returns:
            Extracted element or None if index out of bounds
        """
        source_field = self.config["source_field"]
        delimiter = self.config["delimiter"]
        index = self.config["index"]

        source_value = source_record.get(source_field)
        if not source_value:
            return None

        # Split and extract
        parts = str(source_value).split(delimiter)

        # Handle negative indices
        try:
            return parts[index].strip()
        except IndexError:
            return None
