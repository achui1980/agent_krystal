"""
Base transformer abstract class for Krystal V4.
All transformers must inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List


class BaseTransformer(ABC):
    """
    Abstract base class for all transformers.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration.

        Args:
            config: Transformation-specific configuration
        """
        self.config = config
        self.validate_config()

    @classmethod
    @abstractmethod
    def schema(cls) -> Dict[str, Any]:
        """
        Return the configuration schema for this transformer.

        Returns:
            Dictionary describing the expected config format:
            {
                "description": "What this transformer does",
                "config": {
                    "field_name": {"type": "str", "required": True, "description": "..."},
                    ...
                },
                "examples": [{"config": {...}, "input": ..., "output": ...}]
            }
        """
        pass

    @abstractmethod
    def validate_config(self) -> None:
        """
        Validate configuration for this transformer.
        Should raise ValueError if config is invalid.
        """
        pass

    @abstractmethod
    def transform(self, source_record: Dict[str, Any]) -> Any:
        """
        Apply transformation to source record.

        Args:
            source_record: Dictionary of source field values

        Returns:
            Transformed value
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"
