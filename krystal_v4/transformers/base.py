"""
Base transformer abstract class for Krystal V4.
All transformers must inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


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
