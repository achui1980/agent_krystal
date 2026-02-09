"""
Transformer registry using factory pattern.
Creates transformer instances based on type.
"""

from typing import Dict, Any
from krystal_v4.transformers.base import BaseTransformer
from krystal_v4.transformers.fixed_transformer import FixedTransformer
from krystal_v4.transformers.direct_transformer import DirectTransformer
from krystal_v4.transformers.name_parser_transformer import NameParserTransformer
from krystal_v4.transformers.conditional_transformer import ConditionalTransformer
from krystal_v4.transformers.split_transformer import SplitTransformer
from krystal_v4.transformers.empty_transformer import EmptyTransformer


class TransformerRegistry:
    """
    Factory for creating transformer instances.
    """

    _TRANSFORMER_MAP = {
        "fixed": FixedTransformer,
        "direct": DirectTransformer,
        "name_parser": NameParserTransformer,
        "conditional_map": ConditionalTransformer,
        "split_extract": SplitTransformer,
        "empty": EmptyTransformer,
    }

    @classmethod
    def get_transformer(
        cls, transformation_type: str, config: Dict[str, Any]
    ) -> BaseTransformer:
        """
        Create transformer instance based on type.

        Args:
            transformation_type: Type of transformation
            config: Transformation-specific configuration

        Returns:
            Transformer instance

        Raises:
            ValueError: If transformation type is unknown
        """
        if transformation_type not in cls._TRANSFORMER_MAP:
            raise ValueError(
                f"Unknown transformation type: {transformation_type}. "
                f"Available types: {list(cls._TRANSFORMER_MAP.keys())}"
            )

        transformer_class = cls._TRANSFORMER_MAP[transformation_type]
        return transformer_class(config)

    @classmethod
    def list_types(cls) -> list:
        """
        Get list of available transformation types.

        Returns:
            List of transformation type strings
        """
        return list(cls._TRANSFORMER_MAP.keys())

    @classmethod
    def register_transformer(
        cls, transformation_type: str, transformer_class: type
    ) -> None:
        """
        Register a custom transformer type.

        Args:
            transformation_type: Type identifier
            transformer_class: Transformer class (must inherit from BaseTransformer)

        Raises:
            TypeError: If transformer_class doesn't inherit from BaseTransformer
        """
        if not issubclass(transformer_class, BaseTransformer):
            raise TypeError(f"{transformer_class} must inherit from BaseTransformer")

        cls._TRANSFORMER_MAP[transformation_type] = transformer_class
