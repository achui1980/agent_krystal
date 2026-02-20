"""
Transformer registry using factory pattern.
Creates transformer instances based on type.
"""

import json
from typing import Dict, Any, List
from krystal_v4.transformers.base import BaseTransformer
from krystal_v4.transformers.fixed_transformer import FixedTransformer
from krystal_v4.transformers.direct_transformer import DirectTransformer
from krystal_v4.transformers.name_parser_transformer import NameParserTransformer
from krystal_v4.transformers.conditional_transformer import ConditionalTransformer
from krystal_v4.transformers.split_transformer import SplitTransformer
from krystal_v4.transformers.empty_transformer import EmptyTransformer
from krystal_v4.transformers.substring_transformer import SubstringTransformer
from krystal_v4.transformers.phone_parser_transformer import PhoneParserTransformer


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
        "substring": SubstringTransformer,
        "phone_parser": PhoneParserTransformer,
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

    @classmethod
    def get_all_schemas(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get schemas for all registered transformers.

        Returns:
            Dictionary mapping type name to schema dict
        """
        return {
            name: transformer_cls.schema()
            for name, transformer_cls in cls._TRANSFORMER_MAP.items()
        }

    @classmethod
    def get_schema_prompt(cls) -> str:
        """
        Generate a formatted prompt string describing all available transformer types
        and their exact configuration schemas. Designed to be embedded in LLM prompts.

        Returns:
            Formatted multi-line string with all transformer schemas
        """
        lines = [
            "## Available Transformation Types and Their EXACT Config Schemas",
            "",
            "You MUST use ONLY the transformation types listed below.",
            "The config dict MUST contain exactly the required fields shown.",
            "Do NOT invent new fields or types.",
            "",
        ]

        for type_name, schema in cls.get_all_schemas().items():
            lines.append(f"### `{type_name}`")
            lines.append(f"**Description**: {schema['description']}")
            lines.append("**Config fields**:")

            config_spec = schema.get("config", {})
            if not config_spec:
                lines.append("  (no config required — use empty dict `{}`)")
            else:
                for field_name, field_info in config_spec.items():
                    req = "REQUIRED" if field_info.get("required") else "optional"
                    desc = field_info.get("description", "")
                    ftype = field_info.get("type", "any")
                    enum = field_info.get("enum")
                    line = f"  - `{field_name}` ({ftype}, {req}): {desc}"
                    if enum:
                        line += f" — allowed values: {enum}"
                    lines.append(line)

            examples = schema.get("examples", [])
            if examples:
                lines.append("**Example configs**:")
                for ex in examples:
                    lines.append(f"  config = {json.dumps(ex['config'])}")
                    if "input" in ex:
                        lines.append(f"  input  = {json.dumps(ex['input'])}")
                    lines.append(f"  output = {json.dumps(ex.get('output', ''))}")
            lines.append("")

        lines.append("## Pattern Matching Guide: SPECIAL_RULES Text → Transformer Type")
        lines.append("")
        lines.append("| SPECIAL_RULES pattern | Transformer type | Config hints |")
        lines.append("|---|---|---|")
        lines.append("| (empty / no special rule) | `direct` | `{\"source_field\": \"<CARRIER_COL>\"}` |")
        lines.append("| 'last_name, first_name' / name format | `name_parser` | part='first' or 'last' |")
        lines.append("| 'if X map to Y' / conditional | `conditional_map` | Build mappings dict |")
        lines.append("| 'separated by' / split / extract before/after | `split_extract` | delimiter + index |")
        lines.append("| 'left N chars' / 'first N chars' / substring | `substring` | method='left', length=N |")
        lines.append("| 'right N chars' / 'last N chars' | `substring` | method='right', length=N |")
        lines.append("| 'area code' / 'phone number' / phone parsing | `phone_parser` | part='area_code' or 'phone_number' |")
        lines.append("| field should be blank/empty | `empty` | `{}` |")
        lines.append("")

        return "\n".join(lines)
