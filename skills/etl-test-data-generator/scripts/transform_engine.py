#!/usr/bin/env python3
"""
ETL Transform Engine — standalone transformation framework.

Provides 8 built-in transformation types with a plugin registry pattern.
No external dependencies beyond Python stdlib.

Usage:
    # As a library
    from transform_engine import TransformerRegistry
    t = TransformerRegistry.get_transformer("direct", {"source_field": "Name"})
    result = t.transform({"Name": "Alice"})

    # CLI: validate a rule_config.json
    python transform_engine.py validate --config rule_config.json

    # CLI: apply transformations to a single record (JSON)
    python transform_engine.py transform --config rule_config.json --record '{"Name":"Alice"}'
"""

import argparse
import importlib.util
import json
import os
import re
import sys
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


# =============================================================================
# Base Transformer
# =============================================================================


class BaseTransformer(ABC):
    """Abstract base class for all transformers."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validate_config()

    @classmethod
    @abstractmethod
    def schema(cls) -> Dict[str, Any]:
        """Return configuration schema: {description, config, examples}."""
        pass

    @abstractmethod
    def validate_config(self) -> None:
        """Validate config. Raise ValueError if invalid."""
        pass

    @abstractmethod
    def transform(self, source_record: Dict[str, Any]) -> Any:
        """Apply transformation to source record, return value."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


# =============================================================================
# Built-in Transformers
# =============================================================================


class FixedTransformer(BaseTransformer):
    """Returns a constant value regardless of input."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Returns a constant value regardless of input.",
            "config": {
                "value": {
                    "type": "str",
                    "required": True,
                    "description": "The fixed value to return",
                },
            },
            "examples": [
                {"config": {"value": "ACTIVE"}, "output": "ACTIVE"},
            ],
        }

    def validate_config(self) -> None:
        if "value" not in self.config:
            raise ValueError("FixedTransformer requires 'value' in config")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        return self.config["value"]


class DirectTransformer(BaseTransformer):
    """Directly copies a source field value to the target field."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Directly copies a source field value to the target field.",
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Name of source field to copy from",
                },
            },
            "examples": [
                {
                    "config": {"source_field": "DOB"},
                    "input": {"DOB": "1960-01-15"},
                    "output": "1960-01-15",
                },
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("DirectTransformer requires 'source_field' in config")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        return source_record.get(self.config["source_field"])


class EmptyTransformer(BaseTransformer):
    """Returns empty string. Use for fields that should always be blank."""

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
        pass

    def transform(self, source_record: Dict[str, Any]) -> str:
        return ""


class ConditionalTransformer(BaseTransformer):
    """Maps source values to target values using a lookup table."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Maps source values to target values using a lookup table. Falls back to default if no match.",
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Source field to look up",
                },
                "mappings": {
                    "type": "dict",
                    "required": True,
                    "description": "Mapping from source values to target values",
                },
                "default": {
                    "type": "str",
                    "required": False,
                    "description": "Default value when no mapping matches",
                },
            },
            "examples": [
                {
                    "config": {
                        "source_field": "Status",
                        "mappings": {"A": "Active", "T": "Terminated"},
                        "default": "Unknown",
                    },
                    "input": {"Status": "A"},
                    "output": "Active",
                },
            ],
        }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        raw = self.config.get("mappings", {})
        self.normalized_mappings = {
            str(k).strip(): str(v).strip() for k, v in raw.items()
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("ConditionalTransformer requires 'source_field' in config")
        if "mappings" not in self.config:
            raise ValueError("ConditionalTransformer requires 'mappings' in config")
        if not isinstance(self.config["mappings"], dict):
            raise ValueError("ConditionalTransformer 'mappings' must be a dictionary")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        default = self.config.get("default")
        value = source_record.get(self.config["source_field"])
        if value is None:
            return default
        trimmed = str(value).strip()
        if trimmed in self.normalized_mappings:
            return self.normalized_mappings[trimmed]
        upper = trimmed.upper()
        for k, v in self.normalized_mappings.items():
            if k.upper() == upper:
                return v
        return default


class SubstringTransformer(BaseTransformer):
    """Extracts a substring using LEFT/RIGHT/MID with optional character stripping."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Extracts a substring using LEFT/RIGHT/MID. Optionally strips characters before extraction.",
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Source field to extract from",
                },
                "method": {
                    "type": "str",
                    "required": True,
                    "description": "Extraction method",
                    "enum": ["left", "right", "mid"],
                },
                "length": {
                    "type": "int",
                    "required": True,
                    "description": "Number of characters to extract",
                },
                "start": {
                    "type": "int",
                    "required": False,
                    "description": "Start position for 'mid' method (0-based, default 0)",
                },
                "strip_chars": {
                    "type": "str",
                    "required": False,
                    "description": "Characters to remove before extraction",
                },
            },
            "examples": [
                {
                    "config": {
                        "source_field": "Phone",
                        "method": "left",
                        "length": 3,
                        "strip_chars": "()-. ",
                    },
                    "input": {"Phone": "(555) 867-5309"},
                    "output": "555",
                },
                {
                    "config": {
                        "source_field": "Code",
                        "method": "mid",
                        "length": 3,
                        "start": 2,
                    },
                    "input": {"Code": "ABCDEF"},
                    "output": "CDE",
                },
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("SubstringTransformer requires 'source_field' in config")
        if "method" not in self.config:
            raise ValueError("SubstringTransformer requires 'method' in config")
        if self.config["method"] not in ("left", "right", "mid"):
            raise ValueError(
                "SubstringTransformer 'method' must be 'left', 'right', or 'mid'"
            )
        if "length" not in self.config:
            raise ValueError("SubstringTransformer requires 'length' in config")
        if not isinstance(self.config["length"], int) or self.config["length"] <= 0:
            raise ValueError("SubstringTransformer 'length' must be a positive integer")
        if self.config["method"] == "mid" and "start" in self.config:
            if not isinstance(self.config["start"], int) or self.config["start"] < 0:
                raise ValueError(
                    "SubstringTransformer 'start' must be a non-negative integer"
                )

    def transform(self, source_record: Dict[str, Any]) -> str:
        value = source_record.get(self.config["source_field"])
        if value is None:
            return ""
        value = str(value).strip()
        if not value:
            return ""
        strip_chars = self.config.get("strip_chars")
        if strip_chars:
            for ch in strip_chars:
                value = value.replace(ch, "")
        length = self.config["length"]
        method = self.config["method"]
        if method == "left":
            return value[:length]
        elif method == "right":
            return value[-length:] if len(value) >= length else value
        else:  # mid
            start = self.config.get("start", 0)
            return value[start : start + length]


class PhoneParserTransformer(BaseTransformer):
    """Parses phone numbers (strips non-digits, assumes 10-digit format) and extracts area_code or phone_number."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Parses phone numbers (strips non-digits, assumes 10-digit format) and extracts area_code or phone_number.",
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Source field containing phone number",
                },
                "part": {
                    "type": "str",
                    "required": True,
                    "description": "Which part to extract",
                    "enum": ["area_code", "phone_number"],
                },
                "format": {
                    "type": "str",
                    "required": False,
                    "description": "Output format (e.g. 'nnn-nnnn' inserts dash)",
                },
            },
            "examples": [
                {
                    "config": {"source_field": "Phone", "part": "area_code"},
                    "input": {"Phone": "(555) 867-5309"},
                    "output": "555",
                },
                {
                    "config": {
                        "source_field": "Phone",
                        "part": "phone_number",
                        "format": "nnn-nnnn",
                    },
                    "input": {"Phone": "(555) 867-5309"},
                    "output": "867-5309",
                },
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("PhoneParserTransformer requires 'source_field' in config")
        if "part" not in self.config:
            raise ValueError("PhoneParserTransformer requires 'part' in config")
        if self.config["part"] not in ("area_code", "phone_number"):
            raise ValueError(
                "PhoneParserTransformer 'part' must be 'area_code' or 'phone_number'"
            )

    def transform(self, source_record: Dict[str, Any]) -> str:
        value = source_record.get(self.config["source_field"])
        if value is None:
            return ""
        digits = re.sub(r"\D", "", str(value))
        if not digits:
            return ""
        # Handle 11-digit numbers with leading country code
        if len(digits) == 11 and digits[0] == "1":
            digits = digits[1:]
        part = self.config["part"]
        if len(digits) < 10:
            if part == "area_code":
                return digits[:3] if len(digits) >= 3 else digits
            else:
                return digits[3:] if len(digits) > 3 else ""
        area_code = digits[:3]
        phone_number = digits[3:10]
        if part == "area_code":
            return area_code
        fmt = self.config.get("format")
        if fmt and fmt.lower() == "nnn-nnnn" and len(phone_number) >= 7:
            return f"{phone_number[:3]}-{phone_number[3:7]}"
        return phone_number


# =============================================================================
# Composite Transformer (pipeline of multiple transformers)
# =============================================================================


class CompositeTransformer(BaseTransformer):
    """Chains multiple transformers in a pipeline. Output of each step feeds into the next."""

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return {
            "description": "Chains multiple transformers in sequence. Each step's output becomes the next step's input.",
            "config": {
                "source_field": {
                    "type": "str",
                    "required": True,
                    "description": "Initial source field to read from",
                },
                "pipeline": {
                    "type": "list",
                    "required": True,
                    "description": "List of {type, config} steps to apply in order",
                },
            },
            "examples": [
                {
                    "config": {
                        "source_field": "Phone",
                        "pipeline": [
                            {
                                "type": "substring",
                                "config": {
                                    "method": "left",
                                    "length": 10,
                                    "strip_chars": "()-. ",
                                },
                            },
                            {
                                "type": "substring",
                                "config": {"method": "left", "length": 3},
                            },
                        ],
                    },
                    "input": {"Phone": "(555) 867-5309"},
                    "output": "555",
                }
            ],
        }

    def validate_config(self) -> None:
        if "source_field" not in self.config:
            raise ValueError("CompositeTransformer requires 'source_field'")
        if "pipeline" not in self.config or not self.config["pipeline"]:
            raise ValueError("CompositeTransformer requires non-empty 'pipeline'")
        for i, step in enumerate(self.config["pipeline"]):
            if not isinstance(step, dict) or "type" not in step:
                raise ValueError(f"Pipeline step {i}: must be a dict with 'type' key")

    def transform(self, source_record: Dict[str, Any]) -> Any:
        source_field = self.config["source_field"]
        value = source_record.get(source_field)
        if value is None:
            return ""

        # Each step receives the previous step's output as __pipeline_value__
        for step in self.config["pipeline"]:
            step_config = dict(step.get("config", {}))
            step_config["source_field"] = "__pipeline_value__"
            t = TransformerRegistry.get_transformer(step["type"], step_config)
            value = t.transform({"__pipeline_value__": value})

        return value


# =============================================================================
# Transformer Registry
# =============================================================================


class TransformerRegistry:
    """Factory for creating transformer instances with plugin support."""

    _TRANSFORMER_MAP: Dict[str, type] = {
        "fixed": FixedTransformer,
        "direct": DirectTransformer,
        "conditional_map": ConditionalTransformer,
        "empty": EmptyTransformer,
        "substring": SubstringTransformer,
        "phone_parser": PhoneParserTransformer,
        "composite": CompositeTransformer,
    }

    @classmethod
    def get_transformer(
        cls, transformation_type: str, config: Dict[str, Any]
    ) -> BaseTransformer:
        if transformation_type not in cls._TRANSFORMER_MAP:
            raise ValueError(
                f"Unknown transformation type: '{transformation_type}'. "
                f"Available: {list(cls._TRANSFORMER_MAP.keys())}"
            )
        return cls._TRANSFORMER_MAP[transformation_type](config)

    @classmethod
    def list_types(cls) -> List[str]:
        return list(cls._TRANSFORMER_MAP.keys())

    @classmethod
    def register_transformer(
        cls, transformation_type: str, transformer_class: type
    ) -> None:
        if not issubclass(transformer_class, BaseTransformer):
            raise TypeError(f"{transformer_class} must inherit from BaseTransformer")
        cls._TRANSFORMER_MAP[transformation_type] = transformer_class

    @classmethod
    def get_all_schemas(cls) -> Dict[str, Dict[str, Any]]:
        return {name: tcls.schema() for name, tcls in cls._TRANSFORMER_MAP.items()}

    @classmethod
    def get_schema_prompt(cls) -> str:
        """Generate LLM-embeddable prompt describing all transformer schemas."""
        lines = [
            "## Available Transformation Types and Their EXACT Config Schemas",
            "",
            "You MUST use ONLY the transformation types listed below.",
            "The config dict MUST contain exactly the required fields shown.",
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
                lines.append("**Examples**:")
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
        lines.append(
            '| (empty / no special rule) | `direct` | `{"source_field": "<SOURCE_COL>"}` |'
        )
        lines.append(
            "| 'last_name, first_name' / name format | `name_parser` | part='first' or 'last' |"
        )
        lines.append(
            "| 'if X map to Y' / conditional | `conditional_map` | Build mappings dict |"
        )
        lines.append(
            "| 'separated by' / split / extract before/after | `split_extract` | delimiter + index |"
        )
        lines.append(
            "| 'left N chars' / 'first N chars' / substring | `substring` | method='left', length=N |"
        )
        lines.append(
            "| 'right N chars' / 'last N chars' | `substring` | method='right', length=N |"
        )
        lines.append(
            "| 'area code' / 'phone number' / phone parsing | `phone_parser` | part='area_code' or 'phone_number' |"
        )
        lines.append("| field should be blank/empty | `empty` | `{}` |")
        lines.append("")
        return "\n".join(lines)


# =============================================================================
# Validation Utilities
# =============================================================================


def validate_rule_config(config: Dict[str, Any]) -> List[str]:
    """Validate a rule_config dict. Returns list of error strings (empty = valid)."""
    errors = []

    if "source_format" not in config:
        errors.append("Missing 'source_format'")
    elif config["source_format"] not in ("csv_quoted", "pipe", "comma"):
        errors.append(f"Invalid source_format: '{config['source_format']}'")

    if "target_fields" not in config or not config["target_fields"]:
        errors.append("Missing or empty 'target_fields'")

    # Use dynamic type list (includes custom-loaded transformers)
    valid_types = set(TransformerRegistry.list_types())
    for i, rule in enumerate(config.get("transformation_rules", [])):
        ttype = rule.get("transformation_type", "")
        if ttype not in valid_types:
            errors.append(f"Rule {i}: unknown type '{ttype}'")
        if not rule.get("target_field"):
            errors.append(f"Rule {i}: missing 'target_field'")

    return errors


def validate_rules_against_transformers(
    rules: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Instantiate every transformer to catch config errors before processing."""
    errors = []
    for rule in rules:
        ttype = rule.get("transformation_type", "")
        config = dict(rule.get("config", {}))
        source_field = rule.get("source_field", "")
        if "source_field" not in config and source_field:
            config["source_field"] = source_field
        try:
            TransformerRegistry.get_transformer(ttype, config)
        except (ValueError, TypeError) as e:
            errors.append(
                {
                    "target_field": rule.get("target_field", "?"),
                    "type": ttype,
                    "error": str(e),
                }
            )
    return errors


# =============================================================================
# Dynamic Transformer Loading
# =============================================================================


def load_custom_transformers(directory: str) -> List[str]:
    """
    Load all custom transformer files from a directory.

    Each file must:
    - End with '_transformer.py'
    - Define a BaseTransformer subclass
    - Call TransformerRegistry.register_transformer() at module level

    Args:
        directory: Path to directory containing *_transformer.py files

    Returns:
        List of loaded file names
    """
    loaded = []
    if not os.path.isdir(directory):
        return loaded
    for fname in sorted(os.listdir(directory)):
        if fname.endswith("_transformer.py") and not fname.startswith("__"):
            fpath = os.path.join(directory, fname)
            try:
                spec = importlib.util.spec_from_file_location(fname[:-3], fpath)
                mod = importlib.util.module_from_spec(spec)
                # Inject references so custom transformers can import from us
                mod.BaseTransformer = BaseTransformer
                mod.TransformerRegistry = TransformerRegistry
                spec.loader.exec_module(mod)
                loaded.append(fname)
            except Exception as e:
                print(
                    f"WARNING: Failed to load custom transformer '{fname}': {e}",
                    file=sys.stderr,
                )
    return loaded


# =============================================================================
# Auto-load custom transformers from sibling directory
# =============================================================================

_CUSTOM_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "custom_transformers"
)
if os.path.isdir(_CUSTOM_DIR):
    load_custom_transformers(_CUSTOM_DIR)


# =============================================================================
# CLI
# =============================================================================


def cli():
    parser = argparse.ArgumentParser(description="ETL Transform Engine")
    parser.add_argument(
        "--custom-transformers",
        default=None,
        help="Directory containing custom *_transformer.py files to load",
    )
    sub = parser.add_subparsers(dest="command")

    # validate
    val = sub.add_parser("validate", help="Validate a rule_config.json")
    val.add_argument("--config", required=True, help="Path to rule_config.json")

    # transform
    tr = sub.add_parser("transform", help="Apply transformations to a record")
    tr.add_argument("--config", required=True, help="Path to rule_config.json")
    tr.add_argument("--record", required=True, help="Source record as JSON string")

    # schemas
    sub.add_parser("schemas", help="Print all transformer schemas")

    args = parser.parse_args()

    # Load custom transformers if specified
    if args.custom_transformers:
        loaded = load_custom_transformers(args.custom_transformers)
        if loaded:
            print(f"Loaded custom transformers: {loaded}", file=sys.stderr)

    if args.command == "validate":
        with open(args.config, "r") as f:
            cfg = json.load(f)
        errs = validate_rule_config(cfg)
        rule_errs = validate_rules_against_transformers(
            cfg.get("transformation_rules", [])
        )
        if errs or rule_errs:
            print("VALIDATION FAILED:")
            for e in errs:
                print(f"  - {e}")
            for e in rule_errs:
                print(f"  - {e['target_field']} ({e['type']}): {e['error']}")
            sys.exit(1)
        else:
            print("VALIDATION OK: All rules are valid.")

    elif args.command == "transform":
        with open(args.config, "r") as f:
            cfg = json.load(f)
        record = json.loads(args.record)
        rules = cfg.get("transformation_rules", [])
        fixed = cfg.get("fixed_values", {})
        target_fields = cfg.get("target_fields", [])

        output = {}
        for rule in rules:
            ttype = rule["transformation_type"]
            config = dict(rule.get("config", {}))
            sf = rule.get("source_field", "")
            if "source_field" not in config and sf:
                config["source_field"] = sf
            t = TransformerRegistry.get_transformer(ttype, config)
            val = t.transform(record)
            output[rule["target_field"]] = str(val) if val is not None else ""

        for field, val in fixed.items():
            output[field] = val
        for field in target_fields:
            if field not in output:
                output[field] = ""

        print(json.dumps(output, indent=2, ensure_ascii=False))

    elif args.command == "schemas":
        print(TransformerRegistry.get_schema_prompt())

    else:
        parser.print_help()


if __name__ == "__main__":
    cli()
