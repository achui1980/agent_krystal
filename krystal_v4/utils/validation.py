"""
Validation utilities for Krystal V4.
"""

from typing import Dict, Any, List
from krystal_v4.models.rule_config import RuleConfig, TransformationRule


VALID_TRANSFORMATION_TYPES = {
    "fixed",
    "direct",
    "conditional_map",
    "name_parser",
    "split_extract",
    "empty",
    "substring",
    "phone_parser",
}

VALID_SOURCE_FORMATS = {"csv_quoted", "pipe", "comma"}


def validate_transformation_type(trans_type: str) -> bool:
    """
    Validate transformation type.

    Args:
        trans_type: Transformation type string

    Returns:
        True if valid, False otherwise
    """
    return trans_type in VALID_TRANSFORMATION_TYPES


def validate_source_format(format_str: str) -> bool:
    """
    Validate source format.

    Args:
        format_str: Format string

    Returns:
        True if valid, False otherwise
    """
    return format_str in VALID_SOURCE_FORMATS


def validate_rule_config(config: RuleConfig) -> List[str]:
    """
    Validate RuleConfig object.

    Args:
        config: RuleConfig instance

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    # Check source format
    if not validate_source_format(config.source_format):
        errors.append(
            f"Invalid source_format: {config.source_format}. "
            f"Must be one of: {VALID_SOURCE_FORMATS}"
        )

    # Check field counts
    if not config.source_fields:
        errors.append("source_fields cannot be empty")

    if not config.target_fields:
        errors.append("target_fields cannot be empty")

    # Check transformation rules
    if not config.transformation_rules:
        errors.append("transformation_rules cannot be empty")

    for i, rule in enumerate(config.transformation_rules):
        rule_errors = validate_transformation_rule(rule, i)
        errors.extend(rule_errors)

    # Check record count
    if config.record_count <= 0:
        errors.append(f"record_count must be positive, got {config.record_count}")

    return errors


def validate_transformation_rule(rule: TransformationRule, index: int) -> List[str]:
    """
    Validate a single transformation rule.

    Args:
        rule: TransformationRule instance
        index: Rule index for error reporting

    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    prefix = f"Rule {index} ({rule.target_field})"

    # Check transformation type
    if not validate_transformation_type(rule.transformation_type):
        errors.append(
            f"{prefix}: Invalid transformation_type '{rule.transformation_type}'. "
            f"Must be one of: {VALID_TRANSFORMATION_TYPES}"
        )

    # Type-specific validation
    if rule.transformation_type == "fixed":
        if "value" not in rule.config:
            errors.append(f"{prefix}: 'fixed' type requires 'value' in config")

    elif rule.transformation_type == "direct":
        if "source_field" not in rule.config:
            errors.append(f"{prefix}: 'direct' type requires 'source_field' in config")

    elif rule.transformation_type == "conditional_map":
        if "source_field" not in rule.config:
            errors.append(f"{prefix}: 'conditional_map' requires 'source_field'")
        if "mappings" not in rule.config:
            errors.append(f"{prefix}: 'conditional_map' requires 'mappings'")
        elif not isinstance(rule.config["mappings"], dict):
            errors.append(f"{prefix}: 'mappings' must be a dictionary")

    elif rule.transformation_type == "name_parser":
        if "source_field" not in rule.config:
            errors.append(f"{prefix}: 'name_parser' requires 'source_field'")
        if "part" not in rule.config:
            errors.append(f"{prefix}: 'name_parser' requires 'part' (first/last)")
        elif rule.config["part"] not in ["first", "last"]:
            errors.append(f"{prefix}: 'part' must be 'first' or 'last'")

    elif rule.transformation_type == "split_extract":
        if "source_field" not in rule.config:
            errors.append(f"{prefix}: 'split_extract' requires 'source_field'")
        if "delimiter" not in rule.config:
            errors.append(f"{prefix}: 'split_extract' requires 'delimiter'")
        if "index" not in rule.config:
            errors.append(f"{prefix}: 'split_extract' requires 'index'")
        elif not isinstance(rule.config["index"], int):
            errors.append(f"{prefix}: 'index' must be an integer")

    elif rule.transformation_type == "substring":
        if "source_field" not in rule.config:
            errors.append(f"{prefix}: 'substring' requires 'source_field'")
        if "method" not in rule.config:
            errors.append(f"{prefix}: 'substring' requires 'method'")
        elif rule.config["method"] not in ("left", "right", "mid"):
            errors.append(f"{prefix}: 'method' must be 'left', 'right', or 'mid'")
        if "length" not in rule.config:
            errors.append(f"{prefix}: 'substring' requires 'length'")
        elif not isinstance(rule.config["length"], int) or rule.config["length"] <= 0:
            errors.append(f"{prefix}: 'length' must be a positive integer")

    elif rule.transformation_type == "phone_parser":
        if "source_field" not in rule.config:
            errors.append(f"{prefix}: 'phone_parser' requires 'source_field'")
        if "part" not in rule.config:
            errors.append(f"{prefix}: 'phone_parser' requires 'part'")
        elif rule.config["part"] not in ("area_code", "phone_number"):
            errors.append(f"{prefix}: 'part' must be 'area_code' or 'phone_number'")

    return errors


def validate_source_record(
    record: Dict[str, Any], required_fields: List[str]
) -> List[str]:
    """
    Validate a source data record.

    Args:
        record: Source data record
        required_fields: List of required field names

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    for field in required_fields:
        if field not in record:
            errors.append(f"Missing required field: {field}")

    return errors


def validate_rule_config_against_transformers(
    transformation_rules: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Validate each transformation rule by attempting to instantiate its transformer.
    This catches config mismatches BEFORE processing any records.

    Args:
        transformation_rules: List of rule dicts from rule_config.json

    Returns:
        List of error dicts: [{"target_field": ..., "type": ..., "error": ...}]
    """
    from krystal_v4.transformers.transformer_registry import TransformerRegistry

    errors = []
    for rule in transformation_rules:
        target_field = rule.get("target_field", "UNKNOWN")
        source_field = rule.get("source_field", "")
        transformation_type = rule.get("transformation_type", "")
        config = dict(rule.get("config", {}))

        # Inject source_field into config if not present
        if "source_field" not in config and source_field:
            config["source_field"] = source_field

        try:
            TransformerRegistry.get_transformer(transformation_type, config)
        except Exception as e:
            errors.append({
                "target_field": target_field,
                "type": transformation_type,
                "error": str(e),
            })

    return errors
