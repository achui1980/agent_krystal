#!/usr/bin/env python3
"""
ETL Expected Output Generator — deterministic transformation pipeline.

Reads rule_config.json and generated source data, applies all transformations
and fixed values, outputs the expected result in pipe-delimited format.

No LLM dependency — all transformations are pure Python.

Usage:
    python expected_generator.py \
        --config rule_config.json \
        --source generated_source.txt \
        --output generated_expected.txt
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from typing import Any, Dict, List

# Import transform engine (same directory)
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from transform_engine import (
    TransformerRegistry,
    validate_rules_against_transformers,
    load_custom_transformers,
)


# =============================================================================
# Source Reader
# =============================================================================


def read_source(source_path: str, source_format: str) -> List[Dict[str, str]]:
    """Read source file and return list of record dicts."""
    records = []
    with open(source_path, "r", encoding="utf-8") as f:
        if source_format == "pipe":
            reader = csv.DictReader(f, delimiter="|")
        else:
            reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    return records


# =============================================================================
# Record Processor
# =============================================================================


def process_record(
    source_record: Dict[str, str],
    transformation_rules: List[Dict[str, Any]],
    fixed_values: Dict[str, str],
    target_fields: List[str],
) -> Dict[str, str]:
    """Process a single source record into an expected output record."""
    output = {}

    # Step A: Apply transformation rules
    for rule in transformation_rules:
        target_field = rule.get("target_field", "")
        source_field = rule.get("source_field", "")
        transformation_type = rule.get("transformation_type", "")
        config = dict(rule.get("config", {}))

        # Inject source_field into config for transformers that need it
        if "source_field" not in config and source_field:
            config["source_field"] = source_field

        try:
            transformer = TransformerRegistry.get_transformer(
                transformation_type, config
            )
            value = transformer.transform(source_record)
            output[target_field] = str(value) if value is not None else ""
        except Exception as e:
            print(
                f"WARNING: Transformation failed for '{target_field}' ({transformation_type}): {e}",
                file=sys.stderr,
            )
            output[target_field] = ""

    # Step B: Apply fixed values
    for field, value in fixed_values.items():
        output[field] = value

    # Step C: Fill remaining target fields with empty string
    for field in target_fields:
        if field not in output:
            output[field] = ""

    return output


# =============================================================================
# Output Writer
# =============================================================================


def write_output(
    output_path: str,
    target_fields: List[str],
    output_rows: List[Dict[str, str]],
    output_metadata: Dict[str, str],
    case_name: str,
) -> None:
    """Write pipe-delimited expected output file with metadata header."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    with open(output_path, "w", encoding="utf-8") as f:
        # Metadata header
        action_id = output_metadata.get("ACTION_ID", f"{case_name}-etl-test")
        service_map_id = output_metadata.get("SERVICE_MAP_ID", "")
        source_token = output_metadata.get("SOURCE_TOKEN", f"generated_{timestamp}")

        if action_id:
            f.write(f"ACTION_ID:{action_id}\n")
        if service_map_id:
            f.write(f"SERVICE_MAP_ID:{service_map_id}\n")
        f.write(f"SOURCE_TOKEN:{source_token}\n")
        f.write("\n")

        # Header row
        f.write("|".join(target_fields) + "\n")

        # Data rows
        for row in output_rows:
            values = [row.get(field, "") for field in target_fields]
            f.write("|".join(values) + "\n")


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="ETL Expected Output Generator")
    parser.add_argument("--config", required=True, help="Path to rule_config.json")
    parser.add_argument(
        "--source", required=True, help="Path to generated source data file"
    )
    parser.add_argument(
        "--output", required=True, help="Output file path for expected data"
    )
    parser.add_argument(
        "--custom-transformers",
        default=None,
        help="Directory containing custom *_transformer.py files to load",
    )
    args = parser.parse_args()

    # Load custom transformers if specified
    if args.custom_transformers:
        loaded = load_custom_transformers(args.custom_transformers)
        if loaded:
            print(f"Loaded custom transformers: {loaded}", file=sys.stderr)

    # Step 1: Read rule config
    with open(args.config, "r", encoding="utf-8") as f:
        rule_config = json.load(f)

    case_name = rule_config.get("case_name", "unknown")
    target_fields = rule_config.get("target_fields", [])
    fixed_values = rule_config.get("fixed_values", {})
    transformation_rules = rule_config.get("transformation_rules", [])
    source_format = rule_config.get("source_format", "csv_quoted")
    output_metadata = rule_config.get("output_metadata", {})

    if not target_fields:
        print("ERROR: rule_config.json must have 'target_fields'", file=sys.stderr)
        sys.exit(1)

    # Step 2: Validate all rules before processing
    validation_errors = validate_rules_against_transformers(transformation_rules)
    if validation_errors:
        print("VALIDATION FAILED:", file=sys.stderr)
        for ve in validation_errors:
            print(
                f"  - {ve['target_field']} ({ve['type']}): {ve['error']}",
                file=sys.stderr,
            )

        # Write errors to JSON
        error_path = args.output.replace(".txt", "_errors.json")
        with open(error_path, "w", encoding="utf-8") as ef:
            json.dump({"validation_errors": validation_errors}, ef, indent=2)
        print(f"Validation errors written to: {error_path}", file=sys.stderr)
        sys.exit(1)

    # Step 3: Read source data
    source_records = read_source(args.source, source_format)
    if not source_records:
        print(f"ERROR: No records found in {args.source}", file=sys.stderr)
        sys.exit(1)

    # Step 4: Process each record
    output_rows = []
    errors = []
    for i, record in enumerate(source_records):
        try:
            output = process_record(
                record, transformation_rules, fixed_values, target_fields
            )
            output_rows.append(output)
        except Exception as e:
            errors.append(f"Record {i + 1}: {str(e)}")
            output_rows.append({field: "" for field in target_fields})

    # Step 5: Write output
    write_output(args.output, target_fields, output_rows, output_metadata, case_name)

    # Summary
    print(f"Generated expected output: {args.output}")
    print(f"  Records processed: {len(output_rows)}")
    print(f"  Target fields: {len(target_fields)}")
    print(f"  Transformation rules: {len(transformation_rules)}")
    print(f"  Fixed values: {len(fixed_values)}")
    if errors:
        print(f"  Errors: {len(errors)}")
        for err in errors[:5]:
            print(f"    - {err}")


if __name__ == "__main__":
    main()
