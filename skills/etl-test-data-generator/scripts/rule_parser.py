#!/usr/bin/env python3
"""
ETL Rule Parser — deterministic pre-processing of rules.csv.

Reads a rules CSV file, auto-classifies each row, and outputs a
rule_config_draft.json with:
- Resolved rules (direct mappings, fixed values)
- Unresolved rules (SPECIAL_RULES that need agent/LLM interpretation)

This eliminates ~80-90% of the manual work in Phase 1.

Usage:
    python rule_parser.py \
        --rules case/humanaS10/rules.csv \
        --case humanaS10 \
        --case-dir case/humanaS10 \
        --output output/humanaS10/rule_config_draft.json
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


# =============================================================================
# Format Detection (standalone, no CrewAI dependency)
# =============================================================================


def detect_format(file_path: str) -> str:
    """Detect file delimiter format by analyzing first few lines."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [f.readline() for _ in range(3)]
            lines = [line for line in lines if line.strip()]

        if not lines:
            return "comma"

        first_line = lines[0]
        pipe_count = first_line.count("|")
        comma_count = first_line.count(",")
        quote_count = first_line.count('"')

        if pipe_count > comma_count:
            return "pipe"
        elif quote_count >= 2 and comma_count > 0:
            return "csv_quoted"
        elif comma_count > 0:
            return "comma"
        else:
            return "comma"
    except Exception:
        return "comma"


# =============================================================================
# Metadata Extraction
# =============================================================================


def extract_output_metadata(expected_file: str) -> Dict[str, str]:
    """Extract ACTION_ID, SERVICE_MAP_ID etc. from expected output file header."""
    metadata = {}
    try:
        with open(expected_file, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line:
                    break
                if ":" in line:
                    key, _, value = line.partition(":")
                    metadata[key.strip()] = value.strip()
    except FileNotFoundError:
        pass
    return metadata


# =============================================================================
# Core Rule Parser
# =============================================================================


def parse_rules(
    rules_file: str,
    case_name: str,
    case_dir: Optional[str] = None,
    record_count: int = 10,
) -> Dict[str, Any]:
    """
    Parse rules.csv and produce a draft rule_config.

    Classification logic:
    - Has CARRIER_COLUMN_NAME + no SPECIAL_RULES → direct (auto-resolved)
    - Has CARRIER_COLUMN_NAME + has SPECIAL_RULES → unresolved (needs agent)
    - No CARRIER_COLUMN_NAME + has DEFAULT → fixed_values
    - Neither → empty field (no rule needed)

    Returns:
        Dict with all rule_config fields + unresolved_rules list
    """

    # Read rules CSV
    rows = []
    with open(rules_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    target_fields = []
    source_fields_seen = []
    fixed_values = {}
    transformation_rules = []
    unresolved_rules = []

    for row in rows:
        cs_col = row.get("CS_COLUMN_NAME", "").strip()
        carrier_col = row.get("CARRIER_COLUMN_NAME", "").strip()
        default = row.get("DEFAULT", "").strip()
        special = row.get("SPECIAL_RULES", "").strip()
        note = row.get("NOTE", "").strip()
        csds_flag = row.get("CSDS_FLAG", "").strip()

        # Collect target fields (in order)
        if cs_col:
            target_fields.append(cs_col)

        if carrier_col:
            # Track unique source fields (preserve order)
            if carrier_col not in source_fields_seen:
                source_fields_seen.append(carrier_col)

            if special:
                # Type A + SPECIAL_RULES → unresolved, needs agent
                unresolved_rules.append(
                    {
                        "target_field": cs_col,
                        "source_field": carrier_col,
                        "special_rules": special,
                        "note": note,
                        "csds_flag": csds_flag,
                    }
                )
            else:
                # Type A + no SPECIAL_RULES → direct mapping
                transformation_rules.append(
                    {
                        "target_field": cs_col,
                        "source_field": carrier_col,
                        "transformation_type": "direct",
                        "config": {"source_field": carrier_col},
                    }
                )
        elif default:
            # Type B → fixed value
            fixed_values[cs_col] = default
        # else: empty field, no rule needed

    # Also check CSDS_FLAG=0 rows that have SPECIAL_RULES but no CARRIER_COLUMN_NAME
    # (e.g., AREA_CODE and PHONE_NUMBER in humanaS10 reference existing source fields)
    for row in rows:
        cs_col = row.get("CS_COLUMN_NAME", "").strip()
        carrier_col = row.get("CARRIER_COLUMN_NAME", "").strip()
        special = row.get("SPECIAL_RULES", "").strip()
        note = row.get("NOTE", "").strip()
        csds_flag = row.get("CSDS_FLAG", "").strip()

        # Skip if already processed (has carrier_col or default)
        if carrier_col or row.get("DEFAULT", "").strip():
            continue
        # Only capture rows with SPECIAL_RULES that weren't already captured
        if special and cs_col not in [u["target_field"] for u in unresolved_rules]:
            unresolved_rules.append(
                {
                    "target_field": cs_col,
                    "source_field": "",
                    "special_rules": special,
                    "note": note,
                    "csds_flag": csds_flag,
                }
            )

    # Detect source format
    source_format = "csv_quoted"
    if case_dir:
        for fname in ("source.csv", "source.txt"):
            ref_source = Path(case_dir) / fname
            if ref_source.exists():
                source_format = detect_format(str(ref_source))
                break

    # Extract output metadata
    output_metadata = {}
    if case_dir:
        ref_expected = Path(case_dir) / "expected.txt"
        if ref_expected.exists():
            output_metadata = extract_output_metadata(str(ref_expected))

    # Append LAST_TOUCHED_DATE if it appears in reference expected header
    # but not in rules (common pattern)
    if case_dir:
        ref_expected = Path(case_dir) / "expected.txt"
        if ref_expected.exists():
            with open(ref_expected, "r", encoding="utf-8-sig") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if ":" not in line and "|" in line:
                        # This is the header row
                        ref_fields = line.split("|")
                        for rf in ref_fields:
                            rf = rf.strip()
                            if rf and rf not in target_fields:
                                target_fields.append(rf)
                        break

    # Build draft config
    draft = {
        "case_name": case_name,
        "source_format": source_format,
        "source_fields": source_fields_seen,
        "target_fields": target_fields,
        "fixed_values": fixed_values,
        "transformation_rules": transformation_rules,
        "unresolved_rules": unresolved_rules,
        "field_metadata": [],
        "conditional_coverage": {},
        "record_count": record_count,
        "output_metadata": output_metadata,
    }

    return draft


# =============================================================================
# Summary
# =============================================================================


def print_summary(draft: Dict[str, Any]) -> None:
    """Print a human-readable summary of the parsed rules."""
    print(f"Case: {draft['case_name']}")
    print(f"Source format: {draft['source_format']}")
    print(f"Target fields: {len(draft['target_fields'])}")
    print(f"Source fields: {len(draft['source_fields'])}")
    print(f"Fixed values: {len(draft['fixed_values'])}")
    print(f"Auto-resolved rules (direct): {len(draft['transformation_rules'])}")
    print(f"Unresolved rules (need agent): {len(draft['unresolved_rules'])}")
    if draft["output_metadata"]:
        print(f"Output metadata: {draft['output_metadata']}")
    print()

    if draft["unresolved_rules"]:
        print("--- Unresolved rules (agent must classify) ---")
        for i, rule in enumerate(draft["unresolved_rules"]):
            print(
                f"  [{i + 1}] target={rule['target_field']}, source={rule['source_field']}"
            )
            # Show first 80 chars of special_rules
            sr = rule["special_rules"].replace("\n", " ").strip()
            if len(sr) > 80:
                sr = sr[:77] + "..."
            print(f"      SPECIAL_RULES: {sr}")
            if rule.get("note"):
                note = rule["note"].replace("\n", " ").strip()
                if len(note) > 80:
                    note = note[:77] + "..."
                print(f"      NOTE: {note}")


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="ETL Rule Parser — pre-process rules.csv into draft config"
    )
    parser.add_argument("--rules", required=True, help="Path to rules.csv")
    parser.add_argument("--case", required=True, help="Test case name")
    parser.add_argument(
        "--case-dir",
        default=None,
        help="Directory with reference source/expected files",
    )
    parser.add_argument(
        "--count", type=int, default=10, help="Default record count (default: 10)"
    )
    parser.add_argument(
        "--output", required=True, help="Output path for rule_config_draft.json"
    )
    args = parser.parse_args()

    if not os.path.exists(args.rules):
        print(f"ERROR: Rules file not found: {args.rules}", file=sys.stderr)
        sys.exit(1)

    # Parse
    draft = parse_rules(
        rules_file=args.rules,
        case_name=args.case,
        case_dir=args.case_dir,
        record_count=args.count,
    )

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    # Write draft
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(draft, f, indent=2, ensure_ascii=False)

    print(f"Draft config written to: {args.output}")
    print()
    print_summary(draft)


if __name__ == "__main__":
    main()
