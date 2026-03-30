#!/usr/bin/env python3
"""
ETL Test Data Generator — generates realistic fake source data.

Reads a rule_config.json and produces test source records with:
- Intelligent field-type detection based on field names
- Conditional coverage (ensures all branch values are represented)
- Configurable record count and output format

Dependencies: faker, (optional) pydantic

Usage:
    python data_generator.py \
        --config rule_config.json \
        --count 10 \
        --output generated_source.txt
"""

import argparse
import csv
import io
import json
import random
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

try:
    from faker import Faker
except ImportError:
    print(
        "ERROR: 'faker' package is required. Install: pip install faker",
        file=sys.stderr,
    )
    sys.exit(1)


# =============================================================================
# Field Value Generator
# =============================================================================


class FieldGenerator:
    """Generates realistic fake data based on field name heuristics and metadata."""

    def __init__(self, seed: int = 12345):
        self.faker = Faker()
        Faker.seed(seed)
        random.seed(seed)

    def generate(
        self,
        field_name: str,
        data_type: str = "string",
        format_hint: Optional[str] = None,
        sample_values: Optional[List[str]] = None,
    ) -> Any:
        """
        Generate a single value for a field.

        Priority:
        1. sample_values — if provided, pick randomly from them
        2. format_hint — if matches known pattern, use it
        3. field_name heuristic — match field name to generator
        4. data_type — fallback by type
        5. random word — last resort
        """
        # Priority 1: explicit sample values
        if sample_values:
            return random.choice(sample_values)

        field_lower = field_name.lower()

        # Priority 2: format hint
        if format_hint:
            if "LAST" in format_hint.upper() and "FIRST" in format_hint.upper():
                return f"{self.faker.last_name().upper()},{self.faker.first_name().upper()}"

        # Priority 3: field name heuristics
        return self._generate_by_field_name(
            field_name, field_lower, data_type, format_hint
        )

    def _generate_by_field_name(
        self,
        field_name: str,
        field_lower: str,
        data_type: str,
        format_hint: Optional[str],
    ) -> Any:
        """Generate value based on field name pattern matching."""

        # --- Name fields ---
        if any(kw in field_lower for kw in ("member", "name")):
            if "first" in field_lower:
                return self.faker.first_name().upper()
            elif "last" in field_lower:
                return self.faker.last_name().upper()
            else:
                return f"{self.faker.last_name().upper()},{self.faker.first_name().upper()}"

        # --- Date fields ---
        if "dob" in field_lower or "birth" in field_lower:
            years_ago = random.randint(18, 90)
            dob = datetime.now() - timedelta(days=years_ago * 365)
            return dob.strftime("%Y-%m-%d")

        if any(
            kw in field_lower for kw in ("date", "eff_date", "term_date", "signature")
        ):
            return self.faker.date_between(start_date="-2y", end_date="today").strftime(
                "%Y-%m-%d"
            )

        # --- Address fields ---
        if "address" in field_lower or "addr" in field_lower:
            return self.faker.street_address().upper()

        if field_lower in ("city",):
            return self.faker.city().upper()

        if field_lower in ("state",):
            return self.faker.state_abbr()

        if "zip" in field_lower or "postal" in field_lower:
            return self.faker.postcode()[:5]

        # --- Phone fields ---
        if "phone" in field_lower or "fax" in field_lower:
            area = str(random.randint(200, 999))
            prefix = str(random.randint(200, 999))
            line = str(random.randint(1000, 9999))
            return f"{area}-{prefix}-{line}"

        # --- Email ---
        if "email" in field_lower:
            return self.faker.email()

        # --- ID fields ---
        if "id" in field_lower or "ssn" in field_lower:
            return str(random.randint(100000, 9999999))

        # --- Status / indicator fields ---
        if "status" in field_lower or "indicator" in field_lower:
            return random.choice(["Active", "Inactive"])

        if "flag" in field_lower:
            return random.choice(["Y", "N"])

        # --- Numeric / amount fields ---
        if any(
            kw in field_lower for kw in ("amount", "premium", "price", "cost", "total")
        ):
            return f"{random.uniform(10, 1000):.2f}"

        if "count" in field_lower or "quantity" in field_lower:
            return str(random.randint(1, 100))

        # --- Type-based fallback ---
        if data_type == "date":
            return self.faker.date_between(start_date="-5y", end_date="today").strftime(
                "%Y-%m-%d"
            )

        if data_type in ("number", "integer", "int"):
            return str(random.randint(10000, 99999))

        if data_type == "float":
            return f"{random.uniform(1, 1000):.2f}"

        # --- Last resort ---
        return self.faker.word().upper()


# =============================================================================
# Source Data Generator
# =============================================================================


class SourceDataGenerator:
    """Generates source test records from rule_config with coverage guarantees."""

    def __init__(self, seed: int = 12345):
        self.field_gen = FieldGenerator(seed)

    def generate_records(
        self,
        source_fields: List[str],
        record_count: int,
        field_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
        conditional_coverage: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate source records ensuring conditional coverage.

        Args:
            source_fields: List of source field names
            record_count: Number of records to generate
            field_metadata: Per-field metadata {field: {data_type, format_hint, sample_values}}
            conditional_coverage: {field: {value: mapped_value}} or {field: [values]}

        Returns:
            List of record dicts
        """
        field_metadata = field_metadata or {}
        conditional_coverage = conditional_coverage or {}

        # Parse conditional coverage into field -> [required_values]
        cond_fields: Dict[str, List[str]] = {}
        for field, spec in conditional_coverage.items():
            if isinstance(spec, dict):
                cond_fields[field] = list(spec.keys())
            elif isinstance(spec, list):
                cond_fields[field] = spec
            else:
                cond_fields[field] = [str(spec)]

        # Auto-adjust record count for coverage
        total_required = sum(len(vals) for vals in cond_fields.values())
        if record_count < total_required:
            print(
                f"WARNING: record_count ({record_count}) < required for coverage ({total_required}). "
                f"Auto-adjusting to {total_required}.",
                file=sys.stderr,
            )
            record_count = total_required

        records = []
        for i in range(record_count):
            record = {}
            for field in source_fields:
                # Conditional coverage: round-robin through required values
                if field in cond_fields:
                    required = cond_fields[field]
                    if i < len(required):
                        record[field] = required[i]
                    else:
                        record[field] = random.choice(required)
                else:
                    meta = field_metadata.get(field, {})
                    record[field] = self.field_gen.generate(
                        field_name=field,
                        data_type=meta.get("data_type", "string"),
                        format_hint=meta.get("format_hint"),
                        sample_values=meta.get("sample_values"),
                    )
            records.append(record)

        return records


# =============================================================================
# File Writers
# =============================================================================


def write_records(
    records: List[Dict[str, Any]],
    fields: List[str],
    output_path: str,
    source_format: str = "pipe",
) -> None:
    """Write records to file in the specified format."""

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        if source_format == "pipe":
            # Pipe-delimited, no quoting
            f.write("|".join(fields) + "\n")
            for rec in records:
                vals = [str(rec.get(field, "")) for field in fields]
                f.write("|".join(vals) + "\n")

        elif source_format == "csv_quoted":
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(fields)
            for rec in records:
                row = [str(rec.get(field, "")) for field in fields]
                writer.writerow(row)

        else:  # comma (default CSV)
            writer = csv.writer(f)
            writer.writerow(fields)
            for rec in records:
                row = [str(rec.get(field, "")) for field in fields]
                writer.writerow(row)

    print(
        f"Generated {len(records)} records -> {output_path} (format: {source_format})"
    )


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="ETL Test Data Generator")
    parser.add_argument("--config", required=True, help="Path to rule_config.json")
    parser.add_argument(
        "--count", type=int, help="Override record count (default: from config or 10)"
    )
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument(
        "--seed", type=int, default=12345, help="Random seed for reproducibility"
    )
    args = parser.parse_args()

    # Read config
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    source_fields = config.get("source_fields", [])
    if not source_fields:
        print("ERROR: rule_config.json must have 'source_fields'", file=sys.stderr)
        sys.exit(1)

    record_count = args.count or config.get("record_count", 10)
    source_format = config.get("source_format", "pipe")

    # Build field_metadata lookup
    field_metadata = {}
    for meta in config.get("field_metadata", []):
        name = meta.get("field_name", "")
        if name:
            field_metadata[name] = {
                "data_type": meta.get("data_type", "string"),
                "format_hint": meta.get("format_hint"),
                "sample_values": meta.get("sample_values"),
            }

    conditional_coverage = config.get("conditional_coverage", {})

    # Generate
    gen = SourceDataGenerator(seed=args.seed)
    records = gen.generate_records(
        source_fields=source_fields,
        record_count=record_count,
        field_metadata=field_metadata,
        conditional_coverage=conditional_coverage,
    )

    # Write
    write_records(records, source_fields, args.output, source_format)


if __name__ == "__main__":
    main()
