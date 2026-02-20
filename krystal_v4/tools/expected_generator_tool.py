"""
Expected output generator tool for Krystal V4.
Generates the expected output file in one deterministic call.
Reads rule_config.json, generated_source.txt, applies all transformations,
fills fixed values, and writes generated_expected.txt.
"""

import csv
import json
import io
from datetime import datetime
from typing import Dict, Any, List
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from krystal_v4.transformers.transformer_registry import TransformerRegistry


class ExpectedGeneratorInput(BaseModel):
    """Input schema for ExpectedGeneratorTool"""

    case_name: str = Field(description="Test case name/identifier (e.g., 'humanaS10')")


class ExpectedGeneratorTool(BaseTool):
    name: str = "Expected Output Generator"
    description: str = (
        "Generates the complete expected output file in ONE call. "
        "Reads rule_config.json and generated_source.txt, applies ALL transformations, "
        "fills fixed values, and writes generated_expected.txt. "
        "Input: case_name (string). Returns success or error message."
    )
    args_schema: type[BaseModel] = ExpectedGeneratorInput

    def _run(self, case_name: str) -> str:
        """
        Generate expected output file.

        Args:
            case_name: Test case identifier

        Returns:
            Success or error message with summary
        """
        try:
            # Step 1: Read rule_config.json
            config_path = f"output/{case_name}/rule_config.json"
            with open(config_path, "r", encoding="utf-8") as f:
                rule_config = json.load(f)

            target_fields = rule_config["target_fields"]
            fixed_values = rule_config.get("fixed_values", {})
            transformation_rules = rule_config.get("transformation_rules", [])
            source_format = rule_config.get("source_format", "csv_quoted")
            output_metadata = rule_config.get("output_metadata", {})

            # Step 2: Read generated_source.txt
            source_path = f"output/{case_name}/generated_source.txt"
            source_records = self._read_source(source_path, source_format)

            if not source_records:
                return f"ERROR: No records found in {source_path}"

            # Step 3: Process each record
            output_rows = []
            errors = []

            for i, source_record in enumerate(source_records):
                try:
                    output_record = self._process_record(
                        source_record, transformation_rules, fixed_values, target_fields
                    )
                    output_rows.append(output_record)
                except Exception as e:
                    errors.append(f"Record {i + 1}: {str(e)}")
                    # Still add partial record
                    output_record = {field: "" for field in target_fields}
                    output_rows.append(output_record)

            # Step 4: Build and write output
            output_path = f"output/{case_name}/generated_expected.txt"
            self._write_output(output_path, target_fields, output_rows, output_metadata, case_name)

            # Step 5: Return summary
            summary = (
                f"Successfully generated expected output: {output_path}\n"
                f"- Records processed: {len(output_rows)}\n"
                f"- Target fields: {len(target_fields)}\n"
                f"- Transformation rules applied: {len(transformation_rules)}\n"
                f"- Fixed values applied: {len(fixed_values)}"
            )
            if errors:
                summary += f"\n- Warnings: {len(errors)} records had errors:\n"
                for err in errors[:5]:
                    summary += f"  - {err}\n"

            return summary

        except FileNotFoundError as e:
            return f"ERROR: File not found: {str(e)}"
        except json.JSONDecodeError as e:
            return f"ERROR: Invalid JSON in rule_config.json: {str(e)}"
        except Exception as e:
            return f"ERROR generating expected output: {str(e)}"

    def _read_source(self, source_path: str, source_format: str) -> List[Dict[str, str]]:
        """Read source file and return list of record dicts."""
        records = []
        with open(source_path, "r", encoding="utf-8") as f:
            if source_format == "pipe":
                reader = csv.DictReader(f, delimiter="|")
            else:
                # csv_quoted and comma both use comma delimiter
                reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))
        return records

    def _process_record(
        self,
        source_record: Dict[str, str],
        transformation_rules: List[Dict[str, Any]],
        fixed_values: Dict[str, str],
        target_fields: List[str],
    ) -> Dict[str, str]:
        """Process a single source record into an expected output record."""
        output_record = {}

        # Step A: Apply transformation rules
        for rule in transformation_rules:
            target_field = rule.get("target_field", "")
            source_field = rule.get("source_field", "")
            transformation_type = rule.get("transformation_type", "")
            config = dict(rule.get("config", {}))

            # CRITICAL: Inject source_field into config for transformers that need it
            if "source_field" not in config and source_field:
                config["source_field"] = source_field

            try:
                transformer = TransformerRegistry.get_transformer(transformation_type, config)
                value = transformer.transform(source_record)
                output_record[target_field] = str(value) if value is not None else ""
            except Exception as e:
                output_record[target_field] = ""

        # Step B: Apply fixed values
        for field, value in fixed_values.items():
            output_record[field] = value

        # Step C: Fill remaining target fields with empty string
        for field in target_fields:
            if field not in output_record:
                output_record[field] = ""

        return output_record

    def _write_output(
        self,
        output_path: str,
        target_fields: List[str],
        output_rows: List[Dict[str, str]],
        output_metadata: Dict[str, str],
        case_name: str,
    ) -> None:
        """Write pipe-delimited expected output file with metadata header."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(output_path, "w", encoding="utf-8") as f:
            # Write metadata header
            action_id = output_metadata.get("ACTION_ID", f"{case_name}-cs-data-integration")
            service_map_id = output_metadata.get("SERVICE_MAP_ID", "10003358")
            source_token = output_metadata.get("SOURCE_TOKEN", f"generated_{timestamp}")

            f.write(f"ACTION_ID:{action_id}\n")
            f.write(f"SERVICE_MAP_ID:{service_map_id}\n")
            f.write(f"SOURCE_TOKEN:{source_token}\n")
            f.write("\n")

            # Write header row
            f.write("|".join(target_fields) + "\n")

            # Write data rows
            for row in output_rows:
                values = [row.get(field, "") for field in target_fields]
                f.write("|".join(values) + "\n")
