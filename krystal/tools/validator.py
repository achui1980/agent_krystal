"""
Data Validation Tools
"""

import csv
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class CompareCSVInput(BaseModel):
    """Input for CSV comparison"""

    expected_path: str = Field(description="Path to expected CSV file")
    actual_path: str = Field(description="Path to actual CSV file")
    key_column: str = Field(
        default="", description="Column to use as unique key for matching rows"
    )
    rules: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Validation rules"
    )


class DataValidatorTool(BaseTool):
    """Tool for validating and comparing data"""

    name: str = "data_validator"
    description: str = """
    Validate data by comparing expected vs actual results.
    Supports CSV file comparison with flexible rules.
    """
    args_schema: type[BaseModel] = CompareCSVInput

    def _run(
        self,
        expected_path: str,
        actual_path: str,
        key_column: str = "",
        rules: List[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Compare two CSV files and validate

        Args:
            expected_path: Path to expected data CSV
            actual_path: Path to actual result CSV
            key_column: Column name to use as unique identifier
            rules: List of validation rules

        Returns:
            Validation result with details
        """
        try:
            # Load CSV files
            expected_data = self._load_csv(expected_path)
            actual_data = self._load_csv(actual_path)

            if not expected_data:
                return {
                    "success": False,
                    "passed": False,
                    "message": f"Could not load expected data from {expected_path}",
                    "errors": ["Failed to load expected CSV"],
                }

            if not actual_data:
                return {
                    "success": False,
                    "passed": False,
                    "message": f"Could not load actual data from {actual_path}",
                    "errors": ["Failed to load actual CSV"],
                }

            # Perform validation
            validation_result = self._validate_data(
                expected_data, actual_data, key_column, rules or []
            )

            return validation_result

        except Exception as e:
            return {
                "success": False,
                "passed": False,
                "message": f"Validation failed: {str(e)}",
                "errors": [str(e)],
            }

    def _load_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """Load CSV file into list of dictionaries"""
        path = Path(file_path)
        if not path.exists():
            return []

        data = []
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(dict(row))
        return data

    def _validate_data(
        self,
        expected: List[Dict],
        actual: List[Dict],
        key_column: str,
        rules: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Validate actual data against expected data"""
        errors = []
        warnings = []
        matched_rows = 0

        # Build lookup dict if key_column specified
        if key_column:
            expected_dict = {
                str(row.get(key_column)): row for row in expected if key_column in row
            }
            actual_dict = {
                str(row.get(key_column)): row for row in actual if key_column in row
            }

            # Check for missing rows
            for key in expected_dict:
                if key not in actual_dict:
                    errors.append(f"Missing row with {key_column}={key}")

            # Check for extra rows
            for key in actual_dict:
                if key not in expected_dict:
                    warnings.append(f"Extra row with {key_column}={key}")

            # Compare matching rows
            for key in expected_dict:
                if key in actual_dict:
                    matched_rows += 1
                    exp_row = expected_dict[key]
                    act_row = actual_dict[key]

                    # Check all columns
                    for col in exp_row:
                        if col not in act_row:
                            errors.append(f"Row {key}: Missing column '{col}'")
                        elif exp_row[col] != act_row[col]:
                            # Apply custom rules if specified
                            rule = self._find_rule(rules, col)
                            if rule:
                                error = self._apply_rule(rule, exp_row, act_row, key)
                                if error:
                                    errors.append(error)
                            else:
                                errors.append(
                                    f"Row {key}: Column '{col}' mismatch: expected '{exp_row[col]}', got '{act_row[col]}'"
                                )
        else:
            # Simple row-by-row comparison without key
            if len(expected) != len(actual):
                errors.append(
                    f"Row count mismatch: expected {len(expected)}, got {len(actual)}"
                )

            min_rows = min(len(expected), len(actual))
            for i in range(min_rows):
                matched_rows += 1
                exp_row = expected[i]
                act_row = actual[i]

                for col in exp_row:
                    if col not in act_row:
                        errors.append(f"Row {i}: Missing column '{col}'")
                    elif exp_row[col] != act_row[col]:
                        errors.append(
                            f"Row {i}: Column '{col}' mismatch: expected '{exp_row[col]}', got '{act_row[col]}'"
                        )

        passed = len(errors) == 0

        return {
            "success": True,
            "passed": passed,
            "message": "Validation passed"
            if passed
            else f"Validation failed with {len(errors)} errors",
            "errors": errors,
            "warnings": warnings,
            "expected_rows": len(expected),
            "actual_rows": len(actual),
            "matched_rows": matched_rows,
        }

    def _find_rule(
        self, rules: List[Dict[str, Any]], column: str
    ) -> Optional[Dict[str, Any]]:
        """Find validation rule for a column"""
        for rule in rules:
            if rule.get("field") == column:
                return rule
        return None

    def _apply_rule(
        self, rule: Dict[str, Any], expected_row: Dict, actual_row: Dict, key: str
    ) -> Optional[str]:
        """Apply a validation rule and return error message if failed"""
        rule_type = rule.get("rule", "")
        field = rule.get("field", "")

        if rule_type == "equals":
            ref_field = rule.get("reference_field")
            if ref_field and ref_field in actual_row:
                if actual_row[field] != actual_row[ref_field]:
                    return f"Row {key}: Field '{field}' should equal '{ref_field}': '{actual_row[field]}' != '{actual_row[ref_field]}'"

        elif rule_type == "not_empty":
            if not actual_row.get(field):
                return f"Row {key}: Field '{field}' should not be empty"

        elif rule_type == "range":
            try:
                value = float(actual_row.get(field, 0))
                min_val = rule.get("min")
                max_val = rule.get("max")
                if min_val is not None and value < float(min_val):
                    return f"Row {key}: Field '{field}' value {value} is below minimum {min_val}"
                if max_val is not None and value > float(max_val):
                    return f"Row {key}: Field '{field}' value {value} is above maximum {max_val}"
            except (ValueError, TypeError):
                return f"Row {key}: Field '{field}' is not a valid number"

        return None


class ValidateFileInput(BaseModel):
    """Input for file validation"""

    file_path: str = Field(description="Path to file to validate")
    min_size: Optional[int] = Field(
        default=None, description="Minimum file size in bytes"
    )
    max_size: Optional[int] = Field(
        default=None, description="Maximum file size in bytes"
    )


class FileValidatorTool(BaseTool):
    """Tool for validating file existence and properties"""

    name: str = "file_validator"
    description: str = "Validate file existence, size, and other properties"
    args_schema: type[BaseModel] = ValidateFileInput

    def _run(
        self, file_path: str, min_size: int = None, max_size: int = None
    ) -> Dict[str, Any]:
        """
        Validate file

        Args:
            file_path: Path to file
            min_size: Minimum size in bytes
            max_size: Maximum size in bytes

        Returns:
            Validation result
        """
        path = Path(file_path)

        if not path.exists():
            return {
                "success": False,
                "exists": False,
                "message": f"File does not exist: {file_path}",
            }

        size = path.stat().st_size

        # Check size constraints
        if min_size is not None and size < min_size:
            return {
                "success": False,
                "exists": True,
                "size": size,
                "message": f"File size {size} bytes is below minimum {min_size} bytes",
            }

        if max_size is not None and size > max_size:
            return {
                "success": False,
                "exists": True,
                "size": size,
                "message": f"File size {size} bytes is above maximum {max_size} bytes",
            }

        return {
            "success": True,
            "exists": True,
            "size": size,
            "message": f"File validated successfully: {file_path} ({size} bytes)",
        }
