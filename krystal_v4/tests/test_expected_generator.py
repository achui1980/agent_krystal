"""
Tests for expected generator tool and related new functionality.
Covers:
- ExpectedGeneratorTool end-to-end
- Transformer edge cases (whitespace, empty values)
- TransformationExecutorTool batch mode
- DataGeneratorTool.generate_with_coverage
- pre_parse_rules
"""

import csv
import json
import os
import tempfile
import pytest

from krystal_v4.tools.expected_generator_tool import ExpectedGeneratorTool
from krystal_v4.tools.transformation_executor_tool import TransformationExecutorTool
from krystal_v4.tools.data_generator_tool import DataGeneratorTool
from krystal_v4.transformers.conditional_transformer import ConditionalTransformer
from krystal_v4.transformers.name_parser_transformer import NameParserTransformer
from krystal_v4.transformers.split_transformer import SplitTransformer


# ─── Transformer edge cases ────────────────────────────────────────

class TestConditionalTransformerWhitespace:
    """Test whitespace normalization in ConditionalTransformer."""

    def test_mapping_keys_with_whitespace(self):
        config = {
            "source_field": "Product",
            "mappings": {" HUM ": "MS", "PDP": "MD"},
            "default": "UNKNOWN",
        }
        t = ConditionalTransformer(config)
        assert t.transform({"Product": "HUM"}) == "MS"

    def test_source_value_with_whitespace(self):
        config = {
            "source_field": "Product",
            "mappings": {"HUM": "MS", "PDP": "MD"},
            "default": "UNKNOWN",
        }
        t = ConditionalTransformer(config)
        assert t.transform({"Product": " HUM "}) == "MS"

    def test_case_insensitive_fallback(self):
        config = {
            "source_field": "Product",
            "mappings": {"HUM": "MS"},
            "default": "UNKNOWN",
        }
        t = ConditionalTransformer(config)
        assert t.transform({"Product": "hum"}) == "MS"

    def test_default_when_no_match(self):
        config = {
            "source_field": "Product",
            "mappings": {"HUM": "MS"},
            "default": "MA/MAPD",
        }
        t = ConditionalTransformer(config)
        assert t.transform({"Product": "LPPO"}) == "MA/MAPD"

    def test_none_source_returns_default(self):
        config = {
            "source_field": "Product",
            "mappings": {"HUM": "MS"},
            "default": "FALLBACK",
        }
        t = ConditionalTransformer(config)
        assert t.transform({"other": "value"}) == "FALLBACK"


class TestNameParserWhitespace:
    """Test whitespace handling in NameParserTransformer."""

    def test_whitespace_around_name(self):
        config = {"source_field": "Member", "part": "first"}
        t = NameParserTransformer(config)
        assert t.transform({"Member": "  MOUSE , MICKEY  "}) == "MICKEY"

    def test_whitespace_last_name(self):
        config = {"source_field": "Member", "part": "last"}
        t = NameParserTransformer(config)
        assert t.transform({"Member": "  MOUSE , MICKEY  "}) == "MOUSE"

    def test_empty_string_returns_none(self):
        config = {"source_field": "Member", "part": "first"}
        t = NameParserTransformer(config)
        assert t.transform({"Member": "   "}) is None

    def test_none_returns_none(self):
        config = {"source_field": "Member", "part": "first"}
        t = NameParserTransformer(config)
        assert t.transform({"Member": None}) is None


class TestSplitTransformerEmpty:
    """Test empty/None handling in SplitTransformer."""

    def test_none_returns_empty(self):
        config = {"source_field": "Plan_Name", "delimiter": "-", "index": 0}
        t = SplitTransformer(config)
        assert t.transform({"Plan_Name": None}) == ""

    def test_empty_string_returns_empty(self):
        config = {"source_field": "Plan_Name", "delimiter": "-", "index": 0}
        t = SplitTransformer(config)
        assert t.transform({"Plan_Name": ""}) == ""

    def test_whitespace_only_returns_empty(self):
        config = {"source_field": "Plan_Name", "delimiter": "-", "index": 0}
        t = SplitTransformer(config)
        assert t.transform({"Plan_Name": "   "}) == ""

    def test_missing_field_returns_empty(self):
        config = {"source_field": "Plan_Name", "delimiter": "-", "index": 0}
        t = SplitTransformer(config)
        assert t.transform({"other": "value"}) == ""

    def test_index_out_of_bounds_returns_empty(self):
        config = {"source_field": "Plan_Name", "delimiter": "-", "index": 5}
        t = SplitTransformer(config)
        assert t.transform({"Plan_Name": "S5884-197"}) == ""


# ─── TransformationExecutorTool batch mode ──────────────────────────

class TestTransformationExecutorBatchMode:
    """Test the new batch mode of TransformationExecutorTool."""

    def test_batch_mode_returns_json(self):
        tool = TransformationExecutorTool()
        record = json.dumps({"Member": "MOUSE,MICKEY", "DOB": "1960-01-15"})
        rules = json.dumps([
            {"target_field": "FIRST_NAME", "transformation_type": "name_parser",
             "config": {"source_field": "Member", "part": "first"}},
            {"target_field": "LAST_NAME", "transformation_type": "name_parser",
             "config": {"source_field": "Member", "part": "last"}},
            {"target_field": "BIRTH_DATE", "transformation_type": "direct",
             "config": {"source_field": "DOB"}},
        ])

        result = tool._run(source_record=record, batch_rules=rules)
        parsed = json.loads(result)

        assert parsed["FIRST_NAME"] == "MICKEY"
        assert parsed["LAST_NAME"] == "MOUSE"
        assert parsed["BIRTH_DATE"] == "1960-01-15"

    def test_batch_mode_error_handling(self):
        """Batch mode should record error per field, not fail entirely."""
        tool = TransformationExecutorTool()
        record = json.dumps({"Member": "MOUSE,MICKEY"})
        rules = json.dumps([
            {"target_field": "FIRST_NAME", "transformation_type": "name_parser",
             "config": {"source_field": "Member", "part": "first"}},
            {"target_field": "BAD_FIELD", "transformation_type": "invalid_type",
             "config": {}},
        ])

        result = tool._run(source_record=record, batch_rules=rules)
        parsed = json.loads(result)

        assert parsed["FIRST_NAME"] == "MICKEY"
        assert "ERROR" in parsed["BAD_FIELD"]

    def test_single_mode_still_works(self):
        tool = TransformationExecutorTool()
        record = json.dumps({"DOB": "1960-01-15"})
        config = json.dumps({"source_field": "DOB"})

        result = tool._run(source_record=record, transformation_type="direct", config=config)
        assert "1960-01-15" in result

    def test_no_args_returns_error(self):
        tool = TransformationExecutorTool()
        record = json.dumps({"DOB": "1960-01-15"})
        result = tool._run(source_record=record)
        assert "ERROR" in result


# ─── DataGeneratorTool.generate_with_coverage ───────────────────────

class TestDataGeneratorCoverage:
    """Test the new generate_with_coverage method."""

    def test_all_conditional_values_covered(self):
        tool = DataGeneratorTool()
        required_values = ["PDP", "HAP", "HUM", "HV", "RD", "LPPO"]
        records = tool.generate_with_coverage(
            source_fields=["Product", "Member", "DOB"],
            conditional_coverage={"Product": required_values},
            product_line_mapping={"PDP": "MD", "HAP": "MS", "HUM": "MS", "HV": "MS", "RD": "MS", "LPPO": "MA/MAPD"},
            field_metadata={},
            record_count=10,
        )

        assert len(records) == 10
        generated_products = [r["Product"] for r in records]
        for val in required_values:
            assert val in generated_products, f"{val} not covered in generated records"

    def test_auto_adjusts_record_count(self):
        """If record_count < required values, should auto-adjust."""
        tool = DataGeneratorTool()
        required_values = ["PDP", "HAP", "HUM", "HV", "RD", "LPPO"]
        records = tool.generate_with_coverage(
            source_fields=["Product"],
            conditional_coverage={"Product": required_values},
            product_line_mapping={},
            field_metadata={},
            record_count=3,  # Less than 6 required values
        )

        assert len(records) >= len(required_values)
        generated_products = [r["Product"] for r in records]
        for val in required_values:
            assert val in generated_products

    def test_ms_products_empty_plan_name(self):
        """MS products should have empty Plan_Name."""
        tool = DataGeneratorTool()
        ms_mapping = {"HAP": "MS", "HUM": "MS", "PDP": "MD"}
        records = tool.generate_with_coverage(
            source_fields=["Product", "Plan_Name"],
            conditional_coverage={"Product": ["HAP", "HUM", "PDP"]},
            product_line_mapping=ms_mapping,
            field_metadata={},
            record_count=3,
        )

        for r in records:
            if ms_mapping.get(r["Product"]) == "MS":
                assert r["Plan_Name"] == "", f"MS product {r['Product']} should have empty Plan_Name"

    def test_non_ms_products_have_plan_name(self):
        """Non-MS products should get a Plan_Name."""
        tool = DataGeneratorTool()
        mapping = {"PDP": "MD", "LPPO": "MA/MAPD"}
        records = tool.generate_with_coverage(
            source_fields=["Product", "Plan_Name"],
            conditional_coverage={"Product": ["PDP", "LPPO"]},
            product_line_mapping=mapping,
            field_metadata={},
            record_count=2,
        )

        for r in records:
            assert r["Plan_Name"] != "", f"Non-MS product {r['Product']} should have Plan_Name"


# ─── ExpectedGeneratorTool end-to-end ───────────────────────────────

class TestExpectedGeneratorTool:
    """End-to-end test of the ExpectedGeneratorTool."""

    @pytest.fixture
    def setup_case(self, tmp_path):
        """Create a minimal test case directory with rule_config.json and generated_source.txt."""
        case_name = "test_case"
        case_dir = tmp_path / "output" / case_name
        case_dir.mkdir(parents=True)

        # rule_config.json
        rule_config = {
            "source_format": "csv_quoted",
            "source_fields": ["Member", "Product", "DOB", "Plan_Name"],
            "target_fields": [
                "FIRST_NAME", "LAST_NAME", "PRODUCT_LINE", "BIRTH_DATE",
                "CMS_CONTRACT_ID", "CMS_PLAN_ID", "CARRIER_FAMILY_ID",
            ],
            "fixed_values": {
                "CARRIER_FAMILY_ID": "66,175,206",
            },
            "transformation_rules": [
                {"target_field": "FIRST_NAME", "transformation_type": "name_parser",
                 "source_field": "Member", "config": {"source_field": "Member", "part": "first"}},
                {"target_field": "LAST_NAME", "transformation_type": "name_parser",
                 "source_field": "Member", "config": {"source_field": "Member", "part": "last"}},
                {"target_field": "PRODUCT_LINE", "transformation_type": "conditional_map",
                 "source_field": "Product",
                 "config": {"source_field": "Product", "mappings": {"PDP": "MD", "HAP": "MS"}, "default": "MA/MAPD"}},
                {"target_field": "BIRTH_DATE", "transformation_type": "direct",
                 "source_field": "DOB", "config": {"source_field": "DOB"}},
                {"target_field": "CMS_CONTRACT_ID", "transformation_type": "split_extract",
                 "source_field": "Plan_Name",
                 "config": {"source_field": "Plan_Name", "delimiter": "-", "index": 0}},
                {"target_field": "CMS_PLAN_ID", "transformation_type": "split_extract",
                 "source_field": "Plan_Name",
                 "config": {"source_field": "Plan_Name", "delimiter": "-", "index": 1}},
            ],
            "output_metadata": {
                "ACTION_ID": "test-case-integration",
                "SERVICE_MAP_ID": "10003358",
            },
        }
        with open(case_dir / "rule_config.json", "w") as f:
            json.dump(rule_config, f)

        # generated_source.txt (csv_quoted)
        with open(case_dir / "generated_source.txt", "w", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["Member", "Product", "DOB", "Plan_Name"])
            writer.writerow(["MOUSE,MICKEY", "PDP", "1960-01-15", "S5884-197"])
            writer.writerow(["DUCK,DONALD", "HAP", "1955-06-09", ""])

        return tmp_path, case_name

    def test_generates_expected_file(self, setup_case):
        tmp_path, case_name = setup_case
        tool = ExpectedGeneratorTool()

        # Change working dir so tool can find output/case_name
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = tool._run(case_name=case_name)
        finally:
            os.chdir(original_cwd)

        assert "Successfully generated" in result
        assert "Records processed: 2" in result

    def test_output_content_correct(self, setup_case):
        tmp_path, case_name = setup_case
        tool = ExpectedGeneratorTool()

        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            tool._run(case_name=case_name)
            output_path = tmp_path / "output" / case_name / "generated_expected.txt"
            content = output_path.read_text()
        finally:
            os.chdir(original_cwd)

        lines = content.strip().split("\n")

        # Check metadata header
        assert lines[0].startswith("ACTION_ID:")
        assert lines[1].startswith("SERVICE_MAP_ID:")
        assert lines[2].startswith("SOURCE_TOKEN:")
        assert lines[3] == ""  # blank line

        # Check header row
        header = lines[4].split("|")
        assert header == ["FIRST_NAME", "LAST_NAME", "PRODUCT_LINE", "BIRTH_DATE",
                          "CMS_CONTRACT_ID", "CMS_PLAN_ID", "CARRIER_FAMILY_ID"]

        # Check record 1: PDP with Plan_Name
        row1 = lines[5].split("|")
        assert row1[0] == "MICKEY"       # FIRST_NAME
        assert row1[1] == "MOUSE"        # LAST_NAME
        assert row1[2] == "MD"           # PRODUCT_LINE (PDP→MD)
        assert row1[3] == "1960-01-15"   # BIRTH_DATE
        assert row1[4] == "S5884"        # CMS_CONTRACT_ID
        assert row1[5] == "197"          # CMS_PLAN_ID
        assert row1[6] == "66,175,206"   # CARRIER_FAMILY_ID (fixed)

        # Check record 2: HAP (MS) with empty Plan_Name
        row2 = lines[6].split("|")
        assert row2[0] == "DONALD"       # FIRST_NAME
        assert row2[1] == "DUCK"         # LAST_NAME
        assert row2[2] == "MS"           # PRODUCT_LINE (HAP→MS)
        assert row2[3] == "1955-06-09"   # BIRTH_DATE
        assert row2[4] == ""             # CMS_CONTRACT_ID (empty Plan_Name)
        assert row2[5] == ""             # CMS_PLAN_ID (empty Plan_Name)
        assert row2[6] == "66,175,206"   # CARRIER_FAMILY_ID (fixed)

    def test_missing_source_file(self, tmp_path):
        case_name = "missing_case"
        case_dir = tmp_path / "output" / case_name
        case_dir.mkdir(parents=True)

        # Only create rule_config, no source file
        rule_config = {
            "source_format": "csv_quoted",
            "source_fields": ["Member"],
            "target_fields": ["FIRST_NAME"],
            "fixed_values": {},
            "transformation_rules": [],
            "output_metadata": {},
        }
        with open(case_dir / "rule_config.json", "w") as f:
            json.dump(rule_config, f)

        tool = ExpectedGeneratorTool()
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = tool._run(case_name=case_name)
        finally:
            os.chdir(original_cwd)

        assert "ERROR" in result

    def test_missing_config_file(self, tmp_path):
        tool = ExpectedGeneratorTool()
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = tool._run(case_name="nonexistent_case")
        finally:
            os.chdir(original_cwd)

        assert "ERROR" in result


# ─── pre_parse_rules ────────────────────────────────────────────────

class TestPreParseRules:
    """Test the pre_parse_rules function."""

    def test_basic_parsing(self, tmp_path):
        from krystal_v4.crew import pre_parse_rules

        rules_file = tmp_path / "rules.csv"
        with open(rules_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "CS_COLUMN_NAME", "CARRIER_COLUMN_NAME", "DEFAULT", "SPECIAL_RULES", "CSDS_FLAG"
            ])
            writer.writeheader()
            # Type A: has carrier column
            writer.writerow({
                "CS_COLUMN_NAME": "FIRST_NAME",
                "CARRIER_COLUMN_NAME": "Member",
                "DEFAULT": "",
                "SPECIAL_RULES": "last_name, first_name",
                "CSDS_FLAG": "1",
            })
            # Type B: fixed value
            writer.writerow({
                "CS_COLUMN_NAME": "IS_PAID",
                "CARRIER_COLUMN_NAME": "",
                "DEFAULT": "1",
                "SPECIAL_RULES": "",
                "CSDS_FLAG": "",
            })
            # Type A: direct mapping
            writer.writerow({
                "CS_COLUMN_NAME": "BIRTH_DATE",
                "CARRIER_COLUMN_NAME": "DOB",
                "DEFAULT": "",
                "SPECIAL_RULES": "",
                "CSDS_FLAG": "1",
            })

        result = pre_parse_rules(str(rules_file))

        assert result["target_fields"] == ["FIRST_NAME", "IS_PAID", "BIRTH_DATE"]
        assert result["source_fields"] == ["Member", "DOB"]
        assert result["fixed_values"] == {"IS_PAID": "1"}
        assert len(result["special_rules_rows"]) == 2
        assert result["source_format"] == "csv_quoted"  # default

    def test_deduplicates_source_fields(self, tmp_path):
        from krystal_v4.crew import pre_parse_rules

        rules_file = tmp_path / "rules.csv"
        with open(rules_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "CS_COLUMN_NAME", "CARRIER_COLUMN_NAME", "DEFAULT", "SPECIAL_RULES", "CSDS_FLAG"
            ])
            writer.writeheader()
            writer.writerow({
                "CS_COLUMN_NAME": "FIRST_NAME",
                "CARRIER_COLUMN_NAME": "Member",
                "DEFAULT": "",
                "SPECIAL_RULES": "first",
                "CSDS_FLAG": "",
            })
            writer.writerow({
                "CS_COLUMN_NAME": "LAST_NAME",
                "CARRIER_COLUMN_NAME": "Member",
                "DEFAULT": "",
                "SPECIAL_RULES": "last",
                "CSDS_FLAG": "",
            })

        result = pre_parse_rules(str(rules_file))

        # Member should appear only once
        assert result["source_fields"] == ["Member"]
        assert len(result["special_rules_rows"]) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
