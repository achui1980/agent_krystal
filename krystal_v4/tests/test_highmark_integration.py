"""
Integration test for Highmark case.
Tests the pipeline end-to-end with a hand-crafted rule_config that exercises
the new phone_parser transformer and direct mappings.
"""

import csv
import json
import os
import pytest

from krystal_v4.tools.expected_generator_tool import ExpectedGeneratorTool


class TestHighmarkIntegration:
    """End-to-end test mimicking the Highmark pipeline."""

    @pytest.fixture
    def setup_highmark(self, tmp_path):
        """Create a minimal Highmark-style test case."""
        case_name = "highmark_test"
        case_dir = tmp_path / "output" / case_name
        case_dir.mkdir(parents=True)

        # rule_config.json modeled after actual Highmark rules
        rule_config = {
            "source_format": "pipe",
            "source_fields": [
                "CONFIRMATION_NUMBER", "PLAN_TYPE", "SUBSCRIBER_ID",
                "COVERAGE_EFFECTIVE_DATE", "COVERAGE_END_DATE",
                "APPLICANT_ADDR_1", "APPLICANT_STATE", "APPLICANT_ZIP",
                "APPLICANT_FIRST_NAME", "APPLICANT_MIDDLE_INITIAL",
                "APPLICANT_LAST_NAME", "APPLICANT_EMAIL_ADDRESS",
                "HICN", "APPLICANT_PHONE",
                "CONTRACT_ID", "PLAN_ID", "SEGMENT_ID",
                "AGENT_WRITING_NUMBER",
            ],
            "target_fields": [
                "CARRIER_FAMILY_ID", "IS_PAID", "BUSINESS_LINE",
                "APPLICATION_ID", "MEMBER_NUMBER", "PRODUCT_LINE",
                "POLICY_NUMBER", "EFFECTIVE_START_DATE", "CANCELLATION_DATE",
                "ADDRESS_TYPE", "ADDRESS_LINE_1", "STATE", "ZIP_CODE",
                "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME",
                "EMAIL", "MEDICARE_ID",
                "PHONE_TYPE", "AREA_CODE", "PHONE_NUMBER",
                "CMS_CONTRACT_ID", "CMS_PLAN_ID", "CMS_SEGMENT_ID",
                "AGENT_ID",
            ],
            "fixed_values": {
                "CARRIER_FAMILY_ID": "23",
                "IS_PAID": "1",
                "BUSINESS_LINE": "2",
                "MEMBER_NUMBER": "1",
                "CATEGORY_CLASS_ID": "1",
                "ADDRESS_TYPE": "HOME",
                "PHONE_TYPE": "HOME",
            },
            "transformation_rules": [
                {
                    "target_field": "APPLICATION_ID",
                    "source_field": "CONFIRMATION_NUMBER",
                    "transformation_type": "direct",
                    "config": {"source_field": "CONFIRMATION_NUMBER"},
                },
                {
                    "target_field": "PRODUCT_LINE",
                    "source_field": "PLAN_TYPE",
                    "transformation_type": "conditional_map",
                    "config": {
                        "source_field": "PLAN_TYPE",
                        "mappings": {"MAPD": "MA"},
                        "default": "MA",
                    },
                },
                {
                    "target_field": "POLICY_NUMBER",
                    "source_field": "SUBSCRIBER_ID",
                    "transformation_type": "direct",
                    "config": {"source_field": "SUBSCRIBER_ID"},
                },
                {
                    "target_field": "EFFECTIVE_START_DATE",
                    "source_field": "COVERAGE_EFFECTIVE_DATE",
                    "transformation_type": "direct",
                    "config": {"source_field": "COVERAGE_EFFECTIVE_DATE"},
                },
                {
                    "target_field": "CANCELLATION_DATE",
                    "source_field": "COVERAGE_END_DATE",
                    "transformation_type": "direct",
                    "config": {"source_field": "COVERAGE_END_DATE"},
                },
                {
                    "target_field": "ADDRESS_LINE_1",
                    "source_field": "APPLICANT_ADDR_1",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_ADDR_1"},
                },
                {
                    "target_field": "STATE",
                    "source_field": "APPLICANT_STATE",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_STATE"},
                },
                {
                    "target_field": "ZIP_CODE",
                    "source_field": "APPLICANT_ZIP",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_ZIP"},
                },
                {
                    "target_field": "FIRST_NAME",
                    "source_field": "APPLICANT_FIRST_NAME",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_FIRST_NAME"},
                },
                {
                    "target_field": "MIDDLE_NAME",
                    "source_field": "APPLICANT_MIDDLE_INITIAL",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_MIDDLE_INITIAL"},
                },
                {
                    "target_field": "LAST_NAME",
                    "source_field": "APPLICANT_LAST_NAME",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_LAST_NAME"},
                },
                {
                    "target_field": "EMAIL",
                    "source_field": "APPLICANT_EMAIL_ADDRESS",
                    "transformation_type": "direct",
                    "config": {"source_field": "APPLICANT_EMAIL_ADDRESS"},
                },
                {
                    "target_field": "MEDICARE_ID",
                    "source_field": "HICN",
                    "transformation_type": "direct",
                    "config": {"source_field": "HICN"},
                },
                {
                    "target_field": "AREA_CODE",
                    "source_field": "APPLICANT_PHONE",
                    "transformation_type": "phone_parser",
                    "config": {"source_field": "APPLICANT_PHONE", "part": "area_code"},
                },
                {
                    "target_field": "PHONE_NUMBER",
                    "source_field": "APPLICANT_PHONE",
                    "transformation_type": "phone_parser",
                    "config": {
                        "source_field": "APPLICANT_PHONE",
                        "part": "phone_number",
                        "format": "nnn-nnnn",
                    },
                },
                {
                    "target_field": "CMS_CONTRACT_ID",
                    "source_field": "CONTRACT_ID",
                    "transformation_type": "direct",
                    "config": {"source_field": "CONTRACT_ID"},
                },
                {
                    "target_field": "CMS_PLAN_ID",
                    "source_field": "PLAN_ID",
                    "transformation_type": "direct",
                    "config": {"source_field": "PLAN_ID"},
                },
                {
                    "target_field": "CMS_SEGMENT_ID",
                    "source_field": "SEGMENT_ID",
                    "transformation_type": "direct",
                    "config": {"source_field": "SEGMENT_ID"},
                },
                {
                    "target_field": "AGENT_ID",
                    "source_field": "AGENT_WRITING_NUMBER",
                    "transformation_type": "direct",
                    "config": {"source_field": "AGENT_WRITING_NUMBER"},
                },
            ],
            "output_metadata": {
                "ACTION_ID": "highmark-cs-data-integration",
                "SERVICE_MAP_ID": "10003358",
            },
        }

        with open(case_dir / "rule_config.json", "w") as f:
            json.dump(rule_config, f)

        # generated_source.txt (pipe-delimited, mimicking Highmark source format)
        with open(case_dir / "generated_source.txt", "w") as f:
            fields = [
                "CONFIRMATION_NUMBER", "PLAN_TYPE", "SUBSCRIBER_ID",
                "COVERAGE_EFFECTIVE_DATE", "COVERAGE_END_DATE",
                "APPLICANT_ADDR_1", "APPLICANT_STATE", "APPLICANT_ZIP",
                "APPLICANT_FIRST_NAME", "APPLICANT_MIDDLE_INITIAL",
                "APPLICANT_LAST_NAME", "APPLICANT_EMAIL_ADDRESS",
                "HICN", "APPLICANT_PHONE",
                "CONTRACT_ID", "PLAN_ID", "SEGMENT_ID",
                "AGENT_WRITING_NUMBER",
            ]
            f.write("|".join(fields) + "\n")
            f.write("33470079|MAPD|71017309|01012025||123 CANDY ST|PA|16506|JOE||SMITH|joe@test.com|5MM1X77YX16|(555) 867-5309|H5932|001||17312824\n")
            f.write("33766665|MAPD|71019132|02012025||1319 NORTH ST|PA|19464|BOB||JONES|bob@test.com|4PY7P26KN48|412-555-1234|H5932|012||19107161\n")
            f.write("33893056|MAPD|71020275|03012025|12312025|1245 SOUTH RD|PA|19120|JANE||THOMAS||1M49WJ7GG10||H5932|012||19929113\n")

        return tmp_path, case_name

    def test_phone_fields_populated(self, setup_highmark):
        """AREA_CODE and PHONE_NUMBER should be correctly extracted from APPLICANT_PHONE."""
        tmp_path, case_name = setup_highmark
        tool = ExpectedGeneratorTool()

        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = tool._run(case_name=case_name)
        finally:
            os.chdir(original_cwd)

        assert "Successfully generated" in result
        assert "Records processed: 3" in result

        output_path = tmp_path / "output" / case_name / "generated_expected.txt"
        content = output_path.read_text()
        lines = content.strip().split("\n")

        # Skip metadata (3 lines + blank line)
        header = lines[4].split("|")
        area_code_idx = header.index("AREA_CODE")
        phone_number_idx = header.index("PHONE_NUMBER")

        # Record 1: (555) 867-5309
        row1 = lines[5].split("|")
        assert row1[area_code_idx] == "555"
        assert row1[phone_number_idx] == "867-5309"

        # Record 2: 412-555-1234
        row2 = lines[6].split("|")
        assert row2[area_code_idx] == "412"
        assert row2[phone_number_idx] == "555-1234"

        # Record 3: no phone
        row3 = lines[7].split("|")
        assert row3[area_code_idx] == ""
        assert row3[phone_number_idx] == ""

    def test_direct_fields_correct(self, setup_highmark):
        """Direct-mapped fields should be correctly copied."""
        tmp_path, case_name = setup_highmark
        tool = ExpectedGeneratorTool()

        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            tool._run(case_name=case_name)
        finally:
            os.chdir(original_cwd)

        output_path = tmp_path / "output" / case_name / "generated_expected.txt"
        content = output_path.read_text()
        lines = content.strip().split("\n")

        header = lines[4].split("|")
        first_name_idx = header.index("FIRST_NAME")
        last_name_idx = header.index("LAST_NAME")
        app_id_idx = header.index("APPLICATION_ID")
        product_idx = header.index("PRODUCT_LINE")
        contract_idx = header.index("CMS_CONTRACT_ID")

        row1 = lines[5].split("|")
        assert row1[first_name_idx] == "JOE"
        assert row1[last_name_idx] == "SMITH"
        assert row1[app_id_idx] == "33470079"
        assert row1[product_idx] == "MA"  # MAPD → MA
        assert row1[contract_idx] == "H5932"

    def test_fixed_values_applied(self, setup_highmark):
        """Fixed values should appear in every record."""
        tmp_path, case_name = setup_highmark
        tool = ExpectedGeneratorTool()

        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            tool._run(case_name=case_name)
        finally:
            os.chdir(original_cwd)

        output_path = tmp_path / "output" / case_name / "generated_expected.txt"
        content = output_path.read_text()
        lines = content.strip().split("\n")

        header = lines[4].split("|")
        family_id_idx = header.index("CARRIER_FAMILY_ID")
        is_paid_idx = header.index("IS_PAID")

        for i in range(5, 8):
            row = lines[i].split("|")
            assert row[family_id_idx] == "23"
            assert row[is_paid_idx] == "1"

    def test_no_transformation_errors_file(self, setup_highmark):
        """With valid config, no transformation_errors.json should be created."""
        tmp_path, case_name = setup_highmark
        tool = ExpectedGeneratorTool()

        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            tool._run(case_name=case_name)
        finally:
            os.chdir(original_cwd)

        error_file = tmp_path / "output" / case_name / "transformation_errors.json"
        assert not error_file.exists()


class TestHighmarkSourceFormatDetection:
    """Test that pre_parse_rules correctly falls back to source.txt."""

    def test_detects_source_txt_fallback(self, tmp_path):
        from krystal_v4.crew import pre_parse_rules

        case_dir = tmp_path / "case"
        case_dir.mkdir()

        # Create rules.csv
        rules_file = case_dir / "rules.csv"
        with open(rules_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "CS_COLUMN_NAME", "CARRIER_COLUMN_NAME", "DEFAULT", "SPECIAL_RULES", "CSDS_FLAG"
            ])
            writer.writeheader()
            writer.writerow({
                "CS_COLUMN_NAME": "FIRST_NAME",
                "CARRIER_COLUMN_NAME": "APPLICANT_FIRST_NAME",
                "DEFAULT": "",
                "SPECIAL_RULES": "",
                "CSDS_FLAG": "1",
            })

        # Create source.txt (pipe-delimited), NOT source.csv
        source_file = case_dir / "source.txt"
        source_file.write_text("APPLICANT_FIRST_NAME|APPLICANT_LAST_NAME\nJOE|SMITH\n")

        result = pre_parse_rules(str(rules_file), str(case_dir))
        assert result["source_format"] == "pipe"

    def test_prefers_source_csv_over_txt(self, tmp_path):
        from krystal_v4.crew import pre_parse_rules

        case_dir = tmp_path / "case"
        case_dir.mkdir()

        rules_file = case_dir / "rules.csv"
        with open(rules_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "CS_COLUMN_NAME", "CARRIER_COLUMN_NAME", "DEFAULT", "SPECIAL_RULES", "CSDS_FLAG"
            ])
            writer.writeheader()
            writer.writerow({
                "CS_COLUMN_NAME": "NAME",
                "CARRIER_COLUMN_NAME": "Member",
                "DEFAULT": "",
                "SPECIAL_RULES": "",
                "CSDS_FLAG": "1",
            })

        # Both source.csv and source.txt exist — should prefer source.csv
        csv_file = case_dir / "source.csv"
        csv_file.write_text('"Member","DOB"\n"MOUSE,MICKEY","1960-01-15"\n')

        txt_file = case_dir / "source.txt"
        txt_file.write_text("Member|DOB\nMOUSE,MICKEY|1960-01-15\n")

        result = pre_parse_rules(str(rules_file), str(case_dir))
        assert result["source_format"] == "csv_quoted"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
