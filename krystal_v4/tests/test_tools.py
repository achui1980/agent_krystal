"""
Unit tests for tools.
"""

import pytest
import tempfile
import os
import json
from krystal_v4.tools.format_detector_tool import FormatDetectorTool
from krystal_v4.tools.data_generator_tool import DataGeneratorTool
from krystal_v4.tools.transformation_executor_tool import TransformationExecutorTool


def test_format_detector_csv_quoted():
    """Test format detector identifies CSV with quotes"""
    tool = FormatDetectorTool()

    # Create temp file with CSV quoted format
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        f.write('"Name","Age","City"\n')
        f.write('"John","30","NYC"\n')
        temp_file = f.name

    try:
        result = tool._run(temp_file)
        assert "csv_quoted" in result or result == "csv_quoted"
    finally:
        os.unlink(temp_file)


def test_format_detector_pipe():
    """Test format detector identifies pipe-delimited"""
    tool = FormatDetectorTool()

    # Create temp file with pipe format
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        f.write("Name|Age|City\n")
        f.write("John|30|NYC\n")
        temp_file = f.name

    try:
        result = tool._run(temp_file)
        assert result == "pipe"
    finally:
        os.unlink(temp_file)


def test_data_generator_member_field():
    """Test data generator creates member name in LAST,FIRST format"""
    tool = DataGeneratorTool()

    values = tool.generate_values(
        field_name="Member", format_hint="LAST,FIRST", count=5
    )

    assert len(values) == 5
    for value in values:
        assert "," in value  # Should have comma separator


def test_data_generator_dob_field():
    """Test data generator creates DOB in date format"""
    tool = DataGeneratorTool()

    values = tool.generate_values(field_name="DOB", field_type="date", count=5)

    assert len(values) == 5
    for value in values:
        assert "-" in value  # Should be YYYY-MM-DD format


def test_data_generator_product_field():
    """Test data generator creates product codes"""
    tool = DataGeneratorTool()

    values = tool.generate_values(field_name="Product", count=10)

    assert len(values) == 10
    valid_products = ["PDP", "LPPO", "HUM", "HAP", "HV"]
    for value in values:
        assert value in valid_products


def test_transformation_executor_direct():
    """Test transformation executor with direct mapping"""
    tool = TransformationExecutorTool()

    source_record = {"DOB": "1960-01-15", "Name": "John"}
    result = tool.apply_transformation(source_record, "direct", {"source_field": "DOB"})

    assert result == "1960-01-15"


def test_transformation_executor_fixed():
    """Test transformation executor with fixed value"""
    tool = TransformationExecutorTool()

    source_record = {"any": "value"}
    result = tool.apply_transformation(source_record, "fixed", {"value": "66,175,206"})

    assert result == "66,175,206"


def test_transformation_executor_name_parser():
    """Test transformation executor with name parser"""
    tool = TransformationExecutorTool()

    source_record = {"Member": "MOUSE,MICKEY"}

    # Extract first name
    result = tool.apply_transformation(
        source_record, "name_parser", {"source_field": "Member", "part": "first"}
    )
    assert result == "MICKEY"

    # Extract last name
    result = tool.apply_transformation(
        source_record, "name_parser", {"source_field": "Member", "part": "last"}
    )
    assert result == "MOUSE"


def test_transformation_executor_multiple():
    """Test applying multiple transformations"""
    tool = TransformationExecutorTool()

    source_record = {"Member": "MOUSE,MICKEY", "DOB": "1960-01-15", "Product": "PDP"}

    rules = [
        {
            "target_field": "FIRST_NAME",
            "transformation_type": "name_parser",
            "config": {"source_field": "Member", "part": "first"},
        },
        {
            "target_field": "LAST_NAME",
            "transformation_type": "name_parser",
            "config": {"source_field": "Member", "part": "last"},
        },
        {
            "target_field": "DATE_OF_BIRTH",
            "transformation_type": "direct",
            "config": {"source_field": "DOB"},
        },
        {
            "target_field": "CARRIER_FAMILY_ID",
            "transformation_type": "fixed",
            "config": {"value": "66,175,206"},
        },
    ]

    result = tool.apply_multiple_transformations(source_record, rules)

    assert result["FIRST_NAME"] == "MICKEY"
    assert result["LAST_NAME"] == "MOUSE"
    assert result["DATE_OF_BIRTH"] == "1960-01-15"
    assert result["CARRIER_FAMILY_ID"] == "66,175,206"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
