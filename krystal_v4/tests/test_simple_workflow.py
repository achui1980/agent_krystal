"""
Simple test script for Krystal V4 components without full CrewAI execution.
Tests the transformers and tools directly.
"""

import sys
import csv
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from krystal_v4.tools.format_detector_tool import FormatDetectorTool
from krystal_v4.tools.csv_reader_tool import CSVReaderTool
from krystal_v4.tools.data_generator_tool import DataGeneratorTool
from krystal_v4.tools.file_writer_tool import FileWriterTool
from krystal_v4.tools.transformation_executor_tool import TransformationExecutorTool
from krystal_v4.transformers.transformer_registry import TransformerRegistry
from krystal_v4.utils.logger import logger
from krystal_v4.utils.file_helper import ensure_output_dir, generate_source_token


def test_simple_workflow():
    """Test a simple workflow without full CrewAI execution."""

    logger.info("=" * 60)
    logger.info("Starting Simple Krystal V4 Test")
    logger.info("=" * 60)

    # 1. Test format detection
    logger.info("\n[1] Testing Format Detection")
    format_detector = FormatDetectorTool()
    source_format = format_detector._run("case/humanaS10/source.csv")
    logger.info(f"Detected format: {source_format}")

    # 2. Read a few rules from CSV
    logger.info("\n[2] Reading Rules CSV")
    csv_reader = CSVReaderTool()

    # Read rules manually
    rules_data = []
    with open("case/rules.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rules_data.append(row)

    logger.info(f"Read {len(rules_data)} rules from CSV")

    # 3. Create sample transformation rules
    logger.info("\n[3] Creating Sample Transformation Rules")

    # Example: Extract some simple rules
    sample_rules = [
        {
            "target_field": "CARRIER_FAMILY_ID",
            "transformation_type": "fixed",
            "config": {"value": "66,175,206"},
        },
        {
            "target_field": "IS_PAID",
            "transformation_type": "fixed",
            "config": {"value": "1"},
        },
    ]

    logger.info(f"Created {len(sample_rules)} sample transformation rules")

    # 4. Test data generation
    logger.info("\n[4] Testing Data Generation")
    data_generator = DataGeneratorTool()

    # Generate sample data
    member_names = data_generator.generate_values(
        "Member", format_hint="LAST,FIRST", count=3
    )
    dobs = data_generator.generate_values("DOB", field_type="date", count=3)
    products = data_generator.generate_values("Product", count=3)

    logger.info(f"Generated member names: {member_names}")
    logger.info(f"Generated DOBs: {dobs}")
    logger.info(f"Generated products: {products}")

    # 5. Create source records
    logger.info("\n[5] Creating Source Records")
    source_records = []
    for i in range(3):
        source_records.append(
            {"Member": member_names[i], "DOB": dobs[i], "Product": products[i]}
        )

    logger.info(f"Created {len(source_records)} source records")
    for i, record in enumerate(source_records):
        logger.info(f"  Record {i + 1}: {record}")

    # 6. Test transformations
    logger.info("\n[6] Testing Transformations")
    transformation_executor = TransformationExecutorTool()

    # Apply sample transformations
    for record in source_records:
        result = transformation_executor.apply_multiple_transformations(
            record, sample_rules
        )
        logger.info(f"Transformed: {result}")

    # 7. Write output
    logger.info("\n[7] Writing Output Files")
    output_dir = ensure_output_dir("humanaS10_test")

    file_writer = FileWriterTool()

    # Write source file (CSV quoted)
    source_path = output_dir / "test_source.csv"
    result = file_writer.write_csv_quoted(
        str(source_path), source_records, ["Member", "DOB", "Product"]
    )
    logger.info(f"Source file: {result}")

    # Write expected file (pipe-delimited)
    expected_records = []
    for record in source_records:
        transformed = transformation_executor.apply_multiple_transformations(
            record, sample_rules
        )
        expected_records.append(transformed)

    expected_path = output_dir / "test_expected.txt"
    metadata = {
        "ACTION_ID": "humanaS10_test-cs-data-integration",
        "SERVICE_MAP_ID": "10003358",
        "SOURCE_TOKEN": generate_source_token(),
    }

    result = file_writer.write_pipe_delimited(
        str(expected_path), expected_records, ["CARRIER_FAMILY_ID", "IS_PAID"], metadata
    )
    logger.info(f"Expected file: {result}")

    logger.info("\n" + "=" * 60)
    logger.info("Simple Test Completed Successfully!")
    logger.info("=" * 60)
    logger.info(f"Output directory: {output_dir}")


if __name__ == "__main__":
    test_simple_workflow()
