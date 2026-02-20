"""
KrystalV4Crew - Main crew orchestration for ETL test data generation.
"""

import csv
import json
from pathlib import Path
from crewai import Crew, Process
from typing import Dict, Any, Optional, List
from krystal_v4.agents.rule_analyst_agent import create_rule_analyst_agent
from krystal_v4.agents.source_generator_agent import create_source_generator_agent
from krystal_v4.agents.expected_generator_agent import create_expected_generator_agent
from krystal_v4.tasks.task_definitions import (
    create_analyze_rules_task,
    create_generate_source_task,
    create_generate_expected_task,
)
from krystal_v4.tools.format_detector_tool import FormatDetectorTool
from krystal_v4.tools.csv_reader_tool import CSVReaderTool
from krystal_v4.tools.data_generator_tool import DataGeneratorTool
from krystal_v4.tools.file_writer_tool import FileWriterTool
from krystal_v4.tools.transformation_executor_tool import TransformationExecutorTool
from krystal_v4.tools.json_writer_tool import JSONWriterTool
from krystal_v4.tools.expected_generator_tool import ExpectedGeneratorTool
from krystal_v4.utils.logger import logger, log_step
from krystal_v4.utils.file_helper import ensure_output_dir
from datetime import datetime


def pre_parse_rules(rules_file: str, case_dir: str = None) -> Dict[str, Any]:
    """
    Pre-parse rules.csv to extract deterministic fields.
    Also detects source_format and extracts output_metadata from reference files.

    Returns dict with: target_fields, source_fields, fixed_values,
    special_rules_rows, source_format, output_metadata.
    """
    rows = []
    with open(rules_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    target_fields = []
    source_fields_set = []
    fixed_values = {}
    special_rules_rows = []

    for row in rows:
        cs_col = row.get("CS_COLUMN_NAME", "").strip()
        carrier_col = row.get("CARRIER_COLUMN_NAME", "").strip()
        default = row.get("DEFAULT", "").strip()
        special = row.get("SPECIAL_RULES", "").strip()
        csds_flag = row.get("CSDS_FLAG", "").strip()

        if cs_col:
            target_fields.append(cs_col)

        # Type A: has CARRIER_COLUMN_NAME
        if carrier_col:
            if carrier_col not in source_fields_set:
                source_fields_set.append(carrier_col)
            special_rules_rows.append({
                "CS_COLUMN_NAME": cs_col,
                "CARRIER_COLUMN_NAME": carrier_col,
                "SPECIAL_RULES": special,
                "CSDS_FLAG": csds_flag,
            })
        # Type B: no carrier col, has default
        elif default:
            fixed_values[cs_col] = default

    # Detect source_format from reference source file
    source_format = "csv_quoted"  # safe default for files with commas in fields
    if case_dir:
        ref_source = Path(case_dir) / "source.csv"
        if ref_source.exists():
            detector = FormatDetectorTool()
            source_format = detector._run(str(ref_source))
            if source_format.startswith("ERROR"):
                source_format = "csv_quoted"

    # Extract output_metadata from reference expected file
    output_metadata = {}
    if case_dir:
        ref_expected = Path(case_dir) / "expected.txt"
        if ref_expected.exists():
            with open(ref_expected, "r", encoding="utf-8-sig") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        break
                    if ":" in line:
                        key, _, value = line.partition(":")
                        output_metadata[key.strip()] = value.strip()

    return {
        "target_fields": target_fields,
        "source_fields": source_fields_set,
        "fixed_values": fixed_values,
        "special_rules_rows": special_rules_rows,
        "source_format": source_format,
        "output_metadata": output_metadata,
    }


class KrystalV4Crew:
    """
    Main crew class for Krystal V4 ETL test data generation.

    Orchestrates three agents in sequential pipeline:
    1. Rule Analyst - Parses rules and creates configuration
    2. Source Generator - Generates test source data
    3. Expected Generator - Applies transformations to create expected output
    """

    def __init__(
        self,
        rules_file: str,
        case_name: str,
        case_dir: str = None,
        record_count: int = 10,
        llm: Optional[Any] = None,
    ):
        """
        Initialize the crew.

        Args:
            rules_file: Path to rules.csv
            case_name: Test case identifier
            case_dir: Directory containing reference files (optional)
            record_count: Number of records to generate (default 10)
            llm: Optional language model instance
        """
        self.rules_file = rules_file
        self.case_name = case_name
        self.case_dir = case_dir or f"case/{case_name}"
        self.record_count = record_count
        self.llm = llm

        # Ensure output directory exists
        self.output_dir = ensure_output_dir(case_name)

        logger.info(f"Initializing Krystal V4 Crew for case: {case_name}")
        logger.info(f"Rules file: {rules_file}")
        logger.info(f"Reference directory: {self.case_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Record count: {record_count}")

    def run(self) -> Dict[str, Any]:
        """
        Execute the crew workflow.

        Returns:
            Dictionary with execution results and output file paths
        """
        logger.info("=" * 60)
        logger.info("Starting Krystal V4 ETL Test Data Generation")
        logger.info("=" * 60)

        # Pre-parse rules.csv for deterministic data
        log_step(logger, 0, 3, "Pre-parsing rules.csv")
        parsed = pre_parse_rules(self.rules_file, self.case_dir)
        logger.info(f"  target_fields: {len(parsed['target_fields'])} fields")
        logger.info(f"  source_fields: {parsed['source_fields']}")
        logger.info(f"  fixed_values: {list(parsed['fixed_values'].keys())}")
        logger.info(f"  special_rules rows: {len(parsed['special_rules_rows'])}")
        logger.info(f"  source_format: {parsed['source_format']}")
        logger.info(f"  output_metadata: {parsed['output_metadata']}")

        # Initialize tools
        format_detector = FormatDetectorTool()
        csv_reader = CSVReaderTool()
        data_generator = DataGeneratorTool()
        file_writer = FileWriterTool()
        transformation_executor = TransformationExecutorTool()
        json_writer = JSONWriterTool()
        expected_generator = ExpectedGeneratorTool()

        # Create agents
        log_step(logger, 1, 3, "Creating Agents")
        rule_analyst = create_rule_analyst_agent(llm=self.llm)
        source_generator = create_source_generator_agent(llm=self.llm)
        expected_gen_agent = create_expected_generator_agent(llm=self.llm)

        # Create tasks
        log_step(logger, 2, 3, "Creating Tasks")

        analyze_rules_task = create_analyze_rules_task(
            agent=rule_analyst,
            rules_file=self.rules_file,
            case_name=self.case_name,
            case_dir=self.case_dir,
            tools=[format_detector, csv_reader, json_writer],
            parsed_rules=parsed,
        )

        generate_source_task = create_generate_source_task(
            agent=source_generator,
            case_name=self.case_name,
            record_count=self.record_count,
            source_format=parsed["source_format"],
            tools=[data_generator, file_writer, json_writer],
        )

        generate_expected_task = create_generate_expected_task(
            agent=expected_gen_agent,
            case_name=self.case_name,
            tools=[expected_generator],
        )

        # Create crew with sequential process
        log_step(logger, 3, 3, "Executing Crew Pipeline")

        # CrewAI agent log file (Thought/Action/Observation details)
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        crewai_log_file = str(log_dir / f"crewai_{self.case_name}_{timestamp}.json")

        logger.info(f"Krystal log: {getattr(logger, 'log_file', 'N/A')}")
        logger.info(f"CrewAI agent log: {crewai_log_file}")

        crew = Crew(
            agents=[rule_analyst, source_generator, expected_gen_agent],
            tasks=[analyze_rules_task, generate_source_task, generate_expected_task],
            process=Process.sequential,
            verbose=True,
            output_log_file=crewai_log_file,
        )

        # Execute crew
        try:
            result = crew.kickoff()

            # Validate output files exist and are non-empty
            expected_files = {
                "config": self.output_dir / "rule_config.json",
                "source": self.output_dir / "generated_source.txt",
                "expected": self.output_dir / "generated_expected.txt",
            }

            missing_files = []
            empty_files = []
            for label, fpath in expected_files.items():
                if not fpath.exists():
                    missing_files.append(f"{label}: {fpath}")
                elif fpath.stat().st_size == 0:
                    empty_files.append(f"{label}: {fpath}")

            if missing_files or empty_files:
                logger.error("=" * 60)
                logger.error("Krystal V4 Execution Failed - Output Validation")
                logger.error("=" * 60)
                if missing_files:
                    logger.error("Missing files:")
                    for f in missing_files:
                        logger.error(f"  - {f}")
                if empty_files:
                    logger.error("Empty files:")
                    for f in empty_files:
                        logger.error(f"  - {f}")
                raise RuntimeError(
                    f"Output validation failed: {len(missing_files)} missing, {len(empty_files)} empty"
                )

            logger.info("=" * 60)
            logger.info("Krystal V4 Execution Completed Successfully")
            logger.info("=" * 60)
            logger.info(f"Output files generated in: {self.output_dir}")
            for label, fpath in expected_files.items():
                size = fpath.stat().st_size
                logger.info(f"  - {fpath.name} ({size} bytes)")

            return {
                "status": "success",
                "case_name": self.case_name,
                "output_dir": str(self.output_dir),
                "files": {
                    label: str(fpath) for label, fpath in expected_files.items()
                },
                "result": str(result),
            }

        except Exception as e:
            logger.error("=" * 60)
            logger.error("Krystal V4 Execution Failed")
            logger.error("=" * 60)
            logger.error(f"Error: {str(e)}")
            raise
