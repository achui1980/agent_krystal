"""
KrystalV4Crew - Main crew orchestration for ETL test data generation.
"""

from crewai import Crew, Process
from typing import Dict, Any, Optional
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
from krystal_v4.utils.logger import logger, log_step
from krystal_v4.utils.file_helper import ensure_output_dir


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

        # Initialize tools
        format_detector = FormatDetectorTool()
        csv_reader = CSVReaderTool()
        data_generator = DataGeneratorTool()
        file_writer = FileWriterTool()
        transformation_executor = TransformationExecutorTool()
        json_writer = JSONWriterTool()

        # Create agents
        log_step(logger, 1, 3, "Creating Agents")
        rule_analyst = create_rule_analyst_agent(llm=self.llm)
        source_generator = create_source_generator_agent(llm=self.llm)
        expected_generator = create_expected_generator_agent(llm=self.llm)

        # Create tasks
        log_step(logger, 2, 3, "Creating Tasks")

        analyze_rules_task = create_analyze_rules_task(
            agent=rule_analyst,
            rules_file=self.rules_file,
            case_name=self.case_name,
            case_dir=self.case_dir,
            tools=[format_detector, csv_reader, json_writer],
        )

        generate_source_task = create_generate_source_task(
            agent=source_generator,
            case_name=self.case_name,
            tools=[data_generator, file_writer, json_writer],
        )

        generate_expected_task = create_generate_expected_task(
            agent=expected_generator,
            case_name=self.case_name,
            tools=[csv_reader, transformation_executor, file_writer],
        )

        # Create crew with sequential process
        log_step(logger, 3, 3, "Executing Crew Pipeline")

        crew = Crew(
            agents=[rule_analyst, source_generator, expected_generator],
            tasks=[analyze_rules_task, generate_source_task, generate_expected_task],
            process=Process.sequential,
            verbose=True,
        )

        # Execute crew
        try:
            result = crew.kickoff()

            logger.info("=" * 60)
            logger.info("Krystal V4 Execution Completed Successfully")
            logger.info("=" * 60)
            logger.info(f"Output files generated in: {self.output_dir}")
            logger.info(f"  - rule_config.json")
            logger.info(f"  - generated_source.txt")
            logger.info(f"  - generated_expected.txt")

            return {
                "status": "success",
                "case_name": self.case_name,
                "output_dir": str(self.output_dir),
                "files": {
                    "config": str(self.output_dir / "rule_config.json"),
                    "source": str(self.output_dir / "generated_source.txt"),
                    "expected": str(self.output_dir / "generated_expected.txt"),
                },
                "result": str(result),
            }

        except Exception as e:
            logger.error("=" * 60)
            logger.error("Krystal V4 Execution Failed")
            logger.error("=" * 60)
            logger.error(f"Error: {str(e)}")
            raise
