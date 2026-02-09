"""
Expected Generator Agent for Krystal V4.
Generates expected output data by applying transformation rules.
"""

from crewai import Agent
from typing import Optional


def create_expected_generator_agent(llm=None) -> Agent:
    """
    Create Expected Generator Agent.

    This agent reads the source data and applies transformation rules to generate
    the expected output file in pipe-delimited format.

    Args:
        llm: Optional language model instance

    Returns:
        Configured Agent instance
    """
    return Agent(
        role="ETL Transformation Executor",
        goal=(
            "Read the generated source data file and apply all transformation rules "
            "from rule_config.json to produce the expected output file. The output "
            "must be in pipe-delimited format with proper metadata header."
        ),
        backstory=(
            "You are an expert ETL (Extract, Transform, Load) engineer specializing in data "
            "transformation pipelines. You understand how to execute complex transformation logic "
            "and ensure data quality throughout the process. "
            "\n\n"
            "Your responsibilities include:\n"
            "1. **Reading source data**: Parse the generated_source.txt file in the correct format\n"
            "2. **Applying transformations**: Execute each transformation rule sequentially:\n"
            "   - Fixed values: Return constant values\n"
            "   - Direct mapping: Copy source field to target field\n"
            "   - Name parsing: Extract first/last name from 'LAST,FIRST' format\n"
            "   - Conditional mapping: Apply if-then logic to map values\n"
            "   - String splitting: Extract parts from delimited strings\n"
            "   - Empty values: Return blank strings\n"
            "3. **Generating output**: Write results in pipe-delimited format with metadata\n"
            "\n\n"
            "The expected output file format:\n"
            "```\n"
            "ACTION_ID:case_name-cs-data-integration\n"
            "SERVICE_MAP_ID:10003358\n"
            "SOURCE_TOKEN:generated_YYYYMMDD_HHMMSS\n"
            "\n"
            "FIELD1|FIELD2|FIELD3\n"
            "value1|value2|value3\n"
            "```\n"
            "\n\n"
            "You use the TransformationExecutorTool to apply each rule systematically, ensuring "
            "that all transformations are executed correctly and the output matches the expected "
            "CS (CommonSuite) format for data integration testing."
        ),
        llm=llm,
        verbose=True,
        allow_delegation=False,
    )
