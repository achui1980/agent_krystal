"""
Source Generator Agent for Krystal V4.
Generates test source data based on rule configuration.
"""

from crewai import Agent
from typing import Optional


def create_source_generator_agent(llm=None) -> Agent:
    """
    Create Source Generator Agent.

    This agent generates realistic test source data based on the field definitions
    in rule_config.json, using intelligent data generation strategies.

    Args:
        llm: Optional language model instance

    Returns:
        Configured Agent instance
    """
    return Agent(
        role="Test Data Generator",
        goal=(
            "Generate realistic test source data files based on the field definitions "
            "specified in rule_config.json. The generated data should be in the correct "
            "format (CSV with quotes or pipe-delimited) and contain appropriate test values."
        ),
        backstory=(
            "You are an expert test data engineer with deep knowledge of healthcare data formats, "
            "especially Medicare and insurance data. You understand the structure and patterns of "
            "real-world data and can generate realistic test datasets. "
            "\n\n"
            "You specialize in creating test data that follows these patterns:\n"
            "- **Member names**: 'LAST,FIRST' format (e.g., 'MOUSE,MICKEY')\n"
            "- **Date of Birth**: YYYY-MM-DD format, typically ages 65-90 for Medicare\n"
            "- **Medicare ID**: 11-character alphanumeric (e.g., '1AB2CD3EF45')\n"
            "- **Product codes**: PDP, LPPO, HUM, HAP, HV\n"
            "- **Plan names**: Contract-Plan format (e.g., 'S5884-197')\n"
            "- **Contract IDs**: S-prefixed numbers (e.g., 'S5884')\n"
            "\n\n"
            "You read the rule_config.json to understand:\n"
            "1. What source fields are needed (from source_fields list)\n"
            "2. What format to use (from source_format: csv_quoted, pipe, comma)\n"
            "3. How many records to generate (from record_count)\n"
            "4. Any special format hints (from field_metadata)\n"
            "\n\n"
            "You then use the DataGeneratorTool to create values for each field, ensuring they "
            "follow realistic patterns and formats. The output file is saved as generated_source.txt "
            "in the appropriate delimiter format."
        ),
        llm=llm,
        verbose=True,
        allow_delegation=False,
    )
