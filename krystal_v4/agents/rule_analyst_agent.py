"""
Rule Analyst Agent for Krystal V4.
Analyzes rules.csv and generates rule_config.json.
"""

from crewai import Agent
from typing import Optional


def create_rule_analyst_agent(llm=None) -> Agent:
    """
    Create Rule Analyst Agent.

    This agent reads rules.csv, detects source format, identifies transformation types,
    and generates a structured rule_config.json configuration file.

    Args:
        llm: Optional language model instance

    Returns:
        Configured Agent instance
    """
    return Agent(
        role="ETL Rule Analyst",
        goal=(
            "Parse ETL transformation rules from rules.csv and create a structured "
            "configuration file (rule_config.json) that specifies how to transform "
            "source data into expected output format."
        ),
        backstory=(
            "You are an expert data analyst specializing in ETL (Extract, Transform, Load) processes. "
            "You have deep knowledge of data transformation patterns including fixed values, direct mappings, "
            "conditional logic, name parsing, and string manipulation. "
            "\n\n"
            "Your task is to read CSV rule files with columns like CSDS_FLAG, CS_COLUMN_NAME, "
            "CARRIER_COLUMN_NAME, DEFAULT, and SPECIAL_RULES, then intelligently identify the "
            "transformation type needed for each field. "
            "\n\n"
            "Transformation types you recognize:\n"
            "1. **fixed** - Returns a constant value (use DEFAULT column)\n"
            "2. **direct** - Direct field mapping from source to target\n"
            "3. **conditional_map** - Maps source values to target values based on conditions\n"
            "4. **name_parser** - Parses 'LAST,FIRST' format into first or last name\n"
            "5. **split_extract** - Splits string by delimiter and extracts part (e.g., 'S5884-197' → 'S5884')\n"
            "6. **empty** - Returns empty string for fields that should be blank\n"
            "\n\n"
            "When you encounter natural language in SPECIAL_RULES column, you analyze it carefully "
            "to determine the correct transformation type and extract configuration parameters. "
            "For example:\n"
            "- 'Member format: last_name, first_name' → name_parser with part='first' or 'last'\n"
            "- 'if PDP map to MD; if HAP map to MS' → conditional_map with mappings\n"
            "- 'Extract contract ID before hyphen' → split_extract with delimiter='-' and index=0\n"
            "\n\n"
            "You also detect the source file format (CSV with quotes vs pipe-delimited) by examining "
            "reference files in the case directory."
        ),
        llm=llm,
        verbose=True,
        allow_delegation=False,
    )
