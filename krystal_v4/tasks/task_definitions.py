"""
Task definitions for Krystal V4.
Defines the three main tasks in the ETL test data generation pipeline.
"""

from crewai import Task
from crewai import Agent
from typing import List


def create_analyze_rules_task(
    agent: Agent, rules_file: str, case_name: str, case_dir: str, tools: List
) -> Task:
    """
    Create task for analyzing rules and generating configuration.

    Args:
        agent: Rule Analyst Agent
        rules_file: Path to rules.csv
        case_name: Test case identifier
        case_dir: Directory containing reference files
        tools: List of tools for the agent

    Returns:
        Configured Task instance
    """
    return Task(
        description=(
            f"Analyze the ETL transformation rules file and create a structured configuration.\n\n"
            f"**Input file**: {rules_file}\n"
            f"**Case name**: {case_name}\n"
            f"**Reference directory**: {case_dir}\n\n"
            f"**Steps to complete**:\n"
            f"1. Read the rules CSV file using CSVReaderTool\n"
            f"2. Identify source fields from CARRIER_COLUMN_NAME column (skip if empty or contains DEFAULT value)\n"
            f"3. Identify target fields from CS_COLUMN_NAME column\n"
            f"4. For each rule, determine transformation type:\n"
            f"   - If DEFAULT column has value and CARRIER_COLUMN_NAME is empty → **fixed** transformation\n"
            f"   - If SPECIAL_RULES is empty and CARRIER_COLUMN_NAME exists → **direct** transformation\n"
            f"   - If SPECIAL_RULES mentions 'map', 'if', 'condition' → **conditional_map**\n"
            f"   - If SPECIAL_RULES mentions 'last_name, first_name' or name format → **name_parser**\n"
            f"   - If SPECIAL_RULES mentions 'split', 'extract', 'before', 'after' → **split_extract**\n"
            f"   - If both CARRIER_COLUMN_NAME and DEFAULT are empty → **empty**\n"
            f"5. Extract configuration parameters for each transformation type\n"
            f"6. Detect source format by checking reference files in {case_dir} (if exists) using FormatDetectorTool\n"
            f"7. Build field_metadata with format hints (e.g., Member → 'LAST,FIRST')\n"
            f"8. Create rule_config.json with structure:\n"
            f"   - case_name: {case_name}\n"
            f"   - source_format: detected format\n"
            f"   - source_fields: list of unique source fields\n"
            f"   - target_fields: list of target fields in order\n"
            f"   - transformation_rules: list of rules with type and config\n"
            f"   - field_metadata: list of field metadata for data generation\n"
            f"   - record_count: 10 (default)\n"
            f"   - output_metadata: ACTION_ID, SERVICE_MAP_ID\n\n"
            f"9. Save the configuration to output/{case_name}/rule_config.json using JSONWriterTool\n\n"
            f"**Important**: If you cannot parse a SPECIAL_RULES entry, stop and ask the user for guidance."
        ),
        expected_output=(
            f"A JSON configuration file saved to output/{case_name}/rule_config.json containing:\n"
            f"- Complete list of transformation rules with types and configurations\n"
            f"- Source and target field mappings\n"
            f"- Detected source format\n"
            f"- Field metadata for data generation\n"
            f"- Output metadata for file headers"
        ),
        agent=agent,
        tools=tools,
    )


def create_generate_source_task(agent: Agent, case_name: str, tools: List) -> Task:
    """
    Create task for generating source test data.

    Args:
        agent: Source Generator Agent
        case_name: Test case identifier
        tools: List of tools for the agent

    Returns:
        Configured Task instance
    """
    return Task(
        description=(
            f"Generate realistic test source data based on rule configuration.\n\n"
            f"**Input file**: output/{case_name}/rule_config.json\n"
            f"**Output file**: output/{case_name}/generated_source.txt\n\n"
            f"**Steps to complete**:\n"
            f"1. Read rule_config.json to understand:\n"
            f"   - source_fields: what fields to generate\n"
            f"   - source_format: what delimiter to use (csv_quoted, pipe, comma)\n"
            f"   - record_count: how many records to generate (default 10)\n"
            f"   - field_metadata: format hints for each field\n\n"
            f"2. For each source field, generate realistic test data using DataGeneratorTool:\n"
            f"   - Use field name and format_hint to guide generation\n"
            f"   - Generate record_count values for each field\n"
            f"   - Ensure data follows realistic patterns (e.g., dates, names, IDs)\n\n"
            f"3. Construct data records as list of dictionaries\n\n"
            f"4. Write data to generated_source.txt using FileWriterTool:\n"
            f"   - If source_format is 'csv_quoted': use CSV with quoted fields\n"
            f"   - If source_format is 'pipe': use pipe delimiters (|)\n"
            f"   - If source_format is 'comma': use comma delimiters\n"
            f"   - Include header row with field names\n\n"
            f"**Example output format (CSV quoted)**:\n"
            f"```\n"
            f'"Member","Product","DOB","MEDICARE_ID","Plan_Name"\n'
            f'"MOUSE,MICKEY","PDP","1960-01-15","1AB2CD3EF45","S5884-197"\n'
            f"```\n\n"
            f"**Example output format (pipe-delimited)**:\n"
            f"```\n"
            f"Member|Product|DOB|MEDICARE_ID|Plan_Name\n"
            f"MOUSE,MICKEY|PDP|1960-01-15|1AB2CD3EF45|S5884-197\n"
            f"```"
        ),
        expected_output=(
            f"A source data file saved to output/{case_name}/generated_source.txt containing:\n"
            f"- Header row with source field names\n"
            f"- Generated test records in the correct format\n"
            f"- Realistic data values following field-specific patterns"
        ),
        agent=agent,
        tools=tools,
    )


def create_generate_expected_task(agent: Agent, case_name: str, tools: List) -> Task:
    """
    Create task for generating expected output data.

    Args:
        agent: Expected Generator Agent
        case_name: Test case identifier
        tools: List of tools for the agent

    Returns:
        Configured Task instance
    """
    return Task(
        description=(
            f"Apply transformation rules to generate expected output data.\n\n"
            f"**Input files**:\n"
            f"- output/{case_name}/rule_config.json (transformation rules)\n"
            f"- output/{case_name}/generated_source.txt (source data)\n\n"
            f"**Output file**: output/{case_name}/generated_expected.txt\n\n"
            f"**Steps to complete**:\n"
            f"1. Read rule_config.json to get:\n"
            f"   - transformation_rules: list of transformation rules to apply\n"
            f"   - target_fields: expected output field names (in order)\n"
            f"   - output_metadata: metadata for file header\n\n"
            f"2. Read generated_source.txt in the correct format (using source_format from config)\n"
            f"   - Parse as CSV or pipe-delimited based on detected format\n"
            f"   - Load all source records into memory\n\n"
            f"3. For each source record, apply ALL transformation rules:\n"
            f"   - Use TransformationExecutorTool.apply_multiple_transformations()\n"
            f"   - Pass source_record and transformation_rules list\n"
            f"   - This will return a dictionary with target field values\n\n"
            f"4. Collect all transformed records\n\n"
            f"5. Generate timestamp token: generated_YYYYMMDD_HHMMSS\n\n"
            f"6. Write output file in pipe-delimited format using FileWriterTool.write_pipe_delimited():\n"
            f"   - Add metadata header:\n"
            f"     ACTION_ID:{case_name}-cs-data-integration\n"
            f"     SERVICE_MAP_ID:10003358\n"
            f"     SOURCE_TOKEN:generated_YYYYMMDD_HHMMSS\n"
            f"   - Blank line\n"
            f"   - Header row with target_fields (pipe-delimited)\n"
            f"   - Data rows (pipe-delimited)\n\n"
            f"**Example output format**:\n"
            f"```\n"
            f"ACTION_ID:{case_name}-cs-data-integration\n"
            f"SERVICE_MAP_ID:10003358\n"
            f"SOURCE_TOKEN:generated_20260209_143045\n"
            f"\n"
            f"FIRST_NAME|LAST_NAME|PRODUCT_LINE|DOB|CMS_CONTRACT_ID\n"
            f"MICKEY|MOUSE|MD|1960-01-15|S5884\n"
            f"```"
        ),
        expected_output=(
            f"An expected output file saved to output/{case_name}/generated_expected.txt containing:\n"
            f"- Metadata header (ACTION_ID, SERVICE_MAP_ID, SOURCE_TOKEN)\n"
            f"- Pipe-delimited header row with target field names\n"
            f"- Transformed data rows in pipe-delimited format\n"
            f"- All transformations correctly applied to each record"
        ),
        agent=agent,
        tools=tools,
    )
