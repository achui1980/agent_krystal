"""
Task definitions for Krystal V4.
Defines the three main tasks in the ETL test data generation pipeline.
"""

from crewai import Task
from crewai import Agent
from typing import List


def create_analyze_rules_task(
    agent: Agent, rules_file: str, case_name: str, case_dir: str, tools: List,
    parsed_rules: dict = None,
) -> Task:
    """
    Create task for analyzing rules and generating configuration.

    Args:
        agent: Rule Analyst Agent
        rules_file: Path to rules.csv
        case_name: Test case identifier
        case_dir: Directory containing reference files
        tools: List of tools for the agent
        parsed_rules: Pre-parsed rules data (target_fields, source_fields, fixed_values, special_rules_rows)

    Returns:
        Configured Task instance
    """
    import json as _json

    # Build pre-parsed data section
    pre_parsed_section = ""
    if parsed_rules:
        target_fields_str = _json.dumps(parsed_rules["target_fields"], indent=2)
        source_fields_str = _json.dumps(parsed_rules["source_fields"])
        fixed_values_str = _json.dumps(parsed_rules["fixed_values"], indent=2)
        special_rules_str = _json.dumps(parsed_rules["special_rules_rows"], indent=2, ensure_ascii=False)

        source_format = parsed_rules.get("source_format", "csv_quoted")
        output_metadata_str = _json.dumps(parsed_rules.get("output_metadata", {}), indent=2)

        pre_parsed_section = (
            f"**PRE-PARSED DATA (use these EXACTLY, do NOT modify):**\n\n"
            f"source_format: \"{source_format}\"\n\n"
            f"source_fields (EXACT list, do NOT add or remove):\n{source_fields_str}\n\n"
            f"target_fields (EXACT list of {len(parsed_rules['target_fields'])} fields, do NOT add or remove):\n{target_fields_str}\n\n"
            f"fixed_values (EXACT values, do NOT change):\n{fixed_values_str}\n\n"
            f"output_metadata (EXACT values):\n{output_metadata_str}\n\n"
            f"**YOUR TASK: Analyze ONLY these SPECIAL_RULES rows to create transformation_rules:**\n{special_rules_str}\n\n"
        )

    return Task(
        description=(
            f"Analyze ETL transformation rules and create structured configuration.\n\n"
            f"**Input**: {rules_file}\n"
            f"**Output**: output/{case_name}/rule_config.json\n\n"
            f"{pre_parsed_section}"
            f"**YOUR ONLY JOB**: For each special_rules row, determine the transformation type:\n"
            f"  - **direct**: SPECIAL_RULES is empty → simple field mapping\n"
            f"  - **conditional_map**: SPECIAL_RULES contains 'if...map to' → create mappings dict\n"
            f"  - **name_parser**: SPECIAL_RULES mentions 'last_name, first_name' → parse name parts\n"
            f"  - **split_extract**: SPECIAL_RULES mentions 'separated by' or '-' split → extract part\n\n"
            f"**transformation_rules format**: Each rule must have:\n"
            f"  - target_field: the CS_COLUMN_NAME\n"
            f"  - source_field: the CARRIER_COLUMN_NAME\n"
            f"  - transformation_type: one of direct/conditional_map/name_parser/split_extract\n"
            f"  - config: type-specific config dict\n\n"
            f"**For conditional_map**, also create:\n"
            f"  - conditional_coverage: {{source_field: [all_possible_values]}}\n"
            f"    For 'all others'/'default' cases, use 'LPPO' as the representative value\n"
            f"  - product_line_mapping: {{source_value: target_value}}\n\n"
            f"**Build the final JSON** with ALL of these keys:\n"
            f"  source_format, source_fields, target_fields, fixed_values, transformation_rules,\n"
            f"  conditional_coverage, product_line_mapping, field_metadata, output_metadata\n\n"
            f"⚠️ **MANDATORY**: You MUST call the JSONWriterTool to write the JSON file to disk.\n"
            f"Do NOT just return JSON text. The file MUST exist at output/{case_name}/rule_config.json.\n"
            f"Call: json_writer(file_path='output/{case_name}/rule_config.json', data=<json_string>)"
        ),
        expected_output=(
            f"A JSON configuration file saved to output/{case_name}/rule_config.json containing:\n"
            f"- source_fields: EXACT list from pre-parsed data\n"
            f"- target_fields: EXACT {len(parsed_rules['target_fields']) if parsed_rules else '~93'} fields from pre-parsed data\n"
            f"- fixed_values: EXACT values from pre-parsed data\n"
            f"- transformation_rules: Type A transformations with correct types\n"
            f"- conditional_coverage: Required test values for conditional fields\n"
            f"- product_line_mapping: Product → PRODUCT_LINE mapping\n"
            f"- field_metadata: Format hints for data generation\n"
            f"- output_metadata: ACTION_ID, SERVICE_MAP_ID"
        ),
        agent=agent,
        tools=tools,
    )


def create_generate_source_task(
    agent: Agent, case_name: str, tools: List,
    record_count: int = 10, source_format: str = "csv_quoted",
) -> Task:
    """
    Create task for generating source test data.

    Args:
        agent: Source Generator Agent
        case_name: Test case identifier
        tools: List of tools for the agent
        record_count: Number of records to generate
        source_format: Detected source file format (csv_quoted, pipe, comma)

    Returns:
        Configured Task instance
    """
    return Task(
        description=(
            f"Generate realistic test source data with FULL conditional branch coverage.\n\n"
            f"**Input**: output/{case_name}/rule_config.json\n"
            f"**Output**: output/{case_name}/generated_source.txt\n\n"
            f"**PRE-SET PARAMETERS:**\n"
            f"  record_count: {record_count}\n"
            f"  source_format: {source_format}\n\n"
            f"**CRITICAL: Read Configuration from rule_config.json**\n"
            f"1. source_fields: which fields to include\n"
            f"2. conditional_coverage: which values MUST appear (e.g., Product)\n"
            f"3. product_line_mapping: Product → PRODUCT_LINE (for MS detection)\n\n"
            f"**STEP 1: Validate Record Count** ⚠️\n"
            f"Total records to generate: {record_count}\n"
            f"If conditional_coverage has more required values than {record_count},\n"
            f"increase to cover all values.\n\n"
            f"**STEP 2: Generate with Conditional Coverage** 🎯\n"
            f"For fields WITH conditional_coverage:\n"
            f"  Round-robin through required values first, then random for remaining.\n\n"
            f"**STEP 3: Field-Specific Generation** 📝\n\n"
            f"🔹 **Member**: 'LASTNAME,FIRSTNAME' (NO space after comma)\n"
            f"   Example: 'MOUSE,MICKEY'\n\n"
            f"🔹 **DOB**: 'YYYY-MM-DD', age 65-90\n\n"
            f"🔹 **Product**: Use conditional_coverage values\n\n"
            f"🔹 **Plan_Name**: ⚠️ MS SPECIAL HANDLING\n"
            f"   If product maps to MS: Plan_Name = '' (EMPTY)\n"
            f"   Else: Plan_Name = 'SXXXX-YYY' format\n\n"
            f"🔹 **Address/City/State/Zip**: Use realistic values\n"
            f"🔹 **MEDICARE_ID**: Mix of digits/letters (e.g., 1AB2CD3EF45)\n\n"
            f"**STEP 4: Write Output** 💾\n"
            f"Format: **{source_format}**\n"
            f"{'⚠️ csv_quoted means ALL fields must be wrapped in double quotes!' if source_format == 'csv_quoted' else ''}\n"
            f"{'Example: ' + chr(34) + 'PDP' + chr(34) + ',' + chr(34) + 'MOUSE,MICKEY' + chr(34) + ',...' if source_format == 'csv_quoted' else ''}\n"
            f"This is CRITICAL because fields like Member contain commas (LAST,FIRST).\n\n"
            f"⚠️ **MANDATORY**: You MUST call the FileWriterTool to write the file to disk.\n"
            f"Do NOT just return CSV data as text. The file MUST exist on disk after this task.\n"
            f"Call: file_writer(file_path='output/{case_name}/generated_source.txt', data=<csv_string>)"
        ),
        expected_output=(
            f"A source data file saved to output/{case_name}/generated_source.txt containing:\n"
            f"- {record_count} records (or more if needed for conditional coverage)\n"
            f"- Format: {source_format} (all fields quoted if csv_quoted)\n"
            f"- Header row with source field names\n"
            f"- ALL conditional values covered\n"
            f"- MS products with empty Plan_Name\n"
            f"- Non-MS products with Plan_Name in 'SXXXX-YYY' format"
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
            f"Generate the expected output file by calling the Expected Output Generator tool.\n\n"
            f"**YOUR ONLY JOB**: Call the 'Expected Output Generator' tool with case_name='{case_name}'.\n"
            f"This tool will automatically:\n"
            f"1. Read output/{case_name}/rule_config.json\n"
            f"2. Read output/{case_name}/generated_source.txt\n"
            f"3. Apply ALL transformation rules to each source record\n"
            f"4. Fill fixed values (CARRIER_FAMILY_ID, IS_PAID, etc.)\n"
            f"5. Write output/{case_name}/generated_expected.txt with metadata header\n\n"
            f"⚠️ **MANDATORY**: You MUST call the tool. Do NOT try to do transformations manually.\n"
            f"Call: expected_output_generator(case_name='{case_name}')"
        ),
        expected_output=(
            f"The expected output file generated at output/{case_name}/generated_expected.txt "
            f"with metadata header, pipe-delimited header row, and transformed data rows."
        ),
        agent=agent,
        tools=tools,
    )
