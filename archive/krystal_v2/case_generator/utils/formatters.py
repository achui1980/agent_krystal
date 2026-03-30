"""
Output Formatters for ETL Data

This module provides formatting functions for generating output files:
- Pipe-delimited format with metadata headers
- Field ordering based on expected schema
- Handling of empty/missing fields

Usage:
    from krystal_v2.case_generator.utils.formatters import format_pipe_delimited_output

    output = format_pipe_delimited_output(rows, field_order, metadata)
"""

from typing import List, Dict, Any


def format_pipe_delimited_output(
    rows: List[Dict[str, Any]], field_order: List[str], metadata: Dict[str, str]
) -> str:
    """
    Generate pipe-delimited output with metadata headers.

    Output format:
        ACTION_ID:value
        SERVICE_MAP_ID:value
        SOURCE_TOKEN:value
        (blank line)
        FIELD1|FIELD2|...|FIELD93
        value1|value2||value4|...
        ...

    Args:
        rows: List of data rows (each row is a dictionary with field names as keys)
        field_order: Ordered list of field names (defines column order)
        metadata: Dictionary containing metadata fields:
                  - ACTION_ID
                  - SERVICE_MAP_ID
                  - SOURCE_TOKEN

    Returns:
        Formatted string ready to write to file

    Examples:
        >>> rows = [{'FIRST_NAME': 'John', 'LAST_NAME': 'Doe', 'AGE': ''}]
        >>> field_order = ['FIRST_NAME', 'LAST_NAME', 'AGE']
        >>> metadata = {'ACTION_ID': 'test-123', 'SERVICE_MAP_ID': '456', 'SOURCE_TOKEN': 'abc'}
        >>> output = format_pipe_delimited_output(rows, field_order, metadata)
    """
    lines = []

    # Add metadata headers
    lines.append(f"ACTION_ID:{metadata.get('ACTION_ID', '')}")
    lines.append(f"SERVICE_MAP_ID:{metadata.get('SERVICE_MAP_ID', '')}")
    lines.append(f"SOURCE_TOKEN:{metadata.get('SOURCE_TOKEN', '')}")

    # Add blank line
    lines.append("")

    # Add header row (field names)
    header = "|".join(field_order)
    lines.append(header)

    # Add data rows
    for row in rows:
        # Build row in correct field order
        # Missing fields default to empty string (will appear as || in output)
        values = []
        for field in field_order:
            value = row.get(field, "")
            # Convert None to empty string
            if value is None:
                value = ""
            # Convert to string and strip whitespace
            values.append(str(value).strip())

        # Join with pipe delimiter
        data_line = "|".join(values)
        lines.append(data_line)

    # Join all lines with newline
    return "\n".join(lines)


def format_csv_output(
    rows: List[Dict[str, Any]], field_order: List[str], include_header: bool = True
) -> str:
    """
    Generate CSV output (for debugging or alternative format).

    Args:
        rows: List of data rows
        field_order: Ordered list of field names
        include_header: Whether to include header row

    Returns:
        CSV formatted string
    """
    lines = []

    # Add header if requested
    if include_header:
        header = ",".join(f'"{field}"' for field in field_order)
        lines.append(header)

    # Add data rows
    for row in rows:
        values = []
        for field in field_order:
            value = row.get(field, "")
            if value is None:
                value = ""
            # Escape quotes and wrap in quotes
            escaped_value = str(value).replace('"', '""')
            values.append(f'"{escaped_value}"')

        data_line = ",".join(values)
        lines.append(data_line)

    return "\n".join(lines)


def validate_field_order(
    rows: List[Dict[str, Any]], field_order: List[str]
) -> Dict[str, Any]:
    """
    Validate that all fields in field_order are present in at least one row.

    Args:
        rows: List of data rows
        field_order: Expected field order

    Returns:
        Dictionary with validation results:
        - 'valid': bool
        - 'missing_fields': list of fields in field_order but not in any row
        - 'extra_fields': list of fields in rows but not in field_order
    """
    # Collect all field names from all rows
    all_fields = set()
    for row in rows:
        all_fields.update(row.keys())

    field_order_set = set(field_order)

    # Find missing and extra fields
    missing_fields = field_order_set - all_fields
    extra_fields = all_fields - field_order_set

    return {
        "valid": len(missing_fields) == 0,
        "missing_fields": sorted(list(missing_fields)),
        "extra_fields": sorted(list(extra_fields)),
    }


def create_metadata(
    action_id: str, service_map_id: str, source_token: str = ""
) -> Dict[str, str]:
    """
    Create a metadata dictionary for output formatting.

    Args:
        action_id: Action identifier
        service_map_id: Service map identifier
        source_token: Source token (optional, auto-generated if not provided)

    Returns:
        Metadata dictionary
    """
    import hashlib
    import time

    # Auto-generate source token if not provided
    if not source_token:
        # Generate a simple token based on timestamp and action_id
        timestamp = str(int(time.time() * 1000))
        raw_token = f"{action_id}_{timestamp}"
        source_token = hashlib.md5(raw_token.encode()).hexdigest() + f"_{timestamp}"

    return {
        "ACTION_ID": action_id,
        "SERVICE_MAP_ID": service_map_id,
        "SOURCE_TOKEN": source_token,
    }
