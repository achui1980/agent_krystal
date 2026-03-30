"""
krystal_v2.case_generator.utils

Utility library for ETL data transformation.

This package provides tools for Agent-generated code to transform source data:
- shared_rules: Common rule definitions across all cases
- file_readers: Multi-format file reading (Excel, CSV, TXT)
- data_converters: Data transformation functions (dates, names, etc.)
- formatters: Output file formatting (pipe-delimited with metadata)

Usage:
    from krystal_v2.case_generator.utils import (
        read_source_file,
        convert_date_to_mmddyyyy,
        SHARED_FIXED_VALUES,
        format_pipe_delimited_output
    )

    # Read source data
    df = read_source_file('case/humanaS10/source.csv')

    # Convert dates
    formatted_date = convert_date_to_mmddyyyy(df['DOB'][0])

    # Use shared rules
    output_row = SHARED_FIXED_VALUES.copy()

    # Format output
    output = format_pipe_delimited_output(rows, EXPECTED_FIELD_ORDER, metadata)
"""

# Shared rules
from .shared_rules import (
    SHARED_FIXED_VALUES,
    SHARED_EMPTY_FIELDS,
    EXPECTED_FIELD_ORDER,
    get_field_count,
)

# File readers
from .file_readers import (
    read_source_file,
    read_excel_file,
    read_csv_file,
    read_text_file,
    get_column_names,
    detect_delimiter,
)

# Data converters
from .data_converters import (
    convert_date_to_mmddyyyy,
    parse_full_name,
    split_cms_contract,
    map_product_type,
    clean_phone,
    format_with_commas,
    safe_get,
)

# Formatters
from .formatters import (
    format_pipe_delimited_output,
    format_csv_output,
    validate_field_order,
    create_metadata,
)

__all__ = [
    # Shared rules
    "SHARED_FIXED_VALUES",
    "SHARED_EMPTY_FIELDS",
    "EXPECTED_FIELD_ORDER",
    "get_field_count",
    # File readers
    "read_source_file",
    "read_excel_file",
    "read_csv_file",
    "read_text_file",
    "get_column_names",
    "detect_delimiter",
    # Data converters
    "convert_date_to_mmddyyyy",
    "parse_full_name",
    "split_cms_contract",
    "map_product_type",
    "clean_phone",
    "format_with_commas",
    "safe_get",
    # Formatters
    "format_pipe_delimited_output",
    "format_csv_output",
    "validate_field_order",
    "create_metadata",
]

__version__ = "1.0.0"
