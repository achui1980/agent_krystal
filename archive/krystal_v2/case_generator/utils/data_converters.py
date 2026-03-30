"""
Data Converters for ETL Transformation

This module provides conversion functions for transforming source data fields:
- Date conversion (handles Excel serial numbers, ISO dates, etc.)
- Name parsing (split full names into components)
- CMS contract splitting
- Product type mapping
- Phone number cleaning

All functions handle missing/empty values gracefully and return empty strings.

Usage:
    from krystal_v2.case_generator.utils.data_converters import (
        convert_date_to_mmddyyyy,
        parse_full_name,
        split_cms_contract,
        map_product_type,
        clean_phone
    )
"""

import re
from typing import Any, Dict, Union
from datetime import datetime, timedelta


def convert_date_to_mmddyyyy(date_value: Any) -> str:
    """
    Convert various date formats to MM/DD/YYYY format.

    Supported input formats:
    - Excel serial numbers (e.g., 44197 → "01/01/2021")
    - ISO format strings (e.g., "2021-01-01" → "01/01/2021")
    - Already formatted strings (e.g., "01/01/2021" → "01/01/2021")
    - Special values (e.g., "9999-12-31" → "12/31/9999")
    - Empty/None values → ""

    Args:
        date_value: Date value to convert (can be int, str, or None)

    Returns:
        Date string in MM/DD/YYYY format, or empty string if invalid

    Examples:
        >>> convert_date_to_mmddyyyy(44197)
        '01/01/2021'
        >>> convert_date_to_mmddyyyy("2021-01-01")
        '01/01/2021'
        >>> convert_date_to_mmddyyyy("9999-12-31")
        '12/31/9999'
        >>> convert_date_to_mmddyyyy("")
        ''
    """
    # Handle empty values
    if date_value is None or date_value == "" or str(date_value).strip() == "":
        return ""

    date_str = str(date_value).strip()

    # Handle Excel serial date numbers
    # Excel date serial numbers typically range from 1 (1900-01-01) to ~50000 (2030s)
    if date_str.isdigit() and 1 <= int(date_str) <= 100000:
        try:
            # Excel's epoch is 1899-12-30 (but with a bug treating 1900 as leap year)
            excel_epoch = datetime(1899, 12, 30)
            date_obj = excel_epoch + timedelta(days=int(date_str))
            return date_obj.strftime("%m/%d/%Y")
        except (ValueError, OverflowError):
            pass

    # Try parsing various date string formats
    date_formats = [
        "%m/%d/%Y",  # Already in target format
        "%Y-%m-%d",  # ISO format
        "%m-%d-%Y",  # US format with dashes
        "%d/%m/%Y",  # European format
        "%Y/%m/%d",  # Alternative ISO format
        "%m%d%Y",  # No separators (MMDDYYYY)
        "%Y%m%d",  # No separators (YYYYMMDD)
    ]

    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime("%m/%d/%Y")
        except ValueError:
            continue

    # If all parsing attempts fail, return empty string
    return ""


def parse_full_name(full_name: str) -> Dict[str, str]:
    """
    Parse a full name string into first, middle, and last name components.

    Expected format: "LAST,FIRST MIDDLE" or "LAST, FIRST MIDDLE"

    Args:
        full_name: Full name string to parse

    Returns:
        Dictionary with keys: 'first', 'middle', 'last'
        Returns empty strings for all fields if parsing fails

    Examples:
        >>> parse_full_name("SMITH,JOHN MICHAEL")
        {'first': 'JOHN', 'middle': 'MICHAEL', 'last': 'SMITH'}
        >>> parse_full_name("DOE, JANE")
        {'first': 'JANE', 'middle': '', 'last': 'DOE'}
        >>> parse_full_name("")
        {'first': '', 'middle': '', 'last': ''}
    """
    # Handle empty values
    if not full_name or str(full_name).strip() == "":
        return {"first": "", "middle": "", "last": ""}

    name_str = str(full_name).strip()

    # Default values
    result = {"first": "", "middle": "", "last": ""}

    # Split by comma to separate last name from first/middle
    if "," in name_str:
        parts = name_str.split(",", 1)
        result["last"] = parts[0].strip()

        # Process first and middle names
        if len(parts) > 1:
            first_middle = parts[1].strip()
            name_parts = first_middle.split()

            if len(name_parts) >= 1:
                result["first"] = name_parts[0]
            if len(name_parts) >= 2:
                result["middle"] = " ".join(name_parts[1:])
    else:
        # If no comma, try splitting by space (assume format: First Middle Last)
        name_parts = name_str.split()
        if len(name_parts) >= 1:
            result["first"] = name_parts[0]
        if len(name_parts) >= 2:
            result["last"] = name_parts[-1]
        if len(name_parts) >= 3:
            result["middle"] = " ".join(name_parts[1:-1])

    return result


def split_cms_contract(contract: str) -> Dict[str, str]:
    """
    Split a CMS contract ID into contract and plan components.

    Expected format: "CONTRACT-PLAN" (e.g., "S5884-197")

    Args:
        contract: CMS contract string to split

    Returns:
        Dictionary with keys: 'contract', 'plan'
        Returns empty strings if splitting fails

    Examples:
        >>> split_cms_contract("S5884-197")
        {'contract': 'S5884', 'plan': '197'}
        >>> split_cms_contract("H7617-111")
        {'contract': 'H7617', 'plan': '111'}
        >>> split_cms_contract("")
        {'contract': '', 'plan': ''}
    """
    # Handle empty values
    if not contract or str(contract).strip() == "":
        return {"contract": "", "plan": ""}

    contract_str = str(contract).strip()

    # Split by hyphen or dash
    if "-" in contract_str:
        parts = contract_str.split("-", 1)
        return {
            "contract": parts[0].strip(),
            "plan": parts[1].strip() if len(parts) > 1 else "",
        }

    # If no separator, return as contract only
    return {"contract": contract_str, "plan": ""}


def map_product_type(product: str) -> str:
    """
    Map product codes to standardized product types.

    Mapping rules:
    - PDP → MD
    - HAP, HUM, HV, RD → MS
    - All others → MA/MAPD
    - Empty → ""

    Args:
        product: Product code to map

    Returns:
        Mapped product type string

    Examples:
        >>> map_product_type("PDP")
        'MD'
        >>> map_product_type("HAP")
        'MS'
        >>> map_product_type("OTHER")
        'MA/MAPD'
        >>> map_product_type("")
        ''
    """
    # Handle empty values
    if not product or str(product).strip() == "":
        return ""

    product_str = str(product).strip().upper()

    # Mapping logic
    if product_str == "PDP":
        return "MD"
    elif product_str in ["HAP", "HUM", "HV", "RD"]:
        return "MS"
    else:
        return "MA/MAPD"


def clean_phone(phone: str) -> str:
    """
    Clean phone number by removing special characters.
    Keeps only digits.

    Args:
        phone: Phone number string to clean

    Returns:
        Cleaned phone number (digits only), or empty string if invalid

    Examples:
        >>> clean_phone("(555) 123-4567")
        '5551234567'
        >>> clean_phone("555.123.4567")
        '5551234567'
        >>> clean_phone("")
        ''
    """
    # Handle empty values
    if not phone or str(phone).strip() == "":
        return ""

    phone_str = str(phone).strip()

    # Remove all non-digit characters
    digits_only = re.sub(r"\D", "", phone_str)

    return digits_only


def format_with_commas(number: Union[int, float, str]) -> str:
    """
    Format a number with comma separators (e.g., 66175206 → "66,175,206").

    DEPRECATED: Use format_carrier_family_id() for CARRIER_FAMILY_ID field.
    This function uses standard thousand separators.

    Args:
        number: Number to format (can be int, float, or string)

    Returns:
        Formatted string with commas, or empty string if invalid

    Examples:
        >>> format_with_commas(66175206)
        '66,175,206'
        >>> format_with_commas("1234567")
        '1,234,567'
        >>> format_with_commas("")
        ''
    """
    # Handle empty values
    if number is None or str(number).strip() == "":
        return ""

    try:
        # Convert to integer and format with commas
        num = int(float(str(number).strip()))
        return f"{num:,}"
    except (ValueError, TypeError):
        return ""


def safe_get(data: Dict[str, Any], key: str, default: str = "") -> str:
    """
    Safely get a value from a dictionary, returning empty string if not found.

    Args:
        data: Dictionary to retrieve value from
        key: Key to look up
        default: Default value if key not found (default: empty string)

    Returns:
        Value as string, or default value

    Examples:
        >>> safe_get({'name': 'John'}, 'name')
        'John'
        >>> safe_get({'name': 'John'}, 'age')
        ''
    """
    value = data.get(key, default)
    if value is None or value == "":
        return default
    return str(value).strip()
