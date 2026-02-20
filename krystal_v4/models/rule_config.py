"""
Pydantic models for Krystal V4 configuration.
Defines the structure of rule_config.json output.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class TransformationRule(BaseModel):
    """
    Represents a single transformation rule.
    """

    target_field: str = Field(description="Target field name (CS_COLUMN_NAME)")
    transformation_type: str = Field(
        description="Type: fixed, direct, conditional_map, name_parser, split_extract, empty"
    )
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Transformation-specific configuration"
    )
    csds_flag: int = Field(description="CSDS_FLAG value (0 or 1)")
    source_field: Optional[str] = Field(
        default=None,
        description="Source field name (CARRIER_COLUMN_NAME) if applicable",
    )
    default_value: Optional[str] = Field(
        default=None, description="DEFAULT value from rules.csv"
    )
    special_rules: Optional[str] = Field(
        default=None, description="Original SPECIAL_RULES text"
    )
    note: Optional[str] = Field(default=None, description="NOTE from rules.csv")


class FieldMetadata(BaseModel):
    """
    Metadata for a field in source data.
    """

    field_name: str = Field(description="Field name")
    data_type: str = Field(
        default="string", description="Data type: string, integer, date, etc."
    )
    sample_values: List[str] = Field(
        default_factory=list, description="Sample values for reference"
    )
    format_hint: Optional[str] = Field(
        default=None, description="Format hint (e.g., 'YYYY-MM-DD', 'LAST,FIRST')"
    )


class RuleConfig(BaseModel):
    """
    Complete configuration for ETL test data generation.
    This is the output of Agent 1 (Rule Analyst).
    """

    case_name: str = Field(description="Test case identifier")
    source_format: str = Field(
        description="Detected source format: csv_quoted, pipe, comma"
    )

    # Three types of fields (clearly separated)
    source_fields: List[str] = Field(
        description="List of source field names (non-empty CARRIER_COLUMN_NAME, deduplicated)"
    )
    target_fields: List[str] = Field(
        description="All CS_COLUMN_NAME fields in order (for expected file output)"
    )
    fixed_values: Dict[str, str] = Field(
        default_factory=dict,
        description="Fixed values for Type B fields: {CS_COLUMN_NAME: DEFAULT} where CARRIER_COLUMN_NAME is empty",
    )

    # Transformation rules (only Type A - fields with source mapping)
    transformation_rules: List[TransformationRule] = Field(
        description="List of transformation rules (Type A fields only)"
    )

    # Field metadata for data generation
    field_metadata: List[FieldMetadata] = Field(
        default_factory=list,
        description="Metadata for source fields (for data generation)",
    )

    # Conditional coverage information (NEW)
    conditional_coverage: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Required values for conditional fields: {source_field: [required_values]}",
    )

    # Product line mapping for MS special handling (NEW)
    product_line_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Product to PRODUCT_LINE mapping: {product_value: product_line} for MS detection",
    )

    record_count: int = Field(default=10, description="Number of records to generate")
    output_metadata: Dict[str, str] = Field(
        default_factory=dict, description="Metadata for expected file header"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "case_name": "humanaS10",
                "source_format": "csv_quoted",
                "source_fields": [
                    "Member",
                    "Product",
                    "DOB",
                    "MEDICARE_ID",
                    "Plan_Name",
                ],
                "target_fields": [
                    "CARRIER_STATUS_MAP",
                    "CARRIER_FAMILY_ID",
                    "PARENT_CARRIER_ID",
                    "IS_PAID",
                    "FIRST_NAME",
                    "LAST_NAME",
                    "PRODUCT_LINE",
                ],
                "fixed_values": {
                    "CARRIER_FAMILY_ID": "66,175,206",
                    "IS_PAID": "1",
                    "BUSINESS_LINE": "2",
                    "MEMBER_NUMBER": "1",
                    "CATEGORY_CLASS_ID": "1",
                    "ADDRESS_TYPE": "HOME",
                    "PHONE_TYPE": "HOME",
                },
                "transformation_rules": [
                    {
                        "target_field": "FIRST_NAME",
                        "transformation_type": "name_parser",
                        "config": {"source_field": "Member", "extract_part": "first"},
                        "csds_flag": 1,
                        "source_field": "Member",
                        "special_rules": "Member format: last_name, first_name",
                    }
                ],
                "field_metadata": [
                    {
                        "field_name": "Member",
                        "data_type": "string",
                        "format_hint": "LAST,FIRST",
                    }
                ],
                "conditional_coverage": {
                    "Product": ["PDP", "HAP", "HUM", "HV", "RD", "LPPO"]
                },
                "product_line_mapping": {
                    "PDP": "MD",
                    "HAP": "MS",
                    "HUM": "MS",
                    "HV": "MS",
                    "RD": "MS",
                    "LPPO": "MA/MAPD",
                },
                "record_count": 10,
                "output_metadata": {
                    "ACTION_ID": "humanaS10-cs-data-integration",
                    "SERVICE_MAP_ID": "10003358",
                },
            }
        }
