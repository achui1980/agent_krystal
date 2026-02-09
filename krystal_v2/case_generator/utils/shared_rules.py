"""
Shared Rules Definition for ETL Transformation

This module contains rules that are common across all ETL test cases:
- SHARED_FIXED_VALUES: Fields with the same value for all cases (6 fields)
- SHARED_EMPTY_FIELDS: Fields that are always empty for all cases (85 fields)
- EXPECTED_FIELD_ORDER: Standard order of output fields (93 fields)

Important Notes:
- CARRIER_FAMILY_ID is NOT in SHARED_FIXED_VALUES because each case has a different value
- Case-specific values should be read from the SOURCE_COLUMN in rules.xlsx
- LAST_TOUCHED_DATE (93rd field) is auto-generated and not in rules.xlsx

Usage:
    from krystal_v2.case_generator.utils.shared_rules import (
        SHARED_FIXED_VALUES,
        SHARED_EMPTY_FIELDS,
        EXPECTED_FIELD_ORDER
    )
"""

# Shared fixed values across all cases (4 fields)
# These fields have the same value regardless of the case
SHARED_FIXED_VALUES = {
    "IS_PAID": "1",
    "BUSINESS_LINE": "2",
    "MEMBER_NUMBER": "1",
    "CATEGORY_CLASS_ID": "1",
}

# Fields that are always empty across all cases (87 fields)
# These fields will output as empty strings in the pipe-delimited format (||)
SHARED_EMPTY_FIELDS = [
    "PARENT_CARRIER_ID",
    "APPLICATION_ID",
    "POLICY_ID",
    "CARRIER_ID",
    "PLAN_ID",
    "RIDER_ID",
    "REQUESTED_EFFECTIVE_DATE",
    "STATUS_CODE",
    "REVENUE_IMPACT_DATE",
    "POLICY_NUMBER",
    "MONTHLY_PREMIUM",
    "RATE_TIER",
    "NOT_CANCELLED",
    "IS_DELINQUENT",
    "DELINQUENCY_NOTE",
    "MEMBER_COUNT",
    "CARRIER_EFFECTIVE_DATE",
    "IS_ACTIVE",
    "IS_REVERSED",
    "IS_MASTER_POLICY",
    "HEALTH_RATE_FACTOR",
    "HOUSEHOLE_DISCOUNT",
    "PREMIUM_EFFECTIVE_DATE",
    "FUTURE_RATE_CHANGE_DATE",
    "FUTURE_PREMIUM",
    "FREQUENCY_TO_DEBIT",
    "CARRIER_PAID_THRU_DATE",
    "ADDRESS_TYPE",
    "ADDRESS_LINE_2",
    "ADDRESS_LINE_3",
    "COUNTY",
    "MIDDLE_NAME",
    "GENDER",
    "SSN",
    "EMAIL",
    "CARRIER_CONFIRMATION_NUMBER",
    "PHONE_TYPE",
    "AREA_CODE",
    "PHONE_NUMBER",
    "PHONE_EXTENSION",
    "STATUS_NOTE",
    "CATEGORY",
    "SUBSCRIBER_COUNT",
    "GROUP_NAME",
    "GROUP_ID",
    "RAF",
    "CARRIER_APPLICATION_ID",
    "CARRIER_POLICY_ID",
    "RECORD_TYPE",
    "CREATION_DATE",
    "SUBMIT_DATE",
    "SIGNITURE_DATE",
    "SEP_REASON_CODE",
    "EXTRA_HELP",
    "EXTRA_HELP_LEVEL",
    "CARRIER_NAME",
    "PLAN_NAME",
    "CMS_SEGMENT_ID",
    "RENEWAL_DATE",
    "RENEWAL_TYPE",
    "INITIAL_PAYMENT_MODE",
    "CMS_DISENROLLMENT_CODE",
    "STATUS_DATE",
    "PLAN_CHANGE_NOTE",
    "CMS_CARRIER_POST_TERM",
    "CMS_CONTRACT_ID_POST_TERM",
    "CMS_PLAN_ID_POST_TERM",
    "CMS_SEGMENT_ID_POST_TERM",
    "BROKER_OF_RECORD",
]

# Expected field order for output (93 fields total)
# This defines the exact order of fields in the pipe-delimited output file
# Note: LAST_TOUCHED_DATE is the 93rd field (auto-generated, not in rules.xlsx)
EXPECTED_FIELD_ORDER = [
    "CARRIER_STATUS_MAP",
    "CARRIER_FAMILY_ID",
    "PARENT_CARRIER_ID",
    "IS_PAID",
    "BUSINESS_LINE",
    "APPLICATION_ID",
    "MEMBER_NUMBER",
    "POLICY_ID",
    "CARRIER_ID",
    "PLAN_ID",
    "RIDER_ID",
    "CATEGORY_CLASS_ID",
    "REQUESTED_EFFECTIVE_DATE",
    "STATUS_CODE",
    "PRODUCT_LINE",
    "REVENUE_IMPACT_DATE",
    "POLICY_NUMBER",
    "MONTHLY_PREMIUM",
    "RATE_TIER",
    "NOT_CANCELLED",
    "IS_DELINQUENT",
    "DELINQUENCY_NOTE",
    "MEMBER_COUNT",
    "CARRIER_EFFECTIVE_DATE",
    "EFFECTIVE_START_DATE",
    "CANCELLATION_DATE",
    "IS_ACTIVE",
    "IS_REVERSED",
    "IS_MASTER_POLICY",
    "HEALTH_RATE_FACTOR",
    "HOUSEHOLE_DISCOUNT",
    "PREMIUM_EFFECTIVE_DATE",
    "FUTURE_RATE_CHANGE_DATE",
    "FUTURE_PREMIUM",
    "FREQUENCY_TO_DEBIT",
    "CARRIER_PAID_THRU_DATE",
    "ADDRESS_TYPE",
    "ADDRESS_LINE_1",
    "ADDRESS_LINE_2",
    "ADDRESS_LINE_3",
    "CITY",
    "STATE",
    "ZIP_CODE",
    "COUNTY",
    "FIRST_NAME",
    "MIDDLE_NAME",
    "LAST_NAME",
    "GENDER",
    "BIRTH_DATE",
    "SSN",
    "EMAIL",
    "MEDICARE_ID",
    "CARRIER_CONFIRMATION_NUMBER",
    "PHONE_TYPE",
    "AREA_CODE",
    "PHONE_NUMBER",
    "PHONE_EXTENSION",
    "STATUS_NOTE",
    "CATEGORY",
    "SUBSCRIBER_COUNT",
    "GROUP_NAME",
    "GROUP_ID",
    "RAF",
    "CARRIER_APPLICATION_ID",
    "CARRIER_POLICY_ID",
    "RECORD_TYPE",
    "CREATION_DATE",
    "SUBMIT_DATE",
    "SIGNITURE_DATE",
    "SEP_REASON_CODE",
    "EXTRA_HELP",
    "EXTRA_HELP_LEVEL",
    "CARRIER_NAME",
    "PLAN_NAME",
    "CMS_CONTRACT_ID",
    "CMS_PLAN_ID",
    "CMS_SEGMENT_ID",
    "RENEWAL_DATE",
    "RENEWAL_TYPE",
    "INITIAL_PAYMENT_MODE",
    "CMS_DISENROLLMENT_CODE",
    "STATUS_DATE",
    "PLAN_CHANGE_NOTE",
    "CMS_CARRIER_POST_TERM",
    "CMS_CONTRACT_ID_POST_TERM",
    "CMS_PLAN_ID_POST_TERM",
    "CMS_SEGMENT_ID_POST_TERM",
    "BROKER_OF_RECORD",
    "AGENCY_NAME",
    "AGENCY_ID",
    "AGENT_NAME",
    "AGENT_ID",
    "LAST_TOUCHED_DATE",
]


def get_field_count() -> dict:
    """
    Get statistics about the field definitions.

    Returns:
        Dictionary containing field counts
    """
    return {
        "total_fields": len(EXPECTED_FIELD_ORDER),
        "fixed_value_fields": len(SHARED_FIXED_VALUES),
        "empty_fields": len(SHARED_EMPTY_FIELDS),
    }
