"""
Create a corrected rules.xlsx for highmark case based on actual source.txt columns
"""

import openpyxl
from pathlib import Path
import shutil


def fix_highmark_rules():
    """
    Fix the highmark rules.xlsx to match actual source.txt column names
    """
    # Backup original
    original_path = Path("case/highmark/rules.xlsx")
    backup_path = Path("case/highmark/rules.xlsx.backup")

    if not backup_path.exists():
        shutil.copy(original_path, backup_path)
        print(f"✅ Backed up original to {backup_path}")

    # Load the workbook
    wb = openpyxl.load_workbook(original_path)
    ws = wb.active

    # Column mapping corrections based on actual highmark source.txt
    # Old (humanaS10 format) → New (highmark format)
    column_mappings = {
        "Member": "APPLICANT_FIRST_NAME",  # For FIRST_NAME field (after parsing)
        "DOB": "APPLICANT_BIRTH_DATE",
        "Address1": "APPLICANT_ADDR_1",
        "State": "APPLICANT_STATE",
        "Zip": "APPLICANT_ZIP",
        "MEDICARE_ID": "HICN",
    }

    # Special handling for FIRST_NAME and LAST_NAME (both use same source column)
    special_mappings = {
        "FIRST_NAME": ("APPLICANT_FIRST_NAME", "Direct mapping - no parsing needed"),
        "LAST_NAME": ("APPLICANT_LAST_NAME", "Direct mapping - no parsing needed"),
    }

    changes_made = []

    # Find CARRIER_COLUMN_NAME column (column C, index 3)
    carrier_col_idx = 3
    cs_column_idx = 2  # CS_COLUMN_NAME
    special_rules_idx = 5  # SPECIAL_RULES

    # Iterate through data rows (starting from row 7)
    for row in ws.iter_rows(min_row=7, max_row=ws.max_row):
        cs_column_name = row[cs_column_idx - 1].value
        carrier_column_name = row[carrier_col_idx - 1].value

        # Check if this field needs special handling
        if cs_column_name in special_mappings:
            new_carrier_column, new_special_rules = special_mappings[cs_column_name]
            old_carrier_column = carrier_column_name

            # Update CARRIER_COLUMN_NAME
            row[carrier_col_idx - 1].value = new_carrier_column

            # Clear SPECIAL_RULES (no longer need to parse "last, first" format)
            row[special_rules_idx - 1].value = None

            changes_made.append(
                f"  {cs_column_name}: '{old_carrier_column}' → '{new_carrier_column}' (cleared special rules)"
            )

        # Check if this field uses a column that needs mapping
        elif carrier_column_name in column_mappings:
            new_carrier_column = column_mappings[carrier_column_name]

            # Update CARRIER_COLUMN_NAME
            row[carrier_col_idx - 1].value = new_carrier_column

            changes_made.append(
                f"  {cs_column_name}: '{carrier_column_name}' → '{new_carrier_column}'"
            )

    # Save the corrected file
    wb.save(original_path)
    print(f"\n✅ Fixed {len(changes_made)} field mappings in {original_path}\n")

    if changes_made:
        print("Changes made:")
        for change in changes_made:
            print(change)

    print(f"\n💾 Original file backed up to {backup_path}")
    print(f"📝 Updated file saved to {original_path}")


if __name__ == "__main__":
    fix_highmark_rules()
