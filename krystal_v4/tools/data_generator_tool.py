"""
Data generator tool for Krystal V4.
Generates test data using Faker with intelligent field detection.
"""

from typing import Dict, Any, List
from datetime import datetime, timedelta
import random
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from faker import Faker


class DataGeneratorInput(BaseModel):
    """Input schema for DataGeneratorTool"""

    field_name: str = Field(description="Name of field to generate data for")
    field_type: str = Field(
        default="string", description="Data type hint: string, date, number, etc."
    )
    format_hint: str | None = Field(
        default=None, description="Format hint (e.g., 'LAST,FIRST', 'YYYY-MM-DD')"
    )
    count: int = Field(default=1, description="Number of values to generate")


class DataGeneratorTool(BaseTool):
    name: str = "Data Generator"
    description: str = (
        "Generates realistic test data for a field using Faker library. "
        "Intelligently detects field type from name (e.g., 'Member' -> name format, "
        "'DOB' -> date of birth, 'Product' -> product code). "
        "Input: field_name (string), field_type (optional), format_hint (optional), count (integer)"
    )
    args_schema: type[BaseModel] = DataGeneratorInput

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self):
        super().__init__()
        # Use object.__setattr__ to bypass Pydantic validation
        object.__setattr__(self, "faker", Faker())
        # Seed for reproducibility (optional)
        Faker.seed(12345)
        random.seed(12345)

    def _run(
        self,
        field_name: str,
        field_type: str = "string",
        format_hint: str | None = None,
        count: int = 1,
    ) -> str:
        """
        Generate test data for a field.

        Args:
            field_name: Name of field
            field_type: Data type hint
            format_hint: Format specification
            count: Number of values to generate

        Returns:
            String representation of generated values
        """
        try:
            values = self.generate_values(field_name, field_type, format_hint, count)
            return f"Generated {count} values for '{field_name}': {values[:3]}..."  # Show first 3
        except Exception as e:
            return f"ERROR generating data: {str(e)}"

    def generate_values(
        self,
        field_name: str,
        field_type: str = "string",
        format_hint: str | None = None,
        count: int = 1,
    ) -> List[Any]:
        """
        Generate list of values (programmatic interface).

        Args:
            field_name: Name of field
            field_type: Data type hint
            format_hint: Format specification
            count: Number of values to generate

        Returns:
            List of generated values
        """
        field_lower = field_name.lower()
        values = []

        for _ in range(count):
            value = self._generate_single_value(
                field_name, field_lower, field_type, format_hint
            )
            values.append(value)

        return values

    def generate_with_coverage(
        self,
        source_fields: List[str],
        conditional_coverage: Dict[str, List[str]],
        product_line_mapping: Dict[str, str],
        field_metadata: Dict[str, Any],
        record_count: int,
    ) -> List[Dict[str, Any]]:
        """
        Generate source records ensuring conditional coverage and MS product special handling.

        This method ensures:
        1. All required conditional values are covered (e.g., all Product values)
        2. MS products (HAP, HUM, HV, RD) have empty Plan_Name
        3. Non-MS products have Plan_Name in "SXXXX-YYY" format

        Args:
            source_fields: List of source field names
            conditional_coverage: Dict mapping field names to required values
                Example: {"Product": ["PDP", "HAP", "HUM", "HV", "RD", "LPPO"]}
            product_line_mapping: Dict mapping product values to product lines
                Example: {"PDP": "MD", "HAP": "MS", "HUM": "MS", ...}
            field_metadata: Metadata about fields (type hints, formats)
            record_count: Desired number of records

        Returns:
            List of source record dictionaries with conditional coverage
        """
        import logging

        logger = logging.getLogger(__name__)

        # STEP 1: Validate record count (adjust if needed)
        total_required = sum(len(values) for values in conditional_coverage.values())
        if record_count < total_required:
            logger.warning(
                f"⚠️  Record count ({record_count}) is less than required for conditional coverage ({total_required}). "
                f"Auto-adjusting to {total_required} records."
            )
            record_count = total_required

        # STEP 2: Generate records with round-robin for conditional fields
        records = []

        # Identify MS products (products that map to "MS" product line)
        ms_products = {
            product for product, line in product_line_mapping.items() if line == "MS"
        }
        logger.info(f"🔍 MS Products identified: {ms_products}")

        # Round-robin through conditional values first
        conditional_field = None
        required_values = []
        if conditional_coverage:
            # Assume single conditional field (typically "Product")
            conditional_field = list(conditional_coverage.keys())[0]
            required_values = conditional_coverage[conditional_field]
            logger.info(
                f"📋 Conditional coverage for '{conditional_field}': {required_values}"
            )

        # Generate records ensuring coverage
        for i in range(record_count):
            record = {}

            # Generate each field
            for field in source_fields:
                # Handle conditional field with round-robin
                if (
                    conditional_field
                    and field == conditional_field
                    and i < len(required_values)
                ):
                    # Round-robin through required values
                    value = required_values[i]
                    record[field] = value
                elif conditional_field and field == conditional_field:
                    # After covering all required values, generate randomly
                    value = random.choice(required_values)
                    record[field] = value
                else:
                    # Generate based on field metadata
                    field_type = field_metadata.get(field, {}).get("type", "string")
                    format_hint = field_metadata.get(field, {}).get("format")
                    value = self._generate_single_value(
                        field, field.lower(), field_type, format_hint
                    )
                    record[field] = value

            # STEP 3: Apply MS special handling
            # If Product maps to MS, clear Plan_Name
            if "Product" in record and "Plan_Name" in source_fields:
                product_value = record["Product"]
                if product_value in ms_products:
                    record["Plan_Name"] = ""
                    logger.debug(
                        f"✅ Record {i + 1}: Product={product_value} → MS → Plan_Name cleared"
                    )
                elif not record.get("Plan_Name"):
                    # Non-MS product should have Plan_Name
                    contract = f"S{random.randint(1000, 9999)}"
                    plan = f"{random.randint(100, 999)}"
                    record["Plan_Name"] = f"{contract}-{plan}"

            records.append(record)

        # STEP 4: Log coverage summary
        if conditional_field:
            generated_values = [r.get(conditional_field) for r in records]
            coverage_summary = {
                value: generated_values.count(value) for value in required_values
            }
            logger.info(f"✅ Conditional coverage summary: {coverage_summary}")

        logger.info(f"✅ Generated {len(records)} records with conditional coverage")
        return records

    def _generate_single_value(
        self,
        field_name: str,
        field_lower: str,
        field_type: str,
        format_hint: str | None,
    ) -> Any:
        """Generate a single value based on field characteristics."""

        # Name fields with "LAST,FIRST" format
        if format_hint and "LAST" in format_hint and "FIRST" in format_hint:
            last = self.faker.last_name().upper()
            first = self.faker.first_name().upper()
            # CRITICAL: No space after comma (matches requirements)
            return f"{last},{first}"

        # Member field
        if "member" in field_lower or "name" in field_lower:
            if "first" in field_lower:
                return self.faker.first_name().upper()
            elif "last" in field_lower:
                return self.faker.last_name().upper()
            else:
                # Default to "LAST,FIRST" format (NO space after comma)
                last = self.faker.last_name().upper()
                first = self.faker.first_name().upper()
                return f"{last},{first}"

        # Date of birth
        if "dob" in field_lower or "birth" in field_lower:
            # Generate DOB for ages 65-90 (Medicare age range)
            years_ago = random.randint(65, 90)
            dob = datetime.now() - timedelta(days=years_ago * 365)
            return dob.strftime("%Y-%m-%d")

        # Medicare ID
        if "medicare" in field_lower and "id" in field_lower:
            # Format: 1AB2CD3EF45 (1 digit, 2 letters, pattern)
            letters = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=6))
            numbers = "".join(random.choices("0123456789", k=5))
            return f"{numbers[0]}{letters[0:2]}{numbers[1]}{letters[2:4]}{numbers[2]}{letters[4:6]}{numbers[3:5]}"

        # Address field
        if "address" in field_lower or "addr" in field_lower:
            return self.faker.street_address().upper()

        # City field
        if field_lower in ("city",) or field_lower == "city":
            return self.faker.city().upper()

        # State field
        if field_lower in ("state",) or field_lower == "state":
            return self.faker.state_abbr()

        # Zip/Postal code field
        if "zip" in field_lower or "postal" in field_lower:
            return self.faker.postcode()[:5]

        # Phone field
        if "phone" in field_lower or "fax" in field_lower:
            area = str(random.randint(200, 999))
            prefix = str(random.randint(200, 999))
            line = str(random.randint(1000, 9999))
            return f"{area}-{prefix}-{line}"

        # Email field
        if "email" in field_lower:
            return self.faker.email()

        # SAN / NPN / numeric ID fields
        if field_lower in ("san", "aor_san", "npn"):
            return str(random.randint(1000000, 9999999))

        # Agent / AOR_Name (person name fields)
        if field_lower in ("agent", "aor_name"):
            last = self.faker.last_name().upper()
            first = self.faker.first_name().upper()
            return f"{last}, {first}"

        # UMID field
        if "umid" in field_lower:
            letter = random.choice("ABCDEFGH")
            num = random.randint(10000000, 99999999)
            return f"{letter}{num}"

        # Signature date / Eff_Date / Term_Date
        if field_lower in ("signature_date", "eff_date", "term_date"):
            return self.faker.date_between(start_date="-2y", end_date="today").strftime(
                "%Y-%m-%d"
            )

        # Solar Group Number
        if "solar" in field_lower or "group" in field_lower:
            return f"{random.randint(10000000, 99999999)}K"

        # Policy Indicator
        if "policy_indicator" in field_lower:
            return random.choice(["Active", "Termed"])

        # LIS Indicator
        if "lis" in field_lower and "indicator" in field_lower:
            return random.choice(["Y", "N"])

        # New_P2P
        if "p2p" in field_lower or "new_p2p" in field_lower:
            return random.choice(["New", ""])

        # DOC_ID
        if "doc_id" in field_lower:
            return str(random.randint(100000000, 999999999))

        # EndReasonCd / EndReasonCodeDescription
        if "endreason" in field_lower:
            if "description" in field_lower:
                return random.choice(["", "FREE LOOK PERIOD(MES/MCD)", "PREFERS/ENROLLED IN OTHER HUMANA PLAN"])
            return random.choice(["", "001", "928"])

        # Monthly premium
        if "premium" in field_lower:
            return f"{random.uniform(20, 200):.2f}"

        # Product field
        if "product" in field_lower and "line" not in field_lower:
            products = ["PDP", "LPPO", "HUM", "HAP", "HV"]
            return random.choice(products)

        # Product line (derived field)
        if "product" in field_lower and "line" in field_lower:
            lines = ["MD", "MS", "MA/MAPD"]
            return random.choice(lines)

        # Plan name (contract-plan format)
        if "plan" in field_lower and "name" in field_lower:
            contract = f"S{random.randint(1000, 9999)}"
            plan = f"{random.randint(100, 999)}"
            return f"{contract}-{plan}"

        # Contract ID
        if "contract" in field_lower and "id" in field_lower:
            return f"S{random.randint(1000, 9999)}"

        # Generic date
        if field_type == "date" or "date" in field_lower:
            return self.faker.date_between(start_date="-5y", end_date="today").strftime(
                "%Y-%m-%d"
            )

        # Generic number
        if field_type in ["number", "integer"] or "id" in field_lower:
            return str(random.randint(10000, 99999))

        # Default: random string
        return self.faker.word().upper()
