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
            return f"{last},{first}"

        # Member field
        if "member" in field_lower or "name" in field_lower:
            if "first" in field_lower:
                return self.faker.first_name().upper()
            elif "last" in field_lower:
                return self.faker.last_name().upper()
            else:
                # Default to "LAST,FIRST" format
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
