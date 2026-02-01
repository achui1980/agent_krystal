"""
CSV Data Generation Tools
"""

import uuid
import random
import string
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path
import csv
import re
from jinja2 import Template
from crewai.tools import BaseTool
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class GenerateCSVInput(BaseModel):
    """Input for CSV generation"""

    data_schema: Dict[str, Any] = Field(description="Data schema definition")
    row_count: int = Field(default=100, description="Number of rows to generate")
    output_path: str = Field(description="Output file path")
    template_path: Optional[str] = Field(
        default=None, description="Optional template file path"
    )


class CSVGeneratorTool(BaseTool):
    """Tool for generating test data CSV files"""

    name: str = "csv_generator"
    description: str = """
    Generate CSV test data based on schema definition.
    Supports various data types: uuid, string, int, float, datetime, enum, boolean.
    Can also use Jinja2 templates for custom data generation.
    """
    args_schema: type[BaseModel] = GenerateCSVInput

    def _run(
        self,
        data_schema: Dict[str, Any],
        row_count: int,
        output_path: str,
        template_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate CSV file based on schema or template

        Args:
            data_schema: Data schema with field definitions
            row_count: Number of rows to generate
            output_path: Where to save the CSV file
            template_path: Optional template file for custom generation

        Returns:
            Path to the generated CSV file
        """
        logger.info(f"📊 CSV Generator Tool 执行:")
        logger.info(f"   输出路径: {output_path}")
        logger.info(f"   数据行数: {row_count}")
        logger.info(f"   数据字段数: {len(data_schema.get('fields', []))}")

        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            if template_path and Path(template_path).exists():
                # Use template-based generation
                logger.info(f"   使用模板: {template_path}")
                file_path = self._generate_from_template(
                    template_path, data_schema, row_count, output_file
                )
            else:
                # Use schema-based generation
                logger.info(f"   使用 schema 生成数据")
                file_path = self._generate_from_schema(
                    data_schema, row_count, output_file
                )

            logger.info(f"   ✅ 生成成功: {file_path}")

            return {
                "success": True,
                "file_path": file_path,
                "row_count": row_count,
                "message": f"CSV file generated successfully: {file_path}",
            }
        except Exception as e:
            return {
                "success": False,
                "file_path": str(output_file),
                "row_count": row_count,
                "error": str(e),
                "message": f"Failed to generate CSV file: {str(e)}",
            }

    def _generate_from_schema(
        self, schema: Dict[str, Any], row_count: int, output_file: Path
    ) -> str:
        """Generate CSV from schema definition"""
        fields = schema.get("fields", [])
        headers = [f["name"] for f in fields]

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for _ in range(row_count):
                row = []
                for field in fields:
                    value = self._generate_field_value(field)
                    row.append(value)
                writer.writerow(row)

        return str(output_file.absolute())

    def _generate_from_template(
        self,
        template_path: str,
        schema: Dict[str, Any],
        row_count: int,
        output_file: Path,
    ) -> str:
        """Generate CSV from Jinja2 template"""
        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        template = Template(template_content)

        # Generate sample data for template
        sample_data = []
        fields = schema.get("fields", [])

        for i in range(row_count):
            row_data = {"index": i, "timestamp": datetime.now().isoformat()}
            for field in fields:
                row_data[field["name"]] = self._generate_field_value(field)
            sample_data.append(row_data)

        # Render template
        csv_content = template.render(data=sample_data, count=row_count)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(csv_content)

        return str(output_file.absolute())

    def _generate_field_value(self, field: Dict[str, Any]) -> Any:
        """Generate a value based on field type"""
        field_type = field.get("type", "string")

        if field_type == "uuid":
            return str(uuid.uuid4())

        elif field_type == "string":
            min_len = field.get("min_length", 5)
            max_len = field.get("max_length", 20)
            length = random.randint(min_len, max_len)
            if "pattern" in field:
                return self._generate_from_pattern(field["pattern"])
            return "".join(
                random.choices(string.ascii_letters + string.digits, k=length)
            )

        elif field_type == "int":
            min_val = field.get("min", 0)
            max_val = field.get("max", 100)
            return random.randint(min_val, max_val)

        elif field_type == "float":
            min_val = field.get("min", 0.0)
            max_val = field.get("max", 100.0)
            decimals = field.get("decimals", 2)
            return round(random.uniform(min_val, max_val), decimals)

        elif field_type == "datetime":
            days_offset = field.get("days_offset", 30)
            start_date = datetime.now() - timedelta(days=days_offset)
            end_date = datetime.now()
            random_date = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            format_str = field.get("format", "%Y-%m-%d %H:%M:%S")
            return random_date.strftime(format_str)

        elif field_type == "enum":
            values = field.get("values", [])
            if values:
                return random.choice(values)
            return None

        elif field_type == "boolean":
            return random.choice([True, False])

        elif field_type == "email":
            domains = field.get("domains", ["example.com", "test.com"])
            username = "".join(random.choices(string.ascii_lowercase, k=8))
            return f"{username}@{random.choice(domains)}"

        elif field_type == "phone":
            return f"1{random.randint(1000000000, 9999999999)}"

        else:
            return ""

    def _generate_from_pattern(self, pattern: str) -> str:
        """Generate string matching regex pattern (simplified)"""
        # Simple pattern replacement for common patterns
        result = pattern
        result = re.sub(
            r"\[a-z\]", lambda m: random.choice(string.ascii_lowercase), result
        )
        result = re.sub(
            r"\[A-Z\]", lambda m: random.choice(string.ascii_uppercase), result
        )
        result = re.sub(r"\[0-9\]", lambda m: random.choice(string.digits), result)
        result = re.sub(
            r"\[a-zA-Z\]", lambda m: random.choice(string.ascii_letters), result
        )
        result = re.sub(
            r"\[a-zA-Z0-9\]",
            lambda m: random.choice(string.ascii_letters + string.digits),
            result,
        )

        # Handle quantifiers like {3}, {2,5}
        def replace_quantifier(match):
            inner = match.group(1)
            quantifier = match.group(2)
            if "," in quantifier:
                min_count, max_count = map(int, quantifier.strip("{}").split(","))
                count = random.randint(min_count, max_count)
            else:
                count = int(quantifier.strip("{}"))
            return "".join(
                random.choice(string.ascii_letters + string.digits)
                for _ in range(count)
            )

        result = re.sub(r"\(([a-zA-Z0-9]+)\)(\{[^}]+\})", replace_quantifier, result)

        return result


class LoadTemplateInput(BaseModel):
    """Input for loading template"""

    template_path: str = Field(description="Path to template file")
    variables: Dict[str, Any] = Field(
        default_factory=dict, description="Variables to substitute"
    )


class TemplateLoaderTool(BaseTool):
    """Tool for loading and rendering Jinja2 templates"""

    name: str = "template_loader"
    description: str = "Load and render Jinja2 templates with variables"
    args_schema: type[BaseModel] = LoadTemplateInput

    def _run(
        self, template_path: str, variables: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Load and render template"""
        try:
            if not Path(template_path).exists():
                return {
                    "success": False,
                    "error": f"Template not found: {template_path}",
                    "message": f"Template file does not exist: {template_path}",
                }

            with open(template_path, "r", encoding="utf-8") as f:
                template_content = f.read()

            template = Template(template_content)
            rendered = template.render(**(variables or {}))

            return {
                "success": True,
                "rendered": rendered,
                "template_path": template_path,
                "message": "Template rendered successfully",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "template_path": template_path,
                "message": f"Failed to render template: {str(e)}",
            }
