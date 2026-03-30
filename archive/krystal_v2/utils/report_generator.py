"""
Simple report generator for Krystal v2.0
Generates Markdown and HTML reports without CrewAI
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader


def format_llm_analysis(llm_analysis: str) -> str:
    """
    将JSON格式的LLM分析转换成易读的Markdown格式

    Args:
        llm_analysis: JSON字符串或普通文本

    Returns:
        格式化的Markdown字符串
    """
    if not llm_analysis or not isinstance(llm_analysis, str):
        return "_LLM analysis not available._"

    # 尝试解析JSON
    try:
        data = json.loads(llm_analysis)

        # 如果不是字典，返回原始内容
        if not isinstance(data, dict):
            return llm_analysis

        md_lines = []

        # 严重性（Severity）
        severity = data.get("severity", "").lower()
        if severity:
            if severity == "high":
                md_lines.append(f"**严重性:** 🔴 **HIGH** (高)")
            elif severity == "medium":
                md_lines.append(f"**严重性:** 🟡 **MEDIUM** (中)")
            elif severity == "low":
                md_lines.append(f"**严重性:** 🟢 **LOW** (低)")
            else:
                md_lines.append(f"**严重性:** {severity}")
            md_lines.append("")

        # 可接受性（Acceptable）
        acceptable = data.get("acceptable")
        if acceptable is not None:
            status = "✅ 可接受" if acceptable else "❌ 不可接受"
            md_lines.append(f"**验证结果:** {status}")
            md_lines.append("")

        # 分析（Analysis）
        analysis = data.get("analysis", "")
        if analysis:
            md_lines.append("### 📊 差异分析")
            md_lines.append("")
            # 处理转义的换行符
            analysis_clean = analysis.replace("\\n", "\n")
            md_lines.append(analysis_clean)
            md_lines.append("")

        # 根本原因（Root Cause）
        root_cause = data.get("root_cause", "")
        if root_cause:
            md_lines.append("### 🔍 根本原因")
            md_lines.append("")
            root_cause_clean = root_cause.replace("\\n", "\n")
            md_lines.append(root_cause_clean)
            md_lines.append("")

        # 建议（Recommendations）
        recommendations = data.get("recommendations", [])
        if recommendations and isinstance(recommendations, list):
            md_lines.append("### 💡 改进建议")
            md_lines.append("")
            for i, rec in enumerate(recommendations, 1):
                rec_clean = rec.replace("\\n", "\n")
                # 如果建议包含多行，添加缩进
                if "\n" in rec_clean:
                    lines = rec_clean.split("\n")
                    md_lines.append(f"{i}. {lines[0]}")
                    for line in lines[1:]:
                        if line.strip():
                            md_lines.append(f"   {line}")
                else:
                    md_lines.append(f"{i}. {rec_clean}")
            md_lines.append("")

        return "\n".join(md_lines)

    except json.JSONDecodeError:
        # 不是JSON格式，返回原始内容
        return llm_analysis
    except Exception:
        # 其他错误，返回原始内容
        return llm_analysis


class ReportGenerator:
    """Simple report generator for ETL test results"""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize report generator

        Args:
            output_dir: Directory to save reports (default: current working dir)
        """
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup Jinja2 environment for HTML templates
        template_dir = Path(__file__).parent.parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir), autoescape=True
        )

    def generate_markdown(self, report_data: Dict[str, Any]) -> str:
        """
        Generate a Markdown report

        Args:
            report_data: Dictionary containing report information

        Returns:
            Markdown content as string
        """
        test_id = report_data.get("test_id", "unknown")
        timestamp = report_data.get("timestamp", datetime.now().isoformat())
        service_name = report_data.get("service_name", "Unknown Service")
        environment = report_data.get("environment", "unknown")
        overall_pass = report_data.get("overall_pass", False)
        total_duration = report_data.get("total_duration", 0)

        # ETL Results
        etl_steps = report_data.get("etl_steps", [])

        # Validation Results
        total_rows = report_data.get("total_rows", 0)
        matching_rows = report_data.get("matching_rows", 0)
        different_rows = report_data.get("different_rows", 0)
        similarity = report_data.get("similarity", 0)

        # LLM Analysis
        llm_analysis = report_data.get("llm_analysis", None)

        md_lines = [
            f"# Krystal ETL Test Report",
            "",
            f"**Test ID:** {test_id}",
            f"**Service:** {service_name}",
            f"**Environment:** {environment}",
            f"**Timestamp:** {timestamp}",
            f"**Overall Status:** {'✓ PASSED' if overall_pass else '✗ FAILED'}",
            f"**Duration:** {total_duration}s",
            "",
            "---",
            "",
            "## ETL Execution Results",
            "",
        ]

        # Add ETL steps
        if etl_steps:
            for step in etl_steps:
                name = step.get("name", "Unknown Step")
                duration = step.get("duration", 0)
                success = step.get("success", False)
                status_icon = "✓" if success else "✗"
                md_lines.append(
                    f"- {status_icon} **{name}** ({duration}s) - {'Success' if success else 'Failed'}"
                )
        else:
            md_lines.append("_No ETL steps recorded_")

        md_lines.extend(
            [
                "",
                "---",
                "",
                "## Validation Results",
                "",
                f"- **Total Rows:** {total_rows}",
                f"- **Matching Rows:** {matching_rows}",
                f"- **Different Rows:** {different_rows}",
                f"- **Similarity:** {similarity}%",
                "",
            ]
        )

        # Add comparison rows if available
        comparison_rows = report_data.get("comparison_rows", [])
        if comparison_rows:
            md_lines.extend(
                [
                    "### Detailed Comparison",
                    "",
                    "| Row # | Expected | Actual | Status |",
                    "|-------|----------|--------|--------|",
                ]
            )
            for row in comparison_rows[:20]:  # Limit to first 20 rows
                row_num = row.get("row_number", "-")
                expected = str(row.get("expected", "-"))[:30]
                actual = str(row.get("actual", "-"))[:30]
                match = row.get("match", False)
                status = "✓ Match" if match else "✗ Diff"
                md_lines.append(f"| {row_num} | {expected} | {actual} | {status} |")

            if len(comparison_rows) > 20:
                md_lines.append(f"| ... | ... | ... | ... |")
                md_lines.append(f"_... and {len(comparison_rows) - 20} more rows_")

        md_lines.extend(
            [
                "",
                "---",
                "",
                "## LLM Analysis",
                "",
            ]
        )

        if llm_analysis:
            # 格式化LLM分析（如果是JSON则转换为易读的Markdown）
            formatted_analysis = format_llm_analysis(llm_analysis)
            md_lines.append(formatted_analysis)
        else:
            md_lines.append(
                "_LLM analysis not available. This section can be populated with AI-generated insights about the test results._"
            )

        md_lines.extend(
            [
                "",
                "---",
                "",
                f"*Generated by Krystal v2.0 - Intelligent ETL Testing Framework*",
                f"*© 2026 Krystal Team*",
            ]
        )

        return "\n".join(md_lines)

    def generate_html(self, report_data: Dict[str, Any]) -> str:
        """
        Generate an HTML report using Jinja2 template

        Args:
            report_data: Dictionary containing report information

        Returns:
            HTML content as string
        """
        template = self.jinja_env.get_template("report_template.html")

        # Ensure all required template variables are present
        template_data = {
            "test_id": report_data.get("test_id", "unknown"),
            "timestamp": report_data.get("timestamp", datetime.now().isoformat()),
            "service_name": report_data.get("service_name", "Unknown Service"),
            "environment": report_data.get("environment", "unknown"),
            "overall_pass": report_data.get("overall_pass", False),
            "total_duration": report_data.get("total_duration", 0),
            "total_rows": report_data.get("total_rows", 0),
            "matching_rows": report_data.get("matching_rows", 0),
            "different_rows": report_data.get("different_rows", 0),
            "similarity": report_data.get("similarity", 0),
            "etl_steps": report_data.get("etl_steps", []),
            "comparison_rows": report_data.get("comparison_rows", []),
            "llm_analysis": report_data.get("llm_analysis", None),
        }

        return template.render(**template_data)

    def generate_both_formats(self, report_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Generate both Markdown and HTML reports and save to files

        Args:
            report_data: Dictionary containing report information
            Must include 'test_id' for filename generation

        Returns:
            Dictionary with 'markdown' and 'html' file paths
        """
        test_id = report_data.get("test_id", "unknown")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"report_{test_id}_{timestamp}"

        # Generate Markdown
        md_content = self.generate_markdown(report_data)
        md_path = self.output_dir / f"{base_filename}.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_content)

        # Generate HTML
        html_content = self.generate_html(report_data)
        html_path = self.output_dir / f"{base_filename}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return {"markdown": str(md_path), "html": str(html_path)}

    def generate_simple_report(
        self,
        test_id: str,
        service_name: str,
        environment: str,
        etl_success: bool,
        validation_results: Dict[str, Any],
        etl_steps: Optional[List[Dict[str, Any]]] = None,
        llm_analysis: Optional[str] = None,
        duration: float = 0,
    ) -> Dict[str, str]:
        """
        Convenience method to generate a report from simple parameters

        Args:
            test_id: Unique test identifier
            service_name: Name of the service tested
            environment: Environment name (dev, staging, prod)
            etl_success: Whether ETL execution was successful
            validation_results: Dict with 'total', 'matching', 'different', 'similarity'
            etl_steps: List of ETL step dictionaries
            llm_analysis: Optional LLM-generated analysis text
            duration: Total test duration in seconds

        Returns:
            Dictionary with 'markdown' and 'html' file paths
        """
        report_data = {
            "test_id": test_id,
            "timestamp": datetime.now().isoformat(),
            "service_name": service_name,
            "environment": environment,
            "overall_pass": etl_success
            and validation_results.get("similarity", 0) >= 100,
            "total_duration": duration,
            "total_rows": validation_results.get("total", 0),
            "matching_rows": validation_results.get("matching", 0),
            "different_rows": validation_results.get("different", 0),
            "similarity": validation_results.get("similarity", 0),
            "etl_steps": etl_steps or [],
            "comparison_rows": validation_results.get("rows", []),
            "llm_analysis": llm_analysis,
        }

        return self.generate_both_formats(report_data)
