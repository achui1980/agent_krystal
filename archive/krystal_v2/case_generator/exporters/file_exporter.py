"""
File Exporter - 导出文件
"""

import csv
import json
from typing import Dict, List
from pathlib import Path


class FileExporter:
    """导出文件"""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_source_csv(self, data: List[Dict], filename: str = "source.csv"):
        """导出source.csv"""
        if not data:
            return

        excluded = {
            "_scenario",
            "_scenario_type",
            "_scenario_name",
            "_scenario_id",
            "description",
        }
        fieldnames = [k for k in data[0].keys() if k not in excluded]

        filepath = self.output_dir / filename
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for row in data:
                clean_row = {k: v for k, v in row.items() if k not in excluded}
                writer.writerow(clean_row)

        return str(filepath)

    def export_expected_txt(
        self,
        data: List[Dict],
        headers: List[str],
        metadata: Dict,
        filename: str = "expected.txt",
    ):
        """导出expected.txt"""
        filepath = self.output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            # 写入元数据
            for key, value in metadata.items():
                f.write(f"{key}:{value}\n")
            f.write("\n")

            # 写入表头
            f.write("|".join(headers) + "\n")

            # 写入数据
            for row in data:
                values = [str(row.get(h, "")) for h in headers]
                f.write("|".join(values) + "\n")

        return str(filepath)

    def export_report_json(self, report: Dict, filename: str = "report.json"):
        """导出report.json"""
        filepath = self.output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        return str(filepath)

    def export_markdown_report(
        self, markdown_content: str, filename: str = "report.md"
    ):
        """导出Markdown格式报告"""
        filepath = self.output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(markdown_content)

        return str(filepath)
