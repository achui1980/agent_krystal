"""
Material Loader - 加载和解析输入材料
"""

import pandas as pd
import csv
from typing import Dict, List, Any
from pathlib import Path


class MaterialLoader:
    """加载和解析输入材料"""

    def load_rules(self, rules_path: str) -> List[Dict]:
        """解析rules.xlsx"""
        df = pd.read_excel(rules_path, sheet_name="Sheet1", header=None)

        # 表头在第5行（索引4）
        df.columns = [
            "CSDS_FLAG",
            "CS_COLUMN_NAME",
            "CARRIER_COLUMN_NAME",
            "DEFAULT",
            "SPECIAL_RULES",
            "NOTE",
        ]
        rules_data = df.iloc[5:].copy()

        rules = []
        for _, row in rules_data.iterrows():
            rule = {
                "target_field": row["CS_COLUMN_NAME"],
                "source_field": row["CARRIER_COLUMN_NAME"]
                if pd.notna(row["CARRIER_COLUMN_NAME"])
                else None,
                "default_value": row["DEFAULT"] if pd.notna(row["DEFAULT"]) else None,
                "special_rule": row["SPECIAL_RULES"]
                if pd.notna(row["SPECIAL_RULES"])
                else None,
                "note": row["NOTE"] if pd.notna(row["NOTE"]) else None,
            }
            # 只保留有效的规则行
            if (
                pd.notna(rule["target_field"])
                and rule["target_field"] != "CS_COLUMN_NAME"
            ):
                rules.append(rule)

        return rules

    def load_source_sample(self, source_path: str) -> List[Dict]:
        """加载source.csv样本"""
        with open(source_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def load_expected_sample(self, expected_path: str) -> Dict:
        """加载expected.txt样本"""
        with open(expected_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # 前4行是元数据
        metadata = {}
        for i in range(4):
            if ":" in lines[i]:
                key, value = lines[i].strip().split(":", 1)
                metadata[key] = value

        # 第5行是表头
        headers = lines[4].strip().split("|")

        # 数据行
        data = []
        for line in lines[5:]:
            if line.strip():
                values = line.strip().split("|")
                row = dict(zip(headers, values))
                data.append(row)

        return {"metadata": metadata, "headers": headers, "data": data}
