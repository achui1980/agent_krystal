"""
规则分析 Agent

自动分析 rules.xlsx 并生成结构化配置
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.rule_parser import RuleParser
from models.rule import Rule, TransformationConfig
from utils.logger import get_logger


class RuleAnalyzerAgent:
    """
    规则分析 Agent

    自动分析 rules.xlsx，识别转换逻辑，生成结构化配置
    """

    def __init__(self):
        self.logger = get_logger()

        # 预定义的条件映射模式（正则表达式）
        self.conditional_patterns = [
            r"if\s+'?\"?([^'\"\n]+)'?\"?\s+(?:map\s+to|then)\s+'?\"?([^'\"\n]+)'?\"?",
        ]

        # 预定义的姓名解析模式
        self.name_patterns = [
            r"member\s+format[:：]\s*last\s*[,，]\s*first",
            r"last\s*[,，]\s*first\s+middle",
            r"name\s+format[:：]",
        ]

        # 预定义的分割模式
        self.split_patterns = [
            r"separated\s+by\s+['\"]?([^'\"\s]+)['\"]?",
            r"split\s+by\s+['\"]?([^'\"\s]+)['\"]?",
            r"contains\s+.+?\s+separated\s+by",
        ]

    def analyze(self, rules_path: str) -> Dict[str, Any]:
        """
        分析规则文件

        Args:
            rules_path: rules.xlsx 文件路径

        Returns:
            结构化配置字典
        """
        self.logger.section("Agent 规则分析")
        self.logger.step(1, "解析 Excel 规则文件")

        # 使用 RuleParser 读取规则
        parser = RuleParser(rules_path)
        config = parser.parse()

        self.logger.success(f"读取完成: {len(config.rules)} 条规则")

        # 分析每条规则
        self.logger.step(2, "分析转换逻辑")
        analyzed_rules = []

        for idx, rule in enumerate(config.rules, 1):
            analyzed_rule = self._analyze_rule(rule)
            analyzed_rules.append(analyzed_rule)

            if idx <= 5:  # 只显示前5条的详细日志
                self.logger.info(
                    f"  规则 {idx}: {rule.cs_column_name} -> {analyzed_rule['transformation_type']}"
                )

        if len(config.rules) > 5:
            self.logger.info(f"  ... 还有 {len(config.rules) - 5} 条规则")

        # 构建输出配置
        # 过滤 source_fields，移除无效值
        valid_source_fields = [
            f for f in config.source_fields if f and f not in ["NULL", "n/a", "nan", ""]
        ]

        output_config = {
            "case_name": config.case_name,
            "field_order": config.field_order,
            "source_fields": valid_source_fields,
            "rules": analyzed_rules,
            "metadata": {
                "total_rules": len(analyzed_rules),
                "fixed_values": len(
                    [r for r in analyzed_rules if r["transformation_type"] == "fixed"]
                ),
                "direct_mappings": len(
                    [r for r in analyzed_rules if r["transformation_type"] == "direct"]
                ),
                "conditional_mappings": len(
                    [
                        r
                        for r in analyzed_rules
                        if r["transformation_type"] == "conditional_map"
                    ]
                ),
                "special_transformations": len(
                    [
                        r
                        for r in analyzed_rules
                        if r["transformation_type"] not in ["fixed", "direct", "empty"]
                    ]
                ),
            },
        }

        self.logger.step(3, "分析统计")
        self.logger.info(f"📊 固定值字段: {output_config['metadata']['fixed_values']}")
        self.logger.info(f"📊 直接映射: {output_config['metadata']['direct_mappings']}")
        self.logger.info(
            f"📊 条件映射: {output_config['metadata']['conditional_mappings']}"
        )
        self.logger.info(
            f"📊 特殊转换: {output_config['metadata']['special_transformations']}"
        )

        # 后处理：补充分割提取的相关字段
        self._post_process_split_extract(analyzed_rules)

        return output_config

    def _post_process_split_extract(self, rules: List[Dict[str, Any]]):
        """
        后处理：如果 CMS_CONTRACT_ID 使用 split_extract，
        则 CMS_PLAN_ID 也应该使用 split_extract（同一个 source_field）
        """
        # 查找 split_extract 规则
        split_rules = {}
        for rule in rules:
            if rule.get("transformation_type") == "split_extract":
                source = rule.get("config", {}).get("source_field")
                if source:
                    split_rules[source] = rule

        # 为相关的 PLAN_ID 字段添加 split_extract
        contract_to_plan = {
            "CMS_CONTRACT_ID": "CMS_PLAN_ID",
        }

        for contract_field, plan_field in contract_to_plan.items():
            # 查找 CONTRACT 规则
            contract_rule = None
            for rule in rules:
                if rule["cs_column_name"] == contract_field:
                    contract_rule = rule
                    break

            if (
                contract_rule
                and contract_rule.get("transformation_type") == "split_extract"
            ):
                source = contract_rule["config"]["source_field"]
                delimiter = contract_rule["config"].get("delimiter", "-")

                # 查找对应的 PLAN 规则并更新
                for rule in rules:
                    if rule["cs_column_name"] == plan_field:
                        if rule.get("transformation_type") == "direct":
                            rule["transformation_type"] = "split_extract"
                            rule["config"] = {
                                "source_field": source,
                                "delimiter": delimiter,
                                "index": 1,
                            }
                            self.logger.info(
                                f"  🔧 自动推断: {plan_field} -> split_extract (index=1)"
                            )
                        break

    def _analyze_rule(self, rule: Rule) -> Dict[str, Any]:
        """
        分析单条规则

        Args:
            rule: Rule 对象

        Returns:
            结构化规则字典
        """
        result: Dict[str, Any] = {
            "cs_column_name": rule.cs_column_name,
            "carrier_column_name": rule.carrier_column_name,
            "default_value": rule.default_value,
            "special_rules": rule.special_rules,
            "note": rule.note,
            "transformation_type": "empty",
            "config": {},
        }

        # 1. 检查是否是固定值
        if rule.default_value and rule.default_value not in ["NULL", "n/a", ""]:
            if not rule.carrier_column_name or rule.carrier_column_name in [
                "NULL",
                "n/a",
                "",
            ]:
                result["transformation_type"] = "fixed"
                value = str(rule.default_value)

                # 特殊处理 CARRIER_FAMILY_ID：将 66175206 转换为 66,175,206
                if rule.cs_column_name == "CARRIER_FAMILY_ID":
                    # 移除已有的逗号，然后重新格式化
                    digits = value.replace(",", "").replace(" ", "")
                    if len(digits) == 8:
                        value = f"{digits[:2]},{digits[2:5]},{digits[5:]}"

                result["config"] = {"value": value}
                return result

        # 2. 检查特殊规则
        if rule.special_rules and rule.special_rules not in ["NULL", "n/a", ""]:
            special_config = self._parse_special_rules(rule)
            if special_config:
                result.update(special_config)
                return result

        # 3. 直接映射
        if rule.carrier_column_name and rule.carrier_column_name not in [
            "NULL",
            "n/a",
            "",
        ]:
            result["transformation_type"] = "direct"
            result["config"] = {"source_field": rule.carrier_column_name}
            return result

        # 4. 空字段
        result["transformation_type"] = "empty"
        result["config"] = {}
        return result

    def _parse_special_rules(self, rule: Rule) -> Optional[Dict[str, Any]]:
        """
        解析特殊规则

        Args:
            rule: Rule 对象

        Returns:
            解析后的配置，如果无法解析返回 None
        """
        special_lower = rule.special_rules.lower()

        # 检查条件映射
        if "if" in special_lower and (
            "map to" in special_lower or "then" in special_lower
        ):
            return self._parse_conditional_map(rule)

        # 检查姓名解析
        if any(re.search(pattern, special_lower) for pattern in self.name_patterns):
            return self._parse_name_parser(rule)

        # 检查分割提取
        if any(re.search(pattern, special_lower) for pattern in self.split_patterns):
            return self._parse_split_extract(rule)

        return None

    def _parse_conditional_map(self, rule: Rule) -> Dict[str, Any]:
        """解析条件映射规则"""
        result = {
            "transformation_type": "conditional_map",
            "config": {
                "source_field": rule.carrier_column_name,
                "mappings": {},
                "default": "",
            },
        }

        # 尝试提取映射关系
        # 模式: if 'X' map to Y
        pattern = (
            r"if\s+'?\"?([^'\"\n]+)'?\"?\s+(?:map\s+to|then)\s+'?\"?([^'\"\n]+)'?\"?"
        )
        matches = re.findall(pattern, rule.special_rules, re.IGNORECASE)

        for match in matches:
            key = match[0].strip()
            value = match[1].strip()
            result["config"]["mappings"][key] = value

        # 推理默认值
        result["config"]["default"] = self._infer_default_value(rule)

        return result

    def _parse_name_parser(self, rule: Rule) -> Dict[str, Any]:
        """解析姓名解析规则"""
        result = {
            "transformation_type": "name_parser",
            "config": {
                "source_field": rule.carrier_column_name,
                "format": "last_first",
            },
        }

        # 根据字段名确定提取哪部分
        field_lower = rule.cs_column_name.lower()
        if "first" in field_lower:
            result["config"]["part"] = "first"
        elif "last" in field_lower:
            result["config"]["part"] = "last"
        elif "middle" in field_lower:
            result["config"]["part"] = "middle"
        else:
            result["config"]["part"] = "first"

        return result

    def _parse_split_extract(self, rule: Rule) -> Dict[str, Any]:
        """解析分割提取规则"""
        result = {
            "transformation_type": "split_extract",
            "config": {
                "source_field": rule.carrier_column_name,
                "delimiter": "-",
                "index": 0,
            },
        }

        # 尝试提取分隔符
        delimiter_match = re.search(
            r"separated\s+by\s+['\"]?([^'\"\s]+)['\"]?",
            rule.special_rules,
            re.IGNORECASE,
        )
        if delimiter_match:
            result["config"]["delimiter"] = delimiter_match.group(1)

        # 根据字段名确定提取哪部分
        field_lower = rule.cs_column_name.lower()
        if (
            "contract" in field_lower
            or "id" in field_lower
            and "plan" not in field_lower
        ):
            result["config"]["index"] = 0
        elif "plan" in field_lower:
            result["config"]["index"] = 1

        return result

    def _infer_default_value(self, rule: Rule) -> str:
        """
        推理默认值

        根据字段名和上下文推理未明确提及的默认值
        """
        field_lower = rule.cs_column_name.lower()

        # 产品类型字段
        if "product" in field_lower or "line" in field_lower:
            return "MA/MAPD"  # 最常见的默认值

        # 状态字段
        if "status" in field_lower:
            return "Active"

        # 日期字段
        if "date" in field_lower:
            return ""

        # ID 类字段
        if "id" in field_lower:
            return ""

        # 默认空字符串
        return ""

    def save_config(self, config: Dict[str, Any], output_path: str):
        """
        保存配置到文件

        Args:
            config: 配置字典
            output_path: 输出文件路径
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

        self.logger.success(f"配置已保存: {output_path}")


if __name__ == "__main__":
    # 测试
    agent = RuleAnalyzerAgent()
    config = agent.analyze("case/humanaS10/rules.xlsx")
    agent.save_config(config, "krystal_v3/config/humanaS10_auto.json")
