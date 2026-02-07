"""
Rule Parser - 解析规则，生成RuleObject
"""

import re
from typing import Dict, List, Callable
from ..handlers.predefined import PREDEFINED_HANDLERS


class RuleParser:
    """解析规则，生成RuleObject"""

    def __init__(self, llm_client=None):
        self.llm_client = llm_client
        self.predefined_handlers = PREDEFINED_HANDLERS

    def parse_rules(self, rules: List[Dict]) -> List[Dict]:
        """解析所有规则"""
        parsed_rules = []

        for rule in rules:
            parsed = self._parse_single_rule(rule)
            parsed_rules.append(parsed)

        return parsed_rules

    def _parse_single_rule(self, rule: Dict) -> Dict:
        """解析单条规则"""
        target_field = rule["target_field"]
        source_field = rule["source_field"]
        default_value = rule["default_value"]
        special_rule = rule["special_rule"]

        # 1. 直接映射
        if source_field and not default_value and not special_rule:
            return {
                "target_field": target_field,
                "source_field": source_field,
                "rule_type": "direct_map",
                "handler": self.predefined_handlers["direct_map"](source_field),
                "confidence": 0.95,
            }

        # 2. 默认值
        if default_value and not source_field:
            return {
                "target_field": target_field,
                "rule_type": "default_value",
                "handler": self.predefined_handlers["default_value"](default_value),
                "confidence": 1.0,
            }

        # 3. 检查预定义的特殊规则
        if special_rule:
            for rule_name, handler_factory in self.predefined_handlers.items():
                if rule_name == "direct_map" or rule_name == "default_value":
                    continue

                if self._match_pattern(special_rule, rule_name):
                    return {
                        "target_field": target_field,
                        "source_field": source_field,
                        "rule_type": rule_name,
                        "handler": handler_factory(source_field, special_rule),
                        "raw_rule": special_rule,
                        "confidence": 0.85,
                    }

        # 4. 使用LLM理解（兜底）
        if special_rule and self.llm_client:
            llm_handler = self._generate_llm_handler(
                target_field, source_field, special_rule
            )
            return {
                "target_field": target_field,
                "source_field": source_field,
                "rule_type": "llm_generated",
                "handler": llm_handler,
                "raw_rule": special_rule,
                "confidence": 0.6,  # LLM生成的置信度较低
            }

        # 5. 无法解析 - 尝试使用source_field做直接映射
        if source_field:
            return {
                "target_field": target_field,
                "source_field": source_field,
                "rule_type": "direct_map",
                "handler": self.predefined_handlers["direct_map"](source_field),
                "confidence": 0.5,
            }

        # 6. 完全无法解析
        return {
            "target_field": target_field,
            "rule_type": "unknown",
            "handler": lambda x: "",
            "confidence": 0.0,
        }

    def _match_pattern(self, rule_text: str, rule_type: str) -> bool:
        """匹配规则模式"""
        patterns = {
            "conditional_map": r"if\s+['\"].*?['\"]\s+map\s+to",
            "split_field": r"format.*last.*first|split",
            "date_convert": r"date|time|DOB|Date",
            "derive_status": r"status|active|term|derive",
            "regex_split": r"combination|separated\s+by|regex",
            "clean_string": r"Remove.*Char|clean|special",
        }

        pattern = patterns.get(rule_type, "")
        if pattern:
            return bool(re.search(pattern, rule_text, re.IGNORECASE))
        return False

    def _generate_llm_handler(
        self, target_field: str, source_field: str, rule_text: str
    ) -> Callable:
        """使用LLM生成处理器（简化版）"""

        # MVP阶段：简单返回source_field的值或空
        def handler(source_row: Dict) -> str:
            if source_field:
                return source_row.get(source_field, "")
            return ""

        return handler
