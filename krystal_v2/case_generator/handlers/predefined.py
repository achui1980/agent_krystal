"""
Predefined Handlers - 预定义规则处理器
"""

import re
from typing import Dict, Callable


def direct_map_handler(source_field: str) -> Callable:
    """直接映射处理器"""

    def handler(source_row: Dict) -> str:
        return source_row.get(source_field, "")

    return handler


def default_value_handler(default_value) -> Callable:
    """默认值处理器"""

    def handler(source_row: Dict) -> str:
        return str(default_value)

    return handler


def conditional_map_handler(source_field: str, rule_text: str) -> Callable:
    """条件映射处理器"""
    # 解析 "if 'PDP' map to MD" 这样的规则
    conditions = {}
    default_value = ""

    lines = rule_text.split("\n")
    for line in lines:
        line = line.strip()
        if "map to" in line.lower():
            # 提取 if 'X' map to Y
            match = re.search(
                r"if\s+['\"](\w+)['\"].*?map\s+to\s+['\"]?(\w+)['\"]?",
                line,
                re.IGNORECASE,
            )
            if match:
                source_val, target_val = match.groups()
                conditions[source_val] = target_val

        if "all others" in line.lower() or "default" in line.lower():
            match = re.search(r"is\s+['\"]?(\w+)['\"]?", line, re.IGNORECASE)
            if match:
                default_value = match.group(1)

    def handler(source_row: Dict) -> str:
        source_val = source_row.get(source_field, "")
        return conditions.get(source_val, default_value)

    return handler


def split_field_handler(source_field: str, rule_text: str) -> Callable:
    """字段拆分处理器（如 Member → FIRST_NAME/LAST_NAME）"""
    delimiter = ","
    part = "first"  # 或 "last"

    # 判断是取first还是last
    if "FIRST_NAME" in rule_text or "first" in rule_text.lower():
        part = "first"
    elif "LAST_NAME" in rule_text or "last" in rule_text.lower():
        part = "last"

    def handler(source_row: Dict) -> str:
        value = source_row.get(source_field, "")
        parts = value.split(delimiter)

        if part == "first" and len(parts) >= 2:
            return parts[1].strip()
        elif part == "last" and len(parts) >= 1:
            return parts[0].strip()

        return value.strip()

    return handler


def derive_status_handler(source_field: str, rule_text: str) -> Callable:
    """派生状态处理器（如 Term_Date → Active/Termed）"""

    # 简化为检查是否为9999-12-31
    def handler(source_row: Dict) -> str:
        term_date = source_row.get(source_field, "")
        if term_date == "9999-12-31" or term_date == "":
            return "Active"
        return "Termed"

    return handler


def regex_split_handler(source_field: str, rule_text: str) -> Callable:
    """正则分割处理器（如 Plan_Name → CMS_CONTRACT_ID）"""

    # 简化：按"-"分割，取第一部分
    def handler(source_row: Dict) -> str:
        value = source_row.get(source_field, "")
        parts = value.split("-")
        return parts[0] if parts else value

    return handler


def date_convert_handler(source_field: str, rule_text: str) -> Callable:
    """日期转换处理器"""

    def handler(source_row: Dict) -> str:
        date_val = source_row.get(source_field, "")
        # 简化为直接返回，实际可以做格式转换
        return date_val

    return handler


# 处理器注册表
PREDEFINED_HANDLERS = {
    "direct_map": direct_map_handler,
    "default_value": default_value_handler,
    "conditional_map": conditional_map_handler,
    "split_field": split_field_handler,
    "derive_status": derive_status_handler,
    "regex_split": regex_split_handler,
    "date_convert": date_convert_handler,
    "clean_string": lambda sf, rt: lambda x: x.get(sf, ""),  # 简化
}
