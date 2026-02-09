"""
条件映射转换器

处理 if-then-else 类型的字段映射
"""

from typing import Dict, Any, Callable


def conditional_map(value: str, mappings: Dict[str, str], default: str = "") -> str:
    """
    条件映射：根据字典映射值

    Args:
        value: 输入值
        mappings: 映射字典 {input: output}
        default: 默认值（当输入不在 mappings 中时）

    Returns:
        映射后的值

    Examples:
        >>> conditional_map("PDP", {"PDP": "MD", "HAP": "MS"}, "MA/MAPD")
        'MD'
        >>> conditional_map("UNKNOWN", {"PDP": "MD"}, "MA/MAPD")
        'MA/MAPD'
    """
    if not value:
        return default

    return mappings.get(value, default)


def parse_product_line(product: str) -> str:
    """
    解析产品类型（humanaS10 规则）

    Rules:
        PDP → MD
        HAP, HUM, HV, RD → MS
        others → MA/MAPD

    Args:
        product: 产品代码

    Returns:
        产品线代码
    """
    if not product:
        return "MA/MAPD"

    product_upper = product.upper().strip()

    mappings = {
        "PDP": "MD",
        "HAP": "MS",
        "HUM": "MS",
        "HV": "MS",
        "RD": "MS",
    }

    return mappings.get(product_upper, "MA/MAPD")


def create_mapper_from_description(description: str) -> Dict[str, str]:
    """
    从自然语言描述创建映射字典

    解析类似 "if 'PDP' map to MD, if 'HAP' map to MS" 的描述

    Args:
        description: 规则描述字符串

    Returns:
        映射字典
    """
    mappings = {}

    if not description:
        return mappings

    # 简单的解析逻辑
    import re

    # 匹配 "if 'X' map to Y" 或 "if 'X' then Y" 模式
    pattern = r"if\s+['\"]([^'\"]+)['\"]\s+(?:map\s+to|then)\s+(['\w/]+)"
    matches = re.findall(pattern, description.lower())

    for match in matches:
        input_val = match[0].strip()
        output_val = match[1].strip().strip("'")
        mappings[input_val] = output_val

    return mappings
