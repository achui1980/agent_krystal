"""
姓名解析器

解析 "LAST,FIRST MIDDLE" 格式的姓名
"""

from typing import Dict, Tuple


def parse_full_name(name: str) -> Dict[str, str]:
    """
    解析全名 "LAST,FIRST MIDDLE" 格式

    Args:
        name: 姓名字符串，格式为 "LastName,FirstName MiddleName"

    Returns:
        字典：{"first": "名字", "middle": "中间名", "last": "姓氏"}

    Examples:
        >>> parse_full_name("MOUSE,MICKEY")
        {'first': 'MICKEY', 'middle': '', 'last': 'MOUSE'}
        >>> parse_full_name("MOUSE,MINNEY A")
        {'first': 'MINNEY A', 'middle': '', 'last': 'MOUSE'}
        >>> parse_full_name("CHOPPER,TONY R")
        {'first': 'TONY R', 'middle': '', 'last': 'CHOPPER'}
    """
    result = {"first": "", "middle": "", "last": ""}

    if not name or not isinstance(name, str):
        return result

    name = name.strip()

    # 按逗号分割
    if "," not in name:
        # 没有逗号，整个作为 first
        result["first"] = name
        return result

    parts = name.split(",", 1)
    result["last"] = parts[0].strip()

    if len(parts) > 1:
        first_part = parts[1].strip()
        # first_part 可能包含 "FIRST MIDDLE"，但我们把整体作为 first
        # 因为期望输出中 FIRST_NAME 包含中间名（如 "MINNEY A"）
        result["first"] = first_part

    return result


def extract_name_part(name: str, part: str = "first") -> str:
    """
    提取姓名的某一部分

    Args:
        name: 全名字符串
        part: 要提取的部分 ("first", "middle", "last")

    Returns:
        提取的部分
    """
    parsed = parse_full_name(name)
    return parsed.get(part, "")


def split_cms_contract(contract_str: str) -> Dict[str, str]:
    """
    分割 CMS 合同字符串 "CONTRACT-PLAN"

    Args:
        contract_str: 合同字符串，如 "S5884-197"

    Returns:
        字典：{"contract": "合同号", "plan": "计划号"}

    Examples:
        >>> split_cms_contract("S5884-197")
        {'contract': 'S5884', 'plan': '197'}
        >>> split_cms_contract("H7617-111")
        {'contract': 'H7617', 'plan': '111'}
    """
    result = {"contract": "", "plan": ""}

    if not contract_str or not isinstance(contract_str, str):
        return result

    parts = contract_str.split("-")

    if len(parts) >= 1:
        result["contract"] = parts[0].strip()

    if len(parts) >= 2:
        result["plan"] = parts[1].strip()

    return result
