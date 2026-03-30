"""
字符串操作转换器

处理字符串清理、截取、分割等操作
"""

import re
from typing import List


def remove_chars(value: str, chars: str) -> str:
    """
    移除指定字符

    Args:
        value: 输入字符串
        chars: 要移除的字符集合

    Returns:
        清理后的字符串

    Examples:
        >>> remove_chars("(123) 456-7890", "() -")
        '1234567890'
        >>> remove_chars("$1,234.56", "$,")
        '1234.56'
    """
    if not value:
        return ""

    for char in chars:
        value = value.replace(char, "")

    return value


def remove_special_chars(value: str) -> str:
    """
    移除特殊字符（保留字母数字）

    Args:
        value: 输入字符串

    Returns:
        只保留字母数字的字符串
    """
    if not value:
        return ""

    return re.sub(r"[^a-zA-Z0-9]", "", value)


def extract_digits(value: str) -> str:
    """
    提取所有数字

    Args:
        value: 输入字符串

    Returns:
        数字字符串

    Examples:
        >>> extract_digits("(123) 456-7890")
        '1234567890'
    """
    if not value:
        return ""

    return re.sub(r"\D", "", value)


def extract_left(value: str, n: int) -> str:
    """
    提取左边 N 个字符

    Args:
        value: 输入字符串
        n: 字符数

    Returns:
        左边 N 个字符
    """
    if not value:
        return ""

    return value[:n]


def extract_right(value: str, n: int) -> str:
    """
    提取右边 N 个字符

    Args:
        value: 输入字符串
        n: 字符数

    Returns:
        右边 N 个字符
    """
    if not value:
        return ""

    return value[-n:] if len(value) >= n else value


def remove_left(value: str, n: int) -> str:
    """
    移除左边 N 个字符，保留剩余部分

    Args:
        value: 输入字符串
        n: 要移除的字符数

    Returns:
        移除后的字符串
    """
    if not value:
        return ""

    return value[n:]


def remove_right(value: str, n: int) -> str:
    """
    移除右边 N 个字符，保留剩余部分

    Args:
        value: 输入字符串
        n: 要移除的字符数

    Returns:
        移除后的字符串
    """
    if not value:
        return ""

    return value[:-n] if len(value) > n else ""


def insert_char(value: str, char: str, position: int) -> str:
    """
    在指定位置插入字符

    Args:
        value: 输入字符串
        char: 要插入的字符
        position: 插入位置（0-based）

    Returns:
        插入后的字符串
    """
    if not value:
        return char

    return value[:position] + char + value[position:]


def split_and_extract(value: str, delimiter: str, index: int) -> str:
    """
    按分隔符分割并提取指定部分

    Args:
        value: 输入字符串
        delimiter: 分隔符
        index: 要提取的索引（0-based）

    Returns:
        指定部分，如果不存在返回空字符串

    Examples:
        >>> split_and_extract("S5884-197", "-", 0)
        'S5884'
        >>> split_and_extract("S5884-197", "-", 1)
        '197'
    """
    if not value:
        return ""

    parts = value.split(delimiter)

    if 0 <= index < len(parts):
        return parts[index].strip()

    return ""


def format_phone_number(phone: str) -> Dict[str, str]:
    """
    格式化电话号码，提取区号和号码

    Args:
        phone: 电话号码字符串

    Returns:
        字典：{"area_code": "区号", "number": "号码"}

    Examples:
        >>> format_phone_number("(123) 456-7890")
        {'area_code': '123', 'number': '456-7890'}
    """
    result = {"area_code": "", "number": ""}

    if not phone:
        return result

    # 提取所有数字
    digits = extract_digits(phone)

    if len(digits) >= 10:
        result["area_code"] = digits[:3]
        # 格式化为 XXX-XXXX
        result["number"] = f"{digits[3:6]}-{digits[6:10]}"
    elif len(digits) >= 3:
        result["area_code"] = digits[:3]
        result["number"] = digits[3:]
    else:
        result["number"] = digits

    return result
