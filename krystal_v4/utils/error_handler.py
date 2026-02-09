"""
Interactive error handling for Krystal V4.
Provides user prompts when parsing fails.
"""

import sys
from typing import Dict, Any, Optional, Tuple
from krystal_v4.utils.logger import logger


class RuleParseError(Exception):
    """Raised when a rule cannot be parsed"""

    pass


class UserAbortError(Exception):
    """Raised when user chooses to abort execution"""

    pass


def prompt_user_on_parse_failure(
    row_num: int,
    field_name: str,
    rule_content: str,
    error_message: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Prompt user for action when rule parsing fails.

    Args:
        row_num: Row number in rules.csv
        field_name: Field name (CS_COLUMN_NAME)
        rule_content: Content of SPECIAL_RULES
        error_message: Optional error details

    Returns:
        Tuple of (action, config) where:
        - action: "skip" or "manual" or "exit"
        - config: Configuration dict (empty for skip, user-provided for manual)

    Raises:
        UserAbortError: If user chooses to exit
    """
    logger.error("")
    logger.error("╔════════════════════════════════════════════════════════════════╗")
    logger.error("║                    规则解析失败                                 ║")
    logger.error("╚════════════════════════════════════════════════════════════════╝")
    logger.error("")
    logger.error(f"📍 位置: Row {row_num}")
    logger.error(f"📋 字段: {field_name}")
    logger.error(f"📝 规则内容:")
    logger.error(f'   "{rule_content}"')
    logger.error("")

    if error_message:
        logger.error(f"❌ 错误详情: {error_message}")
        logger.error("")
    else:
        logger.error("❌ Agent 无法理解此规则的转换逻辑。")
        logger.error("")

    logger.error("请选择下一步操作:")
    logger.error("  [1] 跳过此规则 (使用 empty 类型，字段值为空)")
    logger.error("  [2] 手动指定转换类型和配置")
    logger.error("  [3] 退出程序")
    logger.error("")

    while True:
        try:
            choice = input("请输入选项 [1/2/3]: ").strip()

            if choice == "1":
                logger.info(f"⏭ 跳过规则: {field_name} (将使用 empty 类型)")
                return "skip", {"transformation_type": "empty", "config": {}}

            elif choice == "2":
                logger.info("请手动配置转换规则:")
                return _prompt_manual_config(field_name)

            elif choice == "3":
                logger.info("用户选择退出程序")
                raise UserAbortError(f"用户在 Row {row_num} ({field_name}) 处终止执行")

            else:
                print("❌ 无效选项，请输入 1、2 或 3")

        except KeyboardInterrupt:
            print("\n")
            logger.info("用户中断执行 (Ctrl+C)")
            raise UserAbortError("用户通过 Ctrl+C 终止执行")


def _prompt_manual_config(field_name: str) -> Tuple[str, Dict[str, Any]]:
    """
    Prompt user to manually configure a transformation rule.

    Args:
        field_name: Target field name

    Returns:
        Tuple of ("manual", config_dict)
    """
    print("\n可用的转换类型:")
    print("  [1] fixed - 固定值")
    print("  [2] direct - 直接映射源字段")
    print("  [3] conditional_map - 条件映射")
    print("  [4] name_parser - 姓名解析 (LAST,FIRST)")
    print("  [5] split_extract - 字符串分割提取")
    print("  [6] empty - 空值")
    print()

    while True:
        type_choice = input("选择转换类型 [1-6]: ").strip()

        if type_choice == "1":
            value = input("输入固定值: ").strip()
            return "manual", {
                "transformation_type": "fixed",
                "config": {"value": value},
            }

        elif type_choice == "2":
            source = input("输入源字段名: ").strip()
            return "manual", {
                "transformation_type": "direct",
                "config": {"source_field": source},
            }

        elif type_choice == "3":
            source = input("输入源字段名: ").strip()
            print("输入映射规则 (格式: key1=value1,key2=value2)")
            mappings_str = input("映射: ").strip()
            default_val = input("默认值 (可选): ").strip() or None

            mappings = {}
            for pair in mappings_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    mappings[k.strip()] = v.strip()

            return "manual", {
                "transformation_type": "conditional_map",
                "config": {
                    "source_field": source,
                    "mappings": mappings,
                    "default": default_val,
                },
            }

        elif type_choice == "4":
            source = input("输入源字段名: ").strip()
            part = input("提取部分 [first/last]: ").strip().lower()
            return "manual", {
                "transformation_type": "name_parser",
                "config": {"source_field": source, "part": part},
            }

        elif type_choice == "5":
            source = input("输入源字段名: ").strip()
            delimiter = input("分隔符: ").strip()
            index = int(input("提取索引 (0开始): ").strip())
            return "manual", {
                "transformation_type": "split_extract",
                "config": {
                    "source_field": source,
                    "delimiter": delimiter,
                    "index": index,
                },
            }

        elif type_choice == "6":
            return "manual", {"transformation_type": "empty", "config": {}}

        else:
            print("❌ 无效选项，请输入 1-6")


def handle_error_gracefully(error: Exception, context: str = "") -> None:
    """
    Handle unexpected errors gracefully.

    Args:
        error: Exception that occurred
        context: Context description
    """
    logger.error("")
    logger.error("╔════════════════════════════════════════════════════════════════╗")
    logger.error("║                    系统错误                                     ║")
    logger.error("╚════════════════════════════════════════════════════════════════╝")
    logger.error("")
    logger.error(f"❌ 错误类型: {type(error).__name__}")
    logger.error(f"❌ 错误信息: {str(error)}")
    if context:
        logger.error(f"📍 上下文: {context}")
    logger.error("")
    logger.error("程序将终止执行。")
    logger.error("")
