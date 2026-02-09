#!/usr/bin/env python3
"""
Krystal V3 - ETL Test Data Generator

命令行入口
"""

import sys
import json
from pathlib import Path
from typing import Optional

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from core.rule_parser import RuleParser
from core.data_generator import DataGenerator
from models.rule import TransformationConfig, Rule
from utils.logger import get_logger


def load_config_from_json(config_path: str) -> TransformationConfig:
    """从 JSON 文件加载配置"""
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 创建 Rule 对象列表
    rules = []
    for rule_data in data.get("rules", []):
        # 对于 direct 类型，从 config.source_field 获取 carrier_column_name
        carrier_column = rule_data.get("carrier_column_name", "")
        if not carrier_column and rule_data.get("transformation_type") == "direct":
            carrier_column = rule_data.get("config", {}).get("source_field", "")

        rule = Rule(
            cs_column_name=rule_data["cs_column_name"],
            carrier_column_name=carrier_column,
            default_value=rule_data.get("default_value", ""),
            special_rules=rule_data.get("special_rules", ""),
            note=rule_data.get("note", ""),
            transformation_type=rule_data["transformation_type"],
            config=rule_data.get("config", {}),
        )
        rules.append(rule)

    return TransformationConfig(
        case_name=data["case_name"],
        rules=rules,
        field_order=data["field_order"],
        source_fields=data.get("source_fields", []),
    )


def generate_from_config(config_path: str, count: int = 10, output_dir: str = "output"):
    """
    使用配置文件生成数据

    Args:
        config_path: 配置文件路径
        count: 生成数量
        output_dir: 输出目录
    """
    logger = get_logger()
    logger.section("Krystal V3 - 数据生成器")

    # 加载配置
    logger.step(1, "加载配置")
    logger.info(f"📄 配置文件: {config_path}")

    config = load_config_from_json(config_path)
    logger.success(f"配置加载完成: {config.case_name}")
    logger.info(f"  - 规则数: {len(config.rules)}")
    logger.info(f"  - 字段数: {len(config.field_order)}")
    logger.info(f"  - 源字段: {len(config.source_fields)}")

    # 创建生成器并生成数据
    generator = DataGenerator(config)
    result = generator.generate_and_export(count, output_dir)

    # 输出结果
    logger.section("生成结果")
    logger.success(f"✅ 数据生成完成！")
    logger.info(f"📁 输出文件: {result['expected']}")
    logger.info(f"📊 记录数: {result['record_count']}")
    logger.info(f"📊 字段数: {result['field_count']}")

    return result


def generate_from_rules(rules_path: str, count: int = 10, output_dir: str = "output"):
    """
    直接从 rules.xlsx 生成数据（解析规则）

    Args:
        rules_path: rules.xlsx 文件路径
        count: 生成数量
        output_dir: 输出目录
    """
    logger = get_logger()
    logger.section("Krystal V3 - 数据生成器（直接解析规则）")

    # 解析规则
    parser = RuleParser(rules_path)
    config = parser.parse()

    # 创建生成器并生成数据
    generator = DataGenerator(config)
    result = generator.generate_and_export(count, output_dir)

    # 输出结果
    logger.section("生成结果")
    logger.success(f"✅ 数据生成完成！")
    logger.info(f"📁 输出文件: {result['expected']}")
    logger.info(f"📊 记录数: {result['record_count']}")
    logger.info(f"📊 字段数: {result['field_count']}")

    return result


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Krystal V3 - ETL Test Data Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用配置文件生成
  python -m krystal_v3.cli generate --config krystal_v3/config/humanaS10_config.json
  
  # 直接从 rules.xlsx 生成
  python -m krystal_v3.cli generate --rules case/humanaS10/rules.xlsx
  
  # 指定数量和输出目录
  python -m krystal_v3.cli generate --config config.json --count 20 --output my_output
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # generate 命令
    gen_parser = subparsers.add_parser("generate", help="生成测试数据")
    gen_parser.add_argument("--config", type=str, help="配置文件路径 (JSON)")
    gen_parser.add_argument("--rules", type=str, help="规则文件路径 (rules.xlsx)")
    gen_parser.add_argument(
        "--count", type=int, default=10, help="生成记录数量 (默认: 10)"
    )
    gen_parser.add_argument(
        "--output",
        type=str,
        default="krystal_v3/output",
        help="输出目录 (默认: krystal_v3/output)",
    )

    # analyze 命令
    analyze_parser = subparsers.add_parser("analyze", help="分析规则文件")
    analyze_parser.add_argument(
        "--rules", type=str, required=True, help="规则文件路径 (rules.xlsx)"
    )
    analyze_parser.add_argument(
        "--output",
        type=str,
        default="krystal_v3/config/auto_generated.json",
        help="输出配置文件路径 (默认: krystal_v3/config/auto_generated.json)",
    )

    args = parser.parse_args()

    if args.command == "generate":
        if args.config:
            generate_from_config(args.config, args.count, args.output)
        elif args.rules:
            generate_from_rules(args.rules, args.count, args.output)
        else:
            print("❌ 错误: 必须指定 --config 或 --rules")
            parser.print_help()
            sys.exit(1)
    elif args.command == "analyze":
        from agents.rule_analyzer import RuleAnalyzerAgent

        agent = RuleAnalyzerAgent()
        config = agent.analyze(args.rules)
        agent.save_config(config, args.output)

        logger = get_logger()
        logger.success(f"✅ 分析完成！配置已保存到: {args.output}")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
