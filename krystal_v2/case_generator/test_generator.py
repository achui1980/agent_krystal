#!/usr/bin/env python3
"""
测试脚本 - 验证Case Generator功能
"""

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from krystal_v2.case_generator.core.material_loader import MaterialLoader
from krystal_v2.case_generator.core.rule_parser import RuleParser
from krystal_v2.case_generator.core.rule_validator import RuleValidator
from krystal_v2.case_generator.core.data_generator import DataGenerator
from krystal_v2.case_generator.core.expected_calculator import ExpectedCalculator
from krystal_v2.case_generator.core.report_generator import ReportGenerator
from krystal_v2.case_generator.exporters.file_exporter import FileExporter


def test_material_loader():
    """测试材料加载"""
    print("\n=== 测试 MaterialLoader ===")
    loader = MaterialLoader()

    # 加载规则
    rules = loader.load_rules("case/rules.xlsx")
    print(f"✓ 加载了 {len(rules)} 条规则")
    print(f"  第一条规则: {rules[0]}")

    # 加载source sample
    source = loader.load_source_sample("case/source.csv")
    print(f"✓ 加载了 {len(source)} 行source数据")

    # 加载expected sample
    expected = loader.load_expected_sample("case/expected.txt")
    print(f"✓ 加载了 {len(expected['data'])} 行expected数据")
    print(f"  元数据: {expected['metadata']}")
    print(f"  字段数: {len(expected['headers'])}")

    return rules, source, expected


def test_rule_parser(rules):
    """测试规则解析"""
    print("\n=== 测试 RuleParser ===")
    parser = RuleParser()

    parsed_rules = parser.parse_rules(rules)
    print(f"✓ 解析了 {len(parsed_rules)} 条规则")

    # 统计规则类型
    rule_types = {}
    for rule in parsed_rules:
        rt = rule["rule_type"]
        rule_types[rt] = rule_types.get(rt, 0) + 1

    print("\n规则类型分布:")
    for rt, count in rule_types.items():
        print(f"  - {rt}: {count}")

    # 展示几个规则的详情
    print("\n示例规则:")
    for rule in parsed_rules[:3]:
        print(
            f"  {rule['target_field']} -> {rule['rule_type']} (置信度: {rule['confidence']})"
        )

    return parsed_rules


def test_rule_validator(parsed_rules, source, expected):
    """测试规则验证"""
    print("\n=== 测试 RuleValidator ===")
    validator = RuleValidator()

    result = validator.validate(parsed_rules, source, expected)
    print(f"✓ 准确率: {result['accuracy']:.2%}")
    print(f"✓ 匹配: {result['matched_fields']}/{result['total_fields']}")

    if result["mismatches"]:
        print(f"\n⚠️  发现 {len(result['mismatches'])} 处不匹配:")
        for m in result["mismatches"][:5]:  # 只显示前5个
            print(
                f"  行{m['row']}, 字段{m['field']}: 预测={m['predicted']}, 实际={m['actual']}"
            )

    return result


def test_data_generator():
    """测试数据生成"""
    print("\n=== 测试 DataGenerator ===")
    generator = DataGenerator()

    # 生成正常数据
    normal, normal_points = generator.generate_normal_cases(3)
    print(f"✓ 生成 {len(normal)} 行正常数据")
    print(f"  示例: {normal[0]['Member']}, Product={normal[0]['Product']}")

    # 生成异常数据
    scenarios = [{"name": "test", "modifications": {"MEDICARE_ID": ""}}]
    abnormal, abnormal_points = generator.generate_abnormal_cases(scenarios)
    print(f"✓ 生成 {len(abnormal)} 行异常数据")
    print(f"  MEDICARE_ID={abnormal[0]['MEDICARE_ID']}")

    # 生成边界数据
    boundary, boundary_points = generator.generate_boundary_cases(2)
    print(f"✓ 生成 {len(boundary)} 行边界数据")
    print(f"  DOB={boundary[0]['DOB']}")

    # 合并所有测试点
    all_test_points = normal_points + abnormal_points + boundary_points

    return normal + abnormal + boundary, all_test_points


def test_expected_calculator(parsed_rules, source_data):
    """测试预期结果计算"""
    print("\n=== 测试 ExpectedCalculator ===")
    calculator = ExpectedCalculator(parsed_rules)

    expected = calculator.calculate(source_data)
    print(f"✓ 计算了 {len(expected)} 行expected数据")

    # 展示第一行的一些字段
    if expected:
        row = expected[0]
        print(f"\n第一行数据示例:")
        print(f"  CARRIER_STATUS_MAP: {row.get('CARRIER_STATUS_MAP', 'N/A')}")
        print(f"  PRODUCT_LINE: {row.get('PRODUCT_LINE', 'N/A')}")
        print(f"  FIRST_NAME: {row.get('FIRST_NAME', 'N/A')}")
        print(f"  LAST_NAME: {row.get('LAST_NAME', 'N/A')}")

    return expected


def test_file_exporter(
    source_data, expected_data, headers, full_report, markdown_report
):
    """测试文件导出"""
    print("\n=== 测试 FileExporter ===")
    exporter = FileExporter("test_output")

    # 导出source
    source_path = exporter.export_source_csv(source_data)
    print(f"✓ Source导出: {source_path}")

    # 导出expected
    metadata = {
        "ACTION_ID": "test",
        "SERVICE_MAP_ID": "00000000",
        "SOURCE_TOKEN": "test_token",
    }
    expected_path = exporter.export_expected_txt(expected_data, headers, metadata)
    print(f"✓ Expected导出: {expected_path}")

    # 导出JSON报告
    report_path = exporter.export_report_json(full_report, "detailed_report.json")
    print(f"✓ JSON报告导出: {report_path}")

    # 导出Markdown报告
    md_path = exporter.export_markdown_report(markdown_report, "detailed_report.md")
    print(f"✓ Markdown报告导出: {md_path}")


def main():
    """主测试函数"""
    print("=" * 60)
    print("Krystal Case Generator - 功能测试")
    print("=" * 60)

    try:
        # 1. 加载材料
        rules, source, expected = test_material_loader()

        # 2. 解析规则
        parsed_rules = test_rule_parser(rules)

        # 3. 验证规则
        validation = test_rule_validator(parsed_rules, source, expected)

        # 4. 生成测试数据（包含测试点信息）
        test_data, test_points = test_data_generator()

        # 5. 计算Expected
        calculated = test_expected_calculator(parsed_rules, test_data)

        # 6. 生成详细报告
        print("\n=== 生成测试报告 ===")
        report_gen = ReportGenerator(parsed_rules, test_data, test_points, calculated)
        full_report = report_gen.generate_full_report()
        markdown_report = report_gen.generate_markdown_report()
        print(f"✓ 报告生成完成")
        print(f"  - 测试点总数: {full_report['test_points']['total']}")
        print(
            f"  - 规则覆盖率: {full_report['rule_coverage']['summary']['coverage_rate']}%"
        )

        # 7. 导出文件
        test_file_exporter(
            test_data, calculated, expected["headers"], full_report, markdown_report
        )

        print("\n" + "=" * 60)
        print("✅ 所有测试通过!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
