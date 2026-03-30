"""
CLI - 命令行入口
"""

import click
import json
from pathlib import Path
from .core.material_loader import MaterialLoader
from .core.rule_parser import RuleParser
from .core.rule_validator import RuleValidator
from .core.data_generator import DataGenerator
from .core.expected_calculator import ExpectedCalculator
from .exporters.file_exporter import FileExporter


@click.command()
@click.option("--rules", required=True, help="规则文件路径(rules.xlsx)")
@click.option("--sample-source", help="Source样本路径(source.csv)")
@click.option("--sample-expected", help="Expected样本路径(expected.txt)")
@click.option("--output", default="./generated/", help="输出目录")
@click.option("--count-normal", default=10, help="正常场景数量")
@click.option("--count-abnormal", default=5, help="异常场景数量")
@click.option("--count-boundary", default=3, help="边界场景数量")
def generate(
    rules,
    sample_source,
    sample_expected,
    output,
    count_normal,
    count_abnormal,
    count_boundary,
):
    """生成测试用例"""

    click.echo("🚀 开始生成测试用例...")

    # 1. 加载材料
    click.echo("📄 加载规则文件...")
    loader = MaterialLoader()
    rules_data = loader.load_rules(rules)
    click.echo(f"   ✓ 加载了 {len(rules_data)} 条规则")

    source_sample = None
    expected_sample = None

    if sample_source:
        source_sample = loader.load_source_sample(sample_source)
        click.echo(f"   ✓ 加载了 {len(source_sample)} 行source样本")

    if sample_expected:
        expected_sample = loader.load_expected_sample(sample_expected)
        click.echo(f"   ✓ 加载了 {len(expected_sample['data'])} 行expected样本")

    # 2. 解析规则
    click.echo("🔍 解析规则...")
    parser = RuleParser(llm_client=None)  # MVP阶段不使用LLM
    parsed_rules = parser.parse_rules(rules_data)

    rule_types = {}
    for rule in parsed_rules:
        rt = rule["rule_type"]
        rule_types[rt] = rule_types.get(rt, 0) + 1

    click.echo(f"   ✓ 规则类型分布:")
    for rt, count in rule_types.items():
        click.echo(f"     - {rt}: {count}")

    # 3. 验证规则（如果有sample）
    validation_result = None
    if source_sample and expected_sample:
        click.echo("✅ 验证规则...")
        validator = RuleValidator()
        validation_result = validator.validate(
            parsed_rules, source_sample, expected_sample
        )
        click.echo(f"   ✓ 准确率: {validation_result['accuracy']:.1%}")
        click.echo(
            f"   ✓ 匹配: {validation_result['matched_fields']}/{validation_result['total_fields']}"
        )

        if validation_result["mismatches"]:
            click.echo(f"   ⚠️  发现 {len(validation_result['mismatches'])} 处不匹配")

    # 4. 生成测试数据
    click.echo("🎲 生成测试数据...")
    generator = DataGenerator()

    normal_cases = generator.generate_normal_cases(count_normal)
    click.echo(f"   ✓ 生成 {len(normal_cases)} 行正常数据")

    # 异常场景定义
    abnormal_scenarios = [
        {"name": "missing_medicare_id", "modifications": {"MEDICARE_ID": ""}},
        {"name": "invalid_product", "modifications": {"Product": "INVALID"}},
        {"name": "wrong_date_format", "modifications": {"DOB": "07/28/1949"}},
        {"name": "member_format_error", "modifications": {"Member": "MICKEY MOUSE"}},
        {
            "name": "date_logic_error",
            "modifications": {"Eff_Date": "2025-12-31", "Term_Date": "2025-01-01"},
        },
    ]

    abnormal_cases = generator.generate_abnormal_cases(
        abnormal_scenarios[:count_abnormal]
    )
    click.echo(f"   ✓ 生成 {len(abnormal_cases)} 行异常数据")

    boundary_cases = generator.generate_boundary_cases(count_boundary)
    click.echo(f"   ✓ 生成 {len(boundary_cases)} 行边界数据")

    all_cases = normal_cases + abnormal_cases + boundary_cases

    # 5. 计算Expected
    click.echo("🧮 计算预期结果...")
    calculator = ExpectedCalculator(parsed_rules)
    expected_data = calculator.calculate(all_cases)
    click.echo(f"   ✓ 计算完成")

    # 6. 导出文件
    click.echo("💾 导出文件...")
    exporter = FileExporter(output)

    source_path = exporter.export_source_csv(all_cases)
    click.echo(f"   ✓ {source_path}")

    # 使用sample的metadata和headers，如果没有则构造
    if expected_sample:
        metadata = expected_sample["metadata"]
        headers = expected_sample["headers"]
    else:
        metadata = {
            "ACTION_ID": "generated-case",
            "SERVICE_MAP_ID": "00000000",
            "SOURCE_TOKEN": "generated",
        }
        headers = list(expected_data[0].keys()) if expected_data else []

    expected_path = exporter.export_expected_txt(expected_data, headers, metadata)
    click.echo(f"   ✓ {expected_path}")

    # 生成报告
    report = {
        "generation_info": {
            "total_rules": len(rules_data),
            "rule_types": rule_types,
            "total_cases": len(all_cases),
            "case_breakdown": {
                "normal": len(normal_cases),
                "abnormal": len(abnormal_cases),
                "boundary": len(boundary_cases),
            },
        },
        "validation": validation_result,
        "output_files": {"source_csv": source_path, "expected_txt": expected_path},
    }

    report_path = exporter.export_report_json(report)
    click.echo(f"   ✓ {report_path}")

    click.echo("\n✨ 生成完成!")


if __name__ == "__main__":
    generate()
