#!/usr/bin/env python3
"""
智能Agent CLI - 基于CrewAI的测试用例生成
"""

import sys
import os
from pathlib import Path

# 先加载.env文件（必须在任何其他导入之前）
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

import click

from krystal_v2.case_generator.agents.intelligent_flow import (
    IntelligentCaseGenerationFlow,
)


@click.command()
@click.option("--rules", required=True, help="规则文件路径(rules.xlsx)")
@click.option("--source", required=True, help="Source模板路径(source.csv)")
@click.option("--expected", required=True, help="Expected模板路径(expected.txt)")
@click.option("--output", default="./generated_intelligent/", help="输出目录")
@click.option("--model", default="gpt-4o", help="使用的LLM模型")
def generate(rules, source, expected, output, model):
    """
    使用智能Agent生成测试用例
    
    这个命令会启动一个基于CrewAI的Agent，它会：
    1. 自主阅读并理解规则文档
    2. 分析数据结构差异
    3. 设计测试策略
    4. 生成测试数据
    5. 执行规则转换
    6. 生成详细报告
    
    示例:
        python intelligent_cli.py \\
            --rules case/rules.xlsx \\
            --source case/source.csv \\
            --expected case/expected.txt \\
            --output ./generated/
    """

    click.echo("=" * 70)
    click.echo("🤖 Krystal 智能测试用例生成器 (Agent-based)")
    click.echo("=" * 70)
    click.echo()

    # 验证输入文件存在
    for path, name in [
        (rules, "规则文件"),
        (source, "Source文件"),
        (expected, "Expected文件"),
    ]:
        if not Path(path).exists():
            click.echo(f"❌ 错误: {name}不存在: {path}")
            return 1

    click.echo(f"📄 规则文件: {rules}")
    click.echo(f"📄 Source文件: {source}")
    click.echo(f"📄 Expected文件: {expected}")
    click.echo(f"📁 输出目录: {output}")
    click.echo(f"🤖 LLM模型: {model}")
    click.echo()

    try:
        # 创建并运行智能流程
        flow = IntelligentCaseGenerationFlow()

        click.echo("🚀 启动智能Agent...")
        click.echo("   Agent将自主完成以下任务：")
        click.echo("   1️⃣  理解规则文档")
        click.echo("   2️⃣  分析数据结构")
        click.echo("   3️⃣  设计测试策略")
        click.echo("   4️⃣  生成测试数据")
        click.echo("   5️⃣  执行规则转换")
        click.echo("   6️⃣  生成详细报告")
        click.echo()

        result = flow.run(
            rules_path=rules,
            source_path=source,
            expected_path=expected,
            output_dir=output,
        )

        click.echo()
        click.echo("=" * 70)
        click.echo("✅ 智能生成完成!")
        click.echo("=" * 70)
        click.echo()
        click.echo(f"📁 输出文件:")
        click.echo(f"   - {output}/source.csv")
        click.echo(f"   - {output}/expected.txt")
        click.echo(f"   - {output}/detailed_report.json")
        click.echo(f"   - {output}/detailed_report.md")
        click.echo()

    except Exception as e:
        click.echo()
        click.echo("=" * 70)
        click.echo("❌ 生成失败")
        click.echo("=" * 70)
        click.echo(f"错误: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(generate())
