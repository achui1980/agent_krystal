#!/usr/bin/env python3
"""
自主代码生成器 CLI
Agent编写、测试、修复代码直到可运行
"""

import sys
import os
from pathlib import Path

# 加载.env
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import click
from krystal_v2.case_generator.autonomous.autonomous_generator_v2 import (
    AutonomousCodeGeneratorV2 as AutonomousCodeGenerator,
)


@click.command()
@click.option("--rules", required=True, help="规则文件路径(rules.xlsx)")
@click.option("--source", required=True, help="Source模板路径(source.csv)")
@click.option("--expected", required=True, help="Expected模板路径(expected.txt)")
@click.option("--output", default="./generated_autonomous/", help="输出目录")
@click.option("--max-iterations", default=5, help="最大修复次数")
@click.option("--model", default=None, help="LLM模型(默认从.env读取)")
def generate(rules, source, expected, output, max_iterations, model):
    """
    使用自主代码生成器生成测试用例
    
    这个命令会：
    1. Agent分析规则生成规格书
    2. Agent生成Python代码
    3. Agent自我测试代码
    4. 如果失败，Agent分析错误并修复
    5. 循环直到成功
    6. 执行最终代码生成数据
    
    示例:
        python autonomous_cli.py \\
            --rules case/rules.xlsx \\
            --source case/source.csv \\
            --expected case/expected.txt \\
            --max-iterations 5
    """

    click.echo("=" * 80)
    click.echo("🤖 Krystal 自主代码生成器")
    click.echo("   Agent编写→测试→修复→生成")
    click.echo("=" * 80)
    click.echo()

    # 验证输入文件
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
    click.echo(f"🔄 最大修复次数: {max_iterations}")
    click.echo()

    try:
        # 设置模型（如果指定）
        if model:
            os.environ["OPENAI_MODEL"] = model

        # 创建生成器
        generator = AutonomousCodeGenerator(max_iterations=max_iterations)

        # 运行生成
        result = generator.run(
            rules_path=rules, source_path=source, expected_path=expected
        )

        if result["success"]:
            click.echo()
            click.echo("=" * 80)
            click.echo("✅ 生成成功！")
            click.echo("=" * 80)
            click.echo()
            click.echo(f"📊 统计信息:")
            click.echo(f"   - 迭代次数: {result['iterations']} 轮")
            click.echo(f"   - 生成代码: {result['code_path']}")
            click.echo(f"   - 测试数据: {len(result['data'])} 条")
            click.echo()
            click.echo(f"📁 输出文件:")
            click.echo(f"   - {output}/generated_data.json")
            click.echo(f"   - {output}/data_generator_v{result['iterations']}.py")
        else:
            click.echo()
            click.echo("=" * 80)
            click.echo("❌ 生成失败")
            click.echo("=" * 80)
            click.echo()
            click.echo(f"错误: {result.get('error', 'Unknown error')}")
            if "last_error" in result:
                error = result["last_error"]
                click.echo(f"错误类型: {error.get('error_type', 'Unknown')}")
                click.echo(f"错误信息: {error.get('error_message', 'No message')}")
            return 1

    except Exception as e:
        click.echo()
        click.echo("=" * 80)
        click.echo("❌ 执行异常")
        click.echo("=" * 80)
        click.echo(f"错误: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(generate())
