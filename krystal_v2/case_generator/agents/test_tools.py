#!/usr/bin/env python3
"""
快速测试 - 验证Agent工具功能
"""

import sys
import os
from pathlib import Path

# 加载.env
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

from krystal_v2.case_generator.agents.tools import (
    ReadRulesTool,
    AnalyzeDataTool,
    GenerateDataTool,
    ExecuteTransformTool,
)


def test_tools():
    """测试Agent工具"""
    print("=" * 70)
    print("🧪 测试Agent工具集")
    print("=" * 70)

    # 测试1: 读取规则
    print("\n1️⃣ 测试 read_rules 工具")
    read_tool = ReadRulesTool()
    result = read_tool._run("case/rules.xlsx")
    if result["success"]:
        print(f"   ✅ 成功读取 {result['total_rules']} 条规则")
        print(f"   📊 前3条规则:")
        for i, rule in enumerate(result["rules"][:3]):
            print(
                f"      {i + 1}. {rule['target_field']} <- {rule['source_field'] or 'N/A'}"
            )
    else:
        print(f"   ❌ 失败: {result['message']}")

    # 测试2: 分析数据结构
    print("\n2️⃣ 测试 analyze_data_structure 工具")
    analyze_tool = AnalyzeDataTool()
    result = analyze_tool._run("case/source.csv", "case/expected.txt")
    if result["success"]:
        print(f"   ✅ Source: {len(result['source_fields'])}个字段")
        print(f"   ✅ Expected: {len(result['expected_fields'])}个字段")
        print(f"   📋 Source字段: {', '.join(result['source_fields'][:5])}...")
    else:
        print(f"   ❌ 失败: {result['message']}")

    # 测试3: 生成数据
    print("\n3️⃣ 测试 generate_test_data 工具")
    gen_tool = GenerateDataTool()
    result = gen_tool._run("生成5条PDP产品的正常数据", 5)
    if result["success"]:
        print(f"   ✅ 成功生成 {result['count']} 条数据")
        print(f"   📝 场景: {result['data'][0]['_scenario']}")
    else:
        print(f"   ❌ 失败: {result['message']}")

    # 测试4: 执行转换
    print("\n4️⃣ 测试 execute_transformation 工具")
    exec_tool = ExecuteTransformTool()
    source_data = [
        {"Product": "PDP", "Member": "MOUSE,MICKEY"},
        {"Product": "LPPO", "Member": "DUCK,DONALD"},
    ]
    result = exec_tool._run(source_data, "Product→PRODUCT_LINE条件映射")
    if result["success"]:
        print(f"   ✅ 成功转换 {result['input_count']} 条数据")
        print(f"   📝 转换规则已应用: {result['data'][0]['_transformation_applied']}")
    else:
        print(f"   ❌ 失败: {result['message']}")

    print("\n" + "=" * 70)
    print("✅ 所有工具测试完成!")
    print("=" * 70)


if __name__ == "__main__":
    test_tools()
