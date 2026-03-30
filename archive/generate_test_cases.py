#!/usr/bin/env python3
"""
测试用例生成器 - 使用case/目录下的规则和数据生成测试用例
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from generated_autonomous.data_generator_final import SmartDataGenerator
import csv
import json


def main():
    print("=" * 80)
    print("📋 Krystal V2 测试用例生成器")
    print("=" * 80)
    print()

    # 初始化生成器
    gen = SmartDataGenerator(
        rules_path="case/rules.xlsx",
        source_path="case/source.csv",
        expected_path="case/expected.txt",
        strict=False,
    )

    # 构建规格书
    print("📝 步骤1: 解析规则文件...")
    spec = gen.build_spec()

    print(f"   ✅ Source字段: {len(spec['source_fields'])} 个")
    print(f"   ✅ Expected字段: {len(spec['expected_fields'])} 个")
    print(f"   ✅ 映射规则: {len(spec['field_mappings'])} 条")
    print(f"   ✅ 使用字段: {len(spec['used_source_fields'])} 个")
    print(f"   ✅ 未使用字段: {len(spec['unused_source_fields'])} 个")
    print()

    # 保存规格书
    output_dir = Path("generated_autonomous/output")
    output_dir.mkdir(exist_ok=True, parents=True)
    gen.export_spec_json(spec, output_dir / "spec.json")
    print(f"   💾 规格书已保存: output/spec.json")
    print()

    # 生成测试用例
    print("🎯 步骤2: 生成测试用例...")
    print()

    # 2.1 正常场景
    print("   📊 生成正常场景数据...")
    normal = gen.generate_normal_cases(10)
    gen.save_to_csv(normal, output_dir / "normal.csv")
    print(f"      ✅ 已生成 {len(normal)} 条正常场景数据 -> normal.csv")

    # 2.2 异常场景
    print("   📊 生成异常场景数据...")
    abnormal = gen.generate_abnormal_cases(
        [
            {"name": "bad_date_format", "override": {"DOB": "20250230"}},
            {"name": "blank_names", "override": {"FIRST_NAME": "   ", "LAST_NAME": ""}},
            {
                "name": "unknown_state_city",
                "override": {"STATE": "ZZ", "CITY": "Nowhere"},
            },
            {"name": "missing_source_field_zip", "drop_source_fields": ["ZIP"]},
            {"name": "bad_amount", "override": {"AMOUNT": "not_a_number"}},
        ]
    )
    gen.save_to_csv(abnormal, output_dir / "abnormal.csv")
    print(f"      ✅ 已生成 {len(abnormal)} 条异常场景数据 -> abnormal.csv")

    # 2.3 边界场景
    print("   📊 生成边界场景数据...")
    boundary = gen.generate_boundary_cases(7)
    gen.save_to_csv(boundary, output_dir / "boundary.csv")
    print(f"      ✅ 已生成 {len(boundary)} 条边界场景数据 -> boundary.csv")
    print()

    # 2.4 Source格式数据（用于ETL输入）
    print("   📊 生成Source格式输入数据...")
    source_fields = spec["source_fields"]
    source_normal = [gen._generate_source_row_normal(source_fields) for _ in range(10)]
    gen.save_to_csv(source_normal, output_dir / "source_normal.csv")
    print(f"      ✅ 已生成 {len(source_normal)} 条Source格式数据 -> source_normal.csv")
    print()

    # 输出统计
    print("=" * 80)
    print("✅ 测试用例生成完成！")
    print("=" * 80)
    print()
    print("📁 输出文件:")
    print(f"   📄 output/spec.json        - 映射规格书")
    print(f"   📄 output/normal.csv       - Expected格式正常场景 (10条)")
    print(f"   📄 output/abnormal.csv     - Expected格式异常场景 (5条)")
    print(f"   📄 output/boundary.csv     - Expected格式边界场景 (7条)")
    print(f"   📄 output/source_normal.csv - Source格式输入数据 (10条)")
    print()

    # 显示数据样本
    print("📋 数据样本 (source_normal.csv 第一条):")
    print("-" * 80)
    if source_normal:
        row = source_normal[0]
        used_fields = spec["used_source_fields"]
        for field in used_fields[:8]:
            value = row.get(field, "")
            print(f"   {field:20s}: {value}")
        if len(used_fields) > 8:
            print(f"   ... 和另外 {len(used_fields) - 8} 个字段")
    print()

    # 诊断信息
    d = gen.diagnostics()
    if any(v for v in d.values() if isinstance(v, list) and len(v) > 0):
        print("⚠️  诊断信息:")
        for k, v in d.items():
            if isinstance(v, list) and len(v) > 0:
                print(f"   {k}: {len(v)}")
    else:
        print("✅ 所有规则解析成功，无错误！")

    print()
    print("🎉 现在可以使用这些测试用例进行ETL测试了！")
    print()


if __name__ == "__main__":
    main()
