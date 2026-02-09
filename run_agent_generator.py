#!/usr/bin/env python3
"""
运行 Agent 生成的数据生成器，生成 source 和 expected 数据
"""

import sys
from pathlib import Path

# 添加生成器路径
sys.path.insert(0, str(Path(__file__).parent / "generated_autonomous"))
sys.path.insert(0, str(Path(__file__).parent / "krystal_v2/case_generator/utils"))

from data_generator_v2_3 import SmartDataGenerator
from shared_rules import EXPECTED_FIELD_ORDER


def main():
    print("=" * 80)
    print("🚀 运行 Agent 生成的数据生成器")
    print("=" * 80)
    print()

    # 创建生成器
    gen = SmartDataGenerator()

    # 生成正常数据
    print("1. 生成正常场景数据...")
    normal_data = gen.generate_normal_cases(10)
    print(f"   ✅ 生成 {len(normal_data)} 条正常数据")

    # 注意：生成的代码只包含 generate_normal_cases 方法
    # 异常和边界数据生成需要完整实现
    print("\n2. 补充固定值字段...")

    # 添加 CARRIER_FAMILY_ID 和其他固定值
    # CARRIER_FAMILY_ID 从 rules.xlsx 读取，格式为字符串（如 "66,175,206" 或 "66"）
    # 注意：不是数字格式化，而是多个独立的 carrier family ID
    carrier_family_id = "66,175,206"  # 应从 rules.xlsx 读取的实际值

    for row in normal_data:
        row["CARRIER_FAMILY_ID"] = carrier_family_id
        row["IS_PAID"] = "1"
        row["BUSINESS_LINE"] = "2"
        row["MEMBER_NUMBER"] = "1"
        row["CATEGORY_CLASS_ID"] = "1"

    print(f"   ✅ 已为 {len(normal_data)} 条数据添加固定值")

    # 保存数据
    output_dir = Path("generated_autonomous/output")
    output_dir.mkdir(exist_ok=True)

    print("\n3. 保存数据到文件...")

    # 保存 Expected 格式数据（管道符分隔，包含元数据，93个字段）
    def save_to_expected_format(data, filepath, action_id="test-data-integration"):
        if not data:
            return

        # 使用完整的93个字段列表（EXPECTED_FIELD_ORDER 已经包含 LAST_TOUCHED_DATE）
        all_fields = EXPECTED_FIELD_ORDER

        with open(filepath, "w", encoding="utf-8") as f:
            # 1. 元数据头（3行）
            f.write(f"ACTION_ID:{action_id}\n")
            f.write("SERVICE_MAP_ID:10003358\n")
            f.write("SOURCE_TOKEN:auto_generated_token\n")
            f.write("\n")  # 空行

            # 2. 字段头（管道符分隔，93个字段）
            f.write("|".join(all_fields) + "\n")

            # 3. 数据行（管道符分隔）
            for row in data:
                values = [str(row.get(field, "")) for field in all_fields]
                f.write("|".join(values) + "\n")

    save_to_expected_format(normal_data, str(output_dir / "expected_agent.txt"))
    print(f"   ✅ 保存正常数据: {output_dir / 'expected_agent.txt'}")

    # 显示样本
    print("\n" + "=" * 80)
    print("📊 生成统计")
    print("=" * 80)
    print(f"正常数据: {len(normal_data)} 条")

    if normal_data:
        print("\n样本数据 (第一条):")
        first_item = normal_data[0]
        for key in list(first_item.keys())[:10]:
            if not key.startswith("_"):
                print(f"  {key}: {first_item[key]}")

    print("\n" + "=" * 80)
    print("✅ 数据生成完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()
