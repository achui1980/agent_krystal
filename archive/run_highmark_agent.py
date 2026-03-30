#!/usr/bin/env python3
"""
运行 Agent 生成的 highmark 数据生成器
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "generated_autonomous"))
sys.path.insert(0, str(Path(__file__).parent / "krystal_v2/case_generator/utils"))

from data_generator_v2_3 import SmartDataGenerator
from shared_rules import EXPECTED_FIELD_ORDER


def main():
    print("=" * 80)
    print("🚀 运行 Agent 生成的 highmark 数据生成器")
    print("=" * 80)
    print()

    gen = SmartDataGenerator()

    # 生成正常数据
    print("1. 生成正常场景数据...")
    normal_data = gen.generate_normal_cases(10)
    print(f"   ✅ 生成 {len(normal_data)} 条正常数据")

    # 添加固定值字段
    print("\n2. 补充固定值字段...")
    for row in normal_data:
        # CARRIER_FAMILY_ID 直接使用字符串格式
        row["CARRIER_FAMILY_ID"] = "66,175,206"
        row["IS_PAID"] = "1"
        row["BUSINESS_LINE"] = "2"
        row["MEMBER_NUMBER"] = "1"
        row["CATEGORY_CLASS_ID"] = "1"
    print(f"   ✅ 已为 {len(normal_data)} 条数据添加固定值")

    # 保存数据
    output_dir = Path("generated_autonomous/output")
    output_dir.mkdir(exist_ok=True)

    print("\n3. 保存数据到文件...")

    def save_to_expected_format(
        data, filepath, action_id="highmark-cs-data-integration"
    ):
        if not data:
            return

        all_fields = EXPECTED_FIELD_ORDER

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"ACTION_ID:{action_id}\n")
            f.write("SERVICE_MAP_ID:10003358\n")
            f.write("SOURCE_TOKEN:auto_generated_token\n")
            f.write("\n")
            f.write("|".join(all_fields) + "\n")

            for row in data:
                values = [str(row.get(field, "")) for field in all_fields]
                f.write("|".join(values) + "\n")

    save_to_expected_format(
        normal_data, str(output_dir / "expected_highmark_agent.txt")
    )
    print(f"   ✅ 保存数据: {output_dir / 'expected_highmark_agent.txt'}")

    # 显示样本
    print("\n" + "=" * 80)
    print("📊 生成统计")
    print("=" * 80)
    print(f"正常数据: {len(normal_data)} 条")

    if normal_data:
        print("\n样本数据 (第一条):")
        first_item = normal_data[0]
        for key in [
            "PRODUCT_LINE",
            "CARRIER_FAMILY_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "STATE",
        ]:
            print(f"  {key}: {first_item.get(key, '')}")

    print("\n" + "=" * 80)
    print("✅ highmark 数据生成完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()
