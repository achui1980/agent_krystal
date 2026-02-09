#!/usr/bin/env python3
"""
使用 Utils 库生成 highmark 测试数据
基于修正后的 rules.xlsx (CARRIER_FAMILY_ID = 23)
"""

import sys
from pathlib import Path
import openpyxl

sys.path.insert(0, str(Path(__file__).parent / "krystal_v2/case_generator/utils"))

from shared_rules import EXPECTED_FIELD_ORDER, SHARED_FIXED_VALUES
from data_converters import safe_get


def main():
    print("=" * 80)
    print("🚀 生成 highmark 测试数据（基于修正的 rules.xlsx）")
    print("=" * 80)
    print()

    # 读取修正后的 rules.xlsx 获取 CARRIER_FAMILY_ID
    wb = openpyxl.load_workbook("case/highmark/rules.xlsx", data_only=True)
    ws = wb["Sheet1"]

    carrier_family_id = None
    for row in ws.iter_rows(min_row=7, max_row=100):
        cs_col_name = row[1].value
        default_value = row[3].value

        if cs_col_name == "CARRIER_FAMILY_ID":
            carrier_family_id = str(default_value) if default_value is not None else ""
            break

    print(f"✅ 从 rules.xlsx 读取 CARRIER_FAMILY_ID: {carrier_family_id}")
    print()

    # 生成 5 条测试数据
    print("生成测试数据...")
    test_data = []

    sample_records = [
        {
            "APPLICANT_FIRST_NAME": "JOE",
            "APPLICANT_LAST_NAME": "SMITH",
            "APPLICANT_BIRTH_DATE": "08/24/1959",
            "APPLICANT_ADDR_1": "123 CANDY ST",
            "APPLICANT_STATE": "PA",
            "APPLICANT_ZIP": "16506",
            "HICN": "5MM1X77YX16",
            "Product": "MAPD",
        },
        {
            "APPLICANT_FIRST_NAME": "BOB",
            "APPLICANT_LAST_NAME": "JONES",
            "APPLICANT_BIRTH_DATE": "10/08/1949",
            "APPLICANT_ADDR_1": "1319 NORTH ST",
            "APPLICANT_STATE": "PA",
            "APPLICANT_ZIP": "19464",
            "HICN": "4PY7P26KN48",
            "Product": "MAPD",
        },
        {
            "APPLICANT_FIRST_NAME": "JANE",
            "APPLICANT_LAST_NAME": "THOMAS",
            "APPLICANT_BIRTH_DATE": "08/29/1958",
            "APPLICANT_ADDR_1": "1245 SOUTH RD",
            "APPLICANT_STATE": "PA",
            "APPLICANT_ZIP": "19120",
            "HICN": "1M49WJ7GG10",
            "Product": "MAPD",
        },
    ]

    for idx, record in enumerate(sample_records, 1):
        row_data = {}

        # 应用所有字段（默认为空）
        for field in EXPECTED_FIELD_ORDER:
            row_data[field] = ""

        # 应用固定值
        row_data["CARRIER_FAMILY_ID"] = carrier_family_id
        row_data["IS_PAID"] = "1"
        row_data["BUSINESS_LINE"] = "2"
        row_data["MEMBER_NUMBER"] = "1"
        row_data["CATEGORY_CLASS_ID"] = "1"

        # 映射字段
        row_data["FIRST_NAME"] = record["APPLICANT_FIRST_NAME"]
        row_data["LAST_NAME"] = record["APPLICANT_LAST_NAME"]
        row_data["BIRTH_DATE"] = record["APPLICANT_BIRTH_DATE"]
        row_data["ADDRESS_LINE_1"] = record["APPLICANT_ADDR_1"]
        row_data["STATE"] = record["APPLICANT_STATE"]
        row_data["ZIP_CODE"] = record["APPLICANT_ZIP"]
        row_data["MEDICARE_ID"] = record["HICN"]
        row_data["PRODUCT_LINE"] = "MA/MAPD"  # MAPD -> MA/MAPD

        test_data.append(row_data)

    print(f"✅ 生成 {len(test_data)} 条测试数据")
    print()

    # 保存数据
    output_dir = Path("generated_autonomous/output")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "expected_highmark_v2.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        # 元数据
        f.write("ACTION_ID:highmark-cs-data-integration\n")
        f.write("SERVICE_MAP_ID:10003358\n")
        f.write("SOURCE_TOKEN:generated_token_v2\n")
        f.write("\n")

        # 字段头
        f.write("|".join(EXPECTED_FIELD_ORDER) + "\n")

        # 数据行
        for row in test_data:
            values = [str(row.get(field, "")) for field in EXPECTED_FIELD_ORDER]
            f.write("|".join(values) + "\n")

    print(f"✅ 数据已保存: {output_file}")
    print()

    # 验证
    print("=" * 80)
    print("📊 验证结果")
    print("=" * 80)
    print(f"CARRIER_FAMILY_ID: {carrier_family_id}")
    print(f"数据条数: {len(test_data)}")
    print(f"字段数: {len(EXPECTED_FIELD_ORDER)}")
    print()
    print("样本数据 (第一条):")
    first = test_data[0]
    print(f"  FIRST_NAME: {first['FIRST_NAME']}")
    print(f"  LAST_NAME: {first['LAST_NAME']}")
    print(f"  CARRIER_FAMILY_ID: {first['CARRIER_FAMILY_ID']}")
    print(f"  PRODUCT_LINE: {first['PRODUCT_LINE']}")
    print()
    print("✅ highmark 数据生成完成！")


if __name__ == "__main__":
    main()
