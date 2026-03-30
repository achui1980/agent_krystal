#!/usr/bin/env python3
"""
基于实际case/目录规则的测试用例生成器
使用Source.csv的实际字段名生成数据
"""

import pandas as pd
import csv
from pathlib import Path
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker("en_US")


def generate_test_cases():
    print("=" * 80)
    print("📋 Krystal V2 测试用例生成器 (基于实际规则)")
    print("=" * 80)
    print()

    # 1. 读取规则文件
    print("📝 步骤1: 读取规则文件...")
    rules_df = pd.read_excel("case/rules.xlsx", sheet_name="Sheet1", header=5)
    print(f"   ✅ 读取了 {len(rules_df)} 条规则")

    # 2. 读取source字段
    print("\n📝 步骤2: 读取Source字段...")
    with open("case/source.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        source_headers = next(reader)
    source_headers_clean = [h.strip('"') for h in source_headers]
    print(f"   ✅ Source字段数: {len(source_headers_clean)}")

    # 获取有映射的source字段（清洗后的）
    mapped_sources = set(
        rules_df[rules_df["CARRIER_COLUMN_NAME"].notna()]["CS_COLUMN_NAME"].tolist()
    )

    # 创建字段名映射（从规则字段到Source.csv字段的近似匹配）
    field_mapping = {
        "PRODUCT_LINE": "Product",
        "EFFECTIVE_START_DATE": "Eff_Date",
        "CANCELLATION_DATE": "Term_Date",
        "ADDRESS_LINE_1": "Address1",
        "CITY": "City",
        "STATE": "State",
        "ZIP_CODE": "Zip",
        "FIRST_NAME": "Member",
        "LAST_NAME": "Member",
        "BIRTH_DATE": "DOB",
        "MEDICARE_ID": "MEDICARE_ID",
        "CMS_CONTRACT_ID": "Plan_Name",
        "CMS_PLAN_ID": "Plan_Name",
        "AGENCY_NAME": "AOR_Name",
        "AGENCY_ID": "AOR_SAN",
        "AGENT_NAME": "Agent",
        "AGENT_ID": "SAN",
    }

    # 找出在Source.csv中实际存在的、有映射的字段
    used_sources = []
    for rule_field, source_field in field_mapping.items():
        if source_field in source_headers_clean:
            used_sources.append(source_field)
    used_sources = list(set(used_sources))  # 去重

    print(f"   ✅ 有映射的字段: {len(used_sources)} 个")
    print(f"      {', '.join(used_sources)}")
    print()

    # 3. 生成测试数据
    print("🎯 步骤3: 生成测试用例...")

    output_dir = Path("generated_autonomous/output")
    output_dir.mkdir(exist_ok=True, parents=True)

    # 3.1 正常场景 - Source格式
    print("   📊 生成正常场景Source数据...")
    source_data = []
    for i in range(10):
        row = generate_source_row(source_headers, source_headers_clean, used_sources)
        source_data.append(row)

    with open(
        output_dir / "test_source_normal.csv", "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=source_headers)
        writer.writeheader()
        writer.writerows(source_data)
    print(f"      ✅ 已生成 test_source_normal.csv ({len(source_data)} 条)")

    # 3.2 异常场景
    print("   📊 生成异常场景数据...")
    abnormal_data = generate_abnormal_cases(
        source_headers, source_headers_clean, used_sources
    )
    with open(
        output_dir / "test_source_abnormal.csv", "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=source_headers)
        writer.writeheader()
        writer.writerows(abnormal_data)
    print(f"      ✅ 已生成 test_source_abnormal.csv ({len(abnormal_data)} 条)")

    # 3.3 边界场景
    print("   📊 生成边界场景数据...")
    boundary_data = generate_boundary_cases(
        source_headers, source_headers_clean, used_sources
    )
    with open(
        output_dir / "test_source_boundary.csv", "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=source_headers)
        writer.writeheader()
        writer.writerows(boundary_data)
    print(f"      ✅ 已生成 test_source_boundary.csv ({len(boundary_data)} 条)")

    # 4. 生成Expected结果
    print("\n🎯 步骤4: 生成Expected结果...")
    expected_normal = [apply_rules(row, rules_df, field_mapping) for row in source_data]
    with open(
        output_dir / "test_expected_normal.csv", "w", newline="", encoding="utf-8"
    ) as f:
        if expected_normal:
            writer = csv.DictWriter(f, fieldnames=list(expected_normal[0].keys()))
            writer.writeheader()
            writer.writerows(expected_normal)
    print(f"      ✅ 已生成 test_expected_normal.csv")

    # 5. 输出报告
    print("\n" + "=" * 80)
    print("✅ 测试用例生成完成！")
    print("=" * 80)
    print("\n📁 输出文件:")
    print(f"   📄 test_source_normal.csv    - Source正常场景 (10条)")
    print(f"   📄 test_source_abnormal.csv   - Source异常场景 ({len(abnormal_data)}条)")
    print(f"   📄 test_source_boundary.csv   - Source边界场景 ({len(boundary_data)}条)")
    print(f"   📄 test_expected_normal.csv   - Expected正常场景 (10条)")
    print()

    # 显示数据样本
    print("📋 数据样本:")
    print("-" * 80)
    print("\nSource数据 (第一条，有值字段):")
    row = source_data[0]
    for k, v in row.items():
        if v:  # 只显示有值的字段
            print(f"   {k:25s}: {v}")

    print("\nExpected数据 (第一条):")
    for k, v in list(expected_normal[0].items())[:10]:
        print(f"   {k:25s}: {v}")
    if len(expected_normal[0]) > 10:
        print(f"   ... 和另外 {len(expected_normal[0]) - 10} 个字段")
    print()


def generate_source_row(headers, headers_clean, used_fields):
    """生成Source格式的一行数据"""
    row = {}
    state = random.choice(["MO", "CA", "NY", "TX", "FL"])

    for header, header_clean in zip(headers, headers_clean):
        # 如果字段在规则中有映射，生成真实数据
        if header_clean in used_fields:
            if "AOR_Name" in header_clean:
                row[header] = fake.company()
            elif "AOR_SAN" in header_clean:
                row[header] = str(random.randint(100000, 999999))
            elif header_clean == "Agent":
                row[header] = fake.name()
            elif header_clean == "SAN":
                row[header] = str(random.randint(100000000, 999999999))
            elif "NPN" in header_clean:
                row[header] = str(random.randint(1000000000, 9999999999))
            elif "Member" in header_clean:
                row[header] = fake.name()
            elif "Address1" in header_clean:
                row[header] = fake.street_address()
            elif "City" in header_clean:
                row[header] = fake.city()
            elif "State" in header_clean:
                row[header] = state
            elif "Zip" in header_clean:
                row[header] = fake.zipcode_in_state(state_abbr=state)
            elif "MEDICARE_ID" in header_clean:
                row[header] = "".join(random.choices("0123456789ABCDEF", k=11))
            elif "DOB" in header_clean:
                row[header] = fake.date_of_birth(
                    minimum_age=18, maximum_age=90
                ).strftime("%Y-%m-%d")
            elif "Product" in header_clean:
                products = ["PDP", "HMO", "PPO", "HUM", "HEZ", "MPZ"]
                row[header] = random.choice(products)
            elif "Plan_Name" in header_clean:
                row[header] = f"Plan {random.randint(100, 999)}"
            elif (
                "Eff_Date" in header_clean
                or "Term_Date" in header_clean
                or "SIGNATURE_DATE" in header_clean
            ):
                row[header] = fake.date_between(
                    start_date="-2y", end_date="+1y"
                ).strftime("%Y-%m-%d")
            elif "UMID" in header_clean:
                row[header] = str(random.randint(10000000, 99999999))
            elif "EndReason" in header_clean:
                reasons = ["Moving", "Dissatisfied", "Deceased", "Other"]
                row[header] = random.choice(reasons)
            elif "PREMIUM" in header_clean:
                row[header] = f"{random.uniform(10, 200):.2f}"
            elif (
                "P2P" in header_clean
                or "LIS" in header_clean
                or "Indicator" in header_clean
            ):
                row[header] = random.choice(["Y", "N"])
            elif (
                "Provider" in header_clean
                or "Published" in header_clean
                or "DOC_ID" in header_clean
            ):
                row[header] = str(random.randint(100000, 999999))
            elif "Solar_Group" in header_clean:
                row[header] = f"GRP{random.randint(100, 999)}"
            else:
                row[header] = fake.word()
        else:
            # 字段在规则中没有映射，使用空字符串
            row[header] = ""

    return row


def generate_abnormal_cases(headers, headers_clean, used_fields):
    """生成异常场景数据"""
    cases = []

    # 场景1: 空姓名
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "Member" in hc or "Agent" in hc or "AOR_Name" in hc:
            row[h] = ""
    cases.append(row)

    # 场景2: 无效日期
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "Date" in hc or "DOB" in hc:
            row[h] = "2025-02-30"
    cases.append(row)

    # 场景3: 超长地址
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "Address" in hc:
            row[h] = "X" * 500
    cases.append(row)

    # 场景4: 特殊字符
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "Name" in hc or "Member" in hc:
            row[h] = "Test@#$%^&*()"
    cases.append(row)

    # 场景5: 缺失必填字段
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "MEDICARE_ID" in hc or "Member" in hc:
            row[h] = ""
    cases.append(row)

    return cases


def generate_boundary_cases(headers, headers_clean, used_fields):
    """生成边界场景数据"""
    cases = []

    # 场景1: 最小年龄
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "DOB" in hc:
            row[h] = "2007-01-01"
    cases.append(row)

    # 场景2: 最大年龄
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "DOB" in hc:
            row[h] = "1935-01-01"
    cases.append(row)

    # 场景3: 零金额
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "PREMIUM" in hc:
            row[h] = "0.00"
    cases.append(row)

    # 场景4: 超大金额
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if "PREMIUM" in hc:
            row[h] = "999999.99"
    cases.append(row)

    # 场景5: 空字符串（未使用字段）
    row = generate_source_row(headers, headers_clean, used_fields)
    for h, hc in zip(headers, headers_clean):
        if hc not in used_fields:
            row[h] = ""
    cases.append(row)

    return cases


def apply_rules(source_row, rules_df, field_mapping):
    """应用规则转换Source到Expected"""
    expected = {}

    # 创建反向映射：Source字段 -> 规则字段
    reverse_mapping = {v: k for k, v in field_mapping.items()}

    # 遍历Source行中的每个字段
    for source_header, source_value in source_row.items():
        source_clean = source_header.strip('"')

        # 查找对应的规则字段
        if source_clean in reverse_mapping:
            rule_field = reverse_mapping[source_clean]

            # 在规则中查找对应的Target
            rule_row = rules_df[rules_df["CS_COLUMN_NAME"] == rule_field]
            if not rule_row.empty:
                target_col = rule_row.iloc[0]["CARRIER_COLUMN_NAME"]
                default_val = rule_row.iloc[0]["DEFAULT"]

                if pd.notna(target_col):
                    # 如果有目标字段，进行映射
                    expected[target_col] = (
                        source_value
                        if source_value
                        else (default_val if pd.notna(default_val) else "")
                    )
                elif pd.notna(default_val):
                    # 只有默认值
                    expected[rule_field] = default_val

    # 添加一些固定字段
    expected["ACTION_ID"] = "humana-s10-cs-data-integration"
    expected["SERVICE_MAP_ID"] = "10003358"

    return expected


if __name__ == "__main__":
    generate_test_cases()
