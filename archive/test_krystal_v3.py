#!/usr/bin/env python3
"""测试 krystal_v3 数据生成"""

import sys

sys.path.insert(0, "krystal_v3")

from core.data_generator import DataGenerator
from cli import load_config_from_json

# 加载配置
config = load_config_from_json("krystal_v3/config/humanaS10_config.json")

print("=" * 80)
print("Krystal V3 - 调试测试")
print("=" * 80)

# 生成一条源数据
gen = DataGenerator(config)
source = gen.generate_source_record()

print("\n【Source 数据】")
for k, v in source.items():
    print(f"  {k}: {v}")

# 转换一条数据
print("\n【转换测试】")
expected = gen.transform_engine.transform(source)

key_fields = [
    "CARRIER_FAMILY_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "PRODUCT_LINE",
    "ADDRESS_LINE_1",
    "STATE",
    "BIRTH_DATE",
]

for field in key_fields:
    value = expected.get(field, "N/A")
    print(f"  {field}: {value}")

print("\n【规则检查】")
for rule in config.rules[:3]:
    print(f"  {rule.cs_column_name}: {rule.transformation_type}")
    print(f"    config: {rule.config}")
