# Utils 工具库

为 Krystal V2 ETL 测试框架提供数据转换工具。

## 📁 文件结构

```
utils/
├── __init__.py              # 包导出
├── shared_rules.py          # 共享规则定义（6个固定值 + 85个空字段 + 93字段顺序）
├── file_readers.py          # 多格式文件读取器（Excel/CSV/TXT）
├── data_converters.py       # 数据转换函数（日期、姓名、CMS等）
├── formatters.py            # 输出格式化器（pipe分隔）
├── example_usage.py         # 使用示例脚本
└── README.md                # 本文件
```

## 🎯 核心功能

### 1. 共享规则 (shared_rules.py)

定义所有案例通用的规则：

- **SHARED_FIXED_VALUES** (6个字段): 所有案例使用相同值的字段
- **SHARED_EMPTY_FIELDS** (85个字段): 所有案例都为空的字段
- **EXPECTED_FIELD_ORDER** (93个字段): 输出字段的标准顺序

```python
from krystal_v2.case_generator.utils.shared_rules import (
    SHARED_FIXED_VALUES,
    SHARED_EMPTY_FIELDS,
    EXPECTED_FIELD_ORDER
)

# 固定值示例
# {'IS_PAID': '1', 'BUSINESS_LINE': '2', ...}
```

### 2. 文件读取器 (file_readers.py)

支持多种格式的源文件读取：

```python
from krystal_v2.case_generator.utils.file_readers import read_source_file

# 自动检测格式并读取
df = read_source_file('case/humanaS10/source.csv')
df = read_source_file('case/highmark/source.txt')
df = read_source_file('case/example/source.xlsx')

# 所有列都作为字符串读取，避免 pandas 自动转换
```

**支持的格式**：
- `.xlsx`, `.xls` (Excel)
- `.csv` (逗号分隔)
- `.txt` (自动检测 `|` 或 `,` 分隔符)

### 3. 数据转换器 (data_converters.py)

提供常用的数据转换函数：

```python
from krystal_v2.case_generator.utils.data_converters import (
    convert_date_to_mmddyyyy,
    parse_full_name,
    split_cms_contract,
    map_product_type,
    clean_phone,
    format_with_commas
)

# 日期转换（支持 Excel 序列号）
convert_date_to_mmddyyyy(44197)          # → '01/01/2021'
convert_date_to_mmddyyyy("2021-01-01")   # → '01/01/2021'
convert_date_to_mmddyyyy("9999-12-31")   # → '12/31/9999'

# 姓名解析
parse_full_name("SMITH,JOHN MICHAEL")    # → {'first': 'JOHN', 'middle': 'MICHAEL', 'last': 'SMITH'}

# CMS 合同拆分
split_cms_contract("S5884-197")          # → {'contract': 'S5884', 'plan': '197'}

# 数字格式化（添加逗号）
format_with_commas(66175206)             # → '66,175,206'
```

**关键特性**：
- ✅ 所有函数处理空值/None → 返回空字符串 `""`
- ✅ 支持 Excel 日期序列号转换
- ✅ 多种日期格式支持

### 4. 输出格式化器 (formatters.py)

生成符合规范的 pipe 分隔输出：

```python
from krystal_v2.case_generator.utils.formatters import (
    format_pipe_delimited_output,
    create_metadata
)

# 创建元数据
metadata = create_metadata(
    action_id='humana-s10-cs-data-integration',
    service_map_id='10003358'
)

# 格式化输出
output = format_pipe_delimited_output(rows, EXPECTED_FIELD_ORDER, metadata)

# 输出格式：
# ACTION_ID:xxx
# SERVICE_MAP_ID:xxx
# SOURCE_TOKEN:xxx
# (空行)
# FIELD1|FIELD2|...|FIELD93
# value1|value2||value4|...   ← 空字段显示为 ||
```

## 🚀 使用示例

### 完整的数据转换流程

```python
from krystal_v2.case_generator.utils import (
    read_source_file,
    convert_date_to_mmddyyyy,
    format_with_commas,
    SHARED_FIXED_VALUES,
    SHARED_EMPTY_FIELDS,
    EXPECTED_FIELD_ORDER,
    format_pipe_delimited_output,
    create_metadata
)
import openpyxl

# 1. 读取源文件
df = read_source_file('case/humanaS10/source.csv')

# 2. 读取 rules.xlsx 获取案例特定的固定值（如 CARRIER_FAMILY_ID）
wb = openpyxl.load_workbook('case/humanaS10/rules.xlsx')
ws = wb['Sheet1']

carrier_family_id = None
for row in ws.iter_rows(min_row=7, max_row=100):
    cs_col_name = row[1].value if len(row) > 1 else None
    source_col = row[3].value if len(row) > 3 else None
    
    if cs_col_name == 'CARRIER_FAMILY_ID':
        # rules.xlsx 中存储为整数（如 66175206）
        # 需要转换为带逗号的字符串（如 "66,175,206"）
        carrier_family_id = format_with_commas(source_col)
        break

# 3. 转换数据
output_rows = []
for _, row in df.iterrows():
    output_row = {}
    
    # 应用共享固定值
    output_row.update(SHARED_FIXED_VALUES)
    
    # 应用案例特定固定值
    output_row['CARRIER_FAMILY_ID'] = carrier_family_id
    
    # 应用空字段
    for field in SHARED_EMPTY_FIELDS:
        output_row[field] = ""
    
    # 从源数据映射
    output_row['FIRST_NAME'] = row.get('Member', "")
    output_row['BIRTH_DATE'] = convert_date_to_mmddyyyy(row.get('DOB', ""))
    
    output_rows.append(output_row)

# 4. 生成输出
metadata = create_metadata('test-action', '12345')
output = format_pipe_delimited_output(output_rows, EXPECTED_FIELD_ORDER, metadata)

# 5. 保存文件
with open('output.txt', 'w') as f:
    f.write(output)
```

### 运行示例脚本

```bash
cd /Users/portz/js/agent-krystal
python3 krystal_v2/case_generator/utils/example_usage.py
```

## 📊 统计信息

| 文件 | 行数 | 功能 |
|------|------|------|
| `shared_rules.py` | 234 | 共享规则定义 |
| `file_readers.py` | 172 | 文件读取器 |
| `data_converters.py` | 313 | 数据转换函数 |
| `formatters.py` | 185 | 输出格式化 |
| `__init__.py` | 98 | 包导出 |
| **总计** | **1,002** | **5个核心文件** |

## 🔑 设计原则

1. **规则驱动**：所有转换逻辑基于 `rules.xlsx`，不硬编码案例特定规则
2. **灵活性**：支持多种输入格式和数据类型
3. **容错性**：所有函数优雅处理空值/缺失字段
4. **可扩展**：易于添加新的转换函数
5. **向后兼容**：不影响现有的随机数据生成功能

## 📝 注意事项

1. **CARRIER_FAMILY_ID 的处理方式**  
   - **不是共享固定值**：每个案例的值不同
   - **存储格式**：在 `rules.xlsx` 中存储为整数（如 `66175206`）
   - **输出格式**：需要转换为带逗号的字符串（如 `"66,175,206"`）
   - **转换方法**：使用 `format_with_commas()` 函数
   - **示例**：
     ```python
     # rules.xlsx 中: SOURCE_COLUMN = 66175206 (int)
     carrier_family_id = format_with_commas(66175206)
     # 结果: "66,175,206" (string with commas)
     ```

2. **所有列读取为字符串**  
   避免 pandas 自动转换日期和数字，保持原始数据格式

3. **LAST_TOUCHED_DATE 字段**  
   第93个字段，不在 `rules.xlsx` 中，通常输出为空字符串或当前时间

4. **空字段输出格式**  
   在 pipe 分隔文件中显示为 `||`（两个pipe之间无内容）

## 🎯 后续计划

- [ ] 增强 Agent 提示词，告知可用的工具函数
- [ ] 添加单元测试（`tests/test_utils.py`）
- [ ] 支持更多数据转换模式（基于新的业务案例）
- [ ] 性能优化（大文件处理）

## 📞 联系方式

如有问题或建议，请联系 Krystal Team。

---

**版本**: 1.0.0  
**创建日期**: 2026-02-08  
**最后更新**: 2026-02-08
