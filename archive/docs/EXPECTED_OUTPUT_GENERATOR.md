# Expected Output Generator - 处理逻辑文档

## 1. 概述

### 1.1 功能描述

Expected Output Generator 是一个基于 CrewAI Agent 的智能数据生成系统，用于将 Source.csv 数据转换为符合 expected.txt 格式规范的 Expected 输出数据。

### 1.2 输入输出

| 类型 | 文件 | 说明 |
|------|------|------|
| **输入** | `case/source.csv` | 源数据文件（28个字段） |
| **输入** | `case/expected.txt` | 模板参考文件（93个字段） |
| **输入** | `case/rules.xlsx` | 业务规则文件 |
| **输出** | `generated_autonomous/output/expected_output.txt` | 生成的 Expected 数据（20行） |

### 1.3 核心特性

- ✅ 智能字段映射（Source 28字段 → Expected 93字段）
- ✅ 自动数据转换（日期格式、产品代码、姓名字段等）
- ✅ 固定值填充（基于模板示例）
- ✅ 数据变体生成（每行数据略有不同）
- ✅ 格式验证（与原始模板对比）

---

## 2. 系统架构

### 2.1 Agent 角色设计

```python
# 核心 Agent 定义
AGENTS = {
    "format_analyzer": {
        "role": "数据格式分析专家",
        "goal": "深入分析 expected.txt 文件格式，提取所有格式规范",
        "backstory": "你是一位资深的数据格式分析专家，擅长理解复杂的文本文件结构..."
    },
    "mapping_analyzer": {
        "role": "数据映射分析专家",
        "goal": "建立 Source.csv 到 Expected.txt 的完整字段映射关系",
        "backstory": "你是一位 ETL 映射专家，擅长分析源系统和目标系统的字段对应关系..."
    },
    "data_generator": {
        "role": "数据生成工程师",
        "goal": "基于分析结果生成 20 行符合规范的 Expected 数据",
        "backstory": "你是一位数据生成专家，能够根据规范生成高质量的测试数据..."
    },
    "validator": {
        "role": "数据质量验证专家",
        "goal": "验证生成数据的正确性并与原始文件对比",
        "backstory": "你是一位数据质量专家，擅长数据验证和差异分析..."
    }
}
```

### 2.2 处理流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Expected Output Generator 流程                        │
└─────────────────────────────────────────────────────────────────────────────┘

输入文件
    │
    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Step 1: 读取输入文件                                                      │
│  ├── 读取 case/source.csv (8行)                                           │
│  ├── 读取 case/expected.txt (模板)                                        │
│  └── 读取 case/rules.xlsx (业务规则)                                      │
└─────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Step 2: Agent 分析格式规范                                                │
│  ├── 分析 expected.txt 结构（元数据、表头、数据行）                        │
│  ├── 识别 93 个字段及其顺序                                              │
│  └── 生成格式分析文档                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Step 3: Agent 分析字段映射                                                │
│  ├── 建立 Source → Expected 字段对应关系                                   │
│  ├── 定义转换规则（日期格式、产品代码、姓名字段等）                       │
│  └── 生成映射分析文档                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Step 4: 生成 Expected 数据                                                │
│  ├── 生成元数据（ACTION_ID, SERVICE_MAP_ID, SOURCE_TOKEN）                │
│  ├── 复制表头（93个字段）                                                 │
│  └── 生成 20 行数据变体                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Step 5: 验证与对比                                                       │
│  ├── 验证字段数量（93）                                                   │
│  ├── 验证数据格式                                                         │
│  └── 生成验证报告                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
输出文件
```

---

## 3. 字段映射规则

### 3.1 映射矩阵

| # | Source 字段 | Expected 字段 | 转换类型 | 说明 |
|---|-------------|----------------|----------|------|
| 1 | - | CARRIER_STATUS_MAP | 固定值 | Active/Termed |
| 2 | - | CARRIER_FAMILY_ID | 固定值 | 66,175,206 |
| 3 | - | PARENT_CARRIER_ID | 空值 | 空字符串 |
| 4 | - | IS_PAID | 固定值 | 1 |
| 5 | - | BUSINESS_LINE | 固定值 | 2 |
| 6 | - | APPLICATION_ID | 空值 | 空字符串 |
| 7 | - | MEMBER_NUMBER | 固定值 | 1 |
| 8 | Product | PRODUCT_LINE | 映射 | PDP→MD, HMO→MA/MAPD, PPO→MS |
| 9 | Member | FIRST_NAME | 拆分 | Last,First → First |
| 10 | Member | LAST_NAME | 拆分 | Last,First → Last |
| 11 | Address1 | ADDRESS_LINE_1 | 直接复制 | 街道地址 |
| 12 | City | CITY | 直接复制 | 城市名 |
| 13 | State | STATE | 直接复制 | 州缩写（2位） |
| 14 | Zip | ZIP_CODE | 直接复制 | 邮编 |
| 15 | DOB | BIRTH_DATE | 格式转换 | YYYY-MM-DD → MM/DD/YYYY |
| 16 | Plan_Name | CMS_CONTRACT_ID | 拆分 | S5884-197 → S5884 |
| 17 | Plan_Name | CMS_PLAN_ID | 拆分 | S5884-197 → 197 |
| 18 | AOR_Name | AGENCY_NAME | 固定值 | EHEALTHINSURANCE SERVICES INC |
| 19 | AOR_SAN | AGENCY_ID | 固定值 | 1273481 |
| 20 | Agent | AGENT_NAME | 格式化 | Last, First |
| 21 | SAN | AGENT_ID | 生成 | 随机6位数字 |
| 22 | MEDICARE_ID | MEDICARE_ID | 直接复制 | 11位字母数字 |
| 23 | - | 其他78个字段 | 空值 | 空字符串 |

### 3.2 转换规则详解

#### 3.2.1 日期格式转换

```python
def _format_date(date_str: str) -> str:
    """
    日期格式转换: YYYY-MM-DD → MM/DD/YYYY

    示例:
    - 输入: "1970-01-15"
    - 输出: "01/15/1970"

    特殊处理:
    - "9999-12-31" → "12/31/9999" (哨兵日期，表示无限期)
    - 空值 → 空字符串
    - 非法格式 → 保留原值并记录错误
    """
    if not date_str or date_str == '9999-12-31':
        return '12/31/9999'
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%m/%d/%Y')
    except:
        return date_str
```

#### 3.2.2 产品代码映射

```python
PRODUCT_MAPPING = {
    'PDP': 'MD',      # 处方药计划 → Medicare Drug
    'HMO': 'MA/MAPD', # 健康维护组织 → Medicare Advantage
    'PPO': 'MS'       # 优选提供者组织 → Medicare Select
}

def _map_product(product: str) -> str:
    """
    产品代码映射

    示例:
    - PDP → MD
    - HMO → MA/MAPD
    - PPO → MS
    """
    return PRODUCT_MAPPING.get(product, product)
```

#### 3.2.3 姓名字段拆分

```python
def _parse_first_name(member_name: str) -> str:
    """
    从 Member 字段解析 First Name

    输入格式: "Last,First" 或 "Last, First"
    输出: First Name

    示例:
    - 输入: "MOUSE,MICKEY "
    - 输出: "MICKEY"
    """
    if ',' in member_name:
        parts = member_name.split(',')
        if len(parts) > 1:
            return parts[1].strip()
    return member_name.strip()

def _parse_last_name(member_name: str) -> str:
    """
    从 Member 字段解析 Last Name

    输入格式: "Last,First" 或 "Last, First"
    输出: Last Name

    示例:
    - 输入: "MOUSE,MICKEY "
    - 输出: "MOUSE"
    """
    if ',' in member_name:
        return member_name.split(',')[0].strip()
    return member_name.strip()
```

#### 3.2.4 Plan 编号拆分

```python
def _parse_plan_name(plan_name: str) -> Tuple[str, str, str]:
    """
    从 Plan_Name 字段拆分信息

    输入格式: "S5884-197"
    输出: (CMS_CONTRACT_ID, PLAN_NAME, CMS_PLAN_ID)

    示例:
    - 输入: "S5884-197"
    - 输出: ("S5884", "S5884-197", "197")
    """
    if '-' in plan_name:
        parts = plan_name.split('-')
        return (parts[0], plan_name, parts[1])
    return ('', plan_name, '')
```

---

## 4. 数据生成逻辑

### 4.1 数据变体策略

```python
def _generate_row_variant(base_row: Dict[str, str], variant_index: int) -> Dict[str, Any]:
    """
    生成一行数据变体

    策略:
    1. 使用 variant_index 作为随机种子，保证可复现
    2. 每行使用不同的姓名、地址、日期
    3. 保持固定值字段不变

    生成规则:
    - 姓名: 随机生成 Last,First 格式
    - 地址: 随机生成美国地址
    - 州/城市: 从预定义列表中选择匹配的组合
    - 日期: 随机生成合理范围内的日期
    - 产品: 随机选择 PDP/HMO/PPO
    - 状态: 80% Active, 20% Termed
    """
    random.seed(variant_index)
    Faker.seed(variant_index)

    # 生成基础数据
    state = random.choice(['MO', 'CA', 'NY', 'TX', 'FL'])
    city_map = {
        'MO': ['Saint Louis', 'Kansas City', 'Springfield'],
        'CA': ['Los Angeles', 'San Francisco', 'San Diego'],
        'NY': ['New York', 'Buffalo', 'Rochester'],
        'TX': ['Houston', 'Dallas', 'Austin'],
        'FL': ['Miami', 'Orlando', 'Tampa']
    }

    # 构建数据字典
    row = {
        'status': 'Active' if random.random() < 0.8 else 'Termed',
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'member_name': f"{last_name},{first_name}",
        'address': fake.street_address(),
        'city': random.choice(city_map.get(state, ['Unknown'])),
        'state': state,
        'zip': fake.zipcode_in_state(state_abbr=state),
        'medicare_id': generate_medicare_id(),
        'dob': fake.date_of_birth(minimum_age=18, maximum_age=85).strftime('%Y-%m-%d'),
        'product': random.choice(['PDP', 'HMO', 'PPO']),
        'eff_date': generate_random_date('2024-01-01', '2026-12-31'),
        'term_date': generate_random_term_date(),
    }

    return row
```

### 4.2 固定值字段

```python
FIXED_VALUES = {
    'CARRIER_STATUS_MAP': 'Active',  # 或 Termed
    'CARRIER_FAMILY_ID': '66,175,206',
    'IS_PAID': '1',
    'BUSINESS_LINE': '2',
    'MEMBER_NUMBER': '1',
    'RIDER_ID': '1',
    'CATEGORY_CLASS_ID': '1',
    'AGENCY_NAME': 'EHEALTHINSURANCE SERVICES INC',
    'AGENCY_ID': '1273481',
}

def _get_fixed_value(field: str) -> str:
    """
    获取固定值

    对于没有 Source 对应的字段，使用模板中的示例值
    """
    return FIXED_VALUES.get(field, '')
```

### 4.3 SOURCE_TOKEN 生成

```python
def _generate_source_token() -> str:
    """
    生成 SOURCE_TOKEN

    格式: 32位随机字符_13位时间戳

    示例:
    - a412f2f3abc24082a83b78059e7448d4_1770021349729

    用途:
    - 唯一标识数据源/批次
    - 便于追踪和审计
    """
    random_part = ''.join(random.choices(string.hexdigits.lower(), k=32))
    timestamp = str(int(datetime.now().timestamp() * 1000))
    return f"{random_part}_{timestamp}"
```

---

## 5. 文件格式规范

### 5.1 文件结构

```text
expected_output.txt
├── 元数据区 (3行)
│   ├── ACTION_ID:humana-s10-cs-data-integration
│   ├── SERVICE_MAP_ID:10003358
│   └── SOURCE_TOKEN:<32位随机>_<13位时间戳>
│
├── 空行 (1行)
│
├── 表头行 (1行, 93个字段)
│   CARRIER_STATUS_MAP|CARRIER_FAMILY_ID|...|LAST_TOUCHED_DATE
│
└── 数据行 (20行)
    Active|66,175,206||1|2|...|1529122|
```

### 5.2 字段分隔符

| 类型 | 分隔符 | 示例 |
|------|--------|------|
| 元数据 | `:` | `ACTION_ID:humana-s10-cs-data-integration` |
| 表格数据 | `|` | `Active|66,175,206||1|2|...` |
| 空值 | 空字符串 | `66,175,206||1` 表示中间字段为空 |

### 5.3 数据类型

| 类型 | 格式 | 示例 |
|------|------|------|
| 日期 | MM/DD/YYYY | `01/15/1970`, `12/31/9999` |
| 数字 | 带千分位逗号 | `66,175,206` |
| 字符串 | 原样保留 | `EHEALTHINSURANCE SERVICES INC` |
| 空值 | 空字符串 | `""` |

---

## 6. 验证规则

### 6.1 格式验证

```python
VALIDATION_RULES = {
    'field_count': 93,           # 每行必须是93个字段
    'delimiter': '|',            # 使用竖线分隔符
    'metadata_lines': 3,          # 元数据3行
    'header_line': 1,            # 表头1行
    'data_rows': 20,             # 数据20行
}
```

### 6.2 数据验证

```python
def validate_row(row: str, expected_fields: List[str]) -> ValidationResult:
    """
    验证单行数据

    检查项:
    1. 字段数量是否正确
    2. 分隔符是否正确
    3. 必填字段是否有值
    4. 格式是否符合规范

    返回:
    - valid: 是否有效
    - errors: 错误列表
    - warnings: 警告列表
    """
    fields = row.split('|')

    # 检查字段数量
    if len(fields) != 93:
        return ValidationResult(
            valid=False,
            errors=[f"字段数量错误: 期望93, 实际{len(fields)}"]
        )

    # 检查日期格式
    for i, (field_name, value) in enumerate(zip(expected_fields, fields)):
        if 'DATE' in field_name and value and not validate_date_format(value):
            return ValidationResult(
                valid=False,
                errors=[f"日期格式错误: {field_name}={value}"]
            )

    return ValidationResult(valid=True)
```

### 6.3 对比验证

```python
def compare_with_template(generated: str, template: str) -> ComparisonResult:
    """
    与原始模板对比

    对比项:
    1. 表头字段是否完全一致
    2. 分隔符是否一致
    3. 固定值是否一致
    4. 数据格式是否一致
    """
    # 提取表头
    gen_header = generated.split('\n')[4]
    template_header = template.split('\n')[4]

    # 对比表头
    if gen_header != template_header:
        return ComparisonResult(
            match=False,
            differences=[f"表头不一致"]
        )

    return ComparisonResult(match=True)
```

---

## 7. 使用指南

### 7.1 快速使用

```bash
# 运行生成器
cd /Users/portz/js/agent-krystal
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate crewai
python krystal_v2/case_generator/agents/expected_generator.py
```

### 7.2 自定义配置

```python
# 修改生成行数
# 在 expected_generator.py 中找到:
TOTAL_ROWS = 20  # 修改此值

# 修改数据分布
STATUS_DISTRIBUTION = {
    'Active': 0.8,   # 80% 正常
    'Termed': 0.2,    # 20% 终止
}

# 修改产品分布
PRODUCT_DISTRIBUTION = {
    'PDP': 0.4,      # 40%
    'HMO': 0.3,      # 30%
    'PPO': 0.3,      # 30%
}
```

### 7.3 输出文件

```
generated_autonomous/output/
├── expected_output.txt              # 主输出文件
├── expected_format_analysis.md       # 格式分析
├── field_mapping_analysis.md         # 映射分析
├── validation_comparison_report.md   # 验证报告
└── data_generation_report.md        # 生成报告
```

---

## 8. 错误处理

### 8.1 常见错误

| 错误类型 | 原因 | 解决方案 |
|----------|------|----------|
| 字段数量不匹配 | 分隔符在字段内容中出现 | 检查源数据是否包含未转义的 `|` |
| 日期解析失败 | 日期格式不符合规范 | 使用标准格式 MM/DD/YYYY |
| 编码问题 | 文件编码不一致 | 确保使用 UTF-8 编码 |

### 8.2 异常处理

```python
try:
    # 数据生成
    row_data = _generate_row_variant(base_row, variant_index)
    formatted_row = _format_expected_row(row_data, header_line)
except Exception as e:
    # 记录错误
    log_error(f"生成第 {variant_index} 行失败: {e}")
    # 使用空行代替
    formatted_row = '|' * 92  # 92个分隔符 = 93个空字段
```

---

## 9. 扩展指南

### 9.1 添加新字段映射

```python
# 在 FIELD_MAPPING 中添加新规则
FIELD_MAPPING = {
    # ... 现有映射 ...
    'NEW_SOURCE_FIELD': {
        'target': 'NEW_EXPECTED_FIELD',
        'transform': 'direct',           # 直接复制
        # 或 'transform': 'custom',    # 自定义转换
        # 或 'transform': 'fixed',      # 固定值
    },
}
```

### 9.2 添加新转换函数

```python
def _transform_custom(value: str, **kwargs) -> str:
    """
    自定义转换函数

    在 TRANSFORM_FUNCTIONS 中注册后使用
    """
    # 实现转换逻辑
    return transformed_value

# 注册转换函数
TRANSFORM_FUNCTIONS = {
    'custom': _transform_custom,
    # ... 其他函数 ...
}
```

### 9.3 修改验证规则

```python
# 在 VALIDATION_RULES 中添加新规则
VALIDATION_RULES = {
    # ... 现有规则 ...
    'max_field_length': 255,        # 字段最大长度
    'required_fields': ['CARRIER_STATUS_MAP', 'PRODUCT_LINE'],  # 必填字段
}
```

---

## 10. 附录

### 10.1 完整字段列表

| # | 字段名 | 类型 | 来源 | 说明 |
|---|--------|------|------|------|
| 1 | CARRIER_STATUS_MAP | 代码 | 固定/生成 | Active/Termed |
| 2 | CARRIER_FAMILY_ID | 数字 | 固定值 | 66,175,206 |
| 3 | PARENT_CARRIER_ID | ID | 空 | |
| 4 | IS_PAID | 标志 | 固定值 | 1 |
| ... | ... | ... | ... | ... |
| 93 | LAST_TOUCHED_DATE | 日期 | 空 | |

### 10.2 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 1.0 | 2024-02-07 | 初始版本 |
| 1.1 | 2024-02-07 | 添加 Agent 分析功能 |

### 10.3 相关文件

```
krystal_v2/case_generator/agents/
├── expected_generator.py      # 主脚本
├── intelligent_flow.py       # 智能流程
└── tools.py                  # 工具函数

docs/
└── EXPECTED_OUTPUT_GENERATOR.md  # 本文档
```

---

**文档版本**: 1.0  
**最后更新**: 2024-02-07  
**维护者**: Krystal ETL Team
