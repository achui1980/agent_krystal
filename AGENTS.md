# AGENTS.md

本文件为 AI 编码代理提供 ETL Test Data Generator Skill 的上下文。

## 项目概述

`skills/etl-test-data-generator/` 是一个独立的 ETL 测试数据生成工具，**无 CrewAI / LLM 依赖**。
Agent 负责语义理解（分析规则），脚本负责确定性计算（数据生成和转换执行）。

## 环境

- **Python**: 3.10+
- **依赖**: `pip install -r skills/etl-test-data-generator/scripts/requirements.txt`（仅需 `faker`）

## 目录结构

```
skills/etl-test-data-generator/
├── SKILL.md                         # 完整工作流指南（Agent 必读）
├── scripts/
│   ├── rule_parser.py               # Phase 1: 规则预解析（确定性）
│   ├── transform_engine.py          # 转换引擎（7 种内置 + composite + 插件）
│   ├── data_generator.py            # Phase 2: Faker 测试数据生成
│   ├── expected_generator.py        # Phase 3: 期望输出生成
│   ├── requirements.txt             # faker>=22.0
│   └── custom_transformers/         # 自定义插件目录（自动加载）
│       ├── date_format_transformer.py
│       └── delimiter_split_transformer.py
└── reference/
    ├── rule_config_schema.json      # rule_config.json 的 JSON Schema
    ├── example_rule_config.json
    └── example_rules.csv
```

## 运行命令

```bash
# Phase 1: 规则预解析 → draft config（~80-90% 自动解析）
python scripts/rule_parser.py \
  --rules case/xxx/rules.csv --case my_case \
  --case-dir case/xxx --output output/my_case/rule_config_draft.json

# 验证配置
python scripts/transform_engine.py validate --config output/my_case/rule_config.json

# 查看所有 transformer schema
python scripts/transform_engine.py schemas

# Phase 2: 生成测试源数据
python scripts/data_generator.py \
  --config output/my_case/rule_config.json --count 10 \
  --output output/my_case/generated_source.txt

# Phase 3: 生成期望输出
python scripts/expected_generator.py \
  --config output/my_case/rule_config.json \
  --source output/my_case/generated_source.txt \
  --output output/my_case/generated_expected.txt

# 对单条记录测试转换
python scripts/transform_engine.py transform \
  --config rule_config.json --record '{"Name":"SMITH,JOHN"}'
```

## 三阶段工作流

### Phase 1: 规则分析

1. **运行 `rule_parser.py`**（脚本，确定性）→ 输出 `rule_config_draft.json`
   - 有 `CARRIER_COLUMN_NAME` 且无 `SPECIAL_RULES` → 自动归为 `direct`
   - 有 `DEFAULT` 且无 `CARRIER_COLUMN_NAME` → 自动归为 `fixed_values`
   - 有 `SPECIAL_RULES` → 放入 `unresolved_rules`（需要 Agent 处理）

2. **Agent 处理 `unresolved_rules`**，按四层 Fallback：
   - 匹配已有 transformer → 使用
   - 用 `composite` 组合 → 使用
   - 都不行 → 派 subagent 生成新 transformer（遵循 5 步通用化流程）
   - 生成的 transformer 永久保存在 `custom_transformers/`，自动加载复用

3. **删除 `unresolved_rules` 字段**，补充 `field_metadata` 和 `conditional_coverage`，保存为 `rule_config.json`，运行 `validate`

### Phase 2: 数据生成（脚本）
### Phase 3: 期望输出生成（脚本）

## 转换引擎

内置 transformer:

| 类型 | 用途 | Config 关键字段 |
|------|------|----------------|
| `direct` | 直接复制字段 | `source_field` |
| `fixed` | 固定值 | `value` |
| `empty` | 空字符串 | (无) |
| `conditional_map` | 条件值映射 | `source_field, mappings, default` |
| `substring` | 子字符串提取 | `source_field, method, length` |
| `phone_parser` | 电话号码解析 | `source_field, part` |
| `composite` | 串联多个 transformer | `source_field, pipeline` |

## 自定义 Transformer 插件

放在 `scripts/custom_transformers/` 目录，引擎启动时**自动加载**。

**文件规范**:
- 文件名: `{type_name}_transformer.py`
- 继承 `BaseTransformer`（由加载器注入，不需要 import）
- 实现 `schema()` / `validate_config()` / `transform()`
- 末尾调用 `TransformerRegistry.register_transformer("type_name", ClassName)`

**通用化原则**:
- 类名不含领域术语（用 `DelimiterSplitTransformer` 不用 `NameSplitter`）
- `transform()` 中**不允许硬编码**，所有值从 config 读取
- `schema().examples` 至少包含 2 个不同场景
- 分隔符、格式、位置等可变部分都必须是 config 参数

## rule_config.json 关键字段

```json
{
  "case_name": "string",
  "source_format": "csv_quoted | pipe | comma",
  "source_fields": ["源字段列表"],
  "target_fields": ["目标字段列表"],
  "fixed_values": {"目标字段": "固定值"},
  "transformation_rules": [
    {"target_field": "X", "source_field": "Y",
     "transformation_type": "direct", "config": {"source_field": "Y"}}
  ],
  "field_metadata": [{"field_name": "Y", "data_type": "string", "format_hint": "LAST,FIRST"}],
  "conditional_coverage": {"字段": ["值1", "值2"]},
  "record_count": 10,
  "output_metadata": {"ACTION_ID": "...", "SERVICE_MAP_ID": "..."}
}
```

注意: `conditional_coverage` 用于**数据生成覆盖**（确保测试数据包含这些值），不是映射规则。映射规则在 `transformation_rules` 的 `conditional_map` 中定义。

## 代码风格

- **行宽**: 120 字符 | **缩进**: 4 空格 | **引号**: 双引号
- **导入顺序**: 标准库 → 第三方 → 本地（绝对导入）
- **命名**: PascalCase 类, snake_case 函数/文件, UPPER_SNAKE 常量
- **类型标注**: 函数签名必须标注，使用 `Dict[str, Any]`, `List[str]`, `Optional[str]`
- **文档字符串**: Google 风格 (Args / Returns)
- **错误处理**: 验证前置（先 validate 再执行），逐条记录容错（部分失败不中断）
