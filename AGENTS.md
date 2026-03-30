# AGENTS.md

本文件为 AI 编码代理提供项目上下文。请在操作本仓库前仔细阅读。

## 项目概述

KrystalV4 是一个基于 CrewAI 的 ETL 测试数据生成器，通过三个 Agent 顺序执行：规则分析 → 源数据生成 → 期望输出生成。

**活跃代码**: `krystal_v4/` (CrewAI 流水线) + `skills/etl-test-data-generator/` (独立脚本版本)
**归档代码**: `archive/` (V1/V2/V3，不要修改)

## 环境

- **Python**: 3.10+
- **Conda 环境**: `crewai` (`conda activate crewai`)
- **API Key**: `.env` 文件中配置 `OPENAI_API_KEY`

## 构建 / 测试 / 格式化命令

```bash
# 安装依赖
pip install -r krystal_v4/requirements.txt
pip install -r skills/etl-test-data-generator/scripts/requirements.txt

# 运行全部测试
python -m pytest krystal_v4/tests/ -v

# 运行单个测试文件
python -m pytest krystal_v4/tests/test_transformers.py -v

# 运行单个测试类或方法
python -m pytest krystal_v4/tests/test_expected_generator.py::TestValidationGate -v
python -m pytest krystal_v4/tests/test_transformers.py::TestNameParser::test_basic_parse -v

# 带覆盖率
python -m pytest krystal_v4/tests/ --cov=krystal_v4

# 格式化 (Black, 行宽 120)
black krystal_v4/ --line-length 120
black --check krystal_v4/ --line-length 120

# Lint (Flake8)
flake8 krystal_v4/ --max-line-length=120 --ignore=E501,W503

# 类型检查 (可选，不强制)
mypy krystal_v4/ --ignore-missing-imports
```

## 运行命令

```bash
# 运行 krystal_v4 流水线 (需要 OPENAI_API_KEY)
python -m krystal_v4.main --rules case/humanaS10/rules.csv --case humanaS10 --case-dir case/humanaS10 --count 10

# 运行独立 skill 脚本 (不需要 API Key)
python skills/etl-test-data-generator/scripts/rule_parser.py --rules case/humanaS10/rules.csv --case humanaS10 --case-dir case/humanaS10 --output output/humanaS10/rule_config_draft.json
python skills/etl-test-data-generator/scripts/transform_engine.py validate --config output/humanaS10/rule_config.json
python skills/etl-test-data-generator/scripts/transform_engine.py schemas
python skills/etl-test-data-generator/scripts/data_generator.py --config output/humanaS10/rule_config.json --count 10 --output output/humanaS10/generated_source.txt
python skills/etl-test-data-generator/scripts/expected_generator.py --config output/humanaS10/rule_config.json --source output/humanaS10/generated_source.txt --output output/humanaS10/generated_expected.txt
```

## 代码风格

### 格式化

- **行宽**: 120 字符 (Black 强制)
- **缩进**: 4 空格
- **引号**: 双引号 (Black 强制)
- **尾逗号**: 多行集合中使用
- **空行**: 顶层函数/类之间 2 个空行，方法之间 1 个空行

### 导入顺序

```python
"""模块文档字符串。"""

# 1. 标准库
import csv
import json
import os
from typing import Any, Dict, List, Optional

# 2. 第三方库
from crewai.tools import BaseTool
from faker import Faker
from pydantic import BaseModel, Field

# 3. 本地模块 (绝对导入，不用相对导入)
from krystal_v4.transformers.transformer_registry import TransformerRegistry
from krystal_v4.utils.logger import logger
```

### 命名规范

| 类型 | 规范 | 示例 |
|------|------|------|
| 类 | PascalCase | `BaseTransformer`, `TransformerRegistry` |
| 函数/方法 | snake_case | `pre_parse_rules()`, `validate_config()` |
| 私有方法 | _前缀 | `_read_source()`, `_process_record()` |
| 常量 | UPPER_SNAKE | `VALID_TRANSFORMATION_TYPES` |
| 文件 | snake_case | `transform_engine.py`, `rule_config.py` |
| Pydantic 模型 | PascalCase + Field() | `TransformationRule(BaseModel)` |

### 类型标注

- 函数签名必须有类型标注
- 使用 `typing` 模块: `Dict[str, Any]`, `List[str]`, `Optional[str]`
- Pydantic `BaseModel` 的字段用 `Field(description="...")` 描述

### 文档字符串

Google 风格:
```python
def transform(self, source_record: Dict[str, Any]) -> str:
    """对源记录应用转换。

    Args:
        source_record: 源数据字典

    Returns:
        转换后的字符串值
    """
```

## 错误处理模式

### 模式 1: 验证门控 (先验证，后处理)
```python
validation_errors = validate_rules_against_transformers(rules)
if validation_errors:
    for ve in validation_errors:
        logger.error(f"Invalid rule: {ve['target_field']}: {ve['error']}")
    return f"ERROR: {len(validation_errors)} rules invalid"
```

### 模式 2: 工具返回错误字符串 (CrewAI 模式)
工具不抛异常，而是返回 `"ERROR: ..."` 字符串:
```python
except FileNotFoundError as e:
    return f"ERROR: File not found: {str(e)}"
except Exception as e:
    return f"ERROR: {str(e)}"
```

### 模式 3: 逐条记录容错 (部分失败不中断)
```python
for i, record in enumerate(records):
    try:
        output = process_record(record, ...)
        output_rows.append(output)
    except Exception as e:
        errors.append(f"Record {i + 1}: {str(e)}")
        output_rows.append({field: "" for field in target_fields})
```

## Skill: ETL Test Data Generator

`skills/etl-test-data-generator/` 是 krystalV4 的独立脚本版本，**无 CrewAI 依赖**。

### 三阶段工作流

1. **规则解析**: `rule_parser.py` 自动解析 ~80-90% 的规则，剩余放入 `unresolved_rules`
2. **数据生成**: `data_generator.py` 基于 Faker 生成测试源数据
3. **期望输出**: `expected_generator.py` 确定性执行所有转换

### 转换引擎

内置 7 种 transformer + composite + 自定义插件:

| 类型 | 用途 |
|------|------|
| `direct` | 直接复制字段 |
| `fixed` | 固定值 |
| `empty` | 空字符串 |
| `conditional_map` | 条件值映射 |
| `substring` | 子字符串提取 (left/right/mid) |
| `phone_parser` | 电话号码解析 |
| `composite` | 串联多个 transformer |

### 自定义 Transformer 插件

- 放在 `scripts/custom_transformers/` 目录，引擎启动时自动加载
- 文件命名: `{type_name}_transformer.py`
- 必须继承 `BaseTransformer`，实现 `schema()` / `validate_config()` / `transform()`
- `BaseTransformer` 和 `TransformerRegistry` 由加载器注入，**不需要 import**
- 末尾调用 `TransformerRegistry.register_transformer("type_name", ClassName)`
- **关键**: transformer 必须通用参数化，不得包含领域特定术语或硬编码值

### 四层 Fallback (处理未知规则)

1. 匹配已有 transformer → 使用
2. 用 `composite` 组合已有 transformer → 使用
3. 派 subagent 生成新 transformer 代码 → 遵循 5 步通用化流程 → 测试验证 → 注册
4. 生成的 transformer 永久保存在 `custom_transformers/`，自动加载复用

## 关键设计原则

- **确定性分离**: 脚本做确定性计算，agent 做语义理解
- **注册模式**: `TransformerRegistry` 工厂模式，支持运行时扩展
- **通用化**: 所有 transformer 参数化设计，具体值从 config 读取
- **验证前置**: 先 validate 再执行，不允许静默失败
