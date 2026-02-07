# Krystal Case Generator MVP

基于规则的测试用例智能生成器

## 功能

- 解析 rules.xlsx 规则文档
- 自动生成 source.csv 测试数据
- 根据规则自动计算 expected.txt 预期结果
- 支持正常、异常、边界三类测试场景
- 支持规则验证（对比sample数据）

## 安装依赖

在 crewai 虚拟环境中运行：

```bash
# 激活虚拟环境
source /path/to/crewai/bin/activate  # Linux/Mac
# 或
/path/to/crewai/Scripts/activate  # Windows

# 安装依赖
pip install -r krystal_v2/case_generator/requirements.txt
```

## 使用方法

### 方法1: 使用 CLI

```bash
python -m krystal_v2.case_generator.cli \
    --rules case/rules.xlsx \
    --sample-source case/source.csv \
    --sample-expected case/expected.txt \
    --output ./generated/ \
    --count-normal 10 \
    --count-abnormal 5 \
    --count-boundary 3
```

### 方法2: 运行测试脚本

```bash
cd krystal_v2/case_generator
python test_generator.py
```

### 方法3: 在代码中使用

```python
from krystal_v2.case_generator.core.material_loader import MaterialLoader
from krystal_v2.case_generator.core.rule_parser import RuleParser
from krystal_v2.case_generator.core.data_generator import DataGenerator
from krystal_v2.case_generator.core.expected_calculator import ExpectedCalculator

# 1. 加载规则
loader = MaterialLoader()
rules = loader.load_rules('case/rules.xlsx')

# 2. 解析规则
parser = RuleParser()
parsed_rules = parser.parse_rules(rules)

# 3. 生成测试数据
generator = DataGenerator()
test_data = generator.generate_normal_cases(10)

# 4. 计算预期结果
calculator = ExpectedCalculator(parsed_rules)
expected = calculator.calculate(test_data)
```

## 输出文件

运行后会生成以下文件：

- `source.csv` - 测试输入数据
- `expected.txt` - 预期输出结果（符合Humana格式）
- `report.json` - 生成报告（包含规则统计、准确率等）

## 支持的规则类型

1. **direct_map** - 直接字段映射
2. **default_value** - 默认值
3. **conditional_map** - 条件映射（如 Product→PRODUCT_LINE）
4. **split_field** - 字段拆分（如 Member→FIRST_NAME/LAST_NAME）
5. **derive_status** - 状态派生（如 Term_Date→Active/Termed）
6. **regex_split** - 正则分割（如 Plan_Name→CMS_CONTRACT_ID）
7. **date_convert** - 日期转换

## 目录结构

```
krystal_v2/case_generator/
├── __init__.py
├── cli.py                      # CLI入口
├── requirements.txt            # 依赖
├── test_generator.py           # 测试脚本
├── core/
│   ├── material_loader.py      # 材料加载
│   ├── rule_parser.py          # 规则解析
│   ├── rule_validator.py       # 规则验证
│   ├── data_generator.py       # 数据生成
│   └── expected_calculator.py  # 预期计算
├── handlers/
│   └── predefined.py           # 预定义处理器
└── exporters/
    └── file_exporter.py        # 文件导出
```
