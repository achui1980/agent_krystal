# Krystal 智能测试用例生成器 (Agent-based)

基于CrewAI的智能Agent系统，能够自主理解业务规则并生成测试用例。

## 🚀 与旧方案的区别

| 特性 | 旧方案（硬编码） | 新方案（Agent-based） |
|------|----------------|---------------------|
| **规则理解** | 正则匹配 + 预定义处理器 | Agent自然语言理解 |
| **数据生成** | 固定模板 | Agent自主决策生成策略 |
| **转换逻辑** | 硬编码函数 | Agent动态推理 |
| **泛化能力** | 低（只能处理已知模式） | 高（可理解新规则） |
| **可解释性** | 低（黑盒执行） | 高（Agent展示思考过程） |
| **维护成本** | 高（需不断添加处理器） | 低（Agent自适应） |

## 🏗️ 架构

```
用户输入: rules.xlsx + source.csv + expected.txt
     ↓
智能Agent自主工作:
  ├── Task 1: 理解规则（阅读并分析rules.xlsx）
  ├── Task 2: 分析结构（对比source和expected）
  ├── Task 3: 设计策略（决定生成什么测试数据）
  ├── Task 4: 生成数据（调用工具生成source）
  ├── Task 5: 执行转换（应用规则生成expected）
  └── Task 6: 验证报告（生成详细报告）
     ↓
输出: source.csv + expected.txt + 详细报告
```

## 🛠️ Agent工具集

Agent可以自主调用以下工具：

1. **read_rules** - 读取并理解规则文档
2. **analyze_data_structure** - 分析数据结构差异
3. **generate_test_data** - 根据描述生成测试数据
4. **execute_transformation** - 执行规则转换
5. **generate_python_code** - 为复杂规则生成代码
6. **validate_results** - 验证结果

## 📦 文件结构

```
krystal_v2/case_generator/agents/
├── __init__.py
├── tools.py                    # Agent工具集
├── intelligent_agent.py        # Agent定义
├── intelligent_flow.py         # 任务流程
└── intelligent_cli.py          # CLI入口
```

## 🚀 使用方法

### 运行智能生成

```bash
# 激活环境
source /opt/anaconda3/etc/profile.d/conda.sh && conda activate crewai

# 运行智能Agent
python krystal_v2/case_generator/agents/intelligent_cli.py \
    --rules case/rules.xlsx \
    --source case/source.csv \
    --expected case/expected.txt \
    --output ./generated_intelligent/
```

### 在代码中使用

```python
from krystal_v2.case_generator.agents.intelligent_flow import IntelligentCaseGenerationFlow

# 创建流程
flow = IntelligentCaseGenerationFlow()

# 运行生成
result = flow.run(
    rules_path="case/rules.xlsx",
    source_path="case/source.csv",
    expected_path="case/expected.txt",
    output_dir="./generated/"
)
```

## 🎯 Agent能力示例

**场景**: Agent看到规则 "if 'PDP' map to MD, if HAP/HUM/HV/RD map to MS, all others is MA/MAPD"

**Agent思考过程**:
1. "这是一个条件映射规则"
2. "需要生成多种产品类型的数据来覆盖所有分支"
3. "PDP→MD, HAP→MS, HUM→MS, HV→MS, RD→MS, 其他→MA/MAPD"
4. "我应该生成：PDP数据、HAP数据、其他产品数据"
5. "调用工具生成这些数据"

**对比旧方案**:
- 旧方案：需要预先定义`conditional_map`处理器，且只能匹配特定格式
- 新方案：Agent直接理解自然语言，自主决定如何生成数据

## 🔧 扩展能力

Agent系统具有良好的扩展性：

1. **添加新工具**: 在`tools.py`中添加新工具，Agent会自动学会使用
2. **调整Prompt**: 修改`intelligent_agent.py`中的backstory来调整Agent行为
3. **自定义流程**: 修改`intelligent_flow.py`中的任务序列

## 📊 输出示例

Agent会生成详细的报告，包括：
- 规则理解摘要
- 测试策略说明
- 测试点详细描述
- 规则覆盖矩阵
- 优化建议

## ⚠️ 注意事项

1. 需要配置OpenAI API Key（或其他LLM）
2. Agent推理需要一定时间（比硬编码慢）
3. 可以通过调整LLM模型来平衡质量和速度
