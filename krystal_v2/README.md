# Krystal v2.0 - Intelligent ETL Testing Framework

基于 CrewAI 的智能 ETL 测试系统

## 🌟 新特性

- **3-Agent 协作架构**: ETLOperator → ResultValidator → ReportWriter
- **3次自动重试**: 遇到网络问题自动重试（指数退避）
- **精确行级对比**: 逐行对比文件内容
- **双格式报告**: Markdown + HTML 科技绿主题
- **LLM 智能分析**: 自动分析差异根因

## 📁 架构

```
krystal_v2/
├── cli/main.py              # CLI入口
├── agents/
│   ├── etl_operator.py      # ETL执行（上传→触发→等待→下载）
│   ├── result_validator.py  # 结果验证（行级对比）
│   └── report_writer.py     # 报告生成（MD+HTML）
├── crews/
│   └── etl_test_crew.py     # Crew编排（Sequential+Planning+Memory）
├── utils/
│   └── retry_decorator.py   # 重试装饰器
└── templates/
    └── report_template.html # HTML报告模板（科技绿主题）
```

## 🚀 使用方式

### 基础用法

```bash
python -m krystal_v2.cli.main test \
  --input-file data.csv \
  --expected-file expected.csv \
  --service payment-service
```

### 完整参数

```bash
python -m krystal_v2.cli.main test \
  --input-file data.csv \
  --expected-file expected.csv \
  --service payment-service \
  --env local \
  --output-dir ./reports
```

### 命令说明

```bash
# 执行测试
krystal test -i input.csv -e expected.csv -s payment-service

# 指定环境
krystal test -i input.csv -e expected.csv -s payment-service --env dev

# 查看版本
krystal version
```

## 📊 输出报告

测试完成后生成两份报告：

1. **Markdown报告**: `reports/etl_test_YYYYMMDD_HHMMSS_report.md`
   - 简洁的文本格式
   - 适合查看和分享

2. **HTML报告**: `reports/etl_test_YYYYMMDD_HHMMSS_report.html`
   - 科技绿主题
   - 行级高亮显示差异
   - 美观的可视化

## 🔧 配置

复用现有 `krystal/config/` 配置系统：

```yaml
# config/local/services.yaml
services:
  payment-service:
    name: "支付处理服务"
    sftp:
      host: "localhost"
      port: 2223
      username: "testuser"
      # password 从环境变量读取
    api:
      endpoint: "http://localhost:8000/api/v1/trigger"
      method: "POST"
    # ... 其他配置
```

## 🧪 与旧版对比

| 特性 | v1.0 | v2.0 |
|------|------|------|
| 架构 | 代码编排 | CrewAI 协作 |
| 重试 | 仅 SFTP 有 | 全部步骤 3次重试 |
| 对比 | 代码逻辑 | Agent 执行 |
| 报告 | 简单 Markdown | MD + HTML（科技绿）|
| 分析 | 无 | LLM 智能分析 |

## 📝 环境变量

```bash
# 必需
export OPENAI_API_KEY="your-api-key"

# 可选
export OPENAI_MODEL="gpt-4o"  # 默认使用 gpt-4o
```

## 🎯 适用场景

- ETL 流程自动化测试
- 文件转换服务验证
- 数据管道质量保证
- 批处理任务测试

## 📄 License

MIT License - Krystal Team 2026
