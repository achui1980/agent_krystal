# Krystal v2.0 - Intelligent ETL Testing Framework

基于 CrewAI 的智能 ETL 测试系统 - 快速、稳定、可扩展的端到端测试解决方案。

## 🌟 新特性

- **3-Agent 协作架构**: ETLOperator → ResultValidator → ReportWriter
- **3次自动重试**: 遇到网络问题自动重试（指数退避 2s→4s→10s）
- **精确行级对比**: 逐行对比文件内容，定位差异
- **双格式报告**: Markdown + HTML 科技绿主题
- **LLM 智能分析**: 预留 AI 分析扩展接口
- **简化执行**: 直接代码执行确保速度，Agent 架构预留未来扩展

## 📁 项目架构

```
krystal_v2/
├── cli/
│   └── main.py              # CLI 入口，参数解析
├── agents/
│   ├── etl_operator.py      # ETL 执行 Agent（上传→触发→等待→下载）
│   ├── result_validator.py  # 结果验证 Agent（行级对比）
│   └── report_writer.py     # 报告撰写 Agent（MD+HTML 生成）
├── crews/
│   └── etl_test_crew.py     # Crew 编排（顺序执行 + 规划 + 记忆）
├── execution/
│   └── etl_executor.py      # 真实 ETL 执行逻辑（SFTP/API/轮询）
├── utils/
│   ├── report_generator.py  # 报告生成工具
│   └── retry_decorator.py   # 重试装饰器（Tenacity）
└── templates/
    └── report_template.html # HTML 报告模板（科技绿主题）
```

完整的架构图请查看：[ARCHITECTURE.md](./ARCHITECTURE.md)

## 🚀 快速开始

### 1. 准备环境

```bash
# 安装依赖
pip install -r requirements.txt

# 准备配置文件
cp config/local/secrets.env.example config/local/secrets.env
# 编辑 secrets.env，添加 SFTP 和 API 配置
```

### 2. 启动本地服务（可选）

```bash
cd integration_tests
podman compose up -d  # 或 docker-compose up -d

# 验证服务
# SFTP: telnet localhost 2223
# API: curl http://localhost:8000/health
```

### 3. 运行测试

```bash
# 基础用法
python -m krystal_v2.cli.main test \
  --input-file test_data_v2/input.csv \
  --expected-file test_data_v2/expected.csv \
  --service local-payment-service

# 完整参数
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

## 📊 测试执行流程

```
输入文件 → [上传] → [触发] → [轮询] → [下载] → [验证] → [报告]
               ↓        ↓        ↓         ↓
            SFTP    API调用   状态检查   SFTP
            (3x)    (3x)      (3x)      (3x)
            重试    重试      重试      重试
```

## 📈 输出报告

测试完成后自动生成两份报告：

### 1. Markdown 报告
`./reports_v2/report_{test_id}_{timestamp}.md`

- 简洁的文本格式
- ETL 步骤执行时间线
- 行级对比差异表
- 适合 CI/CD 日志和邮件通知

### 2. HTML 报告
`./reports_v2/report_{test_id}_{timestamp}.html`

- 科技绿主题设计
- 可视化执行时间线
- 差异行高亮显示
- 响应式布局
- 适合浏览器查看和分享

## 🔧 配置说明

复用现有的 `krystal/config/` 配置系统：

```yaml
# config/local/services.yaml
services:
  local-payment-service:
    name: "本地支付处理服务"
    upload:
      remote_path: "/uploads/payment/input"
    trigger:
      endpoint: "http://localhost:8000/api/v1/payment/trigger"
      method: "POST"
      body_template: '{"batch_id": "{{batch_id}}", "file_path": "{{remote_path}}", "row_count": {{row_count}}}'
      task_id_extractor: "$.task_id"
    polling:
      status_check_endpoint: "http://localhost:8000/api/v1/payment/status/{{task_id}}"
      max_attempts: 10
      interval: 1
    validation:
      remote_result_path: "/uploads/payment/output/{{batch_id}}_result.csv"
    sftp:
      # 引用全局 SFTP 配置
      host: "${SFTP_HOST}"
      port: "${SFTP_PORT}"
      username: "${SFTP_USERNAME}"
      password: "${SFTP_PASSWORD}"
```

## 🔄 与旧版对比

| 特性 | v1.0 (Legacy) | v2.0 (Current) |
|------|---------------|----------------|
| **架构** | 纯 CrewAI 编排 | 简化执行 + CrewAI Ready |
| **执行速度** | 慢（LLM 决策开销）| 快（直接代码执行）|
| **重试机制** | 仅 SFTP 有重试 | 全部步骤 3x 自动重试 |
| **验证方式** | Agent 执行对比 | 代码级行对比 |
| **报告格式** | 简单 Markdown | MD + HTML（科技绿）|
| **LLM 依赖** | 必需（运行时）| 可选（配置阶段）|
| **稳定性** | 中等 | 高 |
| **调试难度** | 高（需追踪 Agent 思维链）| 低（直接代码调试）|
| **扩展性** | CrewAI 原生扩展 | 预留 CrewAI 升级路径 |

## 🎯 适用场景

- ✅ **ETL 流程自动化测试** - 验证数据管道端到端流程
- ✅ **文件转换服务验证** - 确保输入输出文件格式正确
- ✅ **批处理任务测试** - 自动化测试定时批处理任务
- ✅ **数据质量保证** - 持续监控数据转换准确性
- ✅ **CI/CD 集成** - 作为持续集成流水线的测试步骤
- ✅ **回归测试** - 快速验证代码变更是否破坏现有功能

## 📝 环境变量

```bash
# 必需
export SFTP_HOST="localhost"
export SFTP_PORT="2223"
export SFTP_USERNAME="testuser"
export SFTP_PASSWORD="testpass"

# 可选（用于 v1.0 或未来 LLM 分析功能）
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL="gpt-4o"

# 代理配置（如需）
export HTTPS_PROXY="http://proxy.example.com:8080"
```

## 🧪 本地集成测试

框架支持在本地 Docker/Podman 环境中运行真实的 E2E 测试：

```bash
# 1. 启动本地服务
cd integration_tests
podman compose up -d

# 2. 运行测试
python -m krystal_v2.cli.main test \
  --input-file test_data_v2/input.csv \
  --expected-file test_data_v2/expected.csv \
  --service local-payment-service \
  --env local

# 3. 查看报告
open ./reports_v2/report_*.html
```

## 🐛 故障排查

### SFTP 连接失败
```bash
# 检查容器状态
podman ps

# 查看日志
podman logs krystal-sftp

# 手动测试连接
curl -v telnet://localhost:2223
```

### API 服务无响应
```bash
# 检查健康状态
curl http://localhost:8000/health

# 查看日志
podman logs krystal-api-stub
```

### 环境变量未加载
```bash
# 检查环境变量加载顺序
# 1. .env 文件（根目录）- 优先级最高
# 2. config/local/secrets.env - 本地服务配置

# 验证环境变量
echo $SFTP_HOST
echo $SFTP_PASSWORD
```

## 📚 相关文档

- [架构图](./ARCHITECTURE.md) - 详细的系统架构设计
- [AGENTS.md](../AGENTS.md) - 开发指南和命令参考
- [v1.0 文档](../README.md) - 旧版框架文档

## 🛣️ 路线图

- [x] **v2.0 基础功能** - 简化执行架构，3x 重试，行级对比，双格式报告
- [ ] **v2.1 LLM 分析** - 集成 LLM 进行差异根因分析
- [ ] **v2.2 并行执行** - 支持多服务并行测试
- [ ] **v2.3 Web UI** - 可视化测试管理和报告浏览
- [ ] **v3.0 完整 CrewAI** - 可选切换到完整 Agent 编排模式

## 📄 License

MIT License - Krystal Team 2026

---

**注意**: v2.0 采用"简化执行 + CrewAI Ready"的混合架构。当前实现专注于速度和稳定性，同时保留了未来升级到完整 CrewAI 编排的能力。如需完整的 LLM 驱动测试，可参考 v1.0 实现或等待 v3.0 发布。