# Krystal 日志功能使用说明

## 🎉 新功能概览

已实现的日志和调试功能：

1. ✅ **彩色终端日志** - 不同级别和内容用不同颜色显示
2. ✅ **文件日志记录** - 所有日志同时写入 `logs/` 目录
3. ✅ **Agent思考过程捕获** - 自动捕获并记录CrewAI Agent的思考过程
4. ✅ **日志查看CLI命令** - 方便的命令行工具查看历史日志

---

## 📁 创建的文件

### 1. `krystal/logging_utils.py` (增强版)
**功能：**
- `KrystalLogger` 类 - 支持颜色高亮和Agent输出捕获
- `ColoredFormatter` - 带颜色的日志格式化器
- `CrewOutputCapture` - 捕获Agent思考过程
- `setup_krystal_logger()` - 快速创建配置好的logger

**颜色方案：**
- 🟢 **INFO** - 绿色
- 🔵 **DEBUG** - 青色
- 🟡 **WARNING** - 黄色
- 🔴 **ERROR** - 红色
- 🟣 **CRITICAL** - 紫色
- 🔵 **Agent思考** - 亮蓝色
- 🟡 **工具调用** - 亮黄色
- 🟢 **执行结果** - 亮绿色

### 2. `krystal/log_viewer.py` (新增)
**功能：**
- `list_logs` - 列出日志文件
- `show_log` - 查看日志内容（支持颜色高亮）
- `tail` 模式 - 实时跟踪日志

### 3. 更新的文件
- `krystal_v2/cli/main.py` - 集成 `logs` 命令
- `krystal_v2/crews/etl_test_crew.py` - 添加Agent输出捕获

---

## 🚀 使用示例

### 1. 查看日志文件列表

```bash
# 列出最近的20个日志文件
python krystal/log_viewer.py list

# 列出最近的30个
python krystal/log_viewer.py list -n 30

# 或使用 krystal CLI (需要安装 crewai)
krystal logs list
```

**输出示例：**
```
================================================================================
📋 Krystal 日志文件列表
================================================================================

序号     文件名                                      日期           时间         大小        
--------------------------------------------------------------------------------
1      krystal_20260208_111000_test001.log      2026-02-08   11:10:00   2.5 KB   
2      krystal_20260208_110500_test002.log      2026-02-08   11:05:00   1.8 KB   
3      krystal_20260208_110000_test003.log      2026-02-08   11:00:00   3.2 KB   

💡 提示: 使用 'krystal logs show <序号>' 或 'krystal logs show <文件名>' 查看详情
```

### 2. 查看日志内容

```bash
# 查看最新日志（显示最后100行）
python krystal/log_viewer.py show 1

# 查看指定日志文件
python krystal/log_viewer.py show krystal_20260208_111000_test001.log

# 显示最后50行
python krystal/log_viewer.py show 1 -n 50

# 过滤特定内容（如只显示错误）
python krystal/log_viewer.py show 1 --filter ERROR

# 过滤Agent思考过程
python krystal/log_viewer.py show 1 --filter "AGENT"

# 实时跟踪最新日志
python krystal/log_viewer.py tail 1

# 实时跟踪并过滤
python krystal/log_viewer.py tail 1 --filter "上传文件"
```

**带颜色高亮的输出示例：**
```
================================================================================
📄 查看日志: krystal_20260208_111000_test001.log
================================================================================

📊 总行数: 156 | 显示最后: 100

[绿色]2026-02-08 11:10:00 - krystal - INFO - 🚀 Krystal 日志系统启动[重置]
[绿色]2026-02-08 11:10:00 - krystal - INFO - 📝 日志文件: logs/krystal_20260208_111000_test001.log[重置]
...
[亮蓝色][etl_test_20260208_111000] ============================================================[重置]
[亮蓝色][etl_test_20260208_111000] 🤖 ETL执行 Agent思考过程与执行详情[重置]
[亮蓝色][etl_test_20260208_111000] ============================================================[重置]
[亮蓝色][etl_test_20260208_111000] [AGENT] 💭 Thought: 我需要上传文件到SFTP服务器[重置]
[亮黄色][etl_test_20260208_111000] [TOOL] 🔧 Action: 使用SFTPClientTool上传文件[重置]
[亮蓝色][etl_test_20260208_111000] [OBSERVE] 👁️   Observation: 文件上传成功[重置]
...
```

### 3. 运行测试并查看日志

```bash
# 运行测试（自动创建日志）
krystal test --input-file data.csv --expected-file expected.csv --service payment-service

# 测试完成后查看最新日志
krystal logs show --latest

# 或者查看指定序号
krystal logs show 1

# 实时跟踪正在运行的测试
krystal logs tail --latest
```

---

## 📝 在代码中使用增强日志

### 基础用法

```python
from krystal.logging_utils import setup_krystal_logger

# 创建logger
logger = setup_krystal_logger(
    name="my_test",
    log_dir="logs",
    use_color=True,
    test_id="payment_test_001"
)

# 记录不同级别的日志
logger.info("这是一条信息日志")
logger.debug("调试信息")
logger.warning("警告信息")
logger.error("错误信息")
```

### 记录测试执行

```python
# 记录测试开始
logger.log_test_start(
    "Payment Service Test",
    service="payment-service",
    env="dev",
    input_file="test_data.csv"
)

# 记录执行步骤
logger.log_step("上传文件", "开始", file="test.csv", remote="/uploads/")
logger.log_step("上传文件", "完成", duration=2.5)

logger.log_step("触发服务", "开始", endpoint="/api/process")
logger.log_step("触发服务", "完成", duration=1.2, task_id="task_123")

# 记录测试结束
logger.log_test_end(
    "Payment Service Test",
    success=True,
    duration=45.3,
    total_rows=100,
    matching_rows=98
)
```

### 捕获Agent思考过程

```python
from crewai import Crew

# 开始捕获Agent输出
logger.start_agent_capture()

# 执行CrewAI
crew = Crew(agents=[...], tasks=[...], verbose=True)
result = crew.kickoff()

# 停止捕获并自动记录到日志
agent_output = logger.stop_agent_capture()

# agent_output 包含完整的Agent思考过程
print(f"捕获了 {len(agent_output)} 字符的Agent输出")
```

---

## 🎯 Agent思考过程记录

当运行 `krystal test` 时，系统会自动捕获并记录：

1. **ETL执行阶段**
   - Agent的思考过程（Thought）
   - 工具调用（Action）
   - 观察结果（Observation）
   - 最终答案（Final Answer）

2. **验证阶段**
   - 验证Agent的差异分析过程
   - 根因分析
   - 严重性评估

3. **报告生成阶段**
   - 报告撰写Agent的思考
   - 总结生成过程

所有内容都会自动分类并标记：
- `[AGENT]` - Agent思考
- `[TOOL]` - 工具调用
- `[RESULT]` - 执行结果
- `[OBSERVE]` - 观察结果

---

## 📊 日志文件结构

```
logs/
├── krystal_20260208_111000_etl_test_001.log  # 测试日志
├── krystal_20260208_110500_etl_test_002.log
└── krystal_20260208_110000_etl_test_003.log
```

**日志文件名格式：**
- `krystal_YYYYMMDD_HHMMSS_[test_id].log`
- 例如：`krystal_20260208_111000_etl_test_20260208_111000.log`

---

## 🔧 高级配置

### 自定义日志级别

```python
logger = setup_krystal_logger(
    name="debug_test",
    log_level=logging.DEBUG,  # 显示DEBUG级别
    use_color=True
)
```

### 禁用颜色输出

```python
# 对于非交互式环境（如CI/CD）
logger = setup_krystal_logger(
    name="ci_test",
    use_color=False  # 禁用颜色
)
```

---

## ✅ 功能验证

运行测试脚本验证所有功能：

```bash
# 运行功能测试
python test_logging_features.py

# 或手动测试各功能
python krystal/log_viewer.py list
python krystal/log_viewer.py show 1
python krystal/log_viewer.py tail 1
```

---

## 💡 使用技巧

1. **快速定位问题**
   ```bash
   # 只查看错误日志
   krystal logs show 1 --filter ERROR
   
   # 查看Agent思考过程
   krystal logs show 1 --filter "AGENT"
   ```

2. **实时监控测试**
   ```bash
   # 在另一个终端实时跟踪日志
   krystal logs tail --latest
   ```

3. **分析测试趋势**
   ```bash
   # 查看最近的5个测试
   krystal logs list -n 5
   ```

---

## 🔍 故障排查

### 问题：没有颜色输出
**解决：** 确保在交互式终端运行，或使用支持ANSI颜色的终端

### 问题：找不到日志文件
**解决：** 
```bash
# 检查logs目录是否存在
ls -la logs/

# 确保有写入权限
mkdir -p logs
chmod 755 logs
```

### 问题：CLI命令不可用
**解决：** 直接使用模块方式运行
```bash
# 替代方案
python krystal/log_viewer.py list
python krystal/log_viewer.py show 1
```

---

## 📚 参考

- **核心模块**: `krystal/logging_utils.py`, `krystal/log_viewer.py`
- **CLI入口**: `krystal_v2/cli/main.py`
- **Agent捕获**: `krystal_v2/crews/etl_test_crew.py`

---

**版本**: 1.0  
**更新日期**: 2026-02-08  
**作者**: Krystal Team
