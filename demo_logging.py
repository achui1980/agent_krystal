#!/usr/bin/env python3
"""
Krystal 日志和 Case 生成功能演示

展示：
1. 彩色日志输出
2. Agent思考过程捕获
3. 测试用例生成
4. 日志查看功能
"""

import sys
from pathlib import Path
from datetime import datetime
import tempfile

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 导入我们的新日志系统
exec(open("krystal/logging_utils.py").read())
exec(open("krystal/log_viewer.py").read())

print("\n" + "=" * 80)
print("🚀 Krystal 日志系统演示")
print("=" * 80)

# 1. 创建增强日志记录器
print("\n📌 步骤1: 创建增强日志记录器")
print("-" * 80)

with tempfile.TemporaryDirectory() as tmpdir:
    logger = setup_krystal_logger(
        name="demo_logger",
        log_dir=tmpdir,
        use_color=True,
        test_id=f"demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )

    print(f"✅ Logger已创建")
    print(f"   日志文件: {logger.log_file}")
    print(f"   Test ID: {logger.test_id}")

    # 2. 模拟Case生成Agent执行过程
    print("\n📌 步骤2: 模拟Case生成Agent执行")
    print("-" * 80)

    logger.log_test_start(
        "智能Case生成",
        rules_file="case/rules.csv",
        source_template="case/source.csv",
        expected_template="case/expected.txt",
    )

    # 记录各步骤
    logger.log_step("规则解析", "开始", file="case/rules.csv")
    logger.info("   读取了 92 条规则")
    logger.info("   字段映射: 15 个字段已识别")
    logger.log_step("规则解析", "完成", duration=0.5)

    logger.log_step("生成正常场景", "开始", count=10)
    logger.info("   使用faker生成真实数据...")
    logger.info("   ✅ 生成 10 条正常场景数据")
    logger.log_step("生成正常场景", "完成", duration=1.2)

    logger.log_step("生成异常场景", "开始", count=5)
    logger.info("   场景1: 空姓名 ✓")
    logger.info("   场景2: 无效日期 ✓")
    logger.info("   场景3: 超长地址 ✓")
    logger.info("   场景4: 特殊字符 ✓")
    logger.info("   场景5: 缺失必填字段 ✓")
    logger.log_step("生成异常场景", "完成", duration=0.8)

    logger.log_step("生成边界场景", "开始", count=5)
    logger.info("   边界1: 最小年龄 ✓")
    logger.info("   边界2: 最大年龄 ✓")
    logger.info("   边界3: 零金额 ✓")
    logger.info("   边界4: 超大金额 ✓")
    logger.info("   边界5: 空字符串 ✓")
    logger.log_step("生成边界场景", "完成", duration=0.6)

    # 3. 模拟Agent思考过程捕获
    print("\n📌 步骤3: Agent思考过程捕获演示")
    print("-" * 80)

    logger.info("🤖 启动Agent智能分析...")
    logger.start_agent_capture()

    # 模拟CrewAI输出
    print("Thought: 我需要分析这些规则并确定最佳的数据生成策略")
    print("Action: 分析规则文件中的字段映射关系")
    print("Observation: 发现了 92 条规则，涉及 15 个核心字段")
    print("Thought: 基于这些映射，我应该生成正常、异常和边界三种场景")
    print("Action: 使用faker库生成真实感的数据")
    print("Observation: 数据生成成功，共 20 条记录")
    print("Final Answer: Case生成完成，包括10条正常、5条异常、5条边界场景")

    agent_output = logger.stop_agent_capture()
    print(f"\n✅ 捕获了 {len(agent_output)} 字符的Agent思考过程")

    # 4. 记录测试结果
    logger.log_test_end(
        "智能Case生成",
        success=True,
        duration=3.1,
        total_cases=20,
        normal_cases=10,
        abnormal_cases=5,
        boundary_cases=5,
    )

    print("\n📌 步骤4: 查看生成的日志文件")
    print("-" * 80)

    # 读取并显示日志内容
    with open(logger.log_file, "r", encoding="utf-8") as f:
        content = f.read()

    print(f"\n日志文件大小: {format_file_size(len(content.encode('utf-8')))}")
    print(f"总行数: {len(content.split(chr(10)))}")

    print("\n📋 日志内容预览（带颜色高亮）:")
    print("=" * 80)

    # 显示最后20行，带颜色
    lines = content.split("\n")
    for line in lines[-20:]:
        if line.strip():
            highlighted = highlight_log_line(line)
            print(highlighted)

    print("=" * 80)

# 5. 创建实际测试文件供日志查看器使用
print("\n📌 步骤5: 创建示例日志文件到 logs/ 目录")
print("-" * 80)

logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# 创建几个示例日志文件
for i in range(3):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"krystal_{timestamp}_{i:03d}.log"

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - 🚀 Krystal Case生成器启动\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - 📄 规则文件: case/rules.xlsx\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - DEBUG - 读取了 92 条规则\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - ✅ 识别了 15 个映射字段\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - [AGENT] 💭 Thought: 开始分析规则...\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - [TOOL] 🔧 Action: 解析字段映射\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - [RESULT] ✅ 完成规则解析\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - 🎯 生成正常场景: 10条\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - 🎯 生成异常场景: 5条\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - 🎯 生成边界场景: 5条\n"
        )
        f.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - krystal - INFO - ✅ Case生成完成！总计20条\n"
        )

print(f"✅ 创建了 3 个示例日志文件到 logs/ 目录")

# 6. 演示日志查看器
print("\n📌 步骤6: 使用日志查看器")
print("-" * 80)

print("\n📋 列出日志文件:")
list_logs(limit=5)

print("\n📄 查看最新日志 (带颜色):")
log_files = get_log_files(limit=1)
if log_files:
    show_log(str(log_files[0].name), lines=15)

print("\n" + "=" * 80)
print("✅ 演示完成!")
print("=" * 80)
print("\n💡 提示:")
print("   1. 日志文件保存在 logs/ 目录")
print("   2. 使用 'python krystal/log_viewer.py list' 查看所有日志")
print("   3. 使用 'python krystal/log_viewer.py show 1' 查看特定日志")
print("   4. 使用 'python krystal/log_viewer.py tail 1' 实时跟踪日志")
print()
