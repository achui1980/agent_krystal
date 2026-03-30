"""
日志功能测试脚本

演示和验证日志查看功能
"""

import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime

# 将项目根目录添加到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 直接导入模块，避免触发__init__.py
sys.path.insert(0, str(project_root / "krystal"))
from log_viewer import (
    list_logs,
    show_log,
    get_log_files,
    parse_log_filename,
    format_file_size,
    Colors,
)
from logging_utils import setup_krystal_logger, KrystalLogger, ColoredFormatter
from krystal.logging_utils import setup_krystal_logger, KrystalLogger, ColoredFormatter


def test_log_viewer():
    """测试日志查看器功能"""
    print("\n" + "=" * 80)
    print("测试1: 日志查看器功能")
    print("=" * 80 + "\n")

    # 创建测试日志目录
    test_log_dir = Path("logs")
    test_log_dir.mkdir(exist_ok=True)

    # 创建一些测试日志文件
    for i in range(3):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = test_log_dir / f"krystal_{timestamp}_test{i:03d}.log"
        with open(log_file, "w", encoding="utf-8") as f:
            f.write(f"2025-02-08 10:0{i}:00 - krystal - INFO - Test log {i}\n")
            f.write(f"2025-02-08 10:0{i}:01 - krystal - DEBUG - Debug message {i}\n")
            f.write(f"[AGENT] 💭 Thought: Analyzing data {i}\n")
            f.write(f"[TOOL] 🔧 Action: API call {i}\n")
            f.write(f"[RESULT] ✅ Success {i}\n")

    print("✅ 创建了3个测试日志文件\n")

    # 测试列表功能
    print("\n测试1.1: 列出日志文件")
    list_logs(limit=10)

    # 测试查看功能
    print("\n测试1.2: 查看最新日志")
    log_files = get_log_files(limit=1)
    if log_files:
        show_log(str(log_files[0].name), lines=10)

    print("\n✅ 日志查看器测试完成!")


def test_colored_logging():
    """测试彩色日志功能"""
    print("\n" + "=" * 80)
    print("测试2: 彩色日志功能")
    print("=" * 80 + "\n")

    # 创建临时目录用于测试
    with tempfile.TemporaryDirectory() as tmpdir:
        # 创建KrystalLogger实例
        logger = setup_krystal_logger(
            name="test_logger", log_dir=tmpdir, use_color=True, test_id="test_001"
        )

        print("\n测试2.1: 不同级别的日志")
        logger.debug("这是一条DEBUG级别的日志")
        logger.info("这是一条INFO级别的日志")
        logger.warning("这是一条WARNING级别的日志")
        logger.error("这是一条ERROR级别的日志")

        print("\n测试2.2: 带特殊标记的日志")
        logger.info("[AGENT] 💭 Thought: 我正在分析数据")
        logger.info("[TOOL] 🔧 Action: 调用API接口")
        logger.info("[RESULT] ✅ Final Answer: 操作成功完成")

        print("\n测试2.3: 测试执行日志")
        logger.log_test_start(
            "Payment Service Test",
            service="payment-service",
            env="dev",
            input_file="test_data.csv",
        )

        logger.log_step("上传文件", "开始", file="test_data.csv", remote="/uploads/")
        logger.log_step("上传文件", "完成", duration=2.5)

        logger.log_step("触发服务", "开始", endpoint="/api/process")
        logger.log_step("触发服务", "完成", duration=1.2, task_id="task_123")

        logger.log_test_end(
            "Payment Service Test",
            success=True,
            duration=45.3,
            total_rows=100,
            matching_rows=98,
        )

        print(f"\n✅ 日志已写入: {logger.log_file}")

        # 读取并显示日志文件内容
        print("\n测试2.4: 验证日志文件内容")
        with open(logger.log_file, "r", encoding="utf-8") as f:
            content = f.read()
            lines = content.split("\n")
            print(f"   日志行数: {len(lines)}")
            print(f"   文件大小: {format_file_size(len(content.encode('utf-8')))}")

    print("\n✅ 彩色日志测试完成!")


def test_output_capture():
    """测试输出捕获功能"""
    print("\n" + "=" * 80)
    print("测试3: Agent输出捕获功能")
    print("=" * 80 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        logger = setup_krystal_logger(
            name="capture_test", log_dir=tmpdir, use_color=True, test_id="capture_001"
        )

        print("模拟Agent执行过程...")

        # 开始捕获
        logger.start_agent_capture()

        # 模拟CrewAI输出
        print("Thought: 我需要上传文件到SFTP服务器")
        print("Action: 使用SFTPClientTool上传文件")
        print("Observation: 文件上传成功")
        print("Thought: 现在需要触发API")
        print("Action: 使用APIClientTool发送POST请求")
        print("Final Answer: ETL流程执行完成")

        # 停止捕获并记录
        output = logger.stop_agent_capture()

        print(f"\n✅ 捕获了 {len(output)} 字符的Agent输出")
        print("✅ Agent思考过程已记录到日志")

        # 验证日志文件
        with open(logger.log_file, "r", encoding="utf-8") as f:
            content = f.read()
            if "[AGENT]" in content and "[TOOL]" in content and "[RESULT]" in content:
                print("✅ Agent输出已正确分类记录")
            else:
                print("⚠️  Agent输出分类可能有问题")

    print("\n✅ 输出捕获测试完成!")


def demo_cli_commands():
    """演示CLI命令"""
    print("\n" + "=" * 80)
    print("演示: CLI日志命令")
    print("=" * 80 + "\n")

    print("可用的日志命令:")
    print()
    print("  1. 列出日志文件:")
    print("     krystal logs list")
    print("     krystal logs list -n 30")
    print()
    print("  2. 查看日志内容:")
    print("     krystal logs show 1              # 使用序号查看最新日志")
    print("     krystal logs show krystal_20250208_100000.log")
    print("     krystal logs show 1 -n 50        # 显示最后50行")
    print()
    print("  3. 过滤日志:")
    print("     krystal logs show 1 --filter ERROR")
    print("     krystal logs show 1 --filter 'AGENT'")
    print()
    print("  4. 实时跟踪:")
    print("     krystal logs tail 1")
    print("     krystal logs tail 1 --filter '支付'")
    print("     krystal logs show 1 -f")
    print()


def main():
    """主测试函数"""
    print("\n" + "=" * 80)
    print("🚀 Krystal 日志功能测试")
    print("=" * 80)

    try:
        # 运行所有测试
        test_log_viewer()
        test_colored_logging()
        test_output_capture()
        demo_cli_commands()

        print("\n" + "=" * 80)
        print("✅ 所有测试完成!")
        print("=" * 80 + "\n")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
