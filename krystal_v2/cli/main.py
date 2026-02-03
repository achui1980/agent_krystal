"""
Krystal v2.0 CLI - 命令行入口
用法: krystal test --input-file x.csv --expected-file y.csv --service z
"""

import argparse
import sys
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()  # 加载默认 .env
# 注意：环境特定的配置在 run_test 中根据 --env 参数加载

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 配置日志
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
date_format = "%H:%M:%S"

# 控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(log_format, date_format))

# 根日志配置
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt=date_format,
    handlers=[console_handler],
)

from krystal_v2.crews.etl_test_crew import ETLTestCrew
from krystal.config import ConfigManager


def cli():
    """主CLI入口"""
    parser = argparse.ArgumentParser(
        prog="krystal",
        description="Krystal v2.0 - Intelligent ETL Testing Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 基础用法
  krystal test --input-file data.csv --expected-file expected.csv --service payment-service
  
  # 指定环境
  krystal test --input-file data.csv --expected-file expected.csv --service payment-service --env local
  
        # 指定输出目录
  krystal test --input-file data.csv --expected-file expected.csv --service payment-service --output-dir ./reports_v2
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # test 命令
    test_parser = subparsers.add_parser("test", help="执行ETL测试")
    test_parser.add_argument(
        "--input-file", "-i", required=True, help="输入测试文件路径 (CSV格式)"
    )
    test_parser.add_argument(
        "--expected-file", "-e", required=True, help="预期结果文件路径 (CSV格式)"
    )
    test_parser.add_argument(
        "--service", "-s", required=True, help="服务名称 (对应config中的配置)"
    )
    test_parser.add_argument(
        "--env",
        default="local",
        choices=["local", "dev", "staging", "prod"],
        help="环境 (默认: local)",
    )
    test_parser.add_argument(
        "--output-dir",
        "-o",
        default="./reports_v2",
        help="报告输出目录 (默认: ./reports_v2)",
    )
    test_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=True,
        help="详细输出模式 (默认: True)",
    )
    test_parser.add_argument(
        "--mode",
        "-m",
        default="fast",
        choices=["fast", "crewai"],
        help="执行模式: fast=直接代码执行 (默认), crewai=Agent编排模式",
    )

    # version 命令
    version_parser = subparsers.add_parser("version", help="显示版本信息")

    args = parser.parse_args()

    if args.command == "test":
        run_test(args)
    elif args.command == "version":
        show_version()
    else:
        parser.print_help()
        sys.exit(1)


def run_test(args):
    """执行测试命令"""
    from dotenv import load_dotenv
    from datetime import datetime

    # 1. 先加载 .env 文件（根目录）
    root_env = Path(".env")
    if root_env.exists():
        load_dotenv(root_env)
        logging.info(f"✅ 已加载 .env 文件")

    # 2. 加载环境特定的 secrets.env（作为基础配置）
    env_file = Path(f"config/{args.env}/secrets.env")
    if env_file.exists():
        load_dotenv(env_file)
        logging.info(f"✅ 已加载 {env_file}")

    # 3. 再次加载 .env，用 .env 的值覆盖 secrets.env（.env 优先级更高）
    if root_env.exists():
        load_dotenv(root_env, override=True)
        logging.info(f"✅ 用 .env 覆盖 secrets.env（优先级：.env > secrets.env）")

    # 设置文件日志处理器 - 放到 logs 目录
    logs_path = Path("./logs")
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / f"krystal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    # 添加到根日志记录器
    logging.getLogger().addHandler(file_handler)

    logging.info(f"📝 日志文件: {log_file}")

    print(f"🔮 Krystal v2.0 - Intelligent ETL Testing")
    print(f"{'=' * 60}")
    print(f"输入文件: {args.input_file}")
    print(f"预期文件: {args.expected_file}")
    print(f"服务: {args.service}")
    print(f"环境: {args.env}")
    print(f"执行模式: {args.mode}")
    print(f"报告目录: {args.output_dir}")
    print(f"日志目录: {logs_path}")
    print(f"日志文件: {log_file}")
    print(f"{'=' * 60}\n")

    # 验证输入文件存在
    if not Path(args.input_file).exists():
        print(f"❌ 错误: 输入文件不存在: {args.input_file}")
        sys.exit(1)

    if not Path(args.expected_file).exists():
        print(f"❌ 错误: 预期文件不存在: {args.expected_file}")
        sys.exit(1)

    # 加载配置（复用现有配置系统）
    try:
        config_manager = ConfigManager(args.env)
        global_config = config_manager.load()
        service_config = config_manager.get_service(args.service)
        if not service_config:
            print(f"❌ 错误: 服务配置未找到: {args.service}")
            available = (
                [s.name for s in global_config.services]
                if hasattr(global_config, "services")
                else []
            )
            print(f"   可用服务: {available}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ 错误: 加载配置失败: {e}")
        sys.exit(1)

    # 创建并运行Crew
    try:
        crew = ETLTestCrew(
            input_file=args.input_file,
            expected_file=args.expected_file,
            service_config=service_config,
            global_config=global_config,
            environment=args.env,
            output_dir=args.output_dir,
            mode=args.mode,
        )

        result = crew.run()

        print(f"\n{'=' * 60}")
        if result["success"]:
            print(f"✅ 测试完成!")
            print(f"测试ID: {result['test_id']}")
            print(f"报告位置: {result['output_dir']}")
            print(f"{'=' * 60}")
            sys.exit(0)
        else:
            print(f"❌ 测试失败!")
            print(f"测试ID: {result['test_id']}")
            print(f"错误: {result.get('error', 'Unknown error')}")
            print(f"{'=' * 60}")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ 执行错误: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def show_version():
    """显示版本信息"""
    from krystal_v2 import __version__

    print(f"Krystal v{__version__}")
    print(f"Intelligent ETL Testing Framework")
    print(f"Built with CrewAI")


if __name__ == "__main__":
    cli()
