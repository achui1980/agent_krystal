"""
Krystal v2.0 CLI - 命令行入口
用法: krystal test --input-file x.csv --expected-file y.csv --service z
"""

import argparse
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()  # 加载默认 .env
# 注意：环境特定的配置在 run_test 中根据 --env 参数加载

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

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
  krystal test --input-file data.csv --expected-file expected.csv --service payment-service --output-dir ./my-reports
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
        "--output-dir", "-o", default="./reports", help="报告输出目录 (默认: ./reports)"
    )
    test_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=True,
        help="详细输出模式 (默认: True)",
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
    # 强制加载环境特定的 secrets.env（覆盖 .env 中的值）
    from dotenv import load_dotenv

    env_file = Path(f"config/{args.env}/secrets.env")
    if env_file.exists():
        load_dotenv(env_file, override=True)

    print(f"🔮 Krystal v2.0 - Intelligent ETL Testing")
    print(f"{'=' * 60}")
    print(f"输入文件: {args.input_file}")
    print(f"预期文件: {args.expected_file}")
    print(f"服务: {args.service}")
    print(f"环境: {args.env}")
    print(f"输出目录: {args.output_dir}")
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
