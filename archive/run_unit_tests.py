#!/usr/bin/env python3
"""
运行单元测试

使用方法：
    python run_unit_tests.py          # 运行所有测试
    python run_unit_tests.py -v       # 详细输出
    python run_unit_tests.py -k test  # 运行匹配的测试
"""

import sys
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser(description="Run Krystal unit tests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-k", "--keyword", help="Run tests matching keyword")
    parser.add_argument("--cov", action="store_true", help="Run with coverage")

    args = parser.parse_args()

    # 构建 pytest 命令
    cmd = ["python", "-m", "pytest", "tests/"]

    if args.verbose:
        cmd.append("-v")

    if args.keyword:
        cmd.extend(["-k", args.keyword])

    if args.cov:
        cmd.extend(["--cov=krystal", "--cov-report=term-missing"])

    # 运行测试
    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)

    result = subprocess.run(cmd)

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
