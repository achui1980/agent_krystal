"""
Krystal 日志查看和管理模块

提供CLI命令查看和分析日志文件
"""

import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Iterator
import re


# ANSI颜色代码（与logging_utils.py保持一致）
class Colors:
    """终端颜色代码"""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DEBUG = "\033[36m"  # 青色
    INFO = "\033[32m"  # 绿色
    WARNING = "\033[33m"  # 黄色
    ERROR = "\033[31m"  # 红色
    CRITICAL = "\033[35m"  # 紫色
    AGENT_THOUGHT = "\033[94m"  # 亮蓝色
    TOOL_CALL = "\033[93m"  # 亮黄色
    TASK_RESULT = "\033[92m"  # 亮绿色
    SYSTEM = "\033[90m"  # 灰色


def colored_print(text: str, color: str = "", end: str = "\n"):
    """带颜色的打印"""
    if color and sys.stdout.isatty():
        print(f"{color}{text}{Colors.RESET}", end=end)
    else:
        print(text, end=end)


def get_log_files(log_dir: str = "logs", limit: int = 20) -> List[Path]:
    """获取日志文件列表，按修改时间倒序"""
    log_path = Path(log_dir)
    if not log_path.exists():
        return []

    log_files = [
        f
        for f in log_path.iterdir()
        if f.is_file() and f.suffix == ".log" and f.name.startswith("krystal_")
    ]

    # 按修改时间倒序
    log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

    return log_files[:limit]


def parse_log_filename(filename: str) -> dict:
    """解析日志文件名获取信息"""
    # 格式: krystal_YYYYMMDD_HHMMSS[_testid].log
    pattern = r"krystal_(\d{8})_(\d{6})(?:_(.+))?\.log"
    match = re.match(pattern, filename)

    if match:
        date_str, time_str, test_id = match.groups()
        return {
            "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
            "time": f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:]}",
            "test_id": test_id or "N/A",
            "filename": filename,
        }
    return {"filename": filename, "date": "N/A", "time": "N/A", "test_id": "N/A"}


def format_file_size(size_bytes: int) -> str:
    """格式化文件大小"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def list_logs(log_dir: str = "logs", limit: int = 20):
    """列出日志文件"""
    log_files = get_log_files(log_dir, limit)

    if not log_files:
        colored_print(f"⚠️  没有找到日志文件 (目录: {log_dir})", Colors.WARNING)
        return

    colored_print("=" * 80, Colors.BOLD)
    colored_print("📋 Krystal 日志文件列表", Colors.BOLD)
    colored_print("=" * 80, Colors.BOLD)
    print()

    print(f"{'序号':<6} {'文件名':<40} {'日期':<12} {'时间':<10} {'大小':<10}")
    print("-" * 80)

    for idx, log_file in enumerate(log_files, 1):
        info = parse_log_filename(log_file.name)
        size = format_file_size(log_file.stat().st_size)

        # 高亮今天的日志
        today = datetime.now().strftime("%Y-%m-%d")
        color = Colors.INFO if info["date"] == today else ""

        colored_print(
            f"{idx:<6} {info['filename']:<40} {info['date']:<12} {info['time']:<10} {size:<10}",
            color,
        )

    print()
    colored_print(
        f"💡 提示: 使用 'krystal logs show <序号>' 或 'krystal logs show <文件名>' 查看详情",
        Colors.SYSTEM,
    )
    print()


def highlight_log_line(line: str) -> str:
    """为日志行添加颜色高亮"""
    # 日志级别高亮
    if " - DEBUG - " in line:
        return f"{Colors.DEBUG}{line}{Colors.RESET}"
    elif " - INFO - " in line:
        return f"{Colors.INFO}{line}{Colors.RESET}"
    elif " - WARNING - " in line:
        return f"{Colors.WARNING}{line}{Colors.RESET}"
    elif " - ERROR - " in line:
        return f"{Colors.ERROR}{line}{Colors.RESET}"
    elif " - CRITICAL - " in line:
        return f"{Colors.CRITICAL}{line}{Colors.RESET}"

    # 特殊内容高亮
    if "[AGENT]" in line or "💭" in line:
        return f"{Colors.AGENT_THOUGHT}{line}{Colors.RESET}"
    elif "[TOOL]" in line or "🔧" in line:
        return f"{Colors.TOOL_CALL}{line}{Colors.RESET}"
    elif "[RESULT]" in line or "✅" in line:
        return f"{Colors.TASK_RESULT}{line}{Colors.RESET}"
    elif "🚀" in line or "🧪" in line or "🏁" in line:
        return f"{Colors.BOLD}{line}{Colors.RESET}"

    return line


def show_log(
    log_file: str,
    lines: int = 100,
    filter_keyword: Optional[str] = None,
    follow: bool = False,
):
    """显示日志文件内容"""
    log_path = Path(log_file)

    # 如果是数字，尝试从列表中获取
    if log_file.isdigit():
        log_files = get_log_files(limit=int(log_file) + 5)
        idx = int(log_file) - 1
        if 0 <= idx < len(log_files):
            log_path = log_files[idx]
        else:
            colored_print(f"❌ 无效的序号: {log_file}", Colors.ERROR)
            return
    elif not log_path.is_absolute():
        # 尝试在logs目录中查找
        logs_dir = Path("logs")
        if (logs_dir / log_file).exists():
            log_path = logs_dir / log_file

    if not log_path.exists():
        colored_print(f"❌ 日志文件不存在: {log_path}", Colors.ERROR)
        return

    colored_print("=" * 80, Colors.BOLD)
    colored_print(f"📄 查看日志: {log_path.name}", Colors.BOLD)
    if filter_keyword:
        colored_print(f"🔍 过滤关键字: {filter_keyword}", Colors.WARNING)
    colored_print("=" * 80, Colors.BOLD)
    print()

    try:
        if follow:
            # 实时跟踪模式
            _tail_log(log_path, filter_keyword)
        else:
            # 普通查看模式
            _show_log_content(log_path, lines, filter_keyword)
    except KeyboardInterrupt:
        print()
        colored_print("\n⏹️  已停止查看", Colors.WARNING)
    except Exception as e:
        colored_print(f"\n❌ 读取日志失败: {e}", Colors.ERROR)


def _show_log_content(log_path: Path, lines: int, filter_keyword: Optional[str]):
    """显示日志内容（非跟踪模式）"""
    with open(log_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    # 过滤
    if filter_keyword:
        filtered_lines = [l for l in all_lines if filter_keyword.lower() in l.lower()]
    else:
        filtered_lines = all_lines

    # 显示最后N行
    display_lines = (
        filtered_lines[-lines:] if len(filtered_lines) > lines else filtered_lines
    )

    if not display_lines:
        colored_print("⚠️  没有匹配的行", Colors.WARNING)
        return

    # 显示统计
    total = len(all_lines)
    showing = len(display_lines)
    filtered = len(filtered_lines)

    if filter_keyword:
        colored_print(
            f"📊 总行数: {total} | 匹配: {filtered} | 显示: {showing}\n", Colors.SYSTEM
        )
    else:
        colored_print(f"📊 总行数: {total} | 显示最后: {showing}\n", Colors.SYSTEM)

    # 打印内容
    for line in display_lines:
        highlighted = highlight_log_line(line.rstrip())
        print(highlighted)

    print()
    colored_print("-" * 80, Colors.SYSTEM)
    colored_print("💡 提示: 使用 --follow 参数实时跟踪日志更新", Colors.SYSTEM)


def _tail_log(log_path: Path, filter_keyword: Optional[str]):
    """实时跟踪日志（类似tail -f）"""
    colored_print("👁️  实时跟踪模式 (按 Ctrl+C 停止)\n", Colors.INFO)

    with open(log_path, "r", encoding="utf-8") as f:
        # 跳到文件末尾
        f.seek(0, 2)

        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue

            if filter_keyword and filter_keyword.lower() not in line.lower():
                continue

            highlighted = highlight_log_line(line.rstrip())
            print(highlighted)


def create_logs_subparser(subparsers):
    """创建logs子命令解析器"""
    logs_parser = subparsers.add_parser(
        "logs",
        help="查看和管理日志文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 列出最近的日志文件
  krystal logs list
  krystal logs list -n 30

  # 查看日志文件（使用序号）
  krystal logs show 1              # 查看最新的日志
  krystal logs show 1 -n 50        # 查看最后50行
  krystal logs show 1 -f           # 实时跟踪

  # 查看日志文件（使用文件名）
  krystal logs show krystal_20260208_103000.log

  # 过滤日志
  krystal logs show 1 --filter ERROR     # 只显示错误
  krystal logs show 1 --filter "AGENT"   # 只显示Agent思考过程
  krystal logs show 1 -f --filter "支付"  # 实时跟踪包含"支付"的日志
        """,
    )

    logs_subparsers = logs_parser.add_subparsers(dest="logs_command", help="日志子命令")

    # list 子命令
    list_parser = logs_subparsers.add_parser("list", help="列出日志文件")
    list_parser.add_argument(
        "-n", "--limit", type=int, default=20, help="显示最近的N个日志文件 (默认: 20)"
    )

    # show 子命令
    show_parser = logs_subparsers.add_parser("show", help="查看日志文件内容")
    show_parser.add_argument("log_file", help="日志文件（序号或文件名）")
    show_parser.add_argument(
        "-n", "--lines", type=int, default=100, help="显示最后N行 (默认: 100)"
    )
    show_parser.add_argument(
        "-f", "--follow", action="store_true", help="实时跟踪模式 (类似 tail -f)"
    )
    show_parser.add_argument("--filter", help="过滤关键字")

    # tail 子命令（show --follow 的快捷方式）
    tail_parser = logs_subparsers.add_parser("tail", help="实时跟踪日志")
    tail_parser.add_argument(
        "log_file", nargs="?", default="1", help="日志文件（序号或文件名，默认: 1最新）"
    )
    tail_parser.add_argument("--filter", help="过滤关键字")

    return logs_parser


def handle_logs_command(args):
    """处理logs命令"""
    if args.logs_command == "list":
        list_logs(limit=args.limit)
    elif args.logs_command == "show":
        show_log(
            args.log_file,
            lines=args.lines,
            filter_keyword=args.filter,
            follow=args.follow,
        )
    elif args.logs_command == "tail":
        show_log(args.log_file, lines=50, filter_keyword=args.filter, follow=True)
    else:
        # 如果没有子命令，显示帮助
        print("使用 'krystal logs list' 列出日志文件")
        print("使用 'krystal logs show <文件>' 查看日志内容")
        print("使用 'krystal logs tail <文件>' 实时跟踪日志")


if __name__ == "__main__":
    # 测试代码
    import sys

    if len(sys.argv) < 2:
        print("测试: python -m krystal.log_viewer list")
        print("      python -m krystal.log_viewer show 1")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "list":
        list_logs()
    elif cmd == "show" and len(sys.argv) >= 3:
        show_log(sys.argv[2])
    else:
        print(f"未知命令: {cmd}")
