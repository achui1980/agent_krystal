"""
日志工具模块

提供结构化的日志输出
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class KrystalLogger:
    """Krystal V3 日志管理器"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, name: str = "krystal_v3", log_dir: str = "logs"):
        if hasattr(self, "initialized"):
            return

        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        # 创建 logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # 清除已有 handlers
        self.logger.handlers.clear()

        # 控制台 handler（带颜色）
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        # 文件 handler
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"{name}_{timestamp}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

        self.initialized = True
        self.log_file = log_file

        self.info(f"🚀 Krystal V3 日志系统启动")
        self.info(f"📄 日志文件: {log_file}")

    def debug(self, msg: str):
        self.logger.debug(msg)

    def info(self, msg: str):
        self.logger.info(msg)

    def warning(self, msg: str):
        self.logger.warning(f"⚠️  {msg}")

    def error(self, msg: str):
        self.logger.error(f"❌ {msg}")

    def success(self, msg: str):
        self.logger.info(f"✅ {msg}")

    def section(self, title: str):
        """输出章节标题"""
        self.logger.info("=" * 80)
        self.logger.info(f"📋 {title}")
        self.logger.info("=" * 80)

    def step(self, step_num: int, title: str):
        """输出步骤标题"""
        self.logger.info(f"\n【Step {step_num}】{title}")
        self.logger.info("-" * 60)

    def statistic(self, label: str, value: any):
        """输出统计信息"""
        self.logger.info(f"  📊 {label}: {value}")

    def get_log_file(self) -> Path:
        return self.log_file


# 全局 logger 实例
_logger = None


def get_logger(
    name: str = "krystal_v3", log_dir: str = "krystal_v3/logs"
) -> KrystalLogger:
    """获取日志管理器实例"""
    global _logger
    if _logger is None:
        _logger = KrystalLogger(name, log_dir)
    return _logger
