"""
增强的日志工具 - 支持颜色高亮和Agent思考过程捕获

功能：
1. 终端颜色高亮显示
2. 同时输出到终端和文件
3. 捕获CrewAI Agent思考过程
4. 支持日志查看和过滤
"""

import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from io import StringIO
import re


# ANSI颜色代码
class Colors:
    """终端颜色代码"""

    RESET = "\033[0m"
    BOLD = "\033[1m"

    # 日志级别颜色
    DEBUG = "\033[36m"  # 青色
    INFO = "\033[32m"  # 绿色
    WARNING = "\033[33m"  # 黄色
    ERROR = "\033[31m"  # 红色
    CRITICAL = "\033[35m"  # 紫色

    # 特殊标记颜色
    AGENT_THOUGHT = "\033[94m"  # 亮蓝色 - Agent思考
    TOOL_CALL = "\033[93m"  # 亮黄色 - 工具调用
    TASK_RESULT = "\033[92m"  # 亮绿色 - 任务结果
    SYSTEM = "\033[90m"  # 灰色 - 系统信息


class ColoredFormatter(logging.Formatter):
    """带颜色的日志格式化器"""

    # 日志级别到颜色的映射
    LEVEL_COLORS = {
        logging.DEBUG: Colors.DEBUG,
        logging.INFO: Colors.INFO,
        logging.WARNING: Colors.WARNING,
        logging.ERROR: Colors.ERROR,
        logging.CRITICAL: Colors.CRITICAL,
    }

    def __init__(self, use_color: bool = True, fmt: Optional[str] = None):
        super().__init__(fmt or "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.use_color = use_color and sys.stdout.isatty()  # 只在交互式终端使用颜色

    def format(self, record: logging.LogRecord) -> str:
        # 获取原始消息
        message = super().format(record)

        if not self.use_color:
            return message

        # 根据内容添加颜色
        color = self.LEVEL_COLORS.get(record.levelno, Colors.RESET)

        # 特殊标记处理
        if "[AGENT]" in message or "思考过程" in message or "Thought:" in message:
            message = message.replace(
                "[AGENT]", f"{Colors.AGENT_THOUGHT}[AGENT]{Colors.RESET}"
            )
        elif "[TOOL]" in message or "工具调用" in message or "Action:" in message:
            message = message.replace(
                "[TOOL]", f"{Colors.TOOL_CALL}[TOOL]{Colors.RESET}"
            )
        elif (
            "[RESULT]" in message or "任务完成" in message or "Final Answer:" in message
        ):
            message = message.replace(
                "[RESULT]", f"{Colors.TASK_RESULT}[RESULT]{Colors.RESET}"
            )

        # 高亮日志级别
        for level_name in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            if f" - {level_name} - " in message:
                level_color = self.LEVEL_COLORS.get(
                    getattr(logging, level_name), Colors.RESET
                )
                message = message.replace(
                    f" - {level_name} - ",
                    f" - {level_color}{level_name}{Colors.RESET} - ",
                )

        return message


class CrewOutputCapture:
    """捕获CrewAI输出（Agent思考过程）"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.captured_output = StringIO()
        self.original_stdout = None
        self.is_capturing = False

    def start_capture(self):
        """开始捕获stdout"""
        if self.is_capturing:
            return

        self.original_stdout = sys.stdout
        sys.stdout = self.captured_output
        self.is_capturing = True
        self.logger.debug("[SYSTEM] 开始捕获Agent输出")

    def stop_capture(self) -> str:
        """停止捕获并返回内容"""
        if not self.is_capturing:
            return ""

        sys.stdout = self.original_stdout
        self.is_capturing = False

        output = self.captured_output.getvalue()
        self.captured_output = StringIO()  # 重置

        if output:
            self.logger.debug(f"[SYSTEM] 捕获完成，内容长度: {len(output)} 字符")

        return output

    def log_captured_content(self, output: str, test_id: Optional[str] = None):
        """将捕获的内容记录到日志"""
        if not output.strip():
            return

        prefix = f"[Test:{test_id}] " if test_id else ""

        self.logger.info(f"{prefix}{'=' * 60}")
        self.logger.info(f"{prefix}🤖 Agent 思考过程与执行详情")
        self.logger.info(f"{prefix}{'=' * 60}")

        for line in output.split("\n"):
            if line.strip():
                # 识别不同类型的内容
                if any(keyword in line for keyword in ["Thought:", "思考:", "想法:"]):
                    self.logger.info(f"{prefix}[AGENT] 💭 {line}")
                elif any(keyword in line for keyword in ["Action:", "工具:", "调用:"]):
                    self.logger.info(f"{prefix}[TOOL] 🔧 {line}")
                elif any(
                    keyword in line for keyword in ["Final Answer:", "结果:", "答案:"]
                ):
                    self.logger.info(f"{prefix}[RESULT] ✅ {line}")
                elif any(keyword in line for keyword in ["Observation:", "观察:"]):
                    self.logger.info(f"{prefix}[OBSERVE] 👁️  {line}")
                else:
                    self.logger.info(f"{prefix}    {line}")

        self.logger.info(f"{prefix}{'=' * 60}")


class KrystalLogger:
    """Krystal增强日志管理器"""

    def __init__(
        self,
        name: str = "krystal",
        log_dir: str = "logs",
        log_level: int = logging.INFO,
        use_color: bool = True,
        test_id: Optional[str] = None,
    ):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_level = log_level
        self.use_color = use_color
        self.test_id = test_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.logger: logging.Logger = logging.getLogger(name)
        self.log_file: Optional[Path] = None
        self.capture: Optional[CrewOutputCapture] = None

        self._setup_logger()

    def _setup_logger(self):
        """配置日志记录器"""
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)

        # 清除现有处理器
        self.logger.handlers = []

        # 创建日志目录
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 生成日志文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"krystal_{timestamp}_{self.test_id}.log"

        # 1. 控制台处理器（带颜色）
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_formatter = ColoredFormatter(
            use_color=self.use_color,
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # 2. 文件处理器（纯文本，无颜色）
        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setLevel(self.log_level)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        # 3. 初始化输出捕获器
        self.capture = CrewOutputCapture(self.logger)

        # 记录启动信息
        self.logger.info(f"🚀 Krystal 日志系统启动")
        self.logger.info(f"📝 日志文件: {self.log_file}")
        self.logger.info(f"🎯 Test ID: {self.test_id}")
        self.logger.info("-" * 60)

    def start_agent_capture(self):
        """开始捕获Agent输出"""
        if self.capture:
            self.capture.start_capture()

    def stop_agent_capture(self) -> str:
        """停止捕获并记录Agent输出"""
        if self.capture:
            output = self.capture.stop_capture()
            self.capture.log_captured_content(output, self.test_id)
            return output
        return ""

    def get_logger(self) -> logging.Logger:
        """获取日志记录器"""
        return self.logger

    def info(self, msg: str):
        """记录INFO级别日志"""
        self.logger.info(msg)

    def debug(self, msg: str):
        """记录DEBUG级别日志"""
        self.logger.debug(msg)

    def warning(self, msg: str):
        """记录WARNING级别日志"""
        self.logger.warning(msg)

    def error(self, msg: str):
        """记录ERROR级别日志"""
        self.logger.error(msg)

    def critical(self, msg: str):
        """记录CRITICAL级别日志"""
        self.logger.critical(msg)

    def log_test_start(self, test_name: str, **kwargs):
        """记录测试开始"""
        self.logger.info("=" * 60)
        self.logger.info(f"🧪 测试开始: {test_name}")
        for key, value in kwargs.items():
            self.logger.info(f"   {key}: {value}")
        self.logger.info("=" * 60)

    def log_test_end(self, test_name: str, success: bool, duration: float, **kwargs):
        """记录测试结束"""
        status = "✅ 通过" if success else "❌ 失败"
        self.logger.info("=" * 60)
        self.logger.info(f"🏁 测试结束: {test_name}")
        self.logger.info(f"   状态: {status}")
        self.logger.info(f"   耗时: {duration:.2f}秒")
        for key, value in kwargs.items():
            self.logger.info(f"   {key}: {value}")
        self.logger.info("=" * 60)

    def log_step(self, step_name: str, status: str = "开始", **kwargs):
        """记录执行步骤"""
        icons = {
            "开始": "▶️",
            "完成": "✅",
            "失败": "❌",
            "跳过": "⏭️",
            "重试": "🔄",
        }
        icon = icons.get(status, "➡️")
        self.logger.info(f"{icon} 步骤 [{step_name}]: {status}")
        for key, value in kwargs.items():
            self.logger.info(f"      {key}: {value}")


def setup_krystal_logger(
    name: str = "krystal",
    log_dir: str = "logs",
    log_level: int = logging.INFO,
    use_color: bool = True,
    test_id: Optional[str] = None,
) -> KrystalLogger:
    """
    快速设置Krystal增强日志

    示例:
        logger = setup_krystal_logger(test_id="my_test_001")
        logger.log_test_start("Payment Service Test", service="payment", env="dev")

        # 开始捕获Agent输出
        logger.start_agent_capture()
        result = crew.kickoff()
        agent_output = logger.stop_agent_capture()

        logger.log_test_end("Payment Service Test", success=True, duration=45.2)
    """
    return KrystalLogger(
        name=name,
        log_dir=log_dir,
        log_level=log_level,
        use_color=use_color,
        test_id=test_id,
    )


# 保持向后兼容的函数
def setup_logger(
    name: str = "krystal",
    log_dir: str = "logs",
    log_level: int = logging.INFO,
    log_to_console: bool = True,
    log_to_file: bool = True,
) -> logging.Logger:
    """
    基础日志设置（向后兼容）
    建议使用新的 KrystalLogger 类
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # 清除现有处理器
    logger.handlers = []

    # 创建格式化器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 控制台处理器
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 文件处理器
    if log_to_file:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_path / f"krystal_{timestamp}.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.info(f"Logging to file: {log_file}")

    return logger


def get_logger(name: str = "krystal") -> logging.Logger:
    """获取日志记录器"""
    return logging.getLogger(name)
