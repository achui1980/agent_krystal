"""
Krystal Integration Tests - pytest Configuration

提供测试 fixtures 和共享配置
"""

import os
import sys
import time
import socket
import pytest
import logging
from pathlib import Path
from datetime import datetime
from typing import Generator

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# 配置日志 - 同时输出到控制台和文件
from pathlib import Path

log_dir = Path(__file__).parent.parent / "logs"
log_dir.mkdir(exist_ok=True)

# 创建文件 handler
log_file = log_dir / f"integration_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_file, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)

# 配置根日志记录器
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        file_handler,
    ],
)

print(f"📄 Integration test logs will be saved to: {log_file}")


def check_port_open(host: str, port: int, timeout: int = 2) -> bool:
    """检查指定端口是否开放"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


@pytest.fixture(scope="session")
def ensure_dependencies():
    """
    确保测试依赖已启动

    检查：
    1. SFTP 服务 (localhost:2222)
    2. API Stub (localhost:8000)
    """
    sftp_ok = check_port_open("localhost", 2223)
    api_ok = check_port_open("localhost", 8000)

    if not sftp_ok or not api_ok:
        pytest.fail(
            f"\n"
            f"测试依赖未启动！\n"
            f"SFTP (localhost:2222): {'✅' if sftp_ok else '❌'}\n"
            f"API (localhost:8000): {'✅' if api_ok else '❌'}\n"
            f"\n"
            f"请先启动依赖服务：\n"
            f"  cd integration_tests && podman compose up -d\n"
            f"\n"
            f"或使用 Docker：\n"
            f"  cd integration_tests && docker-compose up -d\n"
        )

    return {"sftp": sftp_ok, "api": api_ok}


@pytest.fixture(scope="function")
def test_logger(tmp_path) -> logging.Logger:
    """
    为每个测试函数创建独立的日志记录器

    日志同时输出到控制台和文件
    """
    logger = logging.getLogger(f"test_{datetime.now().strftime('%H%M%S')}")
    logger.setLevel(logging.DEBUG)

    # 创建文件 handler
    log_file = tmp_path / "test.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    # 添加到 logger
    logger.addHandler(file_handler)

    return logger


@pytest.fixture(scope="function")
def log_capture() -> Generator:
    """
    捕获测试期间的日志输出

    用于捕获 CrewAI agents 的 verbose 输出
    """
    import io

    # 创建 StringIO 捕获 stdout
    captured_output = io.StringIO()

    # 保存原始 stdout
    old_stdout = sys.stdout

    # 重定向 stdout
    sys.stdout = captured_output

    yield captured_output

    # 恢复原始 stdout
    sys.stdout = old_stdout

    # 返回捕获的内容
    captured_output.seek(0)


@pytest.fixture(scope="function")
def temp_test_dir(tmp_path) -> Path:
    """创建临时测试目录"""
    test_dir = tmp_path / "krystal_test"
    test_dir.mkdir(parents=True, exist_ok=True)
    return test_dir


@pytest.fixture(scope="function")
def test_service_config():
    """提供测试服务配置"""
    from krystal.config import ServiceConfig, DataGenerationConfig, DataField

    return ServiceConfig(
        name="test-service",
        enabled=True,
        data_generation=DataGenerationConfig(
            row_count=3,
            output_filename="test_{timestamp}.csv",
            data_schema=[
                DataField(name="id", type="uuid", required=True),
                DataField(
                    name="amount", type="float", min=1.0, max=100.0, required=True
                ),
            ],
        ),
        upload={"remote_path": "/uploads/test/incoming"},
        trigger=None,
        polling=None,
        validation=None,
    )


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """
    测试会话级别的环境设置

    1. 确保环境变量加载（先加载 .env，再加载 secrets.env 补充 SFTP/API 配置）
    2. 创建必要的目录
    """
    # 加载环境变量
    from dotenv import load_dotenv

    # 首先加载根目录的 .env 文件（包含真实的 API key 和代理设置）
    root_env = project_root / ".env"
    if root_env.exists():
        load_dotenv(root_env, override=True)
        print(f"Loaded .env from {root_env}")

    # 然后加载 secrets.env（用于本地特定配置如 SFTP/API，覆盖 .env 的默认值但不覆盖 API key）
    env_file = project_root / "config" / "local" / "secrets.env"
    if env_file.exists():
        with open(env_file, "r") as f:
            content = f.read()
            # 检查是否有占位符 API key
            has_placeholder_key = (
                "sk-your-openai-api-key-here" in content or "sk-your-openai" in content
            )

            for line in content.split("\n"):
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, value = line.split("=", 1)
                        # 对于 API key：只加载有效的（非占位符）
                        if "OPENAI_API_KEY" in key:
                            current_key = os.getenv("OPENAI_API_KEY", "")
                            if not has_placeholder_key and len(value) > 20:
                                os.environ[key] = value
                        # 对于其他变量：总是覆盖 .env 的默认值
                        elif key in [
                            "SFTP_HOST",
                            "SFTP_PORT",
                            "SFTP_USERNAME",
                            "SFTP_PASSWORD",
                            "SFTP_REMOTE_BASE_PATH",
                            "API_TOKEN",
                        ]:
                            os.environ[key] = value
        print(f"Loaded local config from {env_file}")

    # 打印关键环境变量（隐藏敏感信息）
    openai_key = os.getenv("OPENAI_API_KEY", "")
    https_proxy = os.getenv("HTTPS_PROXY", "")
    sftp_host = os.getenv("SFTP_HOST", "")
    sftp_port = os.getenv("SFTP_PORT", "")
    print(
        f"OPENAI_API_KEY: {'Set (' + openai_key[:20] + '...)' if openai_key and len(openai_key) > 20 and 'sk-' in openai_key else 'Not set or invalid'}"
    )
    print(f"HTTPS_PROXY: {https_proxy}")
    print(f"SFTP_HOST: {sftp_host}")
    print(f"SFTP_PORT: {sftp_port}")

    # 确保必要目录存在
    dirs_to_create = [
        project_root / "logs",
        project_root / "reports",
        Path("/tmp/krystal/downloads"),
    ]

    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)

    yield

    # 测试会话结束后的清理（如果需要）
    pass
