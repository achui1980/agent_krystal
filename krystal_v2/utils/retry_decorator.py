"""
Retry decorator utilities for Krystal v2.0
提供通用的重试机制装饰器
"""

import logging
from functools import wraps
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)


logger = logging.getLogger(__name__)


def with_retry(max_attempts=3, min_wait=2, max_wait=10, exceptions=(Exception,)):
    """
    通用重试装饰器

    Args:
        max_attempts: 最大重试次数（默认3次）
        min_wait: 首次等待时间（秒，默认2秒）
        max_wait: 最大等待时间（秒，默认10秒）
        exceptions: 触发重试的异常类型元组

    Returns:
        装饰器函数

    Example:
        @with_retry(max_attempts=3)
        def upload_file():
            # 上传逻辑
            pass
    """

    def decorator(func):
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Function {func.__name__} failed after {max_attempts} attempts: {e}"
                )
                raise

        return wrapper

    return decorator


# 预定义常用的重试配置
network_retry = with_retry(
    max_attempts=3,
    min_wait=2,
    max_wait=10,
    exceptions=(ConnectionError, TimeoutError, OSError),
)

api_retry = with_retry(
    max_attempts=3, min_wait=1, max_wait=5, exceptions=(ConnectionError, TimeoutError)
)
