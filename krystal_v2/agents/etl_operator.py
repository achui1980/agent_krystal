"""
ETL Operator Agent - ETL流程执行专家
负责执行上传-触发-等待-下载的标准ETL流程
"""

import logging
from typing import Dict, Any, List
from crewai import Agent
from pydantic import BaseModel, Field

from krystal.tools.sftp_client import SFTPClientTool, SFTPFileCheckTool
from krystal.tools.api_client import APIClientTool, JSONExtractorTool


logger = logging.getLogger(__name__)


logger = logging.getLogger(__name__)


class SFTPUploadInput(BaseModel):
    """Input for SFTP upload"""

    local_path: str = Field(description="本地文件路径")
    remote_path: str = Field(description="远程目标路径")
    host: str = Field(description="SFTP服务器地址")
    port: int = Field(default=22, description="SFTP端口")
    username: str = Field(description="用户名")
    password: str = Field(description="密码")


class APITriggerInput(BaseModel):
    """Input for API trigger"""

    endpoint: str = Field(description="API端点URL")
    method: str = Field(default="POST", description="HTTP方法")
    payload: Dict = Field(default={}, description="请求体")
    headers: Dict = Field(default={}, description="请求头")


class PollingInput(BaseModel):
    """Input for polling"""

    task_id: str = Field(description="任务ID")
    status_endpoint: str = Field(description="状态查询端点")
    max_attempts: int = Field(default=30, description="最大轮询次数")
    interval: int = Field(default=10, description="轮询间隔（秒）")


class SFTPDownloadInput(BaseModel):
    """Input for SFTP download"""

    remote_path: str = Field(description="远程文件路径")
    local_path: str = Field(description="本地保存路径")
    host: str = Field(description="SFTP服务器地址")
    port: int = Field(default=22, description="SFTP端口")
    username: str = Field(description="用户名")
    password: str = Field(description="密码")


class ETLOperatorAgent:
    """
    ETL流程执行专家

    职责：
    1. 上传输入文件到SFTP（带3次重试）
    2. 触发服务处理（带3次重试）
    3. 轮询等待完成（带3次重试）
    4. 下载结果文件（带3次重试）
    """

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        创建ETL操作员Agent

        Args:
            llm: LLM模型
            environment_context: 环境上下文信息

        Returns:
            Agent实例
        """
        return Agent(
            role="ETL流程执行专家",
            goal="精确执行上传-触发-等待-下载的ETL流程，确保每个步骤成功完成，遇到问题自动重试",
            backstory=f"""你是一位经验丰富的ETL操作员，专注于准确执行每个步骤。
            
            你的工作流程：
            1. 使用SFTP工具上传输入文件到指定目录
            2. 调用API触发服务开始处理
            3. 轮询检查处理状态，直到完成或失败
            4. 从SFTP下载生成的结果文件
            
            你做事严谨，遇到网络问题或临时故障时会自动重试（最多3次），
            但如果认证失败或配置错误，你会立即上报而不是盲目重试。
            
            你记录每个步骤的执行状态和时间，为后续分析提供详细日志。
            
            {environment_context}
            """,
            verbose=True,
            allow_delegation=False,
            llm=llm,
            tools=[
                SFTPClientTool(),
                SFTPFileCheckTool(),
                APIClientTool(),
                JSONExtractorTool(),
            ],
        )
