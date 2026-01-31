"""
SFTP Operator Agent
Handles file upload and download operations
"""

from crewai import Agent
from krystal.tools.sftp_client import SFTPClientTool, SFTPFileCheckTool


class SFTPOperatorAgent:
    """Agent responsible for SFTP file operations"""

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        Create and configure the SFTP Operator Agent

        Args:
            llm: Language model to use
            environment_context: Context about the current environment

        Returns:
            Configured Agent instance
        """
        tools = [SFTPClientTool(), SFTPFileCheckTool()]

        return Agent(
            role="SFTP文件操作专家",
            goal="安全地将文件上传到SFTP服务器，并从SFTP下载结果文件",
            backstory=f"""
你是一位专业的文件传输专家，精通SFTP协议和文件操作。
你能确保文件安全传输，处理各种网络异常，并验证文件完整性。

你的工作流程：
1. 连接到SFTP服务器（支持密码或私钥认证）
2. 确保远程目录存在（必要时创建）
3. 上传文件到指定路径
4. 或从远程路径下载文件
5. 验证传输成功（检查文件大小）
6. 返回操作结果和文件路径

{environment_context}

SFTP操作要点：
- 支持密码认证和SSH私钥认证
- 自动创建远程目录结构
- 自动重试机制（默认3次，指数退避）
- 验证文件完整性
- 详细的错误报告

错误处理：
- 连接失败会重试
- 传输失败会重试
- 所有错误都有详细日志
            """,
            verbose=True,
            allow_delegation=False,
            tools=tools,
            llm=llm,
        )
