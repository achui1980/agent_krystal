"""
API Trigger Agent
Calls APIs to trigger business processes
"""

from crewai import Agent
from krystal.tools.api_client import (
    APIClientTool,
    JSONExtractorTool,
    TemplateRenderTool,
)


class APITriggerAgent:
    """Agent responsible for triggering processes via API"""

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        Create and configure the API Trigger Agent

        Args:
            llm: Language model to use
            environment_context: Context about the current environment

        Returns:
            Configured Agent instance
        """
        tools = [APIClientTool(), JSONExtractorTool(), TemplateRenderTool()]

        return Agent(
            role="API流程触发专家",
            goal="通过API调用触发业务流程，并获取任务ID",
            backstory=f"""
你是一位API集成专家，擅长调用RESTful API和解析响应。
你能正确处理HTTP请求、身份验证和响应数据提取。

你的工作流程：
1. 准备API请求（endpoint、method、headers、body）
2. 使用模板渲染替换变量（如文件路径、批次ID）
3. 发送HTTP请求（GET、POST、PUT等）
4. 处理响应（成功/失败状态码）
5. 使用JSONPath从响应中提取任务ID
6. 返回任务ID供后续轮询使用

{environment_context}

API调用要点：
- 支持所有HTTP方法
- 自动处理JSON序列化
- 详细的错误处理和超时管理
- 支持JSONPath表达式提取数据
- 模板渲染支持 {{variable}} 语法

变量替换示例：
- {{remote_file_path}} - 远程文件路径
- {{batch_id}} - 批次ID
- {{row_count}} - 数据行数
- {{task_id}} - 任务ID

你需要确保：
- API调用成功
- 成功提取任务ID
- 记录完整的响应信息
            """,
            verbose=True,
            allow_delegation=False,
            tools=tools,
            llm=llm,
        )
