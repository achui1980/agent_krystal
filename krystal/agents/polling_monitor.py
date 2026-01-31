"""
Polling Monitor Agent
Monitors task execution status through polling
"""

from crewai import Agent
from krystal.tools.polling_service import PollingServiceTool


class PollingMonitorAgent:
    """Agent responsible for monitoring task status through polling"""

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        Create and configure the Polling Monitor Agent

        Args:
            llm: Language model to use
            environment_context: Context about the current environment

        Returns:
            Configured Agent instance
        """
        tools = [PollingServiceTool()]

        return Agent(
            role="流程执行监控专家",
            goal="轮询检查任务执行状态，直到任务完成或失败",
            backstory=f"""
你是一位流程监控专家，擅长定期轮询API检查任务状态。
你能正确识别成功和失败状态，并记录执行时间。

你的工作流程：
1. 接收任务ID和轮询配置
2. 定期调用状态检查API（按照配置的间隔时间）
3. 解析响应，提取当前状态
4. 判断是否达到终态：
   - 成功状态：任务正常完成
   - 失败状态：任务执行失败
   - 超时：达到最大轮询次数仍未完成
5. 返回轮询结果，包括：
   - 最终状态
   - 尝试次数
   - 耗时
   - 最后响应

{environment_context}

轮询配置参数：
- max_attempts: 最大尝试次数（防止无限轮询）
- interval: 轮询间隔（秒）
- status_extractor: JSONPath提取状态字段
- success_statuses: 表示成功的状态值列表
- failure_statuses: 表示失败的状态值列表

终态判断：
- 成功：status 在 success_statuses 中
- 失败：status 在 failure_statuses 中
- 超时：达到 max_attempts 仍未达到终态

你需要确保：
- 轮询间隔正确
- 终态判断准确
- 记录完整的轮询历史
- 超时处理得当
            """,
            verbose=True,
            allow_delegation=False,
            tools=tools,
            llm=llm,
        )
