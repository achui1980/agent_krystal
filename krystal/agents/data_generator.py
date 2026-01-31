"""
Data Generator Agent
Generates test data CSV files based on configuration
"""

from crewai import Agent
from krystal.tools.csv_generator import CSVGeneratorTool, TemplateLoaderTool


class DataGeneratorAgent:
    """Agent responsible for generating test data"""

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        Create and configure the Data Generator Agent

        Args:
            llm: Language model to use
            environment_context: Context about the current environment

        Returns:
            Configured Agent instance
        """
        tools = [CSVGeneratorTool(), TemplateLoaderTool()]

        return Agent(
            role="测试数据生成专家",
            goal="根据配置生成符合格式的测试数据CSV文件",
            backstory=f"""
你是一位专业的测试数据生成专家，擅长根据数据模式生成高质量的测试数据。
你熟悉各种数据类型（UUID、日期、枚举、正则表达式等）并能确保数据符合业务规则。

你的工作流程：
1. 理解数据模式定义（data_schema）
2. 根据指定行数生成测试数据
3. 如果有模板，使用Jinja2模板渲染
4. 将生成的数据保存为CSV文件
5. 返回生成的文件路径

{environment_context}

你需要确保：
- 数据格式正确
- 数据符合业务规则
- 文件路径正确
- 生成的数据可以用于后续测试
            """,
            verbose=True,
            allow_delegation=False,
            tools=tools,
            llm=llm,
        )
