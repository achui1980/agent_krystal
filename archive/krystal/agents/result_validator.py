"""
Result Validator Agent
Validates test results against expectations
"""

from crewai import Agent
from krystal.tools.validator import DataValidatorTool, FileValidatorTool
from krystal.tools.sftp_client import SFTPClientTool


class ResultValidatorAgent:
    """Agent responsible for validating test results"""

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        Create and configure the Result Validator Agent

        Args:
            llm: Language model to use
            environment_context: Context about the current environment

        Returns:
            Configured Agent instance
        """
        tools = [DataValidatorTool(), FileValidatorTool(), SFTPClientTool()]

        return Agent(
            role="测试结果验证专家",
            goal="下载结果文件并与预期进行对比，生成验证报告",
            backstory=f"""
你是一位质量验证专家，擅长数据对比和结果验证。
你能识别数据差异，验证数据完整性，并生成详细的测试报告。

你的工作流程：
1. 从SFTP下载结果文件（如果配置需要）
2. 验证文件存在性和属性（大小等）
3. 对比实际结果与预期：
   - CSV模式：对比行列数据
   - 文件存在模式：仅验证文件存在
4. 应用验证规则（如字段非空、值范围等）
5. 生成详细的验证报告：
   - 测试通过/失败状态
   - 数据对比详情
   - 错误列表和警告
   - 匹配行数统计

{environment_context}

验证模式：
1. CSV对比模式：
   - 对比预期CSV和实际CSV
   - 支持主键匹配或无主键行对比
   - 验证列存在性和值匹配
   - 支持自定义验证规则

2. 文件存在模式：
   - 仅验证结果文件是否存在
   - 验证文件大小范围
   - 适用于PDF、XML等非CSV输出

验证规则类型：
- equals: 字段等于另一个字段或期望值
- not_empty: 字段非空
- range: 数值在指定范围内
- regex: 匹配正则表达式

报告内容：
- 验证通过状态
- 错误详情列表
- 警告信息
- 数据行数统计
- 匹配率

你需要确保：
- 下载文件成功
- 对比逻辑准确
- 错误描述清晰
- 报告格式规范
            """,
            verbose=True,
            allow_delegation=False,
            tools=tools,
            llm=llm,
        )
