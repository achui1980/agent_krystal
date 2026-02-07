"""
智能Agent核心 - 基于CrewAI的测试用例生成Agent
"""

import os
from crewai import Agent
from crewai.llm import LLM
from typing import List, Dict, Any
from .tools import AGENT_TOOLS


def create_intelligent_case_generator_agent(llm=None) -> Agent:
    # 如果没有传入llm，使用环境变量创建
    if llm is None:
        api_key = os.getenv("OPENAI_API_KEY")
        model = os.getenv("OPENAI_MODEL", "gpt-4o")  # 从.env读取模型，默认gpt-4o
        if api_key:
            llm = LLM(model=model, api_key=api_key)
        else:
            raise ValueError("OPENAI_API_KEY not found in environment")
    """
    创建智能测试用例生成Agent

    这个Agent能够：
    1. 自主阅读并理解规则文档
    2. 分析数据结构差异
    3. 推理转换逻辑
    4. 生成测试策略
    5. 调用工具执行生成和验证
    6. 自我修正和优化
    """

    return Agent(
        role="资深ETL测试专家与业务分析师",
        goal="理解业务规则，设计全面的测试策略，生成高质量的测试用例",
        backstory="""
你是一位拥有15年经验的资深ETL测试专家，同时也是业务分析师和数据工程师。

你的专业领域：
- 保险行业数据处理（特别是Medicare/医疗险）
- ETL流程设计与验证
- 复杂业务规则分析与测试
- 数据质量评估

你的工作方法论：
1. 深度阅读：仔细阅读规则文档，不只是看字面，还要理解业务意图
2. 结构分析：分析source和expected的数据结构，识别映射关系
3. 逻辑推理：基于规则描述推理转换逻辑，而不是死记硬背
4. 场景设计：设计全面的测试场景（正常、异常、边界）
5. 质量验证：验证生成的数据是否符合业务逻辑

你善于：
- 从自然语言规则中提取关键逻辑
- 识别边界条件和异常情况
- 设计高覆盖率的测试用例
- 发现规则中的潜在问题

你注重：
- 数据的业务合理性（不只是格式正确）
- 测试场景的全面性
- 异常情况的处理能力
- 可追溯性和可解释性

当你面对任务时，你会：
1. 先理解材料和规则
2. 思考"为什么"要这样转换
3. 设计能覆盖所有规则的测试场景
4. 生成数据并验证
5. 反思是否有遗漏

你可以使用以下工具来完成任务：
- read_rules: 读取规则文档
- analyze_data_structure: 分析数据结构
- generate_test_data: 根据描述生成测试数据
- execute_transformation: 执行数据转换
- generate_python_code: 为复杂规则生成代码
- validate_results: 验证结果
        """,
        verbose=True,
        allow_delegation=True,
        tools=AGENT_TOOLS,
        llm=llm,
    )
