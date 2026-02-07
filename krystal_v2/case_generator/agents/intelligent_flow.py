"""
智能生成流程 - Agent-based工作流
"""

from crewai import Crew, Task, Process
from pathlib import Path
from typing import Dict, Any

from .intelligent_agent import create_intelligent_case_generator_agent


class IntelligentCaseGenerationFlow:
    """
    智能测试用例生成流程

    不依赖硬编码规则，完全由Agent自主完成
    """

    def __init__(self, llm=None):
        self.agent = create_intelligent_case_generator_agent(llm)
        self.output_dir = None

    def run(
        self,
        rules_path: str,
        source_path: str,
        expected_path: str,
        output_dir: str = "./generated",
    ) -> Dict[str, Any]:
        """
        执行完整的智能生成流程

        Args:
            rules_path: 规则文件路径
            source_path: Source模板路径
            expected_path: Expected模板路径
            output_dir: 输出目录

        Returns:
            生成结果报告
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 创建任务流程
        tasks = self._create_tasks(rules_path, source_path, expected_path)

        # 创建Crew
        crew = Crew(
            agents=[self.agent], tasks=tasks, process=Process.sequential, verbose=True
        )

        # 执行
        result = crew.kickoff()

        return {
            "status": "completed",
            "result": result,
            "output_dir": str(self.output_dir),
        }

    def _create_tasks(
        self, rules_path: str, source_path: str, expected_path: str
    ) -> list:
        """创建任务序列"""

        # Task 1: 理解规则文档
        task_understand_rules = Task(
            description=f"""
作为资深ETL测试专家，请仔细阅读并深度理解规则文档：{rules_path}

你需要：
1. 使用read_rules工具读取规则文件
2. 逐行分析每条规则的业务含义
3. 识别不同类型的转换逻辑：
   - 直接字段映射
   - 条件映射（如if...then...）
   - 默认值设置
   - 字段拆分/合并
   - 复杂转换逻辑
4. 总结关键业务规则和数据约束
5. 思考：这些规则背后反映了什么业务流程？

请输出详细的规则理解报告，包括：
- 规则分类统计
- 关键转换逻辑说明
- 潜在的业务约束
- 需要特别注意的规则
            """,
            expected_output="详细的规则理解报告，包含规则分类和关键逻辑说明",
            agent=self.agent,
        )

        # Task 2: 分析数据结构
        task_analyze_structure = Task(
            description=f"""
分析source和expected的数据结构差异：
- Source文件: {source_path}
- Expected文件: {expected_path}

请使用analyze_data_structure工具分析并回答：

1. Source数据结构：
   - 有哪些字段？
   - 各字段的数据类型和格式？
   - 样本数据示例？

2. Expected数据结构：
   - 有哪些字段？
   - 元数据信息（ACTION_ID等）？
   - 字段数量差异？

3. 映射关系分析：
   - 哪些source字段直接映射到expected？
   - 哪些字段需要转换？
   - 哪些字段是新增的（无source来源）？
   - 字段数量为什么有差异？

4. 结合规则理解，推断转换逻辑：
   - 每条expected字段是如何从source计算得到的？
   - 需要哪些中间转换步骤？
            """,
            expected_output="数据结构分析报告，包含字段映射关系和转换逻辑推断",
            agent=self.agent,
            context=[task_understand_rules],
        )

        # Task 3: 设计测试策略
        task_design_strategy = Task(
            description="""
基于规则理解和结构分析，设计全面的测试策略。

请回答以下问题并设计测试场景：

**正常场景设计：**
1. 需要覆盖哪些产品类型？（PDP/LPPO/HUM/HV/RD等）
2. 每种产品类型的预期输出是什么？
3. 需要覆盖哪些状态？（Active/Termed）
4. 需要测试哪些字段组合？
5. 建议生成多少条正常数据？

**异常场景设计：**
1. 哪些字段缺失会导致问题？
2. 哪些无效值需要测试？
3. 格式错误的情况有哪些？
4. 业务逻辑错误的情况？
5. 建议生成多少条异常数据？

**边界场景设计：**
1. 日期边界：最早/最晚出生日期？
2. 数值边界：金额最大/最小值？
3. 字符串边界：最长/最短值？
4. 状态边界：Active和Termed的临界点？
5. 建议生成多少条边界数据？

**测试策略输出：**
- 场景列表（每种场景的目的和预期）
- 数据生成计划
- 规则覆盖目标
            """,
            expected_output="详细的测试策略文档，包含正常/异常/边界场景设计",
            agent=self.agent,
            context=[task_understand_rules, task_analyze_structure],
        )

        # Task 4: 生成Source测试数据
        task_generate_source = Task(
            description=f"""
根据测试策略，生成Source格式的测试数据。

输出将保存到: {self.output_dir}/source.csv

执行步骤：
1. 根据策略中的场景设计，逐一生成数据
2. 对于每个场景：
   - 使用generate_test_data工具生成数据
   - 确保数据符合source字段结构
   - 添加场景标识（_scenario字段）
   
3. 生成顺序建议：
   - 先生成正常场景数据
   - 再生成异常场景数据
   - 最后生成边界场景数据
   
4. 数据质量检查：
   - 所有必填字段有值
   - 数据格式正确
   - 符合业务逻辑（如州和城市匹配）

请记录每个测试点的：
- 场景描述
- 覆盖的规则
- 数据特征
            """,
            expected_output="生成的source.csv文件和测试点清单",
            agent=self.agent,
            context=[task_design_strategy],
        )

        # Task 5: 执行规则转换
        task_execute_transformation = Task(
            description=f"""
将生成的source数据转换为expected格式。

输入: {self.output_dir}/source.csv
输出: {self.output_dir}/expected.txt

执行步骤：
1. 读取source数据
2. 根据规则理解，逐条执行转换：
   - 直接映射的字段直接复制
   - 条件映射的字段根据条件判断
   - 需要计算的字段执行计算
   - 默认值的字段填充默认值
   
3. 对于复杂规则：
   - 使用execute_transformation工具
   - 或调用generate_python_code生成代码后执行
   
4. 确保输出格式符合expected.txt要求：
   - 元数据头（ACTION_ID等）
   - 竖线分隔符
   - 正确的字段顺序
   
5. 处理特殊情况：
   - 空值处理
   - 格式转换（如日期格式）
   - 字段拆分/合并
            """,
            expected_output="生成的expected.txt文件",
            agent=self.agent,
            context=[task_generate_source],
        )

        # Task 6: 验证和报告
        task_validate_and_report = Task(
            description=f"""
验证生成的测试用例质量，并生成详细报告。

执行步骤：
1. 使用validate_results工具验证数据质量
2. 检查规则覆盖情况：
   - 哪些规则被覆盖了？
   - 覆盖了多少次？
   - 哪些规则未被覆盖？
   
3. 生成详细报告，包括：
   - 测试点说明（每个测试用例的目的）
   - 规则覆盖矩阵
   - 数据样例
   - 优化建议
   
4. 输出格式：
   - JSON格式报告（detailed_report.json）
   - Markdown格式报告（detailed_report.md）
   
5. 保存到: {self.output_dir}/

报告内容要求：
- 生成概况（总数、分类统计）
- 测试点详细说明（场景、目的、覆盖规则）
- 规则覆盖矩阵（规则→测试用例映射）
- 未覆盖规则分析
- 优化建议
            """,
            expected_output="详细的测试报告（JSON和Markdown格式）",
            agent=self.agent,
            context=[task_execute_transformation],
        )

        return [
            task_understand_rules,
            task_analyze_structure,
            task_design_strategy,
            task_generate_source,
            task_execute_transformation,
            task_validate_and_report,
        ]
