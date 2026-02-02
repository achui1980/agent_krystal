"""
ETL Test Crew - CrewAI编排
3-Agent顺序执行：ETLOperator → ResultValidator → ReportWriter
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from crewai import Crew, Task, Process
from crewai.llm import LLM

from ..agents import ETLOperatorAgent, ResultValidatorAgent, ReportWriterAgent


logger = logging.getLogger(__name__)


class ETLTestCrew:
    """
    ETL测试Crew

    编排3个Agent顺序执行：
    1. ETLOperator: 执行ETL流程（上传→触发→等待→下载）
    2. ResultValidator: 对比实际结果和预期结果
    3. ReportWriter: 生成Markdown和HTML报告

    配置：
    - process: sequential（顺序执行）
    - planning: True（启用智能规划）
    - memory: True（启用上下文记忆）
    """

    def __init__(
        self,
        input_file: str,
        expected_file: str,
        service_config: Any,
        global_config: Any = None,
        environment: str = "local",
        output_dir: str = "./reports",
        llm=None,
    ):
        """
        初始化ETL测试Crew

        Args:
            input_file: 输入测试文件路径
            expected_file: 预期结果文件路径
            service_config: 服务配置（包含upload、trigger、polling等）
            global_config: 全局配置（包含SFTP、API等）
            environment: 环境名称
            output_dir: 报告输出目录
            llm: LLM模型（可选，默认使用环境变量配置的模型）
        """
        self.input_file = input_file
        self.expected_file = expected_file
        self.service_config = service_config
        self.global_config = global_config or {}
        self.environment = environment
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 生成测试ID
        self.test_id = f"etl_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 初始化LLM
        if llm is None:
            llm = LLM(
                model=os.getenv("OPENAI_MODEL", "gpt-4o"),
                api_key=os.getenv("OPENAI_API_KEY"),
            )
        self.llm = llm

        # 创建环境上下文
        # 将配置转换为字典以便使用
        if hasattr(service_config, "name"):
            service_name = service_config.name
            self.service_config_dict = {
                "name": service_config.name,
                "upload": {},
                "trigger": {},
                "polling": {},
            }

            # 尝试提取服务配置
            if hasattr(service_config, "upload") and service_config.upload:
                upload = service_config.upload
                self.service_config_dict["upload"] = {
                    "remote_path": getattr(upload, "remote_path", "/uploads"),
                }
            if hasattr(service_config, "trigger") and service_config.trigger:
                trigger = service_config.trigger
                self.service_config_dict["trigger"] = {
                    "endpoint": getattr(trigger, "endpoint", ""),
                    "method": getattr(trigger, "method", "POST"),
                    "headers": getattr(trigger, "headers", {}),
                    "body_template": getattr(trigger, "body_template", ""),
                    "task_id_extractor": getattr(trigger, "task_id_extractor", ""),
                }
            if hasattr(service_config, "polling") and service_config.polling:
                polling = service_config.polling
                self.service_config_dict["polling"] = {
                    "max_attempts": getattr(polling, "max_attempts", 30),
                    "interval": getattr(polling, "interval", 10),
                    "status_check_endpoint": getattr(
                        polling, "status_check_endpoint", ""
                    ),
                }
        else:
            service_name = (
                service_config.get("name", "unknown")
                if isinstance(service_config, dict)
                else "unknown"
            )
            self.service_config_dict = (
                service_config if isinstance(service_config, dict) else {}
            )

        # 从全局配置提取 SFTP 配置
        self.sftp_config = {}
        if global_config and hasattr(global_config, "sftp") and global_config.sftp:
            sftp = global_config.sftp
            self.sftp_config = {
                "host": getattr(sftp, "host", "localhost"),
                "port": getattr(sftp, "port", 2223),
                "username": getattr(sftp, "username", "testuser"),
                "password": getattr(sftp, "password", ""),
                "remote_base_path": getattr(sftp, "remote_base_path", "/uploads"),
            }
        elif isinstance(global_config, dict) and "sftp" in global_config:
            self.sftp_config = global_config["sftp"]
        self.llm = llm

        # 创建环境上下文
        # 将 ServiceConfig 对象转换为字典以便使用
        if hasattr(service_config, "name"):
            service_name = service_config.name
            self.service_config_dict = {
                "name": service_config.name,
                "sftp": {},
                "api": {},
                "upload": {},
                "trigger": {},
                "polling": {},
            }
            # 尝试提取 SFTP 配置
            if hasattr(service_config, "sftp") and service_config.sftp:
                sftp = service_config.sftp
                self.service_config_dict["sftp"] = {
                    "host": getattr(sftp, "host", "localhost"),
                    "port": getattr(sftp, "port", 2223),
                    "username": getattr(sftp, "username", "testuser"),
                    "password": getattr(sftp, "password", ""),
                    "remote_base_path": getattr(sftp, "remote_base_path", "/uploads"),
                }

            # 尝试提取其他配置
            if hasattr(service_config, "upload") and service_config.upload:
                upload = service_config.upload
                self.service_config_dict["upload"] = {
                    "remote_path": getattr(upload, "remote_path", "/uploads"),
                }
            if hasattr(service_config, "trigger") and service_config.trigger:
                trigger = service_config.trigger
                self.service_config_dict["trigger"] = {
                    "endpoint": getattr(trigger, "endpoint", ""),
                    "method": getattr(trigger, "method", "POST"),
                    "headers": getattr(trigger, "headers", {}),
                    "body_template": getattr(trigger, "body_template", ""),
                    "task_id_extractor": getattr(trigger, "task_id_extractor", ""),
                }
            if hasattr(service_config, "polling") and service_config.polling:
                polling = service_config.polling
                self.service_config_dict["polling"] = {
                    "max_attempts": getattr(polling, "max_attempts", 30),
                    "interval": getattr(polling, "interval", 10),
                    "status_check_endpoint": getattr(
                        polling, "status_check_endpoint", ""
                    ),
                }
        else:
            service_name = (
                service_config.get("name", "unknown")
                if isinstance(service_config, dict)
                else "unknown"
            )
            self.service_config_dict = (
                service_config if isinstance(service_config, dict) else {}
            )

        self.environment_context = f"""
        当前环境: {environment}
        测试ID: {self.test_id}
        输入文件: {input_file}
        预期文件: {expected_file}
        服务: {service_name}
        """

    def create_crew(self) -> Crew:
        """
        创建并配置Crew

        Returns:
            配置好的Crew实例
        """
        # 创建Agents
        etl_operator = ETLOperatorAgent.create(
            llm=self.llm, environment_context=self.environment_context
        )

        result_validator = ResultValidatorAgent.create(
            llm=self.llm, environment_context=self.environment_context
        )

        report_writer = ReportWriterAgent.create(
            llm=self.llm, environment_context=self.environment_context
        )

        # 创建Tasks
        etl_task = self._create_etl_task(etl_operator)
        validation_task = self._create_validation_task(result_validator, etl_task)
        report_task = self._create_report_task(report_writer, etl_task, validation_task)

        # 创建Crew
        crew = Crew(
            agents=[etl_operator, result_validator, report_writer],
            tasks=[etl_task, validation_task, report_task],
            process=Process.sequential,
            planning=True,
            memory=True,
            verbose=True,
        )

        return crew

    def _create_etl_task(self, agent) -> Task:
        """
        创建ETL执行任务

        Args:
            agent: ETLOperator Agent

        Returns:
            Task实例
        """
        sftp_config = self.sftp_config
        api_config = self.service_config_dict.get("api", {})

        return Task(
            description=f"""
            执行完整的ETL流程，将输入文件上传到SFTP，触发服务处理，等待完成，下载结果。
            
            输入文件: {self.input_file}
            服务配置: {self.service_config_dict.get("name", "unknown")}
            
            执行步骤：
            1. 上传文件到SFTP（使用配置：{sftp_config}）
               - 本地文件: {self.input_file}
               - 远程路径: 根据服务配置确定
               
            2. 触发服务处理（使用配置：{api_config}）
               - 调用API触发端点
               - 获取task_id
               
            3. 轮询等待处理完成
               - 查询状态端点
               - 直到状态为completed或failed
               - 最多等待5分钟（30次轮询，每次10秒）
               
            4. 下载结果文件
               - 从SFTP下载生成的结果文件
               - 保存到本地临时目录
            
            注意：
            - 每个步骤如果失败会自动重试3次
            - 记录每个步骤的执行时间和状态
            - 如果任何步骤失败，停止执行并报告
            
            输出要求：
            返回JSON格式结果：
            {{
                "success": true/false,
                "steps": {{
                    "upload": {{"success": true, "duration": 2.3, "remote_path": "..."}},
                    "trigger": {{"success": true, "duration": 0.8, "task_id": "..."}},
                    "wait": {{"success": true, "duration": 105.2, "status": "completed"}},
                    "download": {{"success": true, "duration": 1.5, "local_path": "..."}}
                }},
                "total_duration": 109.8,
                "result_file": "/path/to/downloaded_result.csv"
            }}
            """,
            expected_output="包含result_file路径的JSON格式执行结果",
            agent=agent,
        )

    def _create_validation_task(self, agent, etl_task: Task) -> Task:
        """
        创建验证任务

        Args:
            agent: ResultValidator Agent
            etl_task: ETL任务（用于获取结果文件路径）

        Returns:
            Task实例
        """
        return Task(
            description=f"""
            对比实际结果和预期结果，进行精确的行级对比。
            
            从ETL任务获取：
            - 实际结果文件路径（下载的文件）
            
            预期结果文件：
            - 文件路径: {self.expected_file}
            
            对比要求：
            1. 加载两个文件
            2. 逐行对比内容是否完全一致
            3. 记录每一行的对比结果（匹配/差异）
            4. 对于差异行，记录：
               - 行号
               - 预期内容
               - 实际内容
            
            统计信息：
            - 总行数
            - 匹配行数
            - 差异行数
            - 相似度百分比
            
            输出要求：
            返回JSON格式结果：
            {{
                "match": true/false,
                "statistics": {{
                    "total_rows": 100,
                    "matching_rows": 98,
                    "different_rows": 2,
                    "similarity": "98.0%"
                }},
                "differences": [
                    {{
                        "row_number": 15,
                        "expected": "内容A",
                        "actual": "内容B"
                    }}
                ],
                "actual_file": "/path/to/actual.csv",
                "expected_file": "/path/to/expected.csv"
            }}
            """,
            expected_output="包含对比结果和差异详情的JSON",
            agent=agent,
            context=[etl_task],
        )

    def _create_report_task(self, agent, etl_task: Task, validation_task: Task) -> Task:
        """
        创建报告生成任务

        Args:
            agent: ReportWriter Agent
            etl_task: ETL任务
            validation_task: 验证任务

        Returns:
            Task实例
        """
        return Task(
            description=f"""
            生成完整的测试报告，包含Markdown和HTML两种格式。
            
            从ETL任务获取：
            - ETL执行步骤详情
            - 各步骤执行时间和状态
            
            从验证任务获取：
            - 对比统计信息
            - 差异详情
            - 是否通过
            
            报告内容：
            1. 测试概览（测试ID、服务、环境、时间）
            2. 统计信息（总行数、匹配数、差异数、相似度）
            3. ETL执行时间线（各步骤状态和时间）
            4. 文件对比详情（行级对比结果）
            5. LLM智能分析（分析差异原因和建议）
            
            输出要求：
            - 生成Markdown报告：{self.output_dir}/{self.test_id}_report.md
            - 生成HTML报告：{self.output_dir}/{self.test_id}_report.html
            - 使用科技绿主题模板
            - HTML报告要有行级高亮（绿色=匹配，红色=差异）
            
            返回报告文件路径列表
            """,
            expected_output="包含生成的报告文件路径列表",
            agent=agent,
            context=[etl_task, validation_task],
        )

    def run(self) -> Dict[str, Any]:
        """
        执行完整的测试流程

        Returns:
            执行结果字典
        """
        logger.info(f"🚀 启动ETL测试: {self.test_id}")
        logger.info(f"   输入文件: {self.input_file}")
        logger.info(f"   预期文件: {self.expected_file}")

        try:
            # 创建并运行Crew
            crew = self.create_crew()
            result = crew.kickoff()

            logger.info(f"✅ 测试完成: {self.test_id}")
            logger.info(f"   报告位置: {self.output_dir}")

            return {
                "success": True,
                "test_id": self.test_id,
                "result": result,
                "output_dir": str(self.output_dir),
            }

        except Exception as e:
            logger.error(f"❌ 测试失败: {e}")
            return {
                "success": False,
                "test_id": self.test_id,
                "error": str(e),
                "output_dir": str(self.output_dir),
            }
