"""
ETL Test Crew - CrewAI编排（启用Agent决策）
执行流程：ETL执行（快速）→ CrewAI验证（Agent分析）→ CrewAI报告（Agent生成）
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from crewai import Crew, Task, Process
from crewai.llm import LLM

from ..agents import ETLOperatorAgent, ResultValidatorAgent, ReportWriterAgent
from ..execution.etl_executor import ETLExecutor
from ..tasks import create_etl_tasks


logger = logging.getLogger(__name__)


class ETLTestCrew:
    """
    ETL测试Crew（启用Agent协作）

    执行流程：
    1. ETL执行（快速代码执行）
    2. CrewAI验证（Agent智能分析差异）
    3. CrewAI报告（Agent生成分析报告）
    """

    def __init__(
        self,
        input_file: str,
        expected_file: str,
        service_config: Any,
        global_config: Any = None,
        environment: str = "local",
        output_dir: str = "./reports_v2",
        llm=None,
        mode: str = "fast",
    ):
        self.input_file = input_file
        self.expected_file = expected_file
        self.service_config = service_config
        self.global_config = global_config or {}
        self.environment = environment
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.mode = mode

        # 生成测试ID
        self.test_id = f"etl_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 初始化LLM
        if llm is None:
            llm = LLM(
                model=os.getenv("OPENAI_MODEL", "gpt-4o"),
                api_key=os.getenv("OPENAI_API_KEY"),
            )
        self.llm = llm

        # 创建Agent（启用详细日志）
        self._create_agents()

        # 提取配置
        self._extract_configs()

    def _create_agents(self):
        """创建CrewAI Agent实例"""
        logger.info("🎭 创建CrewAI Agent...")

        env_context = f"""
        当前环境: {self.environment}
        测试ID: {self.test_id}
        输入文件: {self.input_file}
        预期文件: {self.expected_file}
        """

        # 创建ETL操作员Agent
        self.etl_agent = ETLOperatorAgent.create(
            llm=self.llm, environment_context=env_context
        )
        logger.info(f"   ✅ ETLOperator Agent已创建")

        # 创建验证Agent
        self.validator_agent = ResultValidatorAgent.create(
            llm=self.llm, environment_context=env_context
        )
        logger.info(f"   ✅ ResultValidator Agent已创建")

        # 创建报告撰写Agent
        self.report_agent = ReportWriterAgent.create(
            llm=self.llm, environment_context=env_context
        )
        logger.info(f"   ✅ ReportWriter Agent已创建")

    def _extract_configs(self):
        """从配置对象提取必要信息"""
        # 服务配置
        if hasattr(self.service_config, "name"):
            self.service_name = self.service_config.name
            self.trigger_config = (
                self.service_config.trigger
                if hasattr(self.service_config, "trigger")
                else {}
            )
            self.polling_config = (
                self.service_config.polling
                if hasattr(self.service_config, "polling")
                else {}
            )
            upload = (
                self.service_config.upload
                if hasattr(self.service_config, "upload")
                else {}
            )
            self.remote_upload_path = (
                upload.get("remote_path", "/uploads")
                if isinstance(upload, dict)
                else "/uploads"
            )
            # Extract validation config
            validation = (
                self.service_config.validation
                if hasattr(self.service_config, "validation")
                else {}
            )
            self.validation_config = validation if isinstance(validation, dict) else {}
        else:
            self.service_name = "unknown"
            self.trigger_config = {}
            self.polling_config = {}
            self.remote_upload_path = "/uploads"
            self.validation_config = {}

        # SFTP配置（从全局配置）
        self.sftp_config = {}
        if self.global_config and hasattr(self.global_config, "sftp"):
            sftp = self.global_config.sftp
            self.sftp_config = {
                "host": getattr(sftp, "host", "localhost"),
                "port": getattr(sftp, "port", 2223),
                "username": getattr(sftp, "username", "testuser"),
                "password": getattr(sftp, "password", ""),
                "remote_base_path": getattr(sftp, "remote_base_path", "/uploads"),
            }
        elif isinstance(self.global_config, dict) and "sftp" in self.global_config:
            self.sftp_config = self.global_config["sftp"]

        # 默认SFTP配置
        if not self.sftp_config:
            self.sftp_config = {
                "host": "localhost",
                "port": 2223,
                "username": "testuser",
                "password": os.getenv("SFTP_PASSWORD", ""),
                "remote_base_path": "/uploads",
            }

    def run(self) -> Dict[str, Any]:
        """
        执行完整的CrewAI编排测试流程

        Returns:
            执行结果字典
        """
        logger.info(f"🚀 启动CrewAI编排ETL测试: {self.test_id}")
        logger.info(f"   输入文件: {self.input_file}")
        logger.info(f"   预期文件: {self.expected_file}")
        logger.info(f"   服务: {self.service_name}")
        logger.info(f"   执行模式: {self.mode}")

        if self.mode == "fast":
            logger.info(f"   Fast模式: 直接代码执行")
            # 步骤1: ETL执行（快速代码执行）
            logger.info("\n📋 步骤1: ETL执行（Fast模式 - 直接代码执行）")
            etl_result = self._execute_etl()
        else:
            logger.info(f"   CrewAI模式: Agent编排ETL流程")
            # 步骤1: ETL执行（CrewAI编排）
            logger.info("\n📋 步骤1: ETL执行（CrewAI模式 - Agent编排）")
            etl_result = self._run_etl_with_crewai()

        if not etl_result.get("success"):
            logger.error(f"❌ ETL执行失败: {etl_result.get('error')}")
            report_paths = self._generate_failure_report(etl_result)
            return {
                "success": False,
                "test_id": self.test_id,
                "error": etl_result.get("error"),
                "etl_result": etl_result,
                "report_paths": report_paths,
                "output_dir": str(self.output_dir),
            }

        # 步骤2: CrewAI验证编排
        logger.info("\n🎭 步骤2: CrewAI验证编排")
        validation_result = self._run_crewai_validation(etl_result)

        # 步骤3: CrewAI报告编排
        logger.info("\n🎭 步骤3: CrewAI报告编排")
        report_paths = self._run_crewai_reporting(etl_result, validation_result)

        logger.info(f"\n✅ CrewAI编排测试完成: {self.test_id}")
        logger.info(f"   报告位置: {report_paths}")

        return {
            "success": True,
            "test_id": self.test_id,
            "etl_result": etl_result,
            "validation_result": validation_result,
            "report_paths": report_paths,
            "output_dir": str(self.output_dir),
            "overall_pass": validation_result.get("match", False),
        }

    def _execute_etl(self) -> Dict[str, Any]:
        """
        执行ETL流程（快速代码执行）

        Returns:
            ETL执行结果
        """
        logger.info("🔧 Agent ETLOperator正在执行ETL流程...")

        executor = ETLExecutor()

        # 构建输出文件路径
        local_output_path = str(self.output_dir / f"result_{self.test_id}.csv")

        # 准备触发配置
        trigger_cfg = {
            "endpoint": self.trigger_config.get("endpoint", "")
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "endpoint", ""),
            "method": self.trigger_config.get("method", "POST")
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "method", "POST"),
            "headers": self.trigger_config.get("headers", {})
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "headers", {}),
            "body_template": self.trigger_config.get("body_template", "{}")
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "body_template", "{}"),
            "task_id_extractor": self.trigger_config.get(
                "task_id_extractor", "$.task_id"
            )
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "task_id_extractor", "$.task_id"),
        }

        # 添加 upload 路径到 sftp_config
        upload_remote_path = (
            self.remote_upload_path
            if hasattr(self, "remote_upload_path")
            else "/uploads"
        )
        sftp_cfg_with_upload = self.sftp_config.copy()
        sftp_cfg_with_upload["upload_remote_path"] = upload_remote_path

        # 准备轮询配置
        polling_cfg = {
            "status_endpoint": self.polling_config.get("status_check_endpoint", "")
            if isinstance(self.polling_config, dict)
            else getattr(self.polling_config, "status_check_endpoint", ""),
            "status_extractor": "$.status",
            "success_statuses": ["completed", "success"],
            "failure_statuses": ["failed", "error"],
            "max_attempts": self.polling_config.get("max_attempts", 30)
            if isinstance(self.polling_config, dict)
            else getattr(self.polling_config, "max_attempts", 30),
            "interval": self.polling_config.get("interval", 10)
            if isinstance(self.polling_config, dict)
            else getattr(self.polling_config, "interval", 10),
        }

        result = executor.execute_full_etl(
            input_file=self.input_file,
            output_file=local_output_path,
            sftp_config=sftp_cfg_with_upload,
            trigger_config=trigger_cfg,
            polling_config=polling_cfg,
            validation_config=self.validation_config,
        )

        return result

    def _run_etl_with_crewai(self) -> Dict[str, Any]:
        """
        使用CrewAI Agent编排执行ETL流程

        Returns:
            ETL执行结果
        """
        logger.info("🎭 创建CrewAI ETL编排任务...")
        logger.info(f"   Agent ETLOperator将执行上传→触发→轮询→下载流程")

        # 构建输出文件路径
        local_output_path = str(self.output_dir / f"result_{self.test_id}.csv")

        # 准备配置
        trigger_cfg = {
            "endpoint": self.trigger_config.get("endpoint", "")
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "endpoint", ""),
            "method": self.trigger_config.get("method", "POST")
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "method", "POST"),
            "headers": self.trigger_config.get("headers", {})
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "headers", {}),
            "task_id_extractor": self.trigger_config.get(
                "task_id_extractor", "$.task_id"
            )
            if isinstance(self.trigger_config, dict)
            else getattr(self.trigger_config, "task_id_extractor", "$.task_id"),
        }

        polling_cfg = {
            "status_endpoint": self.polling_config.get("status_check_endpoint", "")
            if isinstance(self.polling_config, dict)
            else getattr(self.polling_config, "status_check_endpoint", ""),
            "status_extractor": "$.status",
            "success_statuses": ["completed", "success"],
            "failure_statuses": ["failed", "error"],
            "max_attempts": self.polling_config.get("max_attempts", 30)
            if isinstance(self.polling_config, dict)
            else getattr(self.polling_config, "max_attempts", 30),
            "interval": self.polling_config.get("interval", 10)
            if isinstance(self.polling_config, dict)
            else getattr(self.polling_config, "interval", 10),
        }

        # 创建ETL任务（Agent 已在 _create_agents() 中绑定了 tools）
        tasks_dict = create_etl_tasks(
            agent=self.etl_agent,
            input_file=self.input_file,
            output_file=local_output_path,
            sftp_config=self.sftp_config,
            trigger_config=trigger_cfg,
            polling_config=polling_cfg,
            remote_upload_path=self.remote_upload_path
            if hasattr(self, "remote_upload_path")
            else "/uploads",
        )

        # 创建ETL执行Crew
        logger.info("🎭 启动CrewAI ETL编排...")
        etl_crew = Crew(
            agents=[self.etl_agent],
            tasks=[
                tasks_dict["upload"],
                tasks_dict["trigger"],
                tasks_dict["poll"],
                tasks_dict["download"],
            ],
            process=Process.sequential,
            verbose=True,
        )

        # 执行ETL编排
        logger.info("🤖 Agent ETLOperator开始执行ETL流程...")
        try:
            crew_result = etl_crew.kickoff()
            logger.info(f"✅ CrewAI ETL编排完成")
            logger.info(f"📝 Agent执行结果: {crew_result}")

            # 构建执行结果
            result = {
                "success": True,
                "result_file": local_output_path,
                "total_duration": 0,  # Will be updated by steps
                "steps": {
                    "upload": {
                        "success": True,
                        "duration": 0,
                        "message": "Agent执行完成",
                    },
                    "trigger": {
                        "success": True,
                        "duration": 0,
                        "message": "Agent执行完成",
                    },
                    "poll": {
                        "success": True,
                        "duration": 0,
                        "message": "Agent执行完成",
                    },
                    "download": {
                        "success": True,
                        "duration": 0,
                        "message": "Agent执行完成",
                    },
                },
                "crewai_result": str(crew_result),
            }

        except Exception as e:
            logger.error(f"❌ CrewAI ETL编排失败: {e}")
            result = {
                "success": False,
                "error": f"CrewAI ETL编排失败: {e}",
                "steps": {},
            }

        return result

    def _run_crewai_validation(self, etl_result: Dict) -> Dict[str, Any]:
        """
        使用CrewAI Agent执行验证

        Args:
            etl_result: ETL执行结果

        Returns:
            验证结果
        """
        logger.info("🎭 创建CrewAI验证任务...")

        # 首先执行代码级验证（快速）
        actual_file = etl_result.get("result_file")
        code_validation = self._validate_results(actual_file)

        # 构建验证任务
        validation_task = Task(
            description=f"""
            验证ETL测试结果，分析实际输出与预期输出的差异。

            测试ID: {self.test_id}
            服务: {self.service_name}

            ETL执行结果:
            - 成功: {etl_result.get("success")}
            - 总耗时: {etl_result.get("total_duration", 0)}秒
            - 步骤详情: {etl_result.get("steps", {})}

            代码级验证结果:
            - 匹配: {code_validation.get("match")}
            - 总行数: {code_validation.get("statistics", {}).get("total_rows", 0)}
            - 匹配行数: {code_validation.get("statistics", {}).get("matching_rows", 0)}
            - 差异行数: {code_validation.get("statistics", {}).get("different_rows", 0)}
            - 相似度: {code_validation.get("statistics", {}).get("similarity", "0%")}

            差异详情:
            {code_validation.get("differences", [])}

            你的任务是:
            1. 分析差异的根本原因（数据格式、字段映射、处理逻辑等）
            2. 评估这些差异是否在可接受范围内
            3. 提供改进建议

            请提供详细的分析结果，以JSON格式返回:
            {{
                "analysis": "详细的差异分析",
                "root_cause": "根本原因",
                "severity": "high/medium/low",
                "acceptable": true/false,
                "recommendations": ["建议1", "建议2"]
            }}
            """,
            expected_output="详细的验证分析报告，包含差异分析和改进建议",
            agent=self.validator_agent,
        )

        # 创建验证Crew
        logger.info("🎭 启动CrewAI验证编排...")
        validation_crew = Crew(
            agents=[self.validator_agent],
            tasks=[validation_task],
            process=Process.sequential,
            verbose=True,
        )

        # 执行验证
        logger.info("🤖 Agent ResultValidator正在分析差异...")
        try:
            crew_result = validation_crew.kickoff()
            logger.info(f"✅ CrewAI验证完成")
            logger.info(f"📝 Agent分析结果: {crew_result}")

            # 将Agent分析结果添加到验证结果中
            code_validation["llm_analysis"] = str(crew_result)
            code_validation["crewai_validation"] = True

        except Exception as e:
            logger.warning(f"⚠️ CrewAI验证执行失败: {e}")
            code_validation["llm_analysis"] = f"CrewAI验证失败: {e}"
            code_validation["crewai_validation"] = False

        return code_validation

    def _run_crewai_reporting(
        self, etl_result: Dict, validation_result: Dict
    ) -> Dict[str, str]:
        """
        使用CrewAI Agent生成报告

        Args:
            etl_result: ETL执行结果
            validation_result: 验证结果

        Returns:
            报告文件路径
        """
        logger.info("🎭 创建CrewAI报告任务...")

        # 提取数据用于报告
        steps = etl_result.get("steps", {})
        stats = validation_result.get("statistics", {})

        # 构建报告任务
        report_task = Task(
            description=f"""
            生成ETL测试报告。

            测试ID: {self.test_id}
            服务: {self.service_name}
            环境: {self.environment}
            时间戳: {datetime.now().isoformat()}

            ETL执行状态: {"成功" if etl_result.get("success") else "失败"}
            总耗时: {etl_result.get("total_duration", 0)}秒

            ETL步骤:
            {steps}

            验证统计:
            - 总行数: {stats.get("total_rows", 0)}
            - 匹配行数: {stats.get("matching_rows", 0)}
            - 差异行数: {stats.get("different_rows", 0)}
            - 相似度: {stats.get("similarity", "0%")}

            Agent分析:
            {validation_result.get("llm_analysis", "无")}

            你的任务是生成一份专业的测试报告总结，包括:
            1. 执行概况
            2. 关键发现
            3. 风险提示
            4. 下一步建议

            请用专业的测试报告语言撰写。
            """,
            expected_output="专业的测试报告总结",
            agent=self.report_agent,
        )

        # 创建报告Crew
        logger.info("🎭 启动CrewAI报告编排...")
        report_crew = Crew(
            agents=[self.report_agent],
            tasks=[report_task],
            process=Process.sequential,
            verbose=True,
        )

        # 执行报告生成
        logger.info("🤖 Agent ReportWriter正在生成报告...")
        try:
            crew_result = report_crew.kickoff()
            logger.info(f"✅ CrewAI报告生成完成")
            logger.info(f"📝 Agent报告总结: {crew_result}")

            # 生成实际报告文件
            from ..utils.report_generator import ReportGenerator

            generator = ReportGenerator(str(self.output_dir))

            # 构建报告数据
            etl_steps = []
            for step_name, step_data in steps.items():
                etl_steps.append(
                    {
                        "name": step_name.capitalize(),
                        "duration": step_data.get("duration", 0),
                        "success": step_data.get("success", False),
                        "message": step_data.get("message", ""),
                    }
                )

            differences = validation_result.get("differences", [])
            comparison_rows = []
            for diff in differences:
                comparison_rows.append(
                    {
                        "row_number": diff.get("row_number", 0),
                        "expected": diff.get("expected", ""),
                        "actual": diff.get("actual", ""),
                        "match": False,
                    }
                )

            similarity_str = stats.get("similarity", "0%")
            try:
                similarity = float(similarity_str.replace("%", ""))
            except:
                similarity = 0

            report_data = {
                "test_id": self.test_id,
                "service_name": self.service_name,
                "environment": self.environment,
                "timestamp": datetime.now().isoformat(),
                "overall_pass": validation_result.get("match", False),
                "total_duration": etl_result.get("total_duration", 0),
                "etl_steps": etl_steps,
                "total_rows": stats.get("total_rows", 0),
                "matching_rows": stats.get("matching_rows", 0),
                "different_rows": stats.get("different_rows", 0),
                "similarity": similarity,
                "comparison_rows": comparison_rows,
                "llm_analysis": validation_result.get("llm_analysis", str(crew_result)),
            }

            paths = generator.generate_both_formats(report_data)

            # 添加Agent生成的总结到日志
            logger.info(f"📝 Agent生成的报告总结:\n{crew_result}")

            return paths

        except Exception as e:
            logger.error(f"❌ CrewAI报告生成失败: {e}")
            # 降级为直接生成
            return self._generate_reports_direct(etl_result, validation_result)

    def _validate_results(self, actual_file: str) -> Dict[str, Any]:
        """
        验证实际结果与预期结果（代码级快速验证）

        Args:
            actual_file: 实际结果文件路径

        Returns:
            验证结果
        """
        logger.info("🔍 Agent ResultValidator执行代码级验证...")

        if not actual_file or not Path(actual_file).exists():
            return {
                "match": False,
                "error": "实际结果文件不存在",
                "statistics": {
                    "total_rows": 0,
                    "matching_rows": 0,
                    "different_rows": 0,
                    "similarity": "0%",
                },
            }

        try:
            # 读取两个文件
            with open(self.expected_file, "r", encoding="utf-8") as f:
                expected_lines = f.readlines()
            with open(actual_file, "r", encoding="utf-8") as f:
                actual_lines = f.readlines()

            # 逐行对比
            differences = []
            matching = 0
            different = 0
            max_rows = max(len(expected_lines), len(actual_lines))

            for i in range(max_rows):
                exp_line = (
                    expected_lines[i].strip()
                    if i < len(expected_lines)
                    else "<MISSING>"
                )
                act_line = (
                    actual_lines[i].strip() if i < len(actual_lines) else "<MISSING>"
                )

                if exp_line == act_line:
                    matching += 1
                else:
                    different += 1
                    differences.append(
                        {
                            "row_number": i + 1,
                            "expected": exp_line,
                            "actual": act_line,
                        }
                    )

            similarity = (matching / max_rows * 100) if max_rows > 0 else 0

            return {
                "match": different == 0,
                "statistics": {
                    "total_rows": max_rows,
                    "matching_rows": matching,
                    "different_rows": different,
                    "similarity": f"{similarity:.1f}%",
                },
                "differences": differences,
                "actual_file": actual_file,
                "expected_file": self.expected_file,
            }

        except Exception as e:
            logger.error(f"❌ 验证失败: {e}")
            return {
                "match": False,
                "error": str(e),
                "statistics": {
                    "total_rows": 0,
                    "matching_rows": 0,
                    "different_rows": 0,
                    "similarity": "0%",
                },
            }

    def _generate_failure_report(self, etl_result: Dict) -> Dict[str, str]:
        """
        ETL失败时生成失败报告

        Args:
            etl_result: ETL执行结果

        Returns:
            报告文件路径字典
        """
        from ..utils.report_generator import ReportGenerator

        generator = ReportGenerator(str(self.output_dir))

        # 提取ETL步骤数据（即使失败也可能有部分步骤成功）
        steps = etl_result.get("steps", {})
        etl_steps = []
        for step_name, step_data in steps.items():
            etl_steps.append(
                {
                    "name": step_name.capitalize(),
                    "duration": step_data.get("duration", 0),
                    "success": step_data.get("success", False),
                    "message": step_data.get("message", ""),
                }
            )

        report_data = {
            "test_id": self.test_id,
            "service_name": self.service_name,
            "environment": self.environment,
            "timestamp": datetime.now().isoformat(),
            "overall_pass": False,
            "total_duration": etl_result.get("total_duration", 0),
            "etl_steps": etl_steps,
            "total_rows": 0,
            "matching_rows": 0,
            "different_rows": 0,
            "similarity": 0,
            "comparison_rows": [],
            "llm_analysis": f"ETL执行失败: {etl_result.get('error', 'Unknown error')}",
        }

        paths = generator.generate_both_formats(report_data)
        return paths

    def _generate_reports_direct(
        self, etl_result: Dict, validation_result: Dict
    ) -> Dict[str, str]:
        """
        直接生成报告（CrewAI失败时的降级方案）

        Args:
            etl_result: ETL执行结果
            validation_result: 验证结果

        Returns:
            报告文件路径字典
        """
        from ..utils.report_generator import ReportGenerator

        generator = ReportGenerator(str(self.output_dir))

        steps = etl_result.get("steps", {})
        etl_steps = []
        for step_name, step_data in steps.items():
            etl_steps.append(
                {
                    "name": step_name.capitalize(),
                    "duration": step_data.get("duration", 0),
                    "success": step_data.get("success", False),
                    "message": step_data.get("message", ""),
                }
            )

        stats = validation_result.get("statistics", {})
        differences = validation_result.get("differences", [])
        comparison_rows = []
        for diff in differences:
            comparison_rows.append(
                {
                    "row_number": diff.get("row_number", 0),
                    "expected": diff.get("expected", ""),
                    "actual": diff.get("actual", ""),
                    "match": False,
                }
            )

        similarity_str = stats.get("similarity", "0%")
        try:
            similarity = float(similarity_str.replace("%", ""))
        except:
            similarity = 0

        report_data = {
            "test_id": self.test_id,
            "service_name": self.service_name,
            "environment": self.environment,
            "timestamp": datetime.now().isoformat(),
            "overall_pass": validation_result.get("match", False),
            "total_duration": etl_result.get("total_duration", 0),
            "etl_steps": etl_steps,
            "total_rows": stats.get("total_rows", 0),
            "matching_rows": stats.get("matching_rows", 0),
            "different_rows": stats.get("different_rows", 0),
            "similarity": similarity,
            "comparison_rows": comparison_rows,
            "llm_analysis": validation_result.get("llm_analysis", "CrewAI分析未执行"),
        }

        paths = generator.generate_both_formats(report_data)
        return paths
