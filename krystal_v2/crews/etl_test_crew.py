"""
ETL Test Crew - CrewAI编排
执行流程：实际ETL执行 → 结果验证 → 报告生成
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


logger = logging.getLogger(__name__)


class ETLTestCrew:
    """
    ETL测试Crew

    执行流程：
    1. 实际执行ETL流程（使用ETLExecutor）
    2. 结果验证（代码对比）
    3. CrewAI生成报告（验证器+报告撰写Agent）
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

        # 提取配置
        self._extract_configs()

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
        执行完整的测试流程

        Returns:
            执行结果字典
        """
        logger.info(f"🚀 启动ETL测试: {self.test_id}")
        logger.info(f"   输入文件: {self.input_file}")
        logger.info(f"   预期文件: {self.expected_file}")
        logger.info(f"   服务: {self.service_name}")

        # 步骤1: 实际执行ETL
        etl_result = self._execute_etl()

        if not etl_result.get("success"):
            logger.error(f"❌ ETL执行失败: {etl_result.get('error')}")
            # 即使失败也生成报告
            report_paths = self._generate_failure_report(etl_result)
            return {
                "success": False,
                "test_id": self.test_id,
                "error": etl_result.get("error"),
                "etl_result": etl_result,
                "report_paths": report_paths,
                "output_dir": str(self.output_dir),
            }

        # 步骤2: 结果验证
        validation_result = self._validate_results(etl_result.get("result_file"))

        # 步骤3: 生成报告（使用CrewAI）
        report_paths = self._generate_reports(etl_result, validation_result)

        logger.info(f"✅ 测试完成: {self.test_id}")
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
        实际执行ETL流程

        Returns:
            ETL执行结果
        """
        logger.info("🔧 执行ETL流程...")

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

    def _validate_results(self, actual_file: str) -> Dict[str, Any]:
        """
        验证实际结果与预期结果

        Args:
            actual_file: 实际结果文件路径

        Returns:
            验证结果
        """
        logger.info("🔍 验证结果...")

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

    def _generate_reports(
        self, etl_result: Dict, validation_result: Dict
    ) -> Dict[str, str]:
        """
        使用CrewAI生成报告

        Args:
            etl_result: ETL执行结果
            validation_result: 验证结果

        Returns:
            生成的报告文件路径列表
        """
        logger.info("📄 生成报告...")

        from ..utils.report_generator import ReportGenerator

        generator = ReportGenerator(str(self.output_dir))

        # 提取ETL步骤数据
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

        # 提取验证统计数据
        stats = validation_result.get("statistics", {})
        total_rows = stats.get("total_rows", 0)
        matching_rows = stats.get("matching_rows", 0)
        different_rows = stats.get("different_rows", 0)
        similarity_str = stats.get("similarity", "0%")
        try:
            similarity = float(similarity_str.replace("%", ""))
        except:
            similarity = 0

        # 提取差异详情
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

        # 构建报告数据（匹配ReportGenerator期望的格式）
        report_data = {
            "test_id": self.test_id,
            "service_name": self.service_name,
            "environment": self.environment,
            "timestamp": datetime.now().isoformat(),
            "overall_pass": validation_result.get("match", False),
            "total_duration": etl_result.get("total_duration", 0),
            "etl_steps": etl_steps,
            "total_rows": total_rows,
            "matching_rows": matching_rows,
            "different_rows": different_rows,
            "similarity": similarity,
            "comparison_rows": comparison_rows,
            "llm_analysis": None,
        }

        paths = generator.generate_both_formats(report_data)
        return paths

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
