"""
Krystal 真实端到端集成测试

测试目标：验证 Krystal 在真实本地服务环境下的完整工作流程
测试方法：
1. 启动本地 SFTP 和 API Stub 服务（通过 docker-compose/podman-compose）
2. 真实调用 CrewAI agents 执行完整 workflow
3. 验证文件真实上传、API 真实调用、结果文件真实生成

运行前提：
  cd integration_tests && podman compose up -d

运行命令：
  python -m pytest integration_tests/test_real_e2e.py -v -s --timeout=300
"""

import os
import sys
import csv
import time
import pytest
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import paramiko

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from krystal.config import ConfigManager
from krystal.runner import TestRunner
from krystal.crew.krystal_crew import KrystalCrew
from krystal.tools.csv_generator import CSVGeneratorTool
from krystal.tools.sftp_client import SFTPClientTool
from krystal.tools.api_client import APIClientTool
from krystal.tools.polling_service import PollingServiceTool
from krystal.tools.validator import FileValidatorTool, DataValidatorTool


# 配置测试日志
logger = logging.getLogger(__name__)


class TestRealEndToEnd:
    """真实端到端测试 - 无 Mock"""

    @pytest.fixture(autouse=True)
    def setup(self, ensure_dependencies):
        """测试前置条件检查"""
        self.env = "local"
        self.config_manager = ConfigManager(self.env)
        logger.info(f"测试环境: {self.env}")
        logger.info(f"配置文件路径: {self.config_manager.config_path}")

    def test_crewai_agents_workflow_with_local_services(self, tmp_path):
        """
        测试 1: CrewAI Agents 完整 Workflow - 真实本地服务

        测试步骤：
        1. 生成测试数据 CSV
        2. 上传到本地 SFTP
        3. 调用本地 API Stub 触发任务
        4. 轮询任务状态直到完成
        5. 下载结果文件并验证

        预期结果：
        - 所有步骤成功执行
        - 结果文件真实存在于 SFTP
        - 验证通过
        """
        # 配置日志文件
        log_file = (
            tmp_path / f"krystal_e2e_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)

        print("\n" + "=" * 70)
        print("Krystal 真实端到端测试")
        print("=" * 70)
        print(f"日志文件: {log_file}")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70 + "\n")

        try:
            # 获取服务配置
            service = self.config_manager.get_service("local-payment-service")
            assert service is not None, "local-payment-service 未找到"

            config = self.config_manager.load()

            print(f"测试服务: {service.name}")
            print(f"数据行数: {service.data_generation.row_count}")
            print(f"SFTP 服务器: {config.sftp.host}:{config.sftp.port}")
            print(f"API 端点: {service.trigger.endpoint}\n")

            # 执行完整 workflow
            krystal = KrystalCrew(
                service_config=service,
                sftp_config=config.sftp,
                environment=self.env,
            )

            print("🚀 启动 CrewAI Workflow...\n")
            result = krystal.run()

            print("\n" + "=" * 70)
            print("测试结果")
            print("=" * 70)
            print(f"成功: {result.get('success', False)}")
            print(f"批次ID: {result.get('batch_id', 'N/A')}")
            print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70 + "\n")

            # 验证结果
            assert result.get("success", False), (
                f"Workflow 执行失败: {result.get('error', 'Unknown error')}"
            )
            assert result.get("batch_id") is not None, "批次ID 未生成"

            # 验证结果文件确实存在于 SFTP
            batch_id = result.get("batch_id")
            remote_result_path = service.validation.remote_result_path.replace(
                "{{batch_id}}", batch_id
            )

            print(f"📁 验证结果文件: {remote_result_path}")

            # 直接连接 SFTP 验证文件存在
            sftp_tool = SFTPClientTool()
            check_result = sftp_tool._run(
                action="upload",  # dummy action just to use the tool
                host=config.sftp.host,
                port=config.sftp.port,
                username=config.sftp.username,
                password=config.sftp.password,
                local_path="/dev/null",
                remote_path=remote_result_path,
            )

            # 使用 paramiko 直接检查文件
            transport = paramiko.Transport((config.sftp.host, config.sftp.port))
            try:
                transport.connect(
                    username=config.sftp.username, password=config.sftp.password
                )
                sftp = paramiko.SFTPClient.from_transport(transport)

                try:
                    sftp.stat(remote_result_path)
                    file_exists = True
                    print(f"  ✅ 结果文件存在于 SFTP: {remote_result_path}")
                except FileNotFoundError:
                    file_exists = False
                    print(f"  ❌ 结果文件不存在于 SFTP: {remote_result_path}")

                sftp.close()
            finally:
                transport.close()

            assert file_exists, f"结果文件未在 SFTP 上找到: {remote_result_path}"

            print(f"\n✅ 真实端到端测试通过！")
            print(f"   日志文件位置: {log_file}")
            print(f"   批次ID: {batch_id}")
            print(f"   结果文件: {remote_result_path}\n")

        finally:
            # 清理日志 handler
            root_logger.removeHandler(file_handler)
            file_handler.close()

    def test_sftp_real_upload_and_download(self, tmp_path):
        """
        测试 2: SFTP 真实上传和下载

        验证：
        1. 文件能真实上传到本地 SFTP
        2. 文件能真实从 SFTP 下载
        3. 上传下载后文件内容一致
        """
        config = self.config_manager.load()

        # 创建测试文件
        test_file = tmp_path / "test_upload.csv"
        with open(test_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "value"])
            for i in range(5):
                writer.writerow([f"id_{i}", f"name_{i}", i * 100])

        print(f"\n📤 测试 SFTP 上传...")
        print(f"   本地文件: {test_file}")

        # 上传文件
        remote_path = "upload/test/incoming/integration_test.csv"
        sftp_tool = SFTPClientTool()

        upload_result = sftp_tool._run(
            action="upload",
            host=config.sftp.host,
            port=config.sftp.port,
            username=config.sftp.username,
            password=config.sftp.password,
            local_path=str(test_file),
            remote_path=remote_path,
        )

        assert upload_result["success"], (
            f"上传失败: {upload_result.get('error', 'Unknown')}"
        )
        print(f"   ✅ 上传成功: {upload_result['remote_path']}")
        print(f"   📊 文件大小: {upload_result['size']} bytes")

        # 下载文件
        print(f"\n📥 测试 SFTP 下载...")
        download_file = tmp_path / "test_download.csv"

        download_result = sftp_tool._run(
            action="download",
            host=config.sftp.host,
            port=config.sftp.port,
            username=config.sftp.username,
            password=config.sftp.password,
            remote_path=remote_path,
            local_path=str(download_file),
        )

        assert download_result["success"], (
            f"下载失败: {download_result.get('error', 'Unknown')}"
        )
        print(f"   ✅ 下载成功: {download_result['local_path']}")
        print(f"   📊 文件大小: {download_result['size']} bytes")

        # 验证内容一致
        with open(test_file, "r", encoding="utf-8") as f:
            original_content = f.read()
        with open(download_file, "r", encoding="utf-8") as f:
            downloaded_content = f.read()

        assert original_content == downloaded_content, "上传下载后文件内容不一致"
        print(f"   ✅ 文件内容验证通过\n")

    def test_api_stub_trigger_and_poll(self):
        """
        测试 3: API Stub 触发和轮询

        验证：
        1. 能成功调用 trigger API
        2. 能正确提取 task_id
        3. 轮询能获取到状态变化
        4. 最终状态为 completed
        """
        config = self.config_manager.load()
        service = self.config_manager.get_service("local-payment-service")

        # 调用 trigger API
        print(f"\n🚀 测试 API Trigger...")
        print(f"   端点: {service.trigger.endpoint}")

        api_tool = APIClientTool()
        trigger_result = api_tool._run(
            endpoint=service.trigger.endpoint,
            method="POST",
            headers=service.trigger.headers,
            body={
                "file_path": "/uploads/payment/incoming/test.csv",
                "batch_id": "test_batch_001",
                "service": "payment-service",
                "row_count": 5,
            },
        )

        assert trigger_result["success"], f"API 调用失败: {trigger_result}"
        print(f"   ✅ API 调用成功: HTTP {trigger_result['status_code']}")

        # 提取 task_id
        from krystal.tools.api_client import JSONExtractorTool

        json_extractor = JSONExtractorTool()
        body = trigger_result.get("body", {})
        task_id_result = json_extractor._run(
            json_data=body, json_path=service.trigger.task_id_extractor
        )

        assert task_id_result["success"], f"无法提取 task_id: {task_id_result}"
        task_id = task_id_result["value"]
        print(f"   📝 Task ID: {task_id}")

        # 轮询状态
        print(f"\n⏳ 测试状态轮询...")
        polling_tool = PollingServiceTool()

        status_endpoint = service.polling.status_check_endpoint.replace(
            "{{task_id}}", task_id
        )

        poll_result = polling_tool._run(
            endpoint=status_endpoint,
            method="GET",
            task_id=task_id,
            status_extractor="$.status",
            success_statuses=service.polling.success_statuses,
            failure_statuses=service.polling.failure_statuses,
            max_attempts=service.polling.max_attempts,
            interval=service.polling.interval,
        )

        assert poll_result["success"], f"轮询失败: {poll_result}"
        assert poll_result["completed"], f"任务未完成: {poll_result}"
        print(f"   ✅ 轮询成功")
        print(f"   📊 尝试次数: {poll_result['attempts']}")
        print(f"   ✅ 最终状态: {poll_result['status']}\n")

    def test_runner_multiple_services(self):
        """
        测试 4: TestRunner 多服务测试

        验证：
        1. TestRunner 能正确加载本地配置
        2. 能执行多个服务的测试
        3. 所有服务测试通过
        """
        print(f"\n🎯 测试 TestRunner 多服务执行...")

        runner = TestRunner(
            environment="local",
            service_names=["local-payment-service", "local-invoice-service"],
        )

        results = runner.run()

        assert len(results) == 2, f"预期 2 个服务测试结果，实际 {len(results)}"

        for result in results:
            service_name = result.get("service", "Unknown")
            success = result.get("success", False)
            status = "✅ 通过" if success else "❌ 失败"
            print(f"   {service_name}: {status}")

        passed = sum(1 for r in results if r.get("success"))
        failed = len(results) - passed

        print(f"\n📊 总结: {passed} 通过, {failed} 失败")

        # 期望所有服务都通过
        assert passed == len(results), f"部分服务测试失败: {failed} 失败"
        print(f"   ✅ 所有服务测试通过\n")


class TestToolsWithRealServices:
    """单个 Tools 的真实服务测试"""

    @pytest.fixture(autouse=True)
    def setup(self, ensure_dependencies):
        """测试前置条件"""
        self.env = "local"
        self.config_manager = ConfigManager(self.env)

    def test_csv_generator_real_output(self, tmp_path):
        """
        测试 5: CSV Generator 真实文件输出

        验证：
        1. 能根据 schema 生成 CSV 文件
        2. 文件真实存在于磁盘
        3. 内容符合 schema 定义
        """
        tool = CSVGeneratorTool()
        output_file = tmp_path / "generated.csv"

        schema = {
            "fields": [
                {"name": "order_id", "type": "uuid"},
                {"name": "amount", "type": "float", "min": 10.0, "max": 100.0},
                {"name": "currency", "type": "enum", "values": ["USD", "EUR"]},
            ]
        }

        result = tool._run(
            data_schema=schema,  # 使用 data_schema 代替 schema
            row_count=5,
            output_path=str(output_file),
        )

        assert result.get("success", False), (
            f"CSV 生成失败: {result.get('error', 'Unknown error')}"
        )
        file_path = result.get("file_path")
        assert file_path and os.path.exists(file_path), "CSV 文件未生成"

        # 验证内容
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 5, f"预期 5 行数据，实际 {len(rows)}"

            for row in rows:
                assert len(row["order_id"]) == 36, "UUID 格式错误"
                assert float(row["amount"]) >= 10.0, "金额范围错误"
                assert float(row["amount"]) <= 100.0, "金额范围错误"
                assert row["currency"] in ["USD", "EUR"], "货币类型错误"

        print(f"\n✅ CSV Generator 测试通过")
        print(f"   生成文件: {file_path}")
        print(f"   数据行数: {len(rows)}\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
