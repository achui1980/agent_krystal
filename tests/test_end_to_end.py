"""
端到端集成测试：完整测试流程

测试目标：验证 Krystal 的完整工作流程
测试方法：使用 mock 模拟外部依赖（SFTP、API）
"""

import pytest
import os
import tempfile
import csv
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from datetime import datetime

from krystal.tools.csv_generator import CSVGeneratorTool
from krystal.tools.sftp_client import SFTPClientTool
from krystal.tools.api_client import APIClientTool
from krystal.tools.polling_service import PollingServiceTool
from krystal.tools.validator import DataValidatorTool, FileValidatorTool
from krystal.config import ConfigManager, ServiceConfig


class TestEndToEndWorkflow:
    """端到端工作流程测试"""

    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_payment_service_workflow(self, temp_dir):
        """测试 1：Payment Service 完整流程

        模拟完整的端到端流程：
        1. 生成 payment 测试数据 CSV
        2. 上传到 SFTP
        3. 调用 API 触发
        4. 轮询状态
        5. 下载并验证结果
        """
        # 步骤 1: 生成 CSV 数据
        csv_tool = CSVGeneratorTool()
        csv_path = os.path.join(temp_dir, "payment_test.csv")

        schema = {
            "fields": [
                {"name": "order_id", "type": "uuid"},
                {
                    "name": "amount",
                    "type": "float",
                    "min": 10.0,
                    "max": 1000.0,
                    "decimals": 2,
                },
                {"name": "currency", "type": "enum", "values": ["USD", "EUR"]},
                {"name": "customer_id", "type": "string", "pattern": "CUST[0-9]{6}"},
            ]
        }

        result1 = csv_tool._run(data_schema=schema, row_count=5, output_path=csv_path)

        assert os.path.exists(result1)

        # 验证生成的数据
        with open(result1, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 5
            for row in rows:
                assert row["currency"] in ["USD", "EUR"]
                assert 10.0 <= float(row["amount"]) <= 1000.0

        # 步骤 2: 模拟 SFTP 上传（使用 mock）
        with patch("krystal.tools.sftp_client.paramiko.Transport") as mock_transport:
            mock_transport_instance = MagicMock()
            mock_transport.return_value = mock_transport_instance

            mock_sftp = MagicMock()
            mock_sftp.stat.return_value = MagicMock(st_size=1024)

            with patch(
                "krystal.tools.sftp_client.paramiko.SFTPClient"
            ) as mock_sftp_client:
                mock_sftp_client.from_transport.return_value = mock_sftp

                sftp_tool = SFTPClientTool()
                result2 = sftp_tool._run(
                    action="upload",
                    host="test.sftp.com",
                    port=22,
                    username="testuser",
                    password="testpass",
                    local_path=result1,
                    remote_path="/uploads/payment/incoming/payment_test.csv",
                    retry_attempts=1,
                )

                assert result2["success"] is True
                remote_file_path = result2["remote_path"]

        # 步骤 3: 调用 API 触发
        api_tool = APIClientTool()
        result3 = api_tool._run(
            endpoint="https://httpbin.org/post",
            method="POST",
            headers={"Content-Type": "application/json"},
            body={
                "file_path": remote_file_path,
                "batch_id": "test_batch_001",
                "service": "payment",
                "row_count": 5,
            },
        )

        assert result3["success"] is True
        assert result3["status_code"] == 200

        # 提取模拟的任务 ID
        task_id = "mock_task_12345"

        # 步骤 4: 模拟轮询（使用 httpbin.org/get 作为状态检查）
        poll_tool = PollingServiceTool()
        result4 = poll_tool._run(
            endpoint="https://httpbin.org/get",
            method="GET",
            task_id=task_id,
            status_extractor="$.headers.Host",
            success_statuses=["httpbin.org"],
            failure_statuses=["error", "failed"],
            max_attempts=3,
            interval=1,
        )

        assert result4["success"] is True
        assert result4["completed"] is True

        # 步骤 5: 模拟结果验证
        # 创建一个模拟的结果文件
        result_csv_path = os.path.join(temp_dir, "payment_result.csv")
        with open(result_csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["order_id", "status", "processed_amount", "transaction_id"]
            )
            for i in range(5):
                writer.writerow([f"order_{i}", "success", f"{100.0 + i}", f"txn_{i}"])

        # 验证文件
        file_validator = FileValidatorTool()
        result5 = file_validator._run(
            file_path=result_csv_path, min_size=50, max_size=10000
        )

        assert result5["success"] is True
        assert result5["exists"] is True

        # 验证数据
        data_validator = DataValidatorTool()

        # 创建预期数据文件
        expected_path = os.path.join(temp_dir, "expected.csv")
        with open(expected_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["order_id", "status", "processed_amount"])
            for i in range(5):
                writer.writerow([f"order_{i}", "success", f"{100.0 + i}"])

        result6 = data_validator._run(
            expected_path=expected_path,
            actual_path=result_csv_path,
            key_column="order_id",
        )

        assert result6["success"] is True
        assert result6["passed"] is True

        print("\n✅ 完整工作流程测试通过！")
        print(f"   - 生成数据: {len(rows)} 行")
        print(f"   - SFTP 上传: 成功")
        print(f"   - API 调用: {result3['status_code']}")
        print(f"   - 轮询状态: {result4['status']}")
        print(f"   - 数据验证: 通过")

    def test_error_handling_workflow(self, temp_dir):
        """测试 2：错误处理流程

        验证当某个步骤失败时，系统正确处理错误
        """
        # 步骤 1: 生成数据（成功）
        csv_tool = CSVGeneratorTool()
        csv_path = os.path.join(temp_dir, "error_test.csv")

        schema = {"fields": [{"name": "id", "type": "uuid"}]}

        result1 = csv_tool._run(data_schema=schema, row_count=3, output_path=csv_path)

        assert os.path.exists(result1)

        # 步骤 2: 模拟 SFTP 失败
        with patch("krystal.tools.sftp_client.paramiko.Transport") as mock_transport:
            # 模拟连接失败
            mock_transport.side_effect = Exception("Connection refused")

            sftp_tool = SFTPClientTool()
            result2 = sftp_tool._run(
                action="upload",
                host="invalid.host.com",
                port=22,
                username="testuser",
                password="wrongpass",
                local_path=result1,
                remote_path="/remote/test.csv",
                retry_attempts=2,  # 减少重试次数加快测试
            )

            # 验证失败被正确处理
            assert result2["success"] is False
            assert "error" in result2
            assert "Failed to upload file" in result2["message"]

    def test_multiple_services_batch(self, temp_dir):
        """测试 3：批量处理多个服务

        验证可以同时处理多个服务的测试
        """
        services = [
            {"name": "payment", "row_count": 5},
            {"name": "invoice", "row_count": 3},
        ]

        results = []

        for service in services:
            # 为每个服务生成数据
            csv_tool = CSVGeneratorTool()
            csv_path = os.path.join(temp_dir, f"{service['name']}_test.csv")

            schema = {
                "fields": [
                    {"name": "id", "type": "uuid"},
                    {"name": "amount", "type": "float", "min": 1.0, "max": 100.0},
                ]
            }

            result = csv_tool._run(
                data_schema=schema, row_count=service["row_count"], output_path=csv_path
            )

            results.append(
                {
                    "service": service["name"],
                    "file": result,
                    "rows": service["row_count"],
                    "success": os.path.exists(result),
                }
            )

        # 验证所有服务都成功
        assert len(results) == 2
        assert all(r["success"] for r in results)
        assert results[0]["rows"] == 5
        assert results[1]["rows"] == 3

        print(f"\n✅ 批量处理测试通过！")
        print(f"   - 服务 1: {results[0]['service']} ({results[0]['rows']} 行)")
        print(f"   - 服务 2: {results[1]['service']} ({results[1]['rows']} 行)")


class TestConfigurationLoading:
    """配置加载测试"""

    def test_dev_environment_config(self):
        """测试 4：开发环境配置加载

        验证可以正确加载 dev 环境的配置
        """
        # 创建临时配置
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / "dev"
            config_dir.mkdir()

            # 创建 services.yaml
            services_config = {
                "services": [
                    {
                        "name": "test-service",
                        "description": "Test service for E2E",
                        "enabled": True,
                        "data_generation": {
                            "row_count": 10,
                            "data_schema": [{"name": "id", "type": "uuid"}],
                        },
                    }
                ]
            }

            with open(config_dir / "services.yaml", "w") as f:
                yaml.dump(services_config, f)

            # 创建空的 secrets.env
            with open(config_dir / "secrets.env", "w") as f:
                f.write("# Test environment variables\n")

            # 加载配置
            config_manager = ConfigManager("dev")
            config_manager.config_path = config_dir

            # 手动加载
            with open(config_dir / "services.yaml", "r") as f:
                data = yaml.safe_load(f)
                services = config_manager._parse_services(data.get("services", []))

            assert len(services) == 1
            assert services[0].name == "test-service"
            assert services[0].enabled is True
            assert services[0].data_generation.row_count == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
