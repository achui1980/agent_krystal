"""
单元测试：CSVGeneratorTool

测试目标：验证 CSVGeneratorTool 的数据生成功能
测试范围：
1. 根据 schema 生成 CSV 文件
2. 支持不同数据类型（uuid、float、enum、datetime）
3. 文件正确保存到指定路径
4. 模板渲染功能
"""

import pytest
import csv
import os
import tempfile
from pathlib import Path
from datetime import datetime

from krystal.tools.csv_generator import CSVGeneratorTool


class TestCSVGeneratorTool:
    """CSVGeneratorTool 测试类"""

    @pytest.fixture
    def tool(self):
        """创建工具实例"""
        return CSVGeneratorTool()

    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_generate_csv_from_schema_basic(self, tool, temp_dir):
        """测试 1：基本 schema 生成 CSV

        验证：
        - 文件成功创建
        - 包含正确的 header
        - 生成指定行数的数据
        """
        # 准备测试数据
        output_path = os.path.join(temp_dir, "test_basic.csv")
        schema = {
            "fields": [
                {"name": "id", "type": "uuid"},
                {
                    "name": "amount",
                    "type": "float",
                    "min": 1.0,
                    "max": 100.0,
                    "decimals": 2,
                },
                {"name": "status", "type": "enum", "values": ["active", "inactive"]},
            ]
        }

        # 执行测试
        result = tool._run(
            data_schema=schema, row_count=5, output_path=output_path, template_path=None
        )

        # 验证结果
        assert result is not None
        assert os.path.exists(result)

        # 读取并验证 CSV 内容
        with open(result, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # 验证 header
            assert reader.fieldnames == ["id", "amount", "status"]

            # 验证行数
            assert len(rows) == 5

            # 验证每行都有数据
            for row in rows:
                assert row["id"]  # UUID 不为空
                assert float(row["amount"]) >= 1.0  # 金额在范围内
                assert float(row["amount"]) <= 100.0
                assert row["status"] in ["active", "inactive"]  # 枚举值正确

    def test_generate_csv_all_data_types(self, tool, temp_dir):
        """测试 2：所有数据类型

        验证：
        - uuid 类型生成有效 UUID
        - int 类型生成整数
        - float 类型生成小数
        - string 类型生成字符串
        - datetime 类型生成日期时间
        - boolean 类型生成布尔值
        - enum 类型生成枚举值
        """
        output_path = os.path.join(temp_dir, "test_types.csv")
        schema = {
            "fields": [
                {"name": "uuid_field", "type": "uuid"},
                {"name": "int_field", "type": "int", "min": 0, "max": 100},
                {
                    "name": "float_field",
                    "type": "float",
                    "min": 0.0,
                    "max": 1.0,
                    "decimals": 4,
                },
                {
                    "name": "string_field",
                    "type": "string",
                    "min_length": 5,
                    "max_length": 10,
                },
                {
                    "name": "datetime_field",
                    "type": "datetime",
                    "format": "%Y-%m-%d %H:%M:%S",
                },
                {"name": "bool_field", "type": "boolean"},
                {"name": "enum_field", "type": "enum", "values": ["A", "B", "C"]},
            ]
        }

        # 执行
        result = tool._run(data_schema=schema, row_count=3, output_path=output_path)

        # 验证
        with open(result, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            for row in rows:
                # 验证 uuid 格式（36字符，包含4个-）
                assert len(row["uuid_field"]) == 36
                assert row["uuid_field"].count("-") == 4

                # 验证 int 范围
                assert 0 <= int(row["int_field"]) <= 100

                # 验证 float 范围和小数位
                val = float(row["float_field"])
                assert 0.0 <= val <= 1.0
                assert len(row["float_field"].split(".")[-1]) <= 4

                # 验证 string 长度
                assert 5 <= len(row["string_field"]) <= 10

                # 验证 datetime 格式
                datetime.strptime(row["datetime_field"], "%Y-%m-%d %H:%M:%S")

                # 验证 bool
                assert row["bool_field"] in ["True", "False"]

                # 验证 enum
                assert row["enum_field"] in ["A", "B", "C"]

    def test_generate_csv_with_template(self, tool, temp_dir):
        """测试 3：使用模板生成 CSV

        验证：
        - 正确读取 Jinja2 模板
        - 模板变量正确替换
        - 生成的文件格式正确
        """
        output_path = os.path.join(temp_dir, "test_template.csv")
        template_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "test_template.csv.j2"
        )

        schema = {
            "fields": [
                {"name": "order_id", "type": "uuid"},
                {"name": "amount", "type": "float", "min": 10.0, "max": 1000.0},
                {"name": "currency", "type": "enum", "values": ["USD", "EUR"]},
                {"name": "customer_id", "type": "string"},
                {"name": "timestamp", "type": "datetime"},
            ]
        }

        # 执行
        result = tool._run(
            data_schema=schema,
            row_count=5,
            output_path=output_path,
            template_path=template_path,
        )

        # 验证文件存在
        assert os.path.exists(result)

        # 验证内容格式
        with open(result, "r", encoding="utf-8") as f:
            lines = f.readlines()

            # 第一行是 header
            assert "order_id,amount,currency,customer_id,timestamp" in lines[0]

            # 验证有5行数据（加上header共6行）
            assert len(lines) == 6

    def test_generate_csv_output_directory_creation(self, tool, temp_dir):
        """测试 4：输出目录自动创建

        验证：
        - 如果输出目录不存在，自动创建
        - 文件正确保存到指定路径
        """
        # 使用不存在的嵌套目录
        nested_dir = os.path.join(temp_dir, "level1", "level2", "level3")
        output_path = os.path.join(nested_dir, "test_nested.csv")

        schema = {
            "fields": [
                {"name": "id", "type": "uuid"},
            ]
        }

        # 执行
        result = tool._run(data_schema=schema, row_count=1, output_path=output_path)

        # 验证目录和文件都创建成功
        assert os.path.exists(nested_dir)
        assert os.path.exists(result)

    def test_generate_csv_zero_rows(self, tool, temp_dir):
        """测试 5：边界情况 - 0 行数据

        验证：
        - 生成 0 行数据时只包含 header
        - 文件仍然有效
        """
        output_path = os.path.join(temp_dir, "test_zero.csv")
        schema = {
            "fields": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "string"},
            ]
        }

        # 执行
        result = tool._run(data_schema=schema, row_count=0, output_path=output_path)

        # 验证
        with open(result, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

            # 只有 header 行
            assert len(rows) == 1
            assert rows[0] == ["id", "name"]

    def test_generate_csv_large_dataset(self, tool, temp_dir):
        """测试 6：性能测试 - 大量数据

        验证：
        - 可以生成 1000 行数据
        - 处理时间在合理范围内
        """
        output_path = os.path.join(temp_dir, "test_large.csv")
        schema = {
            "fields": [
                {"name": "id", "type": "uuid"},
                {"name": "value", "type": "int", "min": 1, "max": 1000},
            ]
        }

        import time

        start_time = time.time()

        # 生成 1000 行
        result = tool._run(data_schema=schema, row_count=1000, output_path=output_path)

        elapsed = time.time() - start_time

        # 验证
        assert os.path.exists(result)
        assert elapsed < 10  # 应该在 10 秒内完成

        # 验证行数
        with open(result, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 1001  # header + 1000 data rows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
