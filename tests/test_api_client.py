"""
单元测试：APIClientTool

测试目标：验证 HTTP API 调用功能
测试方法：使用 httpbin.org 进行真实 API 测试
"""

import pytest
import time
from krystal.tools.api_client import (
    APIClientTool,
    JSONExtractorTool,
    TemplateRenderTool,
)


class TestAPIClientTool:
    """APIClientTool 测试类 - 使用 httpbin.org"""

    @pytest.fixture
    def tool(self):
        """创建工具实例"""
        return APIClientTool()

    def test_get_request(self, tool):
        """测试 1：GET 请求

        验证：
        - GET 请求成功
        - 返回正确的状态码
        - 响应体解析正确
        """
        result = tool._run(
            endpoint="https://httpbin.org/get",
            method="GET",
            headers={"X-Custom-Header": "test-value"},
        )

        assert result["success"] is True
        assert result["status_code"] == 200
        assert "body" in result
        assert result["body"]["headers"]["X-Custom-Header"] == "test-value"

    def test_post_request_with_json_body(self, tool):
        """测试 2：POST 请求带 JSON 数据

        验证：
        - POST 请求成功
        - JSON 数据正确发送
        - 服务器正确接收
        """
        test_data = {
            "name": "test_user",
            "action": "test_action",
            "data": {"key": "value", "number": 123},
        }

        result = tool._run(
            endpoint="https://httpbin.org/post",
            method="POST",
            headers={"Content-Type": "application/json"},
            body=test_data,
        )

        assert result["success"] is True
        assert result["status_code"] == 200

        # 验证返回的数据
        assert result["body"]["json"] == test_data
        assert (
            result["body"]["data"]
            == '{"name": "test_user", "action": "test_action", "data": {"key": "value", "number": 123}}'
        )

    def test_request_with_query_params(self, tool):
        """测试 3：带查询参数的 GET 请求

        验证：
        - 查询参数正确附加到 URL
        - 服务器正确解析参数
        """
        result = tool._run(
            endpoint="https://httpbin.org/get",
            method="GET",
            params={"service": "payment", "batch_id": "batch_123", "status": "active"},
        )

        assert result["success"] is True
        assert result["status_code"] == 200

        # 验证查询参数
        assert result["body"]["args"]["service"] == "payment"
        assert result["body"]["args"]["batch_id"] == "batch_123"
        assert result["body"]["args"]["status"] == "active"

    def test_request_with_auth_header(self, tool):
        """测试 4：带认证头的请求

        验证：
        - Authorization 头正确发送
        - 认证信息正确传递
        """
        result = tool._run(
            endpoint="https://httpbin.org/get",
            method="GET",
            headers={
                "Authorization": "Bearer test_token_12345",
                "X-API-Key": "secret_key",
            },
        )

        assert result["success"] is True
        assert result["body"]["headers"]["Authorization"] == "Bearer test_token_12345"
        assert result["body"]["headers"]["X-Api-Key"] == "secret_key"

    def test_404_error_handling(self, tool):
        """测试 5：404 错误处理

        验证：
        - 当资源不存在时返回 success=False
        - 返回正确的错误状态码
        """
        result = tool._run(endpoint="https://httpbin.org/status/404", method="GET")

        assert result["success"] is False
        assert result["status_code"] == 404
        assert "message" in result

    def test_500_error_handling(self, tool):
        """测试 6：500 错误处理

        验证：
        - 当服务器错误时返回 success=False
        - 返回正确的错误状态码
        """
        result = tool._run(endpoint="https://httpbin.org/status/500", method="GET")

        assert result["success"] is False
        assert result["status_code"] == 500

    def test_timeout_handling(self, tool):
        """测试 7：超时处理

        验证：
        - 当请求超时时返回错误
        - 错误信息包含 timeout
        """
        # httpbin.org/delay/10 会延迟 10 秒返回
        # 我们设置超时为 2 秒，应该会超时
        result = tool._run(
            endpoint="https://httpbin.org/delay/10",
            method="GET",
            timeout=2,  # 2秒超时
        )

        assert result["success"] is False
        assert "Timeout" in result["error"]

    def test_put_request(self, tool):
        """测试 8：PUT 请求

        验证：
        - PUT 请求成功
        - 数据正确发送
        """
        test_data = {"update": "data", "version": 2}

        result = tool._run(
            endpoint="https://httpbin.org/put", method="PUT", body=test_data
        )

        assert result["success"] is True
        assert result["status_code"] == 200
        assert result["body"]["json"] == test_data

    def test_delete_request(self, tool):
        """测试 9：DELETE 请求

        验证：
        - DELETE 请求成功
        """
        result = tool._run(endpoint="https://httpbin.org/delete", method="DELETE")

        assert result["success"] is True
        assert result["status_code"] == 200


class TestJSONExtractorTool:
    """JSONExtractorTool 测试类"""

    @pytest.fixture
    def tool(self):
        """创建工具实例"""
        return JSONExtractorTool()

    def test_extract_simple_field(self, tool):
        """测试 10：提取简单字段

        验证：
        - 使用 $ 提取根级别字段
        """
        json_data = {
            "data": {"task_id": "task_12345", "status": "completed"},
            "message": "Success",
        }

        result = tool._run(json_data=json_data, json_path="$.data.task_id")

        assert result["success"] is True
        assert result["value"] == "task_12345"

    def test_extract_nested_field(self, tool):
        """测试 11：提取嵌套字段

        验证：
        - 使用点号路径提取嵌套字段
        """
        json_data = {"response": {"result": {"details": {"id": "nested_123"}}}}

        result = tool._run(
            json_data=json_data, json_path="$.response.result.details.id"
        )

        assert result["success"] is True
        assert result["value"] == "nested_123"

    def test_extract_array_element(self, tool):
        """测试 12：提取数组元素

        验证：
        - 使用 [index] 提取数组元素
        """
        json_data = {"items": [{"id": "item_1"}, {"id": "item_2"}, {"id": "item_3"}]}

        result = tool._run(json_data=json_data, json_path="$.items[1].id")

        assert result["success"] is True
        assert result["value"] == "item_2"

    def test_extract_nonexistent_path(self, tool):
        """测试 13：提取不存在的路径

        验证：
        - 当路径不存在时返回 success=False
        - 返回适当的错误信息
        """
        json_data = {"data": {"status": "ok"}}

        result = tool._run(json_data=json_data, json_path="$.nonexistent.field")

        assert result["success"] is False
        assert "No matches found" in result["message"]


class TestTemplateRenderTool:
    """TemplateRenderTool 测试类"""

    @pytest.fixture
    def tool(self):
        """创建工具实例"""
        return TemplateRenderTool()

    def test_render_simple_template(self, tool):
        """测试 14：简单模板渲染

        验证：
        - {{variable}} 正确替换
        """
        template = "Hello {{name}}, your ID is {{id}}"
        variables = {"name": "Alice", "id": "12345"}

        result = tool._run(template=template, variables=variables)

        assert result == "Hello Alice, your ID is 12345"

    def test_render_api_body_template(self, tool):
        """测试 15：API 请求体模板渲染

        验证：
        - 复杂的 JSON 模板正确渲染
        - 所有变量正确替换
        """
        template = """{
  "file_path": "{{remote_file_path}}",
  "batch_id": "{{batch_id}}",
  "service": "{{service_name}}",
  "row_count": {{row_count}}
}"""

        variables = {
            "remote_file_path": "/uploads/payment/incoming/test.csv",
            "batch_id": "batch_20240131_001",
            "service_name": "payment",
            "row_count": "100",
        }

        result = tool._run(template=template, variables=variables)

        assert "/uploads/payment/incoming/test.csv" in result
        assert "batch_20240131_001" in result
        assert "payment" in result
        assert "100" in result

    def test_render_with_missing_variables(self, tool):
        """测试 16：缺失变量处理

        验证：
        - 当变量不存在时，保留原占位符
        """
        template = "Hello {{name}}, missing: {{missing}}"
        variables = {"name": "Bob"}

        result = tool._run(template=template, variables=variables)

        assert result == "Hello Bob, missing: {{missing}}"

    def test_render_empty_template(self, tool):
        """测试 17：空模板渲染

        验证：
        - 空模板返回空字符串
        """
        result = tool._run(template="", variables={"name": "test"})

        assert result == ""

    def test_render_no_variables(self, tool):
        """测试 18：无变量模板

        验证：
        - 当没有占位符时，返回原模板
        """
        template = "This is a static text without variables"
        variables = {}

        result = tool._run(template=template, variables=variables)

        assert result == template


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
