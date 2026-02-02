"""
ETL Executor - Performs actual ETL operations

This module provides a simple executor class that performs actual SFTP and API operations
instead of returning mock data. It can be called directly by the ETLTestCrew or agents.
"""

import time
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from krystal.tools.sftp_client import SFTPClientTool
from krystal.tools.api_client import APIClientTool, JSONExtractorTool
from krystal.tools.polling_service import PollingServiceTool
from krystal_v2.utils.retry_decorator import network_retry


logger = logging.getLogger(__name__)


class ETLExecutor:
    """
    ETL操作执行器

    执行实际的ETL流程：上传→触发→轮询→下载
    提供详细的执行时间和状态信息
    """

    def __init__(self):
        """初始化ETL执行器"""
        self.sftp_tool = SFTPClientTool()
        self.api_tool = APIClientTool()
        self.json_extractor = JSONExtractorTool()
        self.polling_tool = PollingServiceTool()

    @network_retry
    def upload_to_sftp(
        self,
        local_path: str,
        remote_path: str,
        sftp_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        上传文件到SFTP服务器

        Args:
            local_path: 本地文件路径
            remote_path: 远程目标路径
            sftp_config: SFTP配置（host, port, username, password）

        Returns:
            包含执行结果和计时信息的字典
        """
        start_time = time.time()
        logger.info(f"📤 ETLExecutor: 开始上传文件 {local_path} → {remote_path}")

        try:
            # 验证本地文件存在
            if not Path(local_path).exists():
                duration = time.time() - start_time
                return {
                    "success": False,
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "duration": duration,
                    "error": f"本地文件不存在: {local_path}",
                    "message": "上传失败 - 文件不存在",
                }

            # 执行上传
            result = self.sftp_tool._run(
                action="upload",
                host=sftp_config.get("host", "localhost"),
                port=sftp_config.get("port", 22),
                username=sftp_config.get("username", ""),
                password=sftp_config.get("password", ""),
                local_path=local_path,
                remote_path=remote_path,
                retry_attempts=sftp_config.get("retry_attempts", 3),
            )

            duration = time.time() - start_time

            return {
                "success": result.get("success", False),
                "local_path": local_path,
                "remote_path": remote_path,
                "duration": round(duration, 3),
                "size": result.get("size", 0),
                "error": result.get("error") if not result.get("success") else None,
                "message": result.get("message", "上传完成"),
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"❌ ETLExecutor: 上传失败 - {e}")
            return {
                "success": False,
                "local_path": local_path,
                "remote_path": remote_path,
                "duration": round(duration, 3),
                "error": str(e),
                "message": f"上传失败: {str(e)}",
            }

    @network_retry
    def trigger_service(
        self,
        endpoint: str,
        method: str,
        headers: Dict[str, str],
        body_template: str,
        task_id_extractor: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        触发服务处理

        Args:
            endpoint: API端点URL
            method: HTTP方法（POST, PUT等）
            headers: HTTP请求头
            body_template: 请求体模板（可包含变量占位符）
            task_id_extractor: JSONPath表达式用于提取task_id
            variables: 用于渲染模板的变量字典

        Returns:
            包含task_id和执行结果的字典
        """
        start_time = time.time()
        logger.info(f"🚀 ETLExecutor: 开始触发服务 {endpoint}")

        try:
            # 渲染模板（如果有变量）
            body = None
            if body_template and variables:
                from krystal.tools.api_client import TemplateRenderTool

                render_tool = TemplateRenderTool()
                render_result = render_tool._run(
                    template=body_template, variables=variables
                )
                if render_result.get("success"):
                    try:
                        import json

                        body = json.loads(render_result.get("rendered", "{}"))
                    except json.JSONDecodeError:
                        body = {"data": render_result.get("rendered")}
                else:
                    body = {"data": body_template}
            elif body_template:
                try:
                    import json

                    body = json.loads(body_template)
                except json.JSONDecodeError:
                    body = {"data": body_template}

            # 执行API调用
            result = self.api_tool._run(
                endpoint=endpoint,
                method=method,
                headers=headers or {},
                body=body,
                timeout=30,
            )

            duration = time.time() - start_time

            if not result.get("success"):
                return {
                    "success": False,
                    "endpoint": endpoint,
                    "duration": round(duration, 3),
                    "status_code": result.get("status_code"),
                    "error": result.get("error"),
                    "message": result.get("message", "触发失败"),
                    "task_id": None,
                }

            # 提取task_id
            task_id = None
            if task_id_extractor and result.get("body"):
                extract_result = self.json_extractor._run(
                    json_data=result.get("body"), json_path=task_id_extractor
                )
                if extract_result.get("success"):
                    task_id = extract_result.get("value")

            logger.info(f"✅ ETLExecutor: 服务触发成功, task_id={task_id}")

            return {
                "success": True,
                "endpoint": endpoint,
                "duration": round(duration, 3),
                "status_code": result.get("status_code"),
                "task_id": task_id,
                "response_body": result.get("body"),
                "message": f"服务触发成功，获取到task_id: {task_id}",
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"❌ ETLExecutor: 触发服务失败 - {e}")
            return {
                "success": False,
                "endpoint": endpoint,
                "duration": round(duration, 3),
                "error": str(e),
                "message": f"触发服务失败: {str(e)}",
                "task_id": None,
            }

    @network_retry
    def poll_until_complete(
        self,
        task_id: str,
        status_endpoint: str,
        status_extractor: str,
        success_statuses: list,
        failure_statuses: list,
        max_attempts: int = 30,
        interval: int = 10,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        轮询等待任务完成

        Args:
            task_id: 任务ID
            status_endpoint: 状态查询端点（可包含{{task_id}}占位符）
            status_extractor: JSONPath表达式用于提取状态
            success_statuses: 表示成功的状态值列表
            failure_statuses: 表示失败的状态值列表
            max_attempts: 最大轮询次数
            interval: 轮询间隔（秒）
            headers: HTTP请求头

        Returns:
            包含最终状态和执行结果的字典
        """
        start_time = time.time()
        logger.info(f"⏳ ETLExecutor: 开始轮询任务 {task_id}")

        try:
            result = self.polling_tool._run(
                endpoint=status_endpoint,
                method="GET",
                headers=headers or {},
                task_id=task_id,
                status_extractor=status_extractor,
                success_statuses=success_statuses or ["completed", "success"],
                failure_statuses=failure_statuses or ["failed", "error"],
                max_attempts=max_attempts,
                interval=interval,
            )

            duration = time.time() - start_time

            return {
                "success": result.get("success", False),
                "task_id": task_id,
                "duration": round(duration, 3),
                "attempts": result.get("attempts", 0),
                "status": result.get("status"),
                "completed": result.get("completed", False),
                "failed": result.get("failed", False),
                "timed_out": result.get("timed_out", False),
                "message": result.get("message", "轮询完成"),
                "last_response": result.get("last_response"),
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"❌ ETLExecutor: 轮询失败 - {e}")
            return {
                "success": False,
                "task_id": task_id,
                "duration": round(duration, 3),
                "error": str(e),
                "message": f"轮询失败: {str(e)}",
                "timed_out": False,
                "completed": False,
                "failed": True,
            }

    @network_retry
    def download_from_sftp(
        self,
        remote_path: str,
        local_path: str,
        sftp_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        从SFTP服务器下载文件

        Args:
            remote_path: 远程文件路径
            local_path: 本地保存路径
            sftp_config: SFTP配置（host, port, username, password）

        Returns:
            包含执行结果和计时信息的字典
        """
        start_time = time.time()
        logger.info(f"📥 ETLExecutor: 开始下载文件 {remote_path} → {local_path}")

        try:
            # 确保本地目录存在
            local_dir = Path(local_path).parent
            local_dir.mkdir(parents=True, exist_ok=True)

            # 执行下载
            result = self.sftp_tool._run(
                action="download",
                host=sftp_config.get("host", "localhost"),
                port=sftp_config.get("port", 22),
                username=sftp_config.get("username", ""),
                password=sftp_config.get("password", ""),
                remote_path=remote_path,
                local_path=local_path,
                retry_attempts=sftp_config.get("retry_attempts", 3),
            )

            duration = time.time() - start_time

            return {
                "success": result.get("success", False),
                "remote_path": remote_path,
                "local_path": local_path,
                "duration": round(duration, 3),
                "size": result.get("size", 0),
                "error": result.get("error") if not result.get("success") else None,
                "message": result.get("message", "下载完成"),
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"❌ ETLExecutor: 下载失败 - {e}")
            return {
                "success": False,
                "remote_path": remote_path,
                "local_path": local_path,
                "duration": round(duration, 3),
                "error": str(e),
                "message": f"下载失败: {str(e)}",
            }

    def execute_full_etl(
        self,
        input_file: str,
        output_file: str,
        sftp_config: Dict[str, Any],
        trigger_config: Dict[str, Any],
        polling_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        执行完整的ETL流程

        Args:
            input_file: 输入文件路径
            output_file: 输出文件保存路径
            sftp_config: SFTP配置
            trigger_config: 触发配置（endpoint, method, headers, body_template, task_id_extractor）
            polling_config: 轮询配置（status_endpoint, status_extractor, success_statuses, failure_statuses, max_attempts, interval）

        Returns:
            完整的ETL执行结果
        """
        total_start = time.time()
        logger.info("🔄 ETLExecutor: 开始执行完整ETL流程")

        results = {
            "success": False,
            "steps": {},
            "total_duration": 0,
            "result_file": None,
            "error": None,
        }

        # Step 1: Upload
        # Use upload remote path from config, fallback to /uploads
        remote_base = sftp_config.get("upload_remote_path", "/uploads")
        remote_path = remote_base + "/" + Path(input_file).name
        upload_result = self.upload_to_sftp(
            local_path=input_file,
            remote_path=remote_path,
            sftp_config=sftp_config,
        )
        results["steps"]["upload"] = upload_result

        if not upload_result["success"]:
            results["error"] = f"上传失败: {upload_result.get('error')}"
            results["total_duration"] = round(time.time() - total_start, 3)
            return results

        # Step 2: Trigger
        trigger_result = self.trigger_service(
            endpoint=trigger_config.get("endpoint", ""),
            method=trigger_config.get("method", "POST"),
            headers=trigger_config.get("headers", {}),
            body_template=trigger_config.get("body_template", ""),
            task_id_extractor=trigger_config.get("task_id_extractor", ""),
            variables={"remote_path": remote_path},
        )
        results["steps"]["trigger"] = trigger_result

        if not trigger_result["success"]:
            results["error"] = f"触发服务失败: {trigger_result.get('error')}"
            results["total_duration"] = round(time.time() - total_start, 3)
            return results

        task_id = trigger_result.get("task_id")
        if not task_id:
            results["error"] = "未能获取task_id"
            results["total_duration"] = round(time.time() - total_start, 3)
            return results

        # Step 3: Poll
        poll_result = self.poll_until_complete(
            task_id=task_id,
            status_endpoint=polling_config.get("status_endpoint", ""),
            status_extractor=polling_config.get("status_extractor", "$.status"),
            success_statuses=polling_config.get(
                "success_statuses", ["completed", "success"]
            ),
            failure_statuses=polling_config.get(
                "failure_statuses", ["failed", "error"]
            ),
            max_attempts=polling_config.get("max_attempts", 30),
            interval=polling_config.get("interval", 10),
            headers=polling_config.get("headers", {}),
        )
        results["steps"]["poll"] = poll_result

        if not poll_result["success"]:
            results["error"] = f"轮询失败或任务失败: {poll_result.get('message')}"
            results["total_duration"] = round(time.time() - total_start, 3)
            return results

        # Step 4: Download
        download_remote_path = (
            sftp_config.get("remote_base_path", "/uploads")
            + "/output/"
            + task_id
            + ".csv"
        )
        download_result = self.download_from_sftp(
            remote_path=download_remote_path,
            local_path=output_file,
            sftp_config=sftp_config,
        )
        results["steps"]["download"] = download_result

        if not download_result["success"]:
            results["error"] = f"下载失败: {download_result.get('error')}"
            results["total_duration"] = round(time.time() - total_start, 3)
            return results

        # Success
        results["success"] = True
        results["total_duration"] = round(time.time() - total_start, 3)
        results["result_file"] = output_file
        results["message"] = "ETL流程执行成功"

        logger.info(
            f"✅ ETLExecutor: ETL流程执行成功，总耗时 {results['total_duration']}秒"
        )

        return results
