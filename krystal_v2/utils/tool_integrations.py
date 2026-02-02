"""
Tool integrations for Krystal v2.0
复用现有 krystal.tools 实现实际功能
"""

import os
import logging
from typing import Dict, Any

# 复用现有工具
from krystal.tools.sftp_client import SFTPClientTool
from krystal.tools.api_client import APIClientTool
from krystal.tools.polling_service import PollingServiceTool

logger = logging.getLogger(__name__)


class ETLTools:
    """ETL工具集合 - 实际执行SFTP和API操作"""

    @staticmethod
    def upload_file(
        local_path: str, remote_path: str, sftp_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        实际上传文件到SFTP

        Args:
            local_path: 本地文件路径
            remote_path: 远程目标路径
            sftp_config: SFTP配置

        Returns:
            上传结果
        """
        logger.info(f"📤 实际上传文件: {local_path} → {remote_path}")

        try:
            tool = SFTPClientTool()
            result = tool._run(
                action="upload",
                host=sftp_config.get("host", "localhost"),
                port=sftp_config.get("port", 2223),
                username=sftp_config.get("username", "testuser"),
                password=sftp_config.get("password", ""),
                local_path=local_path,
                remote_path=remote_path,
            )

            if result.get("success"):
                logger.info(f"✅ 上传成功: {result.get('remote_path')}")
            else:
                logger.error(f"❌ 上传失败: {result.get('error', 'Unknown')}")

            return result

        except Exception as e:
            logger.error(f"❌ 上传异常: {e}")
            return {"success": False, "error": str(e), "message": f"上传失败: {e}"}

    @staticmethod
    def download_file(
        remote_path: str, local_path: str, sftp_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        实际从SFTP下载文件

        Args:
            remote_path: 远程文件路径
            local_path: 本地保存路径
            sftp_config: SFTP配置

        Returns:
            下载结果
        """
        logger.info(f"📥 实际下载文件: {remote_path} → {local_path}")

        try:
            tool = SFTPClientTool()
            result = tool._run(
                action="download",
                host=sftp_config.get("host", "localhost"),
                port=sftp_config.get("port", 2223),
                username=sftp_config.get("username", "testuser"),
                password=sftp_config.get("password", ""),
                remote_path=remote_path,
                local_path=local_path,
            )

            if result.get("success"):
                logger.info(f"✅ 下载成功: {result.get('local_path')}")
            else:
                logger.error(f"❌ 下载失败: {result.get('error', 'Unknown')}")

            return result

        except Exception as e:
            logger.error(f"❌ 下载异常: {e}")
            return {"success": False, "error": str(e), "message": f"下载失败: {e}"}

    @staticmethod
    def trigger_service(
        endpoint: str, payload: Dict[str, Any], api_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        实际触发服务

        Args:
            endpoint: API端点
            payload: 请求体
            api_config: API配置

        Returns:
            触发结果
        """
        logger.info(f"🚀 实际触发服务: {endpoint}")

        try:
            tool = APIClientTool()
            result = tool._run(
                endpoint=endpoint,
                method=api_config.get("method", "POST"),
                body=payload,
                headers=api_config.get("headers", {}),
            )

            if result.get("success"):
                # 提取task_id
                task_id = None
                if "body" in result and result["body"]:
                    body = result["body"]
                    if isinstance(body, dict):
                        task_id = body.get("task_id") or body.get("id")

                logger.info(f"✅ 触发成功, task_id: {task_id}")
                return {
                    "success": True,
                    "task_id": task_id,
                    "response": result,
                    "message": "服务触发成功",
                }
            else:
                logger.error(f"❌ 触发失败: {result.get('error', 'Unknown')}")
                return {
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                    "message": "服务触发失败",
                }

        except Exception as e:
            logger.error(f"❌ 触发异常: {e}")
            return {"success": False, "error": str(e), "message": f"触发失败: {e}"}

    @staticmethod
    def poll_status(
        task_id: str, status_endpoint: str, polling_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        实际轮询任务状态

        Args:
            task_id: 任务ID
            status_endpoint: 状态查询端点
            polling_config: 轮询配置

        Returns:
            轮询结果
        """
        logger.info(f"⏳ 实际轮询任务: {task_id}")

        try:
            tool = PollingServiceTool()
            result = tool._run(
                task_id=task_id,
                endpoint=status_endpoint,
                max_attempts=polling_config.get("max_attempts", 30),
                interval=polling_config.get("interval", 10),
                success_statuses=["completed", "success"],
                failure_statuses=["failed", "error"],
            )

            if result.get("success"):
                logger.info(f"✅ 任务完成: {result.get('status')}")
            elif result.get("failed"):
                logger.error(f"❌ 任务失败: {result.get('status')}")
            elif result.get("timed_out"):
                logger.warning(f"⏰ 轮询超时")

            return result

        except Exception as e:
            logger.error(f"❌ 轮询异常: {e}")
            return {
                "success": False,
                "failed": True,
                "error": str(e),
                "message": f"轮询失败: {e}",
            }
