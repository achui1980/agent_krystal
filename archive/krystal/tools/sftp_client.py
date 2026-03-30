"""
SFTP Client Tools with retry support
"""

import os
import logging
import paramiko
from pathlib import Path
from typing import Dict, Optional
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


logger = logging.getLogger(__name__)


class SFTPUploadInput(BaseModel):
    """Input for SFTP upload"""

    host: str = Field(description="SFTP server host")
    port: int = Field(default=22, description="SFTP server port")
    username: str = Field(description="SFTP username")
    password: str = Field(default="", description="SFTP password (or use private_key)")
    private_key: Optional[str] = Field(
        default=None, description="Path to private key file"
    )
    local_path: str = Field(description="Local file path to upload")
    remote_path: str = Field(description="Remote destination path")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")


class SFTPDownloadInput(BaseModel):
    """Input for SFTP download"""

    host: str = Field(description="SFTP server host")
    port: int = Field(default=22, description="SFTP server port")
    username: str = Field(description="SFTP username")
    password: str = Field(default="", description="SFTP password (or use private_key)")
    private_key: Optional[str] = Field(
        default=None, description="Path to private key file"
    )
    remote_path: str = Field(description="Remote file path to download")
    local_path: str = Field(description="Local destination path")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")


class SFTPClientTool(BaseTool):
    """Tool for SFTP file operations with retry support"""

    name: str = "sftp_client"
    description: str = """
    Perform SFTP operations: upload and download files.
    Supports password and private key authentication.
    Includes automatic retry with exponential backoff.
    """
    args_schema: type[BaseModel] = SFTPUploadInput

    def _run(self, **kwargs) -> Dict[str, any]:
        """
        Execute SFTP operation based on action type

        Returns:
            Dictionary with operation result
        """
        action = kwargs.get("action", "upload")
        host = kwargs.get("host", "")
        remote_path = kwargs.get("remote_path", "")
        local_path = kwargs.get("local_path", "")

        logger.info(f"📤 SFTP Client Tool 执行:")
        logger.info(f"   操作: {action}")
        logger.info(f"   服务器: {host}:{kwargs.get('port', 22)}")
        logger.info(f"   用户名: {kwargs.get('username', '')}")
        if action == "upload":
            logger.info(f"   本地文件: {local_path}")
            logger.info(f"   远程路径: {remote_path}")
        elif action == "download":
            logger.info(f"   远程路径: {remote_path}")
            logger.info(f"   本地文件: {local_path}")

        if action == "upload":
            result = self._upload_file(**kwargs)
            logger.info(
                f"   {'✅ 上传成功' if result.get('success') else '❌ 上传失败'}"
            )
            return result
        elif action == "download":
            result = self._download_file(**kwargs)
            logger.info(
                f"   {'✅ 下载成功' if result.get('success') else '❌ 下载失败'}"
            )
            return result
        else:
            raise ValueError(f"Unknown action: {action}")

    def _upload_file(
        self,
        host: str,
        port: int,
        username: str,
        password: str = "",
        private_key: Optional[str] = None,
        local_path: str = "",
        remote_path: str = "",
        retry_attempts: int = 3,
        **kwargs,
    ) -> Dict[str, any]:
        """Upload file to SFTP server with retry"""

        @retry(
            stop=stop_after_attempt(retry_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(
                (paramiko.SSHException, ConnectionError, TimeoutError)
            ),
            reraise=True,
        )
        def _do_upload():
            transport = paramiko.Transport((host, port))

            try:
                # Connect with password or private key
                if private_key and Path(private_key).exists():
                    pkey = paramiko.RSAKey.from_private_key_file(private_key)
                    transport.connect(username=username, pkey=pkey)
                else:
                    transport.connect(username=username, password=password)

                sftp = paramiko.SFTPClient.from_transport(transport)

                # Ensure remote directory exists
                remote_dir = str(Path(remote_path).parent)
                self._ensure_remote_dir(sftp, remote_dir)

                # Upload file
                sftp.put(local_path, remote_path)

                # Get file stats
                stats = sftp.stat(remote_path)

                sftp.close()

                return {
                    "success": True,
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "size": stats.st_size,
                    "message": f"File uploaded successfully to {remote_path}",
                }
            finally:
                transport.close()

        try:
            return _do_upload()
        except Exception as e:
            return {
                "success": False,
                "local_path": local_path,
                "remote_path": remote_path,
                "error": str(e),
                "message": f"Failed to upload file after {retry_attempts} attempts: {str(e)}",
            }

    def _download_file(
        self,
        host: str,
        port: int,
        username: str,
        password: str = "",
        private_key: Optional[str] = None,
        remote_path: str = "",
        local_path: str = "",
        retry_attempts: int = 3,
        **kwargs,
    ) -> Dict[str, any]:
        """Download file from SFTP server with retry"""

        @retry(
            stop=stop_after_attempt(retry_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(
                (paramiko.SSHException, ConnectionError, TimeoutError)
            ),
            reraise=True,
        )
        def _do_download():
            transport = paramiko.Transport((host, port))

            try:
                # Connect with password or private key
                if private_key and Path(private_key).exists():
                    pkey = paramiko.RSAKey.from_private_key_file(private_key)
                    transport.connect(username=username, pkey=pkey)
                else:
                    transport.connect(username=username, password=password)

                sftp = paramiko.SFTPClient.from_transport(transport)

                # Ensure local directory exists
                local_dir = Path(local_path).parent
                local_dir.mkdir(parents=True, exist_ok=True)

                # Download file
                sftp.get(remote_path, local_path)

                # Get local file stats
                size = Path(local_path).stat().st_size

                sftp.close()

                return {
                    "success": True,
                    "remote_path": remote_path,
                    "local_path": local_path,
                    "size": size,
                    "message": f"File downloaded successfully to {local_path}",
                }
            finally:
                transport.close()

        try:
            return _do_download()
        except Exception as e:
            return {
                "success": False,
                "remote_path": remote_path,
                "local_path": local_path,
                "error": str(e),
                "message": f"Failed to download file after {retry_attempts} attempts: {str(e)}",
            }

    def _ensure_remote_dir(self, sftp: paramiko.SFTPClient, remote_dir: str):
        """Ensure remote directory exists, create if necessary"""
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            # Create directory recursively
            parent = str(Path(remote_dir).parent)
            if parent and parent != ".":
                self._ensure_remote_dir(sftp, parent)
            try:
                sftp.mkdir(remote_dir)
            except IOError:
                pass  # Directory might already exist


class SFTPCheckFileInput(BaseModel):
    """Input for checking if file exists on SFTP"""

    host: str = Field(description="SFTP server host")
    port: int = Field(default=22, description="SFTP server port")
    username: str = Field(description="SFTP username")
    password: str = Field(default="", description="SFTP password")
    private_key: Optional[str] = Field(
        default=None, description="Path to private key file"
    )
    remote_path: str = Field(description="Remote file path to check")


class SFTPFileCheckTool(BaseTool):
    """Tool for checking file existence on SFTP"""

    name: str = "sftp_file_check"
    description: str = "Check if a file exists on SFTP server"
    args_schema: type[BaseModel] = SFTPCheckFileInput

    def _run(
        self,
        host: str,
        port: int,
        username: str,
        password: str = "",
        private_key: Optional[str] = None,
        remote_path: str = "",
    ) -> Dict[str, any]:
        """Check if file exists on SFTP server"""

        transport = paramiko.Transport((host, port))

        try:
            # Connect
            if private_key and Path(private_key).exists():
                pkey = paramiko.RSAKey.from_private_key_file(private_key)
                transport.connect(username=username, pkey=pkey)
            else:
                transport.connect(username=username, password=password)

            sftp = paramiko.SFTPClient.from_transport(transport)

            try:
                sftp.stat(remote_path)
                exists = True
                message = f"File exists: {remote_path}"
            except FileNotFoundError:
                exists = False
                message = f"File does not exist: {remote_path}"

            sftp.close()

            return {"exists": exists, "remote_path": remote_path, "message": message}
        except Exception as e:
            return {
                "exists": False,
                "remote_path": remote_path,
                "error": str(e),
                "message": f"Error checking file: {str(e)}",
            }
        finally:
            transport.close()
