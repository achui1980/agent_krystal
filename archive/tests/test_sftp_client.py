"""
单元测试：SFTPClientTool

测试目标：验证 SFTP 文件上传和下载功能
测试方法：使用 mock 模拟 paramiko 的 SFTP 操作
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from krystal.tools.sftp_client import SFTPClientTool, SFTPFileCheckTool


class TestSFTPClientTool:
    """SFTPClientTool 测试类"""

    @pytest.fixture
    def tool(self):
        """创建工具实例"""
        return SFTPClientTool()

    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def sample_file(self, temp_dir):
        """创建测试文件"""
        file_path = os.path.join(temp_dir, "test_upload.txt")
        with open(file_path, "w") as f:
            f.write("This is test content for SFTP upload")
        return file_path

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    @patch("krystal.tools.sftp_client.paramiko.RSAKey")
    def test_upload_file_with_password(
        self, mock_rsa_key, mock_transport, tool, temp_dir, sample_file
    ):
        """测试 1：使用密码上传文件

        验证：
        - 成功连接到 SFTP 服务器
        - 文件成功上传到远程路径
        - 返回正确的结果信息
        """
        # 设置 mock
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp = MagicMock()
        mock_sftp.stat.return_value = MagicMock(st_size=1024)

        # 模拟 SFTPClient.from_transport
        with patch("krystal.tools.sftp_client.paramiko.SFTPClient") as mock_sftp_client:
            mock_sftp_client.from_transport.return_value = mock_sftp

            # 执行上传
            result = tool._run(
                action="upload",
                host="test.sftp.com",
                port=22,
                username="testuser",
                password="testpass",
                local_path=sample_file,
                remote_path="/remote/test_upload.txt",
                retry_attempts=1,
            )

            # 验证结果
            assert result["success"] is True
            assert result["local_path"] == sample_file
            assert result["remote_path"] == "/remote/test_upload.txt"
            assert result["size"] == 1024

            # 验证连接被调用
            mock_transport.assert_called_once_with(("test.sftp.com", 22))
            mock_transport_instance.connect.assert_called_once_with(
                username="testuser", password="testpass"
            )

            # 验证文件被上传
            mock_sftp.put.assert_called_once_with(
                sample_file, "/remote/test_upload.txt"
            )

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    @patch("krystal.tools.sftp_client.paramiko.RSAKey")
    def test_upload_file_with_private_key(
        self, mock_rsa_key, mock_transport, tool, temp_dir, sample_file
    ):
        """测试 2：使用私钥上传文件

        验证：
        - 使用 SSH 私钥认证成功
        - 文件上传成功
        """
        # 创建临时私钥文件
        key_path = os.path.join(temp_dir, "test_key.pem")
        with open(key_path, "w") as f:
            f.write(
                "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
            )

        # 设置 mock
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp = MagicMock()
        mock_sftp.stat.return_value = MagicMock(st_size=1024)

        with patch("krystal.tools.sftp_client.paramiko.SFTPClient") as mock_sftp_client:
            mock_sftp_client.from_transport.return_value = mock_sftp

            result = tool._run(
                action="upload",
                host="test.sftp.com",
                port=22,
                username="testuser",
                private_key=key_path,
                local_path=sample_file,
                remote_path="/remote/test_upload.txt",
                retry_attempts=1,
            )

            assert result["success"] is True
            assert "File uploaded successfully" in result["message"]

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    @patch("krystal.tools.sftp_client.Path")
    def test_download_file(self, mock_path, mock_transport, tool, temp_dir):
        """测试 3：下载文件

        验证：
        - 成功从远程下载文件
        - 文件保存到本地路径
        """
        # 设置 mock
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp = MagicMock()

        # 模拟 Path 对象
        mock_path_instance = MagicMock()
        mock_path_instance.parent = MagicMock()
        mock_path_instance.stat.return_value = MagicMock(st_size=1024)
        mock_path.return_value = mock_path_instance

        local_download_path = os.path.join(temp_dir, "downloaded.txt")

        with patch("krystal.tools.sftp_client.paramiko.SFTPClient") as mock_sftp_client:
            mock_sftp_client.from_transport.return_value = mock_sftp

            result = tool._run(
                action="download",
                host="test.sftp.com",
                port=22,
                username="testuser",
                password="testpass",
                remote_path="/remote/source.txt",
                local_path=local_download_path,
                retry_attempts=1,
            )

            assert result["success"] is True
            assert result["remote_path"] == "/remote/source.txt"
            assert result["local_path"] == local_download_path
            assert result["size"] == 1024

            # 验证下载被调用
            mock_sftp.get.assert_called_once_with(
                "/remote/source.txt", local_download_path
            )

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    def test_upload_file_failure(self, mock_transport, tool, temp_dir, sample_file):
        """测试 4：上传失败处理

        验证：
        - 当 SFTP 连接失败时，返回错误信息
        - 重试机制正常工作
        """
        # 模拟连接失败
        mock_transport.side_effect = Exception("Connection refused")

        result = tool._run(
            action="upload",
            host="invalid.host.com",
            port=22,
            username="testuser",
            password="wrongpass",
            local_path=sample_file,
            remote_path="/remote/test.txt",
            retry_attempts=3,
        )

        # 验证失败结果
        assert result["success"] is False
        assert "error" in result
        assert "Failed to upload file" in result["message"]

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    def test_create_remote_directory(self, mock_transport, tool, temp_dir, sample_file):
        """测试 5：自动创建远程目录

        验证：
        - 当远程目录不存在时，自动创建
        - 创建嵌套目录结构
        """
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp = MagicMock()
        # 模拟目录不存在
        mock_sftp.stat.side_effect = [
            FileNotFoundError,
            FileNotFoundError,
            MagicMock(),
            MagicMock(),
        ]

        with patch("krystal.tools.sftp_client.paramiko.SFTPClient") as mock_sftp_client:
            mock_sftp_client.from_transport.return_value = mock_sftp

            result = tool._run(
                action="upload",
                host="test.sftp.com",
                port=22,
                username="testuser",
                password="testpass",
                local_path=sample_file,
                remote_path="/very/deep/nested/path/file.txt",
                retry_attempts=1,
            )

            assert result["success"] is True
            # 验证目录创建被调用
            assert mock_sftp.mkdir.call_count >= 1


class TestSFTPFileCheckTool:
    """SFTPFileCheckTool 测试类"""

    @pytest.fixture
    def tool(self):
        """创建工具实例"""
        return SFTPFileCheckTool()

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    def test_check_file_exists(self, mock_transport, tool):
        """测试 6：检查文件存在

        验证：
        - 当文件存在时返回 exists=True
        """
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp = MagicMock()
        # 模拟文件存在
        mock_sftp.stat.return_value = MagicMock(st_size=1024)

        with patch("krystal.tools.sftp_client.paramiko.SFTPClient") as mock_sftp_client:
            mock_sftp_client.from_transport.return_value = mock_sftp

            result = tool._run(
                host="test.sftp.com",
                port=22,
                username="testuser",
                password="testpass",
                remote_path="/remote/existing_file.txt",
            )

            assert result["exists"] is True
            assert "File exists" in result["message"]

    @patch("krystal.tools.sftp_client.paramiko.Transport")
    def test_check_file_not_exists(self, mock_transport, tool):
        """测试 7：检查文件不存在

        验证：
        - 当文件不存在时返回 exists=False
        """
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp = MagicMock()
        # 模拟文件不存在
        mock_sftp.stat.side_effect = FileNotFoundError()

        with patch("krystal.tools.sftp_client.paramiko.SFTPClient") as mock_sftp_client:
            mock_sftp_client.from_transport.return_value = mock_sftp

            result = tool._run(
                host="test.sftp.com",
                port=22,
                username="testuser",
                password="testpass",
                remote_path="/remote/nonexistent_file.txt",
            )

            assert result["exists"] is False
            assert "File does not exist" in result["message"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
