"""
ETL Tasks - CrewAI Task definitions for ETL operations in CrewAI mode
"""

import logging
from typing import Dict, Any, Optional
from crewai import Task


logger = logging.getLogger(__name__)


class SFTPUploadTask:
    """Task for uploading input file to SFTP server"""

    @staticmethod
    def create(
        agent,
        local_file: str,
        remote_path: str,
        sftp_config: Dict[str, Any],
    ) -> Task:
        """
        Create SFTP upload task

        Args:
            agent: The agent to execute this task
            local_file: Local input file path
            remote_path: Remote SFTP destination path
            sftp_config: SFTP configuration (host, port, username, password)

        Returns:
            CrewAI Task instance
        """
        return Task(
            description=f"""
            Upload the input file to SFTP server.

            Task: Upload input file for ETL processing

            File Details:
            - Local file: {local_file}
            - Remote path: {remote_path}
            - SFTP server: {sftp_config.get("host", "unknown")}:{sftp_config.get("port", 22)}

            Steps:
            1. Connect to SFTP server using provided credentials
            2. Ensure remote directory exists (create if needed)
            3. Upload the local file to the specified remote path
            4. Verify upload was successful
            5. Return upload result with file size and path

            Use the sftp_client tool with action="upload" to perform the upload.
            
            SFTP Configuration:
            - host: {sftp_config.get("host", "localhost")}
            - port: {sftp_config.get("port", 22)}
            - username: {sftp_config.get("username", "testuser")}
            - password: (provided in config)
            - remote_path: {remote_path}
            - local_path: {local_file}

            Return a detailed result including:
            - success: boolean indicating if upload succeeded
            - remote_path: the path where file was uploaded
            - size: file size in bytes
            - message: descriptive status message
            """,
            expected_output="JSON object with upload result containing success status, remote path, and file size",
            agent=agent,
        )


class APITriggerTask:
    """Task for triggering ETL service via API"""

    @staticmethod
    def create(
        agent,
        trigger_config: Dict[str, Any],
        sftp_config: Dict[str, Any],
        remote_file_path: str,
    ) -> Task:
        """
        Create API trigger task

        Args:
            agent: The agent to execute this task
            trigger_config: API trigger configuration
            sftp_config: SFTP configuration for building file path
            remote_file_path: Path of uploaded file on SFTP

        Returns:
            CrewAI Task instance
        """
        endpoint = trigger_config.get("endpoint", "")
        method = trigger_config.get("method", "POST")
        task_id_extractor = trigger_config.get("task_id_extractor", "$.task_id")

        return Task(
            description=f"""
            Trigger the ETL service to start processing the uploaded file.

            Task: Call API to initiate ETL processing

            API Details:
            - Endpoint: {endpoint}
            - Method: {method}
            - Task ID JSONPath: {task_id_extractor}

            Steps:
            1. Build the request payload referencing the uploaded file
            2. Call the API endpoint using api_client tool
            3. Extract the task_id from the response using json_extractor tool
            4. Store the task_id for polling

            The payload should include information about the uploaded file:
            - Remote file path: {remote_file_path}

            Use api_client tool to make the API call, then use json_extractor 
            to extract the task_id from the response using path: {task_id_extractor}

            Return a detailed result including:
            - success: boolean indicating if trigger succeeded
            - task_id: the extracted task ID for tracking
            - response: full API response
            - message: descriptive status message
            """,
            expected_output="JSON object with trigger result containing task_id and API response details",
            agent=agent,
        )


class PollingTask:
    """Task for polling ETL service status"""

    @staticmethod
    def create(
        agent,
        task_id: str,
        polling_config: Dict[str, Any],
    ) -> Task:
        """
        Create polling task

        Args:
            agent: The agent to execute this task
            task_id: The task ID to poll
            polling_config: Polling configuration

        Returns:
            CrewAI Task instance
        """
        status_endpoint = polling_config.get("status_endpoint", "")
        status_extractor = polling_config.get("status_extractor", "$.status")
        max_attempts = polling_config.get("max_attempts", 30)
        interval = polling_config.get("interval", 10)
        success_statuses = polling_config.get(
            "success_statuses", ["completed", "success"]
        )
        failure_statuses = polling_config.get("failure_statuses", ["failed", "error"])

        return Task(
            description=f"""
            Poll the ETL service to check processing status until completion.

            Task: Monitor ETL processing status

            Polling Details:
            - Task ID: {task_id}
            - Status endpoint: {status_endpoint}
            - Max attempts: {max_attempts}
            - Interval: {interval} seconds
            - Success statuses: {success_statuses}
            - Failure statuses: {failure_statuses}

            Steps:
            1. Build the status check URL with task_id
            2. Call the status endpoint using api_client tool
            3. Extract the status from response using json_extractor tool
            4. Check if status indicates completion (success or failure)
            5. If not complete, wait {interval} seconds and retry
            6. Continue until max_attempts reached or status is final
            7. Return final status and any relevant metadata

            Important: You must implement the polling loop logic. Make up to {max_attempts} 
            attempts, waiting {interval} seconds between each call.

            Success conditions: status in {success_statuses}
            Failure conditions: status in {failure_statuses}

            Return a detailed result including:
            - success: boolean indicating if processing succeeded
            - status: final status value
            - attempts: number of polling attempts made
            - duration: total polling time in seconds
            - message: descriptive status message
            """,
            expected_output="JSON object with polling result containing final status, attempts count, and success flag",
            agent=agent,
        )


class SFTPDownloadTask:
    """Task for downloading result file from SFTP"""

    @staticmethod
    def create(
        agent,
        remote_path: str,
        local_path: str,
        sftp_config: Dict[str, Any],
    ) -> Task:
        """
        Create SFTP download task

        Args:
            agent: The agent to execute this task
            remote_path: Remote SFTP file path to download
            local_path: Local destination path
            sftp_config: SFTP configuration

        Returns:
            CrewAI Task instance
        """
        return Task(
            description=f"""
            Download the processed result file from SFTP server.

            Task: Download ETL output file

            File Details:
            - Remote path: {remote_path}
            - Local destination: {local_path}
            - SFTP server: {sftp_config.get("host", "unknown")}:{sftp_config.get("port", 22)}

            Steps:
            1. Connect to SFTP server using provided credentials
            2. Verify the remote file exists using sftp_file_check tool
            3. Ensure local directory exists (create if needed)
            4. Download the remote file to local path using sftp_client tool
            5. Verify download was successful and file is readable
            6. Return download result with file size and path

            First check if file exists, then perform download.
            
            SFTP Configuration:
            - host: {sftp_config.get("host", "localhost")}
            - port: {sftp_config.get("port", 22)}
            - username: {sftp_config.get("username", "testuser")}
            - password: (provided in config)
            - remote_path: {remote_path}
            - local_path: {local_path}

            Return a detailed result including:
            - success: boolean indicating if download succeeded
            - local_path: the path where file was saved
            - size: file size in bytes
            - message: descriptive status message
            """,
            expected_output="JSON object with download result containing success status, local path, and file size",
            agent=agent,
        )


def create_etl_tasks(
    agent,
    input_file: str,
    output_file: str,
    sftp_config: Dict[str, Any],
    trigger_config: Dict[str, Any],
    polling_config: Dict[str, Any],
    remote_upload_path: str = "/uploads",
) -> Dict[str, Task]:
    """
    Create all ETL tasks for CrewAI mode

    Args:
        agent: The ETL operator agent
        input_file: Local input file path
        output_file: Local output file path
        sftp_config: SFTP configuration
        trigger_config: API trigger configuration
        polling_config: Polling configuration
        remote_upload_path: Remote SFTP path for uploads

    Returns:
        Dictionary of task name to Task instance
    """
    # Build remote paths
    import os

    input_filename = os.path.basename(input_file)
    remote_input_path = f"{remote_upload_path}/{input_filename}"

    # For download, we need to determine the result file name
    # This would typically come from the API response or be based on task_id
    # For now, we'll use a placeholder that the agent will resolve
    remote_output_path = (
        f"{sftp_config.get('remote_base_path', '/uploads')}/result_{{task_id}}.csv"
    )

    return {
        "upload": SFTPUploadTask.create(
            agent=agent,
            local_file=input_file,
            remote_path=remote_input_path,
            sftp_config=sftp_config,
        ),
        "trigger": APITriggerTask.create(
            agent=agent,
            trigger_config=trigger_config,
            sftp_config=sftp_config,
            remote_file_path=remote_input_path,
        ),
        "poll": PollingTask.create(
            agent=agent,
            task_id="{{task_id}}",  # Will be filled by previous task
            polling_config=polling_config,
        ),
        "download": SFTPDownloadTask.create(
            agent=agent,
            remote_path=remote_output_path,
            local_path=output_file,
            sftp_config=sftp_config,
        ),
    }
