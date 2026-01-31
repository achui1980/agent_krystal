"""
Polling Service Tools for monitoring task completion
"""

import time
from typing import Dict, Any, List, Optional
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from krystal.tools.api_client import APIClientTool, JSONExtractorTool


class PollStatusInput(BaseModel):
    """Input for polling task status"""

    endpoint: str = Field(description="Status check endpoint URL")
    method: str = Field(default="GET", description="HTTP method")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    task_id: str = Field(description="Task ID to check status for")
    status_extractor: str = Field(
        description="JSONPath to extract status from response"
    )
    success_statuses: List[str] = Field(
        default_factory=lambda: ["completed", "success"],
        description="Status values indicating success",
    )
    failure_statuses: List[str] = Field(
        default_factory=lambda: ["failed", "error"],
        description="Status values indicating failure",
    )
    max_attempts: int = Field(default=30, description="Maximum polling attempts")
    interval: int = Field(default=10, description="Seconds between attempts")


class PollingServiceTool(BaseTool):
    """Tool for polling task status until completion or failure"""

    name: str = "polling_service"
    description: str = """
    Poll an API endpoint to check task status until it completes, fails, or times out.
    Useful for monitoring long-running processes.
    """
    args_schema: type[BaseModel] = PollStatusInput

    def _run(
        self,
        endpoint: str,
        method: str = "GET",
        headers: Dict[str, str] = None,
        task_id: str = "",
        status_extractor: str = "",
        success_statuses: List[str] = None,
        failure_statuses: List[str] = None,
        max_attempts: int = 30,
        interval: int = 10,
    ) -> Dict[str, Any]:
        """
        Poll task status until completion

        Args:
            endpoint: Status check endpoint (can include {{task_id}} placeholder)
            method: HTTP method
            headers: HTTP headers
            task_id: Task ID to check
            status_extractor: JSONPath to extract status from response
            success_statuses: List of status values indicating success
            failure_statuses: List of status values indicating failure
            max_attempts: Maximum number of polling attempts
            interval: Seconds between polling attempts

        Returns:
            Dictionary with polling result
        """
        api_client = APIClientTool()
        json_extractor = JSONExtractorTool()

        # Replace task_id placeholder in endpoint
        endpoint = endpoint.replace("{{task_id}}", task_id)

        success_statuses = success_statuses or ["completed", "success"]
        failure_statuses = failure_statuses or ["failed", "error"]

        headers = headers or {}

        attempt = 0
        last_status = None

        while attempt < max_attempts:
            attempt += 1

            # Make status check request
            response = api_client._run(
                endpoint=endpoint, method=method, headers=headers
            )

            if not response.get("success"):
                # API call failed, retry
                time.sleep(interval)
                continue

            # Extract status from response
            body = response.get("body", {})
            status_result = json_extractor._run(
                json_data=body, json_path=status_extractor
            )

            if not status_result.get("success"):
                # Failed to extract status, retry
                time.sleep(interval)
                continue

            status = str(status_result.get("value", "")).lower()
            last_status = status

            # Check if completed
            if status in [s.lower() for s in success_statuses]:
                return {
                    "success": True,
                    "task_id": task_id,
                    "status": status,
                    "attempts": attempt,
                    "completed": True,
                    "failed": False,
                    "message": f"Task completed successfully after {attempt} attempts",
                    "last_response": body,
                }

            # Check if failed
            if status in [s.lower() for s in failure_statuses]:
                return {
                    "success": False,
                    "task_id": task_id,
                    "status": status,
                    "attempts": attempt,
                    "completed": False,
                    "failed": True,
                    "message": f"Task failed with status: {status}",
                    "last_response": body,
                }

            # Still processing, wait and retry
            time.sleep(interval)

        # Max attempts reached without completion
        return {
            "success": False,
            "task_id": task_id,
            "status": last_status,
            "attempts": attempt,
            "completed": False,
            "failed": False,
            "timed_out": True,
            "message": f"Polling timed out after {max_attempts} attempts",
            "last_response": body if "body" in locals() else None,
        }
