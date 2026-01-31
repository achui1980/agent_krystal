"""
API Client Tools
"""

import json
import requests
from typing import Dict, Any, Optional
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from jsonpath_ng import parse as jsonpath_parse


class APIRequestInput(BaseModel):
    """Input for API request"""

    endpoint: str = Field(description="API endpoint URL")
    method: str = Field(
        default="GET", description="HTTP method (GET, POST, PUT, DELETE, etc.)"
    )
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    body: Optional[Dict[str, Any]] = Field(
        default=None, description="Request body for POST/PUT"
    )
    params: Optional[Dict[str, str]] = Field(
        default=None, description="Query parameters"
    )
    timeout: int = Field(default=30, description="Request timeout in seconds")


class APIClientTool(BaseTool):
    """Tool for making HTTP API calls"""

    name: str = "api_client"
    description: str = """
    Make HTTP API requests (GET, POST, PUT, DELETE, etc.).
    Returns the full response including status code, headers, and body.
    Supports JSON parsing and error handling.
    """
    args_schema: type[BaseModel] = APIRequestInput

    def _run(
        self,
        endpoint: str,
        method: str = "GET",
        headers: Dict[str, str] = None,
        body: Dict[str, Any] = None,
        params: Dict[str, str] = None,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """
        Make HTTP API request

        Args:
            endpoint: Full URL or path
            method: HTTP method
            headers: HTTP headers
            body: Request body (will be JSON serialized)
            params: Query parameters
            timeout: Request timeout

        Returns:
            Dictionary with response details
        """
        try:
            headers = headers or {}

            # Ensure Content-Type for POST/PUT
            if body and method.upper() in ["POST", "PUT", "PATCH"]:
                if "Content-Type" not in headers:
                    headers["Content-Type"] = "application/json"

            # Make request
            response = requests.request(
                method=method.upper(),
                url=endpoint,
                headers=headers,
                json=body if body else None,
                params=params,
                timeout=timeout,
            )

            # Parse response
            try:
                response_body = response.json()
            except ValueError:
                response_body = response.text

            return {
                "success": 200 <= response.status_code < 300,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": response_body,
                "url": response.url,
                "message": f"API call successful: {response.status_code}",
            }

        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": "Timeout",
                "message": f"Request timed out after {timeout} seconds",
            }
        except requests.exceptions.ConnectionError as e:
            return {
                "success": False,
                "error": "Connection Error",
                "message": f"Failed to connect: {str(e)}",
            }
        except Exception as e:
            return {
                "success": False,
                "error": type(e).__name__,
                "message": f"API call failed: {str(e)}",
            }


class ExtractJSONValueInput(BaseModel):
    """Input for extracting JSON value"""

    json_data: Dict[str, Any] = Field(description="JSON data to extract from")
    json_path: str = Field(description="JSONPath expression (e.g., '$.data.task_id')")


class JSONExtractorTool(BaseTool):
    """Tool for extracting values from JSON using JSONPath"""

    name: str = "json_extractor"
    description: str = """
    Extract values from JSON data using JSONPath expressions.
    Examples:
    - $.data.task_id (extract task_id from data object)
    - $.result.status (extract status from result)
    - $.items[0].id (extract first item's id)
    """
    args_schema: type[BaseModel] = ExtractJSONValueInput

    def _run(self, json_data: Dict[str, Any], json_path: str) -> Dict[str, Any]:
        """
        Extract value from JSON using JSONPath

        Args:
            json_data: JSON data as dictionary
            json_path: JSONPath expression

        Returns:
            Dictionary with extracted value or error
        """
        try:
            jsonpath_expr = jsonpath_parse(json_path)
            matches = [match.value for match in jsonpath_expr.find(json_data)]

            if not matches:
                return {
                    "success": False,
                    "value": None,
                    "message": f"No matches found for path: {json_path}",
                }

            # Return first match if only one, otherwise return list
            value = matches[0] if len(matches) == 1 else matches

            return {
                "success": True,
                "value": value,
                "path": json_path,
                "message": f"Successfully extracted value: {value}",
            }

        except Exception as e:
            return {
                "success": False,
                "value": None,
                "error": str(e),
                "message": f"Failed to extract value: {str(e)}",
            }


class RenderTemplateInput(BaseModel):
    """Input for template rendering"""

    template: str = Field(description="Template string with placeholders")
    variables: Dict[str, Any] = Field(description="Variables to substitute")


class TemplateRenderTool(BaseTool):
    """Tool for rendering string templates with variables"""

    name: str = "template_renderer"
    description: str = """
    Render a template string by substituting variables.
    Supports {{variable}} syntax.
    Example: "Hello {{name}}!" with {"name": "World"} becomes "Hello World!"
    """
    args_schema: type[BaseModel] = RenderTemplateInput

    def _run(self, template: str, variables: Dict[str, Any]) -> str:
        """
        Render template with variables

        Args:
            template: Template string with {{variable}} placeholders
            variables: Dictionary of variables to substitute

        Returns:
            Rendered string
        """
        result = template
        for key, value in variables.items():
            placeholder = f"{{{{{key}}}}}"
            result = result.replace(placeholder, str(value))
        return result
