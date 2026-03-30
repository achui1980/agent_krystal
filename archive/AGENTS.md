# Agent Guidelines for Krystal Project

This document provides coding guidelines and commands for AI coding agents working on the Krystal ETL Testing Framework.

## Project Overview

**Krystal** is an end-to-end ETL testing agent powered by CrewAI. It has two main components:
- `krystal/` - V1 ETL testing framework with SFTP, API, and polling capabilities
- `krystal_v2/` - V2 autonomous code generation system with semantic knowledge base

**Language:** Python 3.10+  
**Framework:** CrewAI  
**Key Dependencies:** crewai, openai, pandas, faker, pytest

## Build, Test, and Lint Commands

### Running Tests

```bash
# Run all unit tests
python run_unit_tests.py

# Run with verbose output
python run_unit_tests.py -v

# Run specific test by keyword
python run_unit_tests.py -k test_csv_generator

# Run with coverage
python run_unit_tests.py --cov

# Run integration tests (requires environment setup)
python run_tests.py --env dev --validate-only
python run_tests.py --env dev --services payment-service
```

### Running a Single Test File

```bash
# Using pytest directly
python -m pytest tests/test_csv_generator.py -v
python -m pytest tests/test_api_client.py::TestAPIClient::test_get_request -v

# Using run_unit_tests.py
python run_unit_tests.py -k csv_generator
```

### Linting and Formatting

```bash
# Format with Black (line length: 120)
black krystal/ krystal_v2/ --line-length 120

# Check formatting without changes
black --check krystal/ krystal_v2/ --line-length 120

# Lint with Flake8
flake8 krystal/ krystal_v2/ --max-line-length=120 --ignore=E501,W503 --count --statistics

# Type check with MyPy (optional, not enforced)
mypy krystal_v2/case_generator/autonomous/ --ignore-missing-imports
```

### Installing Dependencies

```bash
# Install in development mode
pip install -e .

# Install from requirements
pip install -r requirements.txt
```

## Code Style Guidelines

### Import Organization

Follow this import order with blank lines between groups:

```python
"""Module docstring at the top"""

# 1. Standard library imports
import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

# 2. Third-party imports
import pandas as pd
from crewai import Agent, Task, Crew
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# 3. Local application imports
from krystal.tools.api_client import APIClientTool
from krystal_v2.utils.retry_decorator import retry_on_failure
```

**Rules:**
- Absolute imports preferred over relative imports (except within `krystal_v2/case_generator/autonomous/`)
- Avoid wildcard imports (`from module import *`)
- Group related imports together
- Sort alphabetically within each group

### Formatting

- **Line Length:** 120 characters maximum
- **Indentation:** 4 spaces (no tabs)
- **Quotes:** Double quotes for strings (enforced by Black)
- **Trailing Commas:** Use in multiline collections
- **Blank Lines:** 2 blank lines between top-level functions/classes

### Type Annotations

Use type hints for function signatures and class attributes:

```python
def generate_csv(
    schema: Dict[str, Any],
    output_path: str,
    row_count: int = 100
) -> Dict[str, Any]:
    """Generate CSV file from schema"""
    pass

class ETLConfig(BaseModel):
    service_name: str = Field(description="Service identifier")
    enabled: bool = Field(default=True)
    timeout: Optional[int] = None
```

**Rules:**
- Always annotate function parameters and return types
- Use `Optional[T]` for nullable values
- Use `Dict`, `List`, `Any` from `typing` module
- Pydantic models use `Field()` for validation

### Naming Conventions

- **Classes:** PascalCase (`ETLOperatorAgent`, `APIClientTool`)
- **Functions/Methods:** snake_case (`create_agent`, `run_tests`)
- **Constants:** UPPER_SNAKE_CASE (`MAX_ITERATIONS`, `DEFAULT_TIMEOUT`)
- **Private:** Prefix with underscore (`_internal_method`, `_helper_func`)
- **Files:** snake_case (`api_client.py`, `autonomous_generator_v2.py`)

### Docstrings

Use Google-style docstrings:

```python
def process_etl_task(task_id: str, config: Dict[str, Any]) -> bool:
    """
    Process a single ETL task with retries.

    Args:
        task_id: Unique identifier for the task
        config: Configuration dictionary with SFTP and API settings

    Returns:
        True if task completed successfully, False otherwise

    Raises:
        ConnectionError: If SFTP connection fails after retries
        ValueError: If config is invalid
    """
    pass
```

**Required for:**
- All public functions and methods
- All classes
- Complex private functions

### Error Handling

Always use explicit exception handling:

```python
# Good - specific exceptions
try:
    result = api_client.request(endpoint)
except requests.ConnectionError as e:
    logger.error(f"Connection failed: {e}")
    raise
except requests.Timeout as e:
    logger.warning(f"Request timeout: {e}")
    return None

# Bad - bare except
try:
    result = api_client.request(endpoint)
except:  # Never do this
    pass
```

**Rules:**
- Catch specific exceptions, not `Exception` or bare `except`
- Always log errors before re-raising
- Use `finally` for cleanup operations
- Don't silence exceptions without logging

### Logging

Use structured logging with context:

```python
import logging

logger = logging.getLogger(__name__)

# Good - contextual logging
logger.info(f"Processing task_id={task_id} for service={service_name}")
logger.error(f"API call failed: endpoint={endpoint}, status={status_code}")

# Include emojis for visual clarity (optional but encouraged)
logger.info(f"🌐 API Client Tool executing: {endpoint}")
logger.error(f"❌ Test failed: {error_message}")
```

**Log Levels:**
- `DEBUG`: Detailed diagnostic information
- `INFO`: General informational messages (task start/complete)
- `WARNING`: Unexpected but recoverable issues
- `ERROR`: Errors that prevented operation completion
- `CRITICAL`: System-level failures

## Project-Specific Patterns

### CrewAI Agent Creation

```python
def create_agent(llm=None) -> Agent:
    """Standard pattern for creating agents"""
    return Agent(
        role="Role description",
        goal="What the agent should accomplish",
        backstory="Context and expertise",
        llm=llm,
        tools=[SomeTool(), AnotherTool()],
        verbose=True,
    )
```

### Pydantic Models for Tool Inputs

```python
class MyToolInput(BaseModel):
    """Input schema for MyTool"""
    required_param: str = Field(description="Description of parameter")
    optional_param: int = Field(default=10, description="Optional with default")
```

### Retry Pattern

Use the retry decorator for flaky operations:

```python
from krystal_v2.utils.retry_decorator import retry_on_failure

@retry_on_failure(max_attempts=3, delay=2)
def upload_to_sftp(file_path: str, remote_path: str) -> bool:
    """Upload with automatic retry on failure"""
    pass
```

### Namespace Packages

The following directories do NOT have `__init__.py` files (Python 3.3+ implicit namespace packages):
- `krystal_v2/case_generator/core/`
- `krystal_v2/case_generator/exporters/`
- `krystal_v2/case_generator/handlers/`
- `krystal_v2/case_generator/autonomous/`

**Do not create `__init__.py` in these directories.**

## File References

When discussing code locations, use the format:

```
file_path:line_number
```

Example: "The API client is defined in krystal/tools/api_client.py:35"

## Common Pitfalls

1. **Don't mix V1 and V2 code** - Keep `krystal/` and `krystal_v2/` separate
2. **Don't commit secrets** - Use `.env` for API keys (already in `.gitignore`)
3. **Don't use relative imports in tests** - Always use absolute imports like `from krystal.tools import ...`
4. **Don't skip cleanup** - Use `finally` blocks or context managers for resources
5. **Don't ignore type hints** - MyPy checks are encouraged (not enforced yet)

## Environment Setup

Required environment variables (in `.env`):
```bash
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o  # Optional, defaults to gpt-4o
```

## Testing Best Practices

- Use `pytest.fixture` for setup/teardown
- Mock external services (SFTP, API) in unit tests
- Use `tempfile.TemporaryDirectory()` for file operations
- Test both success and failure cases
- Include docstrings describing what each test validates

## Git Workflow

- Branch: Currently on `v2` branch
- Commit messages: Use Chinese or English, be descriptive
- Don't commit generated files (`generated_autonomous/`, `*.log`, `*REPORT*.md`)

---

**Last Updated:** 2026-02-08  
**Project Version:** 1.0.0
