# Krystal Agent Guidelines

This document provides essential commands and guidelines for agents working on the Krystal end-to-end testing framework.

## Build, Test & Lint Commands

### Installation
```bash
pip install -r requirements.txt
```

### Running Tests

**Run all tests:**
```bash
python run_tests.py --env dev
```

**Run specific service test:**
```bash
python run_tests.py --env dev --services payment-service
```

**Validate configuration only:**
```bash
python run_tests.py --env dev --validate-only
```

**List configured services:**
```bash
python run_tests.py --env dev --list
```

### Using CLI
```bash
# Run tests with report
krystal --env dev run --services payment-service --report

# Validate config
krystal --env dev validate

# List services
krystal --env dev list
```

### Lint & Type Check (if available)
```bash
# Format code
black krystal/

# Type checking
mypy krystal/

# Linting
flake8 krystal/ --max-line-length=120 --ignore=E501,W503
```

## Code Style Guidelines

### Python Style
- **Line length**: 120 characters maximum
- **Quotes**: Use double quotes for strings, single quotes for dict keys
- **Indentation**: 4 spaces (no tabs)

### Imports
```python
# Standard library first
import os
import sys
from typing import Dict, List, Optional

# Third-party packages
from crewai import Agent, Task, Crew
from pydantic import BaseModel, Field

# Local imports last
from krystal.config import ConfigManager
from krystal.tools.csv_generator import CSVGeneratorTool
```

### Type Hints
- Always use type hints for function parameters and return values
- Use `Optional[Type]` for nullable values
- Use `Dict[str, Any]` for flexible dictionaries
- Use `List[Type]` for collections

### Naming Conventions
- **Classes**: PascalCase (e.g., `KrystalCrew`, `DataGeneratorAgent`)
- **Functions/Methods**: snake_case (e.g., `generate_csv`, `run_tests`)
- **Constants**: UPPER_CASE (e.g., `DEFAULT_TIMEOUT`, `MAX_RETRIES`)
- **Private methods**: prefix with underscore (e.g., `_validate_config`)
- **Variables**: snake_case, descriptive names (e.g., `service_config`, `batch_id`)

### Error Handling
```python
# Use try-except with specific exceptions
try:
    result = api_client._run(endpoint=url)
except requests.exceptions.Timeout:
    return {"success": False, "error": "Request timed out"}
except Exception as e:
    return {"success": False, "error": str(e)}

# Always return structured results
return {
    "success": True,
    "data": result,
    "message": "Operation completed"
}
```

### CrewAI Tools Pattern
```python
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

class ToolInput(BaseModel):
    param: str = Field(description="Parameter description")

class MyTool(BaseTool):
    name: str = "tool_name"
    description: str = "Clear description of what this tool does"
    args_schema: type[BaseModel] = ToolInput
    
    def _run(self, param: str) -> Dict[str, Any]:
        # Implementation
        return {"success": True, "result": value}
```

### Configuration
- Environment configs in `config/{env}/services.yaml`
- Secrets in `config/{env}/secrets.env` (never commit!)
- Use `ConfigManager` to load configurations
- Support multiple environments: dev, staging, prod

### Documentation
- Use docstrings for all public functions and classes
- Follow Google-style docstrings
- Include type information in docstrings when complex

### Testing
- Test configurations should be in `config/dev/services.yaml`
- Use httpbin.org for API testing examples
- Mock SFTP operations in unit tests
- Always validate YAML syntax before running

## Project Structure
```
krystal/
├── agents/          # CrewAI agent definitions
├── tools/           # Custom tool implementations
├── crew/            # Crew orchestration
├── config.py        # Configuration management
├── runner.py        # Test execution logic
└── report.py        # Report generation
```

## Environment Variables Required
- `OPENAI_API_KEY` - Required for CrewAI
- `SFTP_HOST`, `SFTP_USERNAME`, `SFTP_PASSWORD` - For file operations
- `API_TOKEN` - For API authentication
