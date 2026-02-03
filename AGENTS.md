# Krystal Agent Guidelines

Essential commands and guidelines for the Krystal ETL testing framework.

## Versions
- **v1.0**: Legacy CrewAI framework (`krystal/`)
- **v2.0**: Hybrid ETL testing framework (`krystal_v2/`)

## Build, Test & Lint Commands

### Installation
```bash
pip install -r requirements.txt
```

### Running Tests
```bash
# Run all tests
python run_tests.py --env dev

# Run single test
python run_tests.py --env dev --services payment-service

# Validate configuration only
python run_tests.py --env dev --validate-only

# List services
python run_tests.py --env dev --list
```

### Krystal v2.0 Commands
```bash
# Run E2E test (fast mode)
python -m krystal_v2.cli.main test \
  --input-file test_data_v2/input.csv \
  --expected-file test_data_v2/expected.csv \
  --service local-payment-service \
  --env local \
  --mode fast

# Run with CrewAI mode
python -m krystal_v2.cli.main test \
  -i input.csv -e expected.csv -s payment-service --mode crewai

# View version
python -m krystal_v2.cli.main version
```

### Lint & Type Check
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
- **Quotes**: Double quotes for strings, single quotes for dict keys
- **Indentation**: 4 spaces (no tabs)

### Imports (Order)
```python
# 1. Standard library
import os
from typing import Dict, List, Optional

# 2. Third-party
from crewai import Agent, Task, Crew
from pydantic import BaseModel, Field

# 3. Local imports
from krystal.config import ConfigManager
```

### Type Hints
- Always use type hints for function parameters and return values
- Use `Optional[Type]` for nullable values
- Use `Dict[str, Any]` for flexible dictionaries
- Use `List[Type]` for collections

### Naming Conventions
- **Classes**: PascalCase (`KrystalCrew`, `DataGeneratorAgent`)
- **Functions/Methods**: snake_case (`generate_csv`, `run_tests`)
- **Constants**: UPPER_CASE (`DEFAULT_TIMEOUT`, `MAX_RETRIES`)
- **Private methods**: `_validate_config`
- **Variables**: snake_case (`service_config`, `batch_id`)

### Error Handling
```python
try:
    result = api_client._run(endpoint=url)
except requests.exceptions.Timeout:
    return {"success": False, "error": "Request timed out"}
except Exception as e:
    return {"success": False, "error": str(e)}

# Always return structured results
return {"success": True, "data": result, "message": "Completed"}
```

### CrewAI Tools Pattern
```python
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

class ToolInput(BaseModel):
    param: str = Field(description="Parameter description")

class MyTool(BaseTool):
    name: str = "tool_name"
    description: str = "Clear description"
    args_schema: type[BaseModel] = ToolInput
    
    def _run(self, param: str) -> Dict[str, Any]:
        return {"success": True, "result": value}
```

## Project Structure
```
krystal_v2/
├── cli/main.py              # CLI entry point
├── agents/                  # CrewAI agents (etl_operator, result_validator, report_writer)
├── crews/                   # Crew orchestration
├── execution/etl_executor.py # Real ETL execution logic
├── tasks/etl_tasks.py       # CrewAI Task definitions
└── utils/                   # Report generator, retry decorator
```

## Configuration
- Environment configs: `config/{env}/services.yaml`
- Secrets: `config/{env}/secrets.env` (never commit!)
- Use `ConfigManager` to load configurations
- Support multiple environments: local, dev, staging, prod

## Environment Variables
- `OPENAI_API_KEY` - Required for CrewAI
- `SFTP_HOST`, `SFTP_USERNAME`, `SFTP_PASSWORD` - For file operations
- `API_TOKEN` - For API authentication

## Testing Guidelines
- Test configurations in `config/dev/services.yaml`
- Use httpbin.org for API testing examples
- Mock SFTP operations in unit tests
- Always validate YAML syntax before running

### Running Integration Tests
```bash
# Run all integration tests
python -m pytest integration_tests/ -v -s --timeout=300

# Run specific test
python -m pytest integration_tests/test_real_e2e.py::TestRealEndToEnd::test_crewai_agents_workflow_with_local_services -v -s
```

## Documentation
- Use docstrings for all public functions and classes
- Follow Google-style docstrings
- Include type information in docstrings when complex
