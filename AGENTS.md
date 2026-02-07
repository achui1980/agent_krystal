# Krystal Agent Guidelines

Essential commands and guidelines for the Krystal ETL testing framework.

## Versions
- **v1.0**: Legacy CrewAI framework (`krystal/`)
- **v2.0**: Hybrid ETL testing framework (`krystal_v2/`)

## Build, Test & Lint Commands

### Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Running Tests

#### Unit Tests
```bash
# Run all unit tests
python run_unit_tests.py

# Run with verbose output
python run_unit_tests.py -v

# Run specific test by keyword
python run_unit_tests.py -k test_api_client

# Run with coverage
python run_unit_tests.py --cov

# Run with pytest directly
python -m pytest tests/ -v -s
python -m pytest tests/test_api_client.py -v -s

# Run single test method
python -m pytest tests/test_api_client.py::TestAPIClientTool::test_get_request -v -s
```

#### Integration Tests
```bash
# Run all integration tests
python -m pytest integration_tests/ -v -s --timeout=300

# Run specific integration test
python -m pytest integration_tests/test_real_e2e.py::TestRealEndToEnd::test_crewai_agents_workflow_with_local_services -v -s

# Prerequisites: Start local services first
cd integration_tests && podman compose up -d
```

#### End-to-End Tests (v1.0)
```bash
# Run all enabled services in dev environment
python run_tests.py --env dev

# Run single service test
python run_tests.py --env dev --services payment-service

# Run multiple specific services
python run_tests.py --env dev --services payment-service,invoice-service

# Validate configuration only
python run_tests.py --env dev --validate-only

# List available services
python run_tests.py --env dev --list

# Generate report
python run_tests.py --env dev --report
```

#### Test Case Generation (v2.0)
```bash
# Generate test cases using autonomous agent
python -m krystal_v2.case_generator.autonomous.autonomous_cli \
  --rules case/rules.xlsx \
  --source case/source.csv \
  --expected case/expected.txt \
  --max-iterations 5

# Generate test cases with CLI
python -m krystal_v2.case_generator.cli \
  --rules case/rules.xlsx \
  --sample-source case/source.csv \
  --sample-expected case/expected.txt \
  --count-normal 10 \
  --count-abnormal 5 \
  --count-boundary 3

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
```

### Lint & Type Check
```bash
# Format code
black krystal/ krystal_v2/

# Format specific files
black krystal_v2/case_generator/

# Type checking
mypy krystal/ krystal_v2/

# Linting
flake8 krystal/ krystal_v2/ --max-line-length=120 --ignore=E501,W503
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
- `OPENAI_API_KEY` - Required for CrewAI (mandatory)
- `OPENAI_MODEL` - LLM model to use (default: gpt-4o)
- `SFTP_HOST`, `SFTP_USERNAME`, `SFTP_PASSWORD` - For file operations
- `API_TOKEN` - For API authentication

## Testing Guidelines
- Test configurations in `config/dev/services.yaml`
- Use httpbin.org for API testing examples (see `tests/test_api_client.py`)
- Mock SFTP operations in unit tests
- Always validate YAML syntax before running
- Integration tests require local services (use podman/docker-compose)
- Use `pytest -v -s` for verbose output with print statements
- Use `--timeout=300` for long-running integration tests

## Documentation
- Use docstrings for all public functions and classes
- Follow Google-style docstrings
- Include type information in docstrings when complex
- Document CrewAI agents with clear descriptions and expected inputs
