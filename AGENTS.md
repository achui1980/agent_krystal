# Krystal Agent Guidelines

This document provides essential commands and guidelines for agents working on the Krystal end-to-end testing framework.

## Versions

- **Krystal v1.0**: Legacy CrewAI-based framework (in `krystal/`)
- **Krystal v2.0**: Hybrid ETL testing framework with simplified execution (in `krystal_v2/`)

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

### Krystal v2.0 Commands

**Run v2.0 E2E test:**
```bash
python -m krystal_v2.cli.main test \
  --input-file test_data_v2/input.csv \
  --expected-file test_data_v2/expected.csv \
  --service local-payment-service \
  --env local \
  --output-dir ./reports_v2
```

**Run with specific service:**
```bash
python -m krystal_v2.cli.main test \
  -i input.csv \
  -e expected.csv \
  -s payment-service \
  --env dev
```

**View version:**
```bash
python -m krystal_v2.cli.main version
```

### Using CLI (v1.0)
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

### v1.0 (Legacy)
```
krystal/
├── agents/          # CrewAI agent definitions
├── tools/           # Custom tool implementations
├── crew/            # Crew orchestration
├── config.py        # Configuration management
├── runner.py        # Test execution logic
└── report.py        # Report generation
```

### v2.0 (Current)
```
krystal_v2/
├── cli/
│   └── main.py              # CLI entry point
├── agents/
│   ├── etl_operator.py      # ETL execution agent
│   ├── result_validator.py  # Result validation agent
│   └── report_writer.py     # Report generation agent
├── crews/
│   └── etl_test_crew.py     # Crew orchestration
├── execution/
│   └── etl_executor.py      # Real ETL execution logic
├── utils/
│   ├── report_generator.py  # Report generation utilities
│   └── retry_decorator.py   # Tenacity retry wrapper
└── templates/
    └── report_template.html # Tech-green HTML template
```

### Shared Components
```
krystal/
└── tools/           # Shared tools (SFTP, API, Polling)
    ├── sftp_client.py
    ├── api_client.py
    └── polling_service.py
```

## Environment Variables Required
- `OPENAI_API_KEY` - Required for CrewAI
- `SFTP_HOST`, `SFTP_USERNAME`, `SFTP_PASSWORD` - For file operations
- `API_TOKEN` - For API authentication

## Local E2E Integration Tests

Krystal supports running real end-to-end tests against local Docker/Podman services (no mocks). This allows you to see CrewAI agents working with real SFTP and API services.

### Integration Test Status ✅

**Completed Features:**
- ✅ Full CrewAI workflow with 5 agents (Data Generator → SFTP → API → Polling → Validation)
- ✅ Proxy support for OpenAI API (HTTPS_PROXY)
- ✅ Local API calls bypass proxy automatically
- ✅ Detailed execution logging for all tools
- ✅ Proper JSON output format from all tools
- ✅ Environment variable loading from both `.env` and `secrets.env`

**Test Results:**
- ✅ SFTP upload/download with local SFTP server (port 2223)
- ✅ API trigger and polling with local API Stub (port 8000)
- ✅ CSV generation with schema/template support
- ✅ Full CrewAI workflow execution (requires valid OpenAI API key)

### Prerequisites

1. **Install Podman** (or Docker)
2. **Prepare secrets.env:**
   ```bash
   cp config/local/secrets.env.example config/local/secrets.env
   # Edit and add your OPENAI_API_KEY
   ```

### Starting Local Services

**Using Podman:**
```bash
cd integration_tests
podman compose up -d
```

**Using Docker:**
```bash
cd integration_tests
docker-compose up -d
```

**Verify services are running:**
```bash
# Check SFTP (port 2223)
telnet localhost 2223

# Check API Stub (port 8000)
curl http://localhost:8000/health
```

### Running Integration Tests

**Run all integration tests:**
```bash
python -m pytest integration_tests/ -v -s --timeout=300
```

**Run specific test:**
```bash
python -m pytest integration_tests/test_real_e2e.py::TestRealEndToEnd::test_crewai_agents_workflow_with_local_services -v -s
```

**Using TestRunner with local environment:**
```bash
python run_tests.py --env local --services local-payment-service --verbose
```

### Stopping Local Services

**Podman:**
```bash
cd integration_tests
podman compose down
```

**Docker:**
```bash
cd integration_tests
docker-compose down
```

### Integration Test Structure

```
integration_tests/
├── docker-compose.yml       # SFTP + API Stub services
├── conftest.py             # pytest fixtures and setup
├── test_real_e2e.py        # Real E2E tests (no mocks)
└── stub/
    ├── api_stub.py         # FastAPI mock service
    ├── Dockerfile          # Container definition
    └── requirements.txt    # Python dependencies
```

### Viewing Agent Logs

Integration tests capture verbose output to log files:
- Log files are saved to the pytest temporary directory
- Each test creates a unique log file: `krystal_e2e_YYYYMMDD_HHMMSS.log`
- Use `-s` flag when running pytest to see real-time output

**Example:**
```bash
python -m pytest integration_tests/ -v -s 2>&1 | tee integration_test.log
```

### Troubleshooting

**SFTP connection refused:**
- Ensure container is running: `podman ps`
- Check logs: `podman logs krystal-sftp`
- Verify port: `netstat -an | grep 2222`

**API Stub not responding:**
- Check health endpoint: `curl http://localhost:8000/health`
- View logs: `podman logs krystal-api-stub`

**CrewAI/OpenAI errors:**
- Verify OPENAI_API_KEY is set in `config/local/secrets.env`
- Check key is valid: `echo $OPENAI_API_KEY`

### Proxy Configuration

If you need to use a proxy to access OpenAI API, configure it in `.env`:

```bash
# .env
HTTPS_PROXY=http://127.0.0.1:2222
```

**Note:** Local API calls (localhost/127.0.0.1) automatically bypass the proxy to avoid interference with local services.

### Environment Variable Loading

Integration tests load environment variables in this order:
1. `.env` file (root directory) - Contains OpenAI API key and proxy settings
2. `config/local/secrets.env` - Contains local SFTP/API configuration

The configuration ensures:
- `.env` takes priority for `OPENAI_API_KEY` and proxy settings
- `secrets.env` provides local service configuration (SFTP_HOST, SFTP_PORT, etc.)
- Placeholder API keys in `secrets.env` are ignored

### Viewing Tool Execution Logs

All tools now print detailed execution information
