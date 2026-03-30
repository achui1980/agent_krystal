# Krystal - End-to-End Testing Agent

A powerful end-to-end testing framework built on CrewAI that automates the complete testing workflow: **CSV data generation → SFTP upload → API trigger → Polling → Result validation**.

## Features

- **Multi-Service Support**: Test multiple services sequentially or in parallel
- **Environment Management**: Separate configurations for dev/staging/prod
- **Intelligent Agents**: 5 specialized CrewAI agents working together
- **Flexible Data Generation**: Support for templates, schemas, and custom data
- **Robust Error Handling**: Configurable retry mechanisms with exponential backoff
- **Rich Reporting**: Markdown-based test reports with detailed insights
- **CLI Interface**: Easy-to-use command line interface

## Architecture

Krystal uses **5 specialized agents** orchestrated by CrewAI:

1. **Data Generator** - Creates test data CSV files based on schema definitions
2. **SFTP Operator** - Handles file upload to and download from SFTP servers
3. **API Trigger** - Calls REST APIs to trigger business processes
4. **Polling Monitor** - Monitors task status until completion or failure
5. **Result Validator** - Validates output against expected results

```
┌─────────────────────────────────────────────────────────────┐
│                     Krystal Test Runner                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Data Generator] → [SFTP Operator] → [API Trigger]        │
│       ↓                                                    │
│  [Polling Monitor] → [Result Validator]                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Installation

```bash
# Clone the repository
cd krystal

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create your environment configuration files:

```bash
# Copy root .env (contains OpenAI API key and proxy settings)
cp .env.example .env
# Edit .env with your OpenAI API key

# Development environment
cp config/dev/secrets.env.example config/dev/secrets.env
# Edit config/dev/secrets.env with your SFTP/API credentials

# Local integration test environment (optional)
cp config/local/secrets.env.example config/local/secrets.env
# Edit config/local/secrets.env for local Docker/Podman testing
```

**Environment Variable Priority:**
1. `.env` file (root directory) - OpenAI API key and proxy settings
2. `config/{env}/secrets.env` - SFTP/API configuration and environment-specific settings

**Note:** If you need to use a proxy to access OpenAI API, set `HTTPS_PROXY` in `.env`. Local API calls (localhost/127.0.0.1) automatically bypass the proxy.

### 3. Validate Configuration

```bash
# Validate configuration before running tests
python run_tests.py --env dev --validate-only

# Or list all configured services
python run_tests.py --env dev --list
```

### 4. Run Tests

```bash
# Run all enabled services in dev environment
python run_tests.py --env dev

# Run specific services
python run_tests.py --env staging --services payment-service,invoice-service

# Run with report generation
python run_tests.py --env prod --report
```

### 5. Using CLI

```bash
# List services
krystal --env dev list

# Validate configuration
krystal --env dev validate

# Run tests
krystal --env dev run --services payment-service --report
```

## Project Structure

```
krystal/
├── config/                    # Environment-specific configurations
│   ├── dev/
│   │   ├── services.yaml      # Service test definitions
│   │   └── secrets.env        # Environment variables (not in git)
│   ├── staging/
│   │   ├── services.yaml
│   │   └── secrets.env
│   └── prod/
│       ├── services.yaml
│       └── secrets.env
│
├── krystal/                   # Main package
│   ├── agents/               # CrewAI agents
│   │   ├── data_generator.py
│   │   ├── sftp_operator.py
│   │   ├── api_trigger.py
│   │   ├── polling_monitor.py
│   │   └── result_validator.py
│   ├── tools/                # Custom tools
│   │   ├── csv_generator.py
│   │   ├── sftp_client.py
│   │   ├── api_client.py
│   │   ├── polling_service.py
│   │   └── validator.py
│   ├── crew/                 # Crew definitions
│   │   └── krystal_crew.py   # Main Krystal crew
│   ├── config.py             # Configuration manager
│   ├── runner.py             # Test orchestrator
│   ├── report.py             # Report generator
│   └── cli.py                # CLI interface
│
├── templates/                # Jinja2 templates for data generation
│   ├── payment.csv.j2
│   └── invoice.csv.j2
│
├── reports/                  # Test report output directory
├── requirements.txt          # Python dependencies
├── run_tests.py             # Main entry point
└── README.md                # This file
```

## Configuration Guide

### Service Configuration

Services are defined in `config/{environment}/services.yaml`:

```yaml
services:
  - name: "payment-service"
    description: "Payment processing service E2E test"
    enabled: true
    retry_attempts: 3
    
    data_generation:
      template: "templates/payment.csv.j2"  # Optional Jinja2 template
      output_filename: "payment_test_{timestamp}.csv"
      row_count: 100
      data_schema:
        - name: "order_id"
          type: "uuid"
        - name: "amount"
          type: "float"
          min: 1.00
          max: 10000.00
        - name: "currency"
          type: "enum"
          values: ["USD", "EUR", "CNY"]
    
    upload:
      remote_path: "/uploads/payment/incoming"
    
    trigger:
      type: "api"
      endpoint: "https://api.example.com/v1/payment/process"
      method: "POST"
      headers:
        Authorization: "Bearer {{API_TOKEN}}"
      body_template: |
        {
          "file_path": "{{remote_file_path}}",
          "batch_id": "{{batch_id}}"
        }
      task_id_extractor: "$.data.task_id"
    
    polling:
      enabled: true
      max_attempts: 30
      interval: 10
      status_check:
        endpoint: "https://api.example.com/v1/payment/status/{{task_id}}"
        success_statuses: ["completed", "success"]
        failure_statuses: ["failed", "error"]
    
    validation:
      download_from_sftp: true
      remote_result_path: "/uploads/payment/output/{{batch_id}}_result.csv"
      comparison_mode: "csv"
```

### Environment Variables

Create `config/{environment}/secrets.env`:

```bash
# OpenAI API (Required)
OPENAI_API_KEY=sk-your-key-here
OPENAI_MODEL=gpt-4o

# SFTP Configuration
SFTP_HOST=sftp.example.com
SFTP_PORT=22
SFTP_USERNAME=username
SFTP_PASSWORD=password
# Or use SSH key
SFTP_PRIVATE_KEY=/path/to/key

# API Tokens
API_TOKEN=your-api-token
```

## Data Generation

### Schema Types

Krystal supports various data types:

| Type | Description | Parameters |
|------|-------------|------------|
| `uuid` | UUID v4 | - |
| `string` | Random string | `min_length`, `max_length`, `pattern` |
| `int` | Integer | `min`, `max` |
| `float` | Float | `min`, `max`, `decimals` |
| `datetime` | Date/Time | `format`, `days_offset` |
| `enum` | Enumeration | `values` |
| `boolean` | True/False | - |
| `email` | Email address | `domains` |

### Template Example

```jinja2
# templates/payment.csv.j2
order_id,amount,currency,customer_id
{% for row in data %}{{ row.order_id }},{{ row.amount }},{{ row.currency }},{{ row.customer_id }}
{% endfor %}
```

## API Trigger Variables

The following variables are available in `body_template`:

- `{{remote_file_path}}` - Path to uploaded file on SFTP
- `{{batch_id}}` - Unique batch identifier
- `{{row_count}}` - Number of data rows
- `{{task_id}}` - Task ID from trigger response (for polling)

## Testing Workflow

1. **Generate Test Data**: Creates CSV based on schema/template
2. **Upload to SFTP**: Transfers file to remote server
3. **Trigger Process**: Calls API to start processing
4. **Poll for Completion**: Monitors task status
5. **Validate Results**: Downloads and validates output

## Report Generation

Krystal generates Markdown reports with:

- Executive summary (passed/failed counts)
- Detailed service results
- Error messages and stack traces
- Data comparison details

Example report location: `reports/krystal_report_20260131_143000.md`

## Recent Improvements

### 🎯 Integration Testing Complete (2026-02)

**Major Updates:**

1. **Real E2E Testing with Local Services**
   - Full CrewAI workflow testing against local SFTP (port 2223) and API Stub (port 8000)
   - No mocks - real service interactions
   - 3 core tests passing: SFTP, API, CSV Generation

2. **Proxy Support**
   - Added HTTPS_PROXY support for OpenAI API access
   - Local API calls (localhost/127.0.0.1) automatically bypass proxy
   - Configurable via `.env` file

3. **Enhanced Logging**
   - Detailed tool execution output with emojis and progress indicators
   - Real-time visibility into CrewAI agent activities
   - Example output:
   ```
   📊 CSV Generator Tool 执行:
      输出路径: /tmp/krystal/...
      数据行数: 5
      ✅ 生成成功
   ```

4. **Environment Variable Management**
   - Smart loading: `.env` (OpenAI key + proxy) → `secrets.env` (SFTP/API config)
   - Prevents placeholder API keys from overriding real credentials

5. **Bug Fixes**
   - Fixed `'str' object has no attribute 'get'` error in CrewAI Task context
   - Fixed tool return values to always return dictionaries (not strings)
   - Fixed API client to bypass proxy for localhost calls

## Advanced Usage

### Custom Validation Rules

```yaml
validation:
  validation_rules:
    - field: "processed_amount"
      rule: "not_empty"
    - field: "status"
      rule: "equals"
      reference_field: "expected_status"
    - field: "amount"
      rule: "range"
      min: 0
      max: 100000
```

### Retry Configuration

```yaml
retry_attempts: 5  # Per step, default is 3
```

## Local Integration Testing

Krystal supports real end-to-end testing against local Docker/Podman services with 5 CrewAI agents working together.

### Setup Local Services

```bash
cd integration_tests

# Using Podman
podman compose up -d

# Or using Docker
docker-compose up -d
```

### Run Integration Tests

```bash
# Run all integration tests
python -m pytest integration_tests/ -v -s --timeout=300

# Run specific test
python -m pytest integration_tests/test_real_e2e.py::TestRealEndToEnd::test_crewai_agents_workflow_with_local_services -v -s

# Using TestRunner
python run_tests.py --env local --services local-payment-service --verbose
```

### Integration Test Features

✅ **Completed:**
- Full CrewAI workflow (5 agents: Data Generator → SFTP → API → Polling → Validation)
- Proxy support for OpenAI API (HTTPS_PROXY)
- Local API calls bypass proxy automatically
- Detailed tool execution logging
- Proper JSON output format from all tools

### Troubleshooting

### Common Issues

1. **OpenAI API Key Missing**
   - Set `OPENAI_API_KEY` in `.env` or environment
   - For proxy users: Set `HTTPS_PROXY` in `.env`

2. **SFTP Connection Failed**
   - Check SFTP credentials in `secrets.env`
   - Verify SFTP server is accessible: `telnet localhost 2223`
   - Check container logs: `podman logs krystal-sftp`

3. **API Stub 503 Error**
   - Local API calls should bypass proxy automatically
   - Verify API Stub is running: `curl http://localhost:8000/health`

4. **Template Not Found**
   - Ensure template file exists in `templates/` directory
   - Check path in service configuration

5. **API Trigger Failed**
   - Verify API endpoint is correct
   - Check authentication tokens
   - Review API response format

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

For issues and questions:
- Create an issue in the repository
- Contact the development team

---

**Built with ❤️ using CrewAI**
