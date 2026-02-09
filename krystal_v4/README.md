# Krystal V4 - ETL Test Data Generator

Krystal V4 is a CrewAI-powered ETL test data generator that reads transformation rules from CSV files and automatically generates test source data and expected output data for data integration validation.

## Features

- **Rule Analysis**: Intelligently parses CSV rule files to identify transformation types
- **Intelligent Data Generation**: Uses Faker library to generate realistic test data
- **Format Detection**: Auto-detects source file format (CSV with quotes vs pipe-delimited)
- **6 Transformation Types**: 
  - Fixed values
  - Direct field mapping
  - Conditional mapping
  - Name parsing ("LAST,FIRST")
  - String splitting
  - Empty values
- **CrewAI Orchestration**: Three-agent pipeline for systematic data generation

## Installation

```bash
# Navigate to krystal_v4 directory
cd krystal_v4

# Install dependencies
pip install -r requirements.txt

# Set OpenAI API key in .env file
echo "OPENAI_API_KEY=sk-your-key-here" > .env
```

## Usage

### Basic Usage

```bash
python -m krystal_v4.main \
  --rules case/rules.csv \
  --case humanaS10 \
  --count 10
```

### Command Line Arguments

- `--rules` (required): Path to rules.csv file
- `--case` (required): Test case name/identifier
- `--case-dir` (optional): Directory containing reference files (default: case/{case_name})
- `--count` (optional): Number of records to generate (default: 10)
- `--verbose` (optional): Enable verbose logging

**LLM Configuration:**
- `--model` (optional): OpenAI model to use (default: gpt-4o-mini)
  - Options: `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo`, `gpt-3.5-turbo`
  - Trade-off: gpt-4o = higher accuracy but 10x cost, gpt-4o-mini = faster and cheaper
- `--temperature` (optional): LLM temperature for creativity (0.0-2.0, default: 0.0)
  - 0.0 = deterministic output (recommended for rule parsing)
  - 0.3-0.7 = more creative data generation
  - 1.0+ = very creative (may introduce errors)
- `--max-tokens` (optional): Maximum tokens for LLM response (default: 4096)

### Examples

```bash
# Generate 10 records for humanaS10 case (using default gpt-4o-mini)
python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10

# Use GPT-4o for better accuracy (higher cost ~$0.50 vs $0.05)
python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10 --model gpt-4o

# Increase creativity for more varied test data
python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10 --temperature 0.3

# Generate 20 records for highmark case with all custom options
python -m krystal_v4.main --rules case/rules.csv --case highmark --case-dir case/highmark --count 20 --model gpt-4o --temperature 0.1
```

## Input Format

### rules.csv Structure

The rules CSV file should have the following columns:

- `CSDS_FLAG`: 0 or 1 (indicates if field should be in output)
- `CS_COLUMN_NAME`: Target field name (output column)
- `CARRIER_COLUMN_NAME`: Source field name (input column)
- `DEFAULT`: Default/fixed value for the field
- `SPECIAL_RULES`: Natural language description of transformation logic
- `NOTE`: Optional notes

### Example Rules

```csv
CSDS_FLAG,CS_COLUMN_NAME,CARRIER_COLUMN_NAME,DEFAULT,SPECIAL_RULES,NOTE
0,CARRIER_FAMILY_ID,,"66,175,206",,Fixed value
1,FIRST_NAME,Member,,"Member format: last_name, first_name",Parse first name
1,PRODUCT_LINE,Product,,"if 'PDP' map to MD; if 'HAP' map to MS",Conditional mapping
1,CMS_CONTRACT_ID,Plan_Name,,"Extract contract ID before hyphen",Split by dash
```

## Output Files

Three files are generated in `output/{case_name}/`:

1. **rule_config.json**: Structured configuration with transformation rules
2. **generated_source.txt**: Test source data in detected format
3. **generated_expected.txt**: Expected output in pipe-delimited format with metadata

### Example Output (generated_expected.txt)

```
ACTION_ID:humanaS10-cs-data-integration
SERVICE_MAP_ID:10003358
SOURCE_TOKEN:generated_20260209_143045

FIRST_NAME|LAST_NAME|PRODUCT_LINE|DOB|CMS_CONTRACT_ID
MICKEY|MOUSE|MD|1960-01-15|S5884
DONALD|DUCK|MS|1958-06-09|S5885
```

## Architecture

### Three-Agent Pipeline

1. **Rule Analyst Agent**
   - Reads rules.csv
   - Identifies transformation types
   - Detects source format
   - Generates rule_config.json

2. **Source Generator Agent**
   - Reads rule_config.json
   - Generates realistic test data
   - Writes generated_source.txt

3. **Expected Generator Agent**
   - Reads source data and config
   - Applies transformations
   - Writes generated_expected.txt

### Transformation Types

| Type | Description | Example |
|------|-------------|---------|
| `fixed` | Returns constant value | "66,175,206" |
| `direct` | Direct field mapping | DOB → DATE_OF_BIRTH |
| `conditional_map` | If-then value mapping | PDP → MD, HAP → MS |
| `name_parser` | Extract from "LAST,FIRST" | "MOUSE,MICKEY" → "MICKEY" |
| `split_extract` | Split and extract part | "S5884-197" → "S5884" |
| `empty` | Returns empty string | "" |

## LLM Model Selection Guide

### Cost and Performance Comparison

| Model | Input Cost | Output Cost | Best For | Estimated Cost per Run* |
|-------|-----------|-------------|----------|------------------------|
| **gpt-4o-mini** (default) | $0.15/1M | $0.60/1M | Daily testing, rapid iteration | **$0.05-0.10** |
| **gpt-4o** | $2.50/1M | $10.00/1M | Production, complex rules | **$0.50-1.00** |
| **gpt-4-turbo** | $10.00/1M | $30.00/1M | Maximum accuracy needed | **$2.00-4.00** |

*Estimated for humanaS10 case with 101 rules (typical run: ~10-15K input tokens, ~3-5K output tokens)

### Recommendations

**Development & Testing (Recommended: gpt-4o-mini)**
```bash
python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 5
# Fast execution (2-3 min), low cost ($0.05-0.10), good accuracy for most cases
```

**Production & Complex Rules (Recommended: gpt-4o)**
```bash
python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 100 --model gpt-4o
# Better rule parsing accuracy, handles ambiguous SPECIAL_RULES better
# Trade-off: 10x cost but 20-30% better accuracy on edge cases
```

**Cost Optimization Tips:**
1. Use `gpt-4o-mini` for development and rule testing
2. Upgrade to `gpt-4o` only if you see rule parsing errors
3. Keep `--temperature 0.0` (default) for deterministic rule analysis
4. Increase `--temperature 0.3-0.5` only if you need more varied test data patterns

## Testing

```bash
# Run all tests
python -m pytest krystal_v4/tests/ -v

# Run transformer tests
python -m pytest krystal_v4/tests/test_transformers.py -v

# Run tools tests
python -m pytest krystal_v4/tests/test_tools.py -v
```

## Project Structure

```
krystal_v4/
├── agents/                 # CrewAI agent definitions
│   ├── rule_analyst_agent.py
│   ├── source_generator_agent.py
│   └── expected_generator_agent.py
├── tasks/                  # Task definitions
│   └── task_definitions.py
├── tools/                  # CrewAI tools
│   ├── format_detector_tool.py
│   ├── csv_reader_tool.py
│   ├── data_generator_tool.py
│   ├── file_writer_tool.py
│   ├── transformation_executor_tool.py
│   └── json_writer_tool.py
├── transformers/           # Transformation engine
│   ├── base.py
│   ├── fixed_transformer.py
│   ├── direct_transformer.py
│   ├── name_parser_transformer.py
│   ├── conditional_transformer.py
│   ├── split_transformer.py
│   ├── empty_transformer.py
│   └── transformer_registry.py
├── models/                 # Pydantic models
│   └── rule_config.py
├── utils/                  # Utilities
│   ├── logger.py
│   ├── error_handler.py
│   ├── file_helper.py
│   └── validation.py
├── tests/                  # Unit tests
│   ├── test_transformers.py
│   └── test_tools.py
├── crew.py                 # Main crew orchestration
├── main.py                 # CLI entry point
└── requirements.txt        # Dependencies
```

## Requirements

- Python 3.10+
- CrewAI 1.9.3+
- OpenAI API key
- Faker 22.0+
- Pandas 2.0+

## License

Internal use only.

## Version

1.0.0 - Initial release (2026-02-09)
