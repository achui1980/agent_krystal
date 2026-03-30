# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Krystal is an ETL testing framework powered by CrewAI. The active development focus is **krystal_v4/** — a 3-agent pipeline that reads `rules.csv`, generates test source data, and produces expected output files.

Legacy modules (`krystal/` for V1 SFTP/API testing, `krystal_v2/` for autonomous code generation) exist but are not under active development.

## Common Commands

### Run Krystal V4

```bash
python -m krystal_v4.main --rules case/humanaS10/rules.csv --case humanaS10 --case-dir case/humanaS10 --count 10
python -m krystal_v4.main --rules case/highmark/rules.csv --case highmark --case-dir case/highmark --count 10
# Custom model/temperature
python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10 --model gpt-4o --temperature 0.1
```

### Testing

```bash
# All V4 tests
python -m pytest krystal_v4/tests/ -v

# Single test file
python -m pytest krystal_v4/tests/test_transformers.py -v

# Single test class or method
python -m pytest krystal_v4/tests/test_expected_generator.py::TestValidationGate -v

# With coverage
python -m pytest krystal_v4/tests/ --cov=krystal_v4
```

### Linting

```bash
black krystal_v4/ --line-length 120
black --check krystal_v4/ --line-length 120
flake8 krystal_v4/ --max-line-length=120 --ignore=E501,W503
```

### Install

```bash
pip install -r requirements.txt
pip install -e .
```

## Architecture (krystal_v4)

### Three-Agent Sequential Pipeline

```
rules.csv + reference files
        │
        ▼
  pre_parse_rules()          ← deterministic extraction (crew.py)
  (target_fields, source_fields, fixed_values, special_rules_rows, source_format)
        │
        ▼
┌─ Agent 1: Rule Analyst ─┐
│  LLM analyzes SPECIAL_RULES column             │
│  Constrained by TransformerRegistry.get_schema_prompt()  │
│  Output: rule_config.json                       │
└──────────────────────────┘
        │
        ▼
┌─ Agent 2: Source Generator ─┐
│  Faker-based data with conditional coverage     │
│  MS product special handling (empty Plan_Name)  │
│  Output: generated_source.txt                   │
└─────────────────────────────┘
        │
        ▼
┌─ Agent 3: Expected Generator ─┐
│  ONE deterministic call to ExpectedGeneratorTool│
│  Validates configs → applies transformers → writes output │
│  Output: generated_expected.txt                 │
└────────────────────────────────┘
```

### Row Classification in rules.csv

- **Type A** (has `CARRIER_COLUMN_NAME`): becomes a `transformation_rule` — LLM determines the transformer type from `SPECIAL_RULES` text
- **Type B** (no `CARRIER_COLUMN_NAME`, has `DEFAULT`): becomes a `fixed_value` — deterministically extracted, no LLM needed

### Transformer System

Factory pattern in `transformers/transformer_registry.py`. Each transformer has:
- `schema()` classmethod — self-describing config format (used in LLM prompts)
- `validate_config()` — raises ValueError on bad config
- `transform(source_record)` — applies the transformation

8 registered types: `fixed`, `direct`, `name_parser`, `conditional_map`, `split_extract`, `empty`, `substring`, `phone_parser`

`TransformerRegistry.get_schema_prompt()` auto-generates the exact config schema and pattern-matching guide that gets embedded into the LLM task prompt. This constrains LLM output to only valid transformer configs.

### Validation Gate

Before processing any records, `ExpectedGeneratorTool` calls `validate_rule_config_against_transformers()` which attempts to instantiate every transformer from the config. If any fail, execution halts and writes `transformation_errors.json`.

### Source Format Detection

`pre_parse_rules()` detects source format from reference files: checks `source.csv` first, falls back to `source.txt`. Supports `csv_quoted`, `pipe`, and `comma` formats.

### Output Structure

```
output/{case_name}/
├── rule_config.json           # LLM-generated transformation config
├── generated_source.txt       # Test source data
├── generated_expected.txt     # Pipe-delimited expected output with metadata header
└── transformation_errors.json # Only created when errors occur
```

The expected output has a metadata header (ACTION_ID, SERVICE_MAP_ID, SOURCE_TOKEN), followed by pipe-delimited header and data rows.

## Code Style

- **Line length**: 120 chars (Black enforced)
- **Quotes**: Double quotes
- **Imports**: Absolute imports, grouped as stdlib → third-party → local
- **Naming**: PascalCase classes, snake_case functions/files, UPPER_SNAKE_CASE constants
- **Docstrings**: Google-style with Args/Returns/Raises
- **Type hints**: Required on function signatures

## Key Conventions

- `pre_parse_rules()` in `crew.py` deterministically extracts everything it can from rules.csv before any LLM involvement — this data is passed to agents as immutable context
- CrewAI tools use Pydantic `BaseModel` for input validation (`args_schema`)
- The `ExpectedGeneratorTool` is the only tool in Agent 3 — it does everything in one deterministic call with no LLM involvement
- All transformers must inherit from `BaseTransformer` and implement `schema()`, `validate_config()`, and `transform()`
- To add a new transformer: create the file, implement the 3 methods, register in `TransformerRegistry._TRANSFORMER_MAP` — the schema prompt auto-updates
- Don't mix V1 (`krystal/`), V2 (`krystal_v2/`), and V4 (`krystal_v4/`) code
- Namespace packages (no `__init__.py`): `krystal_v2/case_generator/core/`, `exporters/`, `handlers/`, `autonomous/`

## Environment

Requires `OPENAI_API_KEY` in `.env` or environment. Copy `.env.example` to `.env` and fill in the key.
