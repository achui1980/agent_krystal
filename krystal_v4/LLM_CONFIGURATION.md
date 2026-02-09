# LLM Configuration Guide - Krystal V4

## Overview

Krystal V4 now supports flexible LLM configuration via command-line arguments, allowing you to optimize for cost, accuracy, or speed based on your needs.

## Configuration Methods

### 1. Environment Variable (Baseline)

**File**: `.env` in project root
```bash
OPENAI_API_KEY=sk-your-key-here
```

This is **required** for any LLM usage.

### 2. Command Line Arguments (Recommended)

Control model selection and behavior dynamically:

```bash
python -m krystal_v4.main \
  --rules case/rules.csv \
  --case humanaS10 \
  --count 10 \
  --model gpt-4o-mini \        # Model selection
  --temperature 0.0 \           # Creativity level
  --max-tokens 4096             # Response length limit
```

## Available Options

### `--model` (Default: gpt-4o-mini)

Choose the OpenAI model for all three agents:

| Model | Speed | Cost | Accuracy | Recommended For |
|-------|-------|------|----------|-----------------|
| **gpt-4o-mini** | ⚡⚡⚡ | 💵 | ⭐⭐⭐ | Development, testing, most use cases |
| **gpt-4o** | ⚡⚡ | 💵💵💵 | ⭐⭐⭐⭐ | Production, complex rules |
| **gpt-4-turbo** | ⚡ | 💵💵💵💵💵 | ⭐⭐⭐⭐⭐ | Maximum accuracy (rarely needed) |
| **gpt-3.5-turbo** | ⚡⚡⚡ | 💵 | ⭐⭐ | Basic testing only (lower accuracy) |

### `--temperature` (Default: 0.0)

Controls randomness and creativity:

| Value | Behavior | Use Case |
|-------|----------|----------|
| **0.0** (default) | Deterministic, consistent | Rule parsing (Agent 1) |
| **0.1-0.3** | Slight variation | More varied test data patterns |
| **0.5-0.7** | Creative | Exploring edge cases |
| **1.0+** | Very random | Not recommended (may introduce errors) |

**Important**: Keep at 0.0 for rule analysis to ensure consistent transformation detection.

### `--max-tokens` (Default: 4096)

Maximum tokens in LLM response:
- **4096** (default): Handles most cases comfortably
- **8192**: For very large rule sets (200+ rules)
- **2048**: For smaller rule sets to reduce cost

## Cost Analysis

### Typical Run (humanaS10 with 101 rules)

**Input tokens**: ~10,000-15,000 (rules.csv + prompts)  
**Output tokens**: ~3,000-5,000 (rule_config.json + data generation)

| Model | Cost per Run | Monthly Cost (100 runs) |
|-------|--------------|------------------------|
| gpt-4o-mini | **$0.05-0.10** | **$5-10** |
| gpt-4o | **$0.50-1.00** | **$50-100** |
| gpt-4-turbo | **$2.00-4.00** | **$200-400** |

### Cost Optimization Strategies

1. **Use gpt-4o-mini by default** - Sufficient for 90% of cases
2. **Cache rule_config.json** - Skip Agent 1 on repeated runs (future feature)
3. **Start small** - Test with `--count 5` before generating hundreds of records
4. **Batch testing** - Generate once, validate, then scale up

## Usage Examples

### Development (Fast & Cheap)
```bash
# Quick validation with minimal cost
python -m krystal_v4.main \
  --rules case/rules.csv \
  --case humanaS10 \
  --count 5 \
  --model gpt-4o-mini
# Cost: ~$0.05, Time: 2-3 min
```

### Production (Higher Accuracy)
```bash
# Generate full test dataset with better accuracy
python -m krystal_v4.main \
  --rules case/rules.csv \
  --case humanaS10 \
  --count 100 \
  --model gpt-4o \
  --temperature 0.0
# Cost: ~$0.50, Time: 3-5 min
```

### Creative Test Data (Varied Patterns)
```bash
# Generate more diverse test data patterns
python -m krystal_v4.main \
  --rules case/rules.csv \
  --case humanaS10 \
  --count 50 \
  --model gpt-4o-mini \
  --temperature 0.3
# Cost: ~$0.08, Time: 2-3 min
```

### Large Rule Set (Complex Integration)
```bash
# Handle 200+ transformation rules
python -m krystal_v4.main \
  --rules case/complex_rules.csv \
  --case enterprise \
  --count 20 \
  --model gpt-4o \
  --max-tokens 8192
# Cost: ~$1.00, Time: 5-8 min
```

## Implementation Details

### Code Location

**CLI Entry Point**: `krystal_v4/main.py`
```python
# Lines 52-73: Argument definitions
parser.add_argument("--model", default="gpt-4o-mini", ...)
parser.add_argument("--temperature", type=float, default=0.0, ...)
parser.add_argument("--max-tokens", type=int, default=4096, ...)

# Lines 78-91: Validation
if not 0.0 <= args.temperature <= 2.0:
    errors.append(...)

# Lines 119-126: LLM initialization
llm = ChatOpenAI(
    model=args.model,
    temperature=args.temperature,
    max_tokens=args.max_tokens,
)
```

**Crew Class**: `krystal_v4/crew.py`
```python
# Line 43-48: LLM parameter passed to all agents
def __init__(self, ..., llm: Optional[Any] = None):
    self.llm = llm
```

**Agent Creation**: `krystal_v4/agents/*.py`
```python
# Each agent receives the same LLM configuration
def create_rule_analyst_agent(llm=None) -> Agent:
    return Agent(..., llm=llm, ...)
```

### Validation Rules

1. **Temperature**: Must be between 0.0 and 2.0
2. **Max Tokens**: Must be at least 100
3. **Model**: Any valid OpenAI model name (no validation, will fail at runtime if invalid)
4. **API Key**: Must be present in environment

## Troubleshooting

### "OPENAI_API_KEY not found"
**Solution**: Create `.env` file with your API key:
```bash
echo "OPENAI_API_KEY=sk-your-key" > .env
```

### "Temperature must be between 0.0 and 2.0"
**Solution**: Use valid temperature range:
```bash
--temperature 0.5  # ✓ Valid
--temperature 3.0  # ✗ Invalid
```

### "Rate limit exceeded"
**Solution**: 
- Use slower model: `--model gpt-4o-mini` (higher rate limits)
- Add delays between runs
- Upgrade OpenAI tier

### High costs
**Solution**:
- Switch to `gpt-4o-mini` (10x cheaper)
- Reduce `--count` value
- Test with smaller rule sets first

## Best Practices

1. **Start with defaults** - The system is optimized for `gpt-4o-mini` + `temperature=0.0`
2. **Test incrementally** - Use `--count 5` first, then scale up
3. **Monitor costs** - Check OpenAI usage dashboard regularly
4. **Keep temperature low** - Agent 1 needs deterministic rule parsing
5. **Use gpt-4o selectively** - Only when gpt-4o-mini fails to parse complex rules correctly

## Future Enhancements

Planned features for cost reduction:
- [ ] Rule config caching (skip Agent 1 if rule_config.json exists)
- [ ] Local model support (Ollama, LLaMA)
- [ ] Prompt optimization (reduce token usage by 30-40%)
- [ ] Selective agent execution (skip unchanged steps)

---

**Last Updated**: 2026-02-09  
**Version**: 1.0.0
