"""
Unit tests for transformer classes.
"""

import pytest
from krystal_v4.transformers.transformer_registry import TransformerRegistry
from krystal_v4.transformers.fixed_transformer import FixedTransformer
from krystal_v4.transformers.direct_transformer import DirectTransformer
from krystal_v4.transformers.name_parser_transformer import NameParserTransformer
from krystal_v4.transformers.conditional_transformer import ConditionalTransformer
from krystal_v4.transformers.split_transformer import SplitTransformer
from krystal_v4.transformers.empty_transformer import EmptyTransformer


def test_fixed_transformer():
    """Test FixedTransformer returns fixed value"""
    config = {"value": "66,175,206"}
    transformer = FixedTransformer(config)

    result = transformer.transform({"any_field": "any_value"})
    assert result == "66,175,206"


def test_direct_transformer():
    """Test DirectTransformer maps source field"""
    config = {"source_field": "DOB"}
    transformer = DirectTransformer(config)

    result = transformer.transform({"DOB": "1960-01-15", "other": "value"})
    assert result == "1960-01-15"


def test_name_parser_first():
    """Test NameParserTransformer extracts first name"""
    config = {"source_field": "Member", "part": "first"}
    transformer = NameParserTransformer(config)

    result = transformer.transform({"Member": "MOUSE,MICKEY"})
    assert result == "MICKEY"


def test_name_parser_last():
    """Test NameParserTransformer extracts last name"""
    config = {"source_field": "Member", "part": "last"}
    transformer = NameParserTransformer(config)

    result = transformer.transform({"Member": "MOUSE,MICKEY"})
    assert result == "MOUSE"


def test_conditional_transformer():
    """Test ConditionalTransformer applies mappings"""
    config = {
        "source_field": "Product",
        "mappings": {"PDP": "MD", "HAP": "MS", "HUM": "MS"},
        "default": "MA/MAPD",
    }
    transformer = ConditionalTransformer(config)

    assert transformer.transform({"Product": "PDP"}) == "MD"
    assert transformer.transform({"Product": "HAP"}) == "MS"
    assert transformer.transform({"Product": "LPPO"}) == "MA/MAPD"


def test_split_transformer():
    """Test SplitTransformer extracts part"""
    config = {"source_field": "Plan_Name", "delimiter": "-", "index": 0}
    transformer = SplitTransformer(config)

    result = transformer.transform({"Plan_Name": "S5884-197"})
    assert result == "S5884"

    # Test second part
    config2 = {"source_field": "Plan_Name", "delimiter": "-", "index": 1}
    transformer2 = SplitTransformer(config2)
    result2 = transformer2.transform({"Plan_Name": "S5884-197"})
    assert result2 == "197"


def test_empty_transformer():
    """Test EmptyTransformer returns empty string"""
    config = {}
    transformer = EmptyTransformer(config)

    result = transformer.transform({"any": "value"})
    assert result == ""


def test_transformer_registry():
    """Test TransformerRegistry creates correct transformer types"""
    # Test all types
    types_to_test = [
        ("fixed", {"value": "test"}),
        ("direct", {"source_field": "test"}),
        ("name_parser", {"source_field": "test", "part": "first"}),
        ("conditional_map", {"source_field": "test", "mappings": {"a": "b"}}),
        ("split_extract", {"source_field": "test", "delimiter": "-", "index": 0}),
        ("empty", {}),
        ("substring", {"source_field": "test", "method": "left", "length": 3}),
        ("phone_parser", {"source_field": "test", "part": "area_code"}),
    ]

    for trans_type, config in types_to_test:
        transformer = TransformerRegistry.get_transformer(trans_type, config)
        assert transformer is not None


def test_transformer_registry_invalid_type():
    """Test TransformerRegistry raises error for invalid type"""
    with pytest.raises(ValueError, match="Unknown transformation type"):
        TransformerRegistry.get_transformer("invalid_type", {})


def test_transformer_registry_list_types():
    """Test that list_types includes new transformer types"""
    types = TransformerRegistry.list_types()
    assert "substring" in types
    assert "phone_parser" in types
    assert "fixed" in types
    assert "direct" in types


def test_transformer_registry_get_all_schemas():
    """Test get_all_schemas returns schemas for all types"""
    schemas = TransformerRegistry.get_all_schemas()
    assert len(schemas) == 8  # 6 original + 2 new
    for type_name, schema in schemas.items():
        assert "description" in schema
        assert "config" in schema
        assert "examples" in schema


def test_transformer_registry_get_schema_prompt():
    """Test get_schema_prompt returns formatted string"""
    prompt = TransformerRegistry.get_schema_prompt()
    assert "## Available Transformation Types" in prompt
    assert "substring" in prompt
    assert "phone_parser" in prompt
    assert "Pattern Matching Guide" in prompt


def test_transformer_validation_errors():
    """Test transformers raise validation errors"""
    # FixedTransformer without value
    with pytest.raises(ValueError, match="requires 'value'"):
        FixedTransformer({})

    # DirectTransformer without source_field
    with pytest.raises(ValueError, match="requires 'source_field'"):
        DirectTransformer({})

    # NameParserTransformer with invalid part
    with pytest.raises(ValueError, match="must be 'first' or 'last'"):
        NameParserTransformer({"source_field": "test", "part": "invalid"})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
