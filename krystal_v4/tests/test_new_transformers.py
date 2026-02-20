"""
Unit tests for SubstringTransformer and PhoneParserTransformer.
"""

import pytest
from krystal_v4.transformers.substring_transformer import SubstringTransformer
from krystal_v4.transformers.phone_parser_transformer import PhoneParserTransformer


# ─── SubstringTransformer ────────────────────────────────────────────

class TestSubstringTransformerLeft:
    def test_left_basic(self):
        t = SubstringTransformer({"source_field": "Code", "method": "left", "length": 3})
        assert t.transform({"Code": "ABCDEF"}) == "ABC"

    def test_left_with_strip_chars(self):
        t = SubstringTransformer({
            "source_field": "Phone", "method": "left", "length": 3,
            "strip_chars": "()-. ",
        })
        assert t.transform({"Phone": "(555) 867-5309"}) == "555"

    def test_left_shorter_than_length(self):
        t = SubstringTransformer({"source_field": "Code", "method": "left", "length": 10})
        assert t.transform({"Code": "AB"}) == "AB"

    def test_left_empty_returns_empty(self):
        t = SubstringTransformer({"source_field": "Code", "method": "left", "length": 3})
        assert t.transform({"Code": ""}) == ""

    def test_left_none_returns_empty(self):
        t = SubstringTransformer({"source_field": "Code", "method": "left", "length": 3})
        assert t.transform({"Code": None}) == ""

    def test_left_missing_field_returns_empty(self):
        t = SubstringTransformer({"source_field": "Code", "method": "left", "length": 3})
        assert t.transform({"other": "value"}) == ""


class TestSubstringTransformerRight:
    def test_right_basic(self):
        t = SubstringTransformer({"source_field": "Code", "method": "right", "length": 3})
        assert t.transform({"Code": "ABCDEF"}) == "DEF"

    def test_right_with_strip_chars(self):
        t = SubstringTransformer({
            "source_field": "Phone", "method": "right", "length": 7,
            "strip_chars": "()-. ",
        })
        assert t.transform({"Phone": "(555) 867-5309"}) == "8675309"

    def test_right_shorter_than_length(self):
        t = SubstringTransformer({"source_field": "Code", "method": "right", "length": 10})
        assert t.transform({"Code": "AB"}) == "AB"


class TestSubstringTransformerMid:
    def test_mid_basic(self):
        t = SubstringTransformer({"source_field": "Code", "method": "mid", "length": 3, "start": 2})
        assert t.transform({"Code": "ABCDEF"}) == "CDE"

    def test_mid_default_start(self):
        t = SubstringTransformer({"source_field": "Code", "method": "mid", "length": 2})
        assert t.transform({"Code": "ABCDEF"}) == "AB"


class TestSubstringTransformerValidation:
    def test_missing_source_field(self):
        with pytest.raises(ValueError, match="requires 'source_field'"):
            SubstringTransformer({"method": "left", "length": 3})

    def test_missing_method(self):
        with pytest.raises(ValueError, match="requires 'method'"):
            SubstringTransformer({"source_field": "X", "length": 3})

    def test_invalid_method(self):
        with pytest.raises(ValueError, match="must be 'left', 'right', or 'mid'"):
            SubstringTransformer({"source_field": "X", "method": "top", "length": 3})

    def test_missing_length(self):
        with pytest.raises(ValueError, match="requires 'length'"):
            SubstringTransformer({"source_field": "X", "method": "left"})

    def test_invalid_length(self):
        with pytest.raises(ValueError, match="'length' must be a positive integer"):
            SubstringTransformer({"source_field": "X", "method": "left", "length": -1})

    def test_schema_exists(self):
        schema = SubstringTransformer.schema()
        assert "description" in schema
        assert "config" in schema
        assert "source_field" in schema["config"]
        assert "method" in schema["config"]
        assert "length" in schema["config"]


# ─── PhoneParserTransformer ──────────────────────────────────────────

class TestPhoneParserAreaCode:
    def test_area_code_parentheses(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"Phone": "(555) 867-5309"}) == "555"

    def test_area_code_dashes(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"Phone": "555-867-5309"}) == "555"

    def test_area_code_plain_digits(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"Phone": "5558675309"}) == "555"

    def test_area_code_with_country_code(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"Phone": "1-555-867-5309"}) == "555"

    def test_area_code_empty(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"Phone": ""}) == ""

    def test_area_code_none(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"Phone": None}) == ""

    def test_area_code_missing_field(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "area_code"})
        assert t.transform({"other": "value"}) == ""


class TestPhoneParserPhoneNumber:
    def test_phone_number_basic(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "phone_number"})
        assert t.transform({"Phone": "(555) 867-5309"}) == "8675309"

    def test_phone_number_with_format(self):
        t = PhoneParserTransformer({
            "source_field": "Phone", "part": "phone_number", "format": "nnn-nnnn",
        })
        assert t.transform({"Phone": "(555) 867-5309"}) == "867-5309"

    def test_phone_number_plain_digits(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "phone_number"})
        assert t.transform({"Phone": "5558675309"}) == "8675309"

    def test_phone_number_with_country_code(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "phone_number"})
        assert t.transform({"Phone": "15558675309"}) == "8675309"

    def test_phone_number_empty(self):
        t = PhoneParserTransformer({"source_field": "Phone", "part": "phone_number"})
        assert t.transform({"Phone": ""}) == ""


class TestPhoneParserValidation:
    def test_missing_source_field(self):
        with pytest.raises(ValueError, match="requires 'source_field'"):
            PhoneParserTransformer({"part": "area_code"})

    def test_missing_part(self):
        with pytest.raises(ValueError, match="requires 'part'"):
            PhoneParserTransformer({"source_field": "Phone"})

    def test_invalid_part(self):
        with pytest.raises(ValueError, match="must be 'area_code' or 'phone_number'"):
            PhoneParserTransformer({"source_field": "Phone", "part": "extension"})

    def test_schema_exists(self):
        schema = PhoneParserTransformer.schema()
        assert "description" in schema
        assert "config" in schema
        assert "source_field" in schema["config"]
        assert "part" in schema["config"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
