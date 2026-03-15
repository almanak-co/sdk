"""Unit tests for schema formatter helpers in almanak.framework.cli.ax."""

from almanak.framework.cli.ax import _format_type, _resolve_ref


class TestResolveRef:
    def test_valid_ref(self):
        defs = {"MyEnum": {"type": "string", "enum": ["a", "b"]}}
        result = _resolve_ref({"$ref": "#/$defs/MyEnum"}, defs)
        assert result == {"type": "string", "enum": ["a", "b"]}

    def test_missing_ref(self):
        result = _resolve_ref({"$ref": "#/$defs/Missing"}, {})
        assert result is None

    def test_malformed_ref(self):
        result = _resolve_ref({"$ref": "other/path"}, {"path": {"type": "string"}})
        assert result is None

    def test_no_ref_key(self):
        result = _resolve_ref({"type": "string"}, {})
        assert result is None


class TestFormatType:
    def test_simple_type(self):
        assert _format_type({"type": "string"}) == "string"

    def test_missing_type_defaults_to_any(self):
        assert _format_type({}) == "any"

    def test_enum_values(self):
        result = _format_type({"type": "string", "enum": ["rsi", "sma", "ema"]})
        assert result == "string ('rsi' | 'sma' | 'ema')"

    def test_array_simple(self):
        result = _format_type({"type": "array", "items": {"type": "string"}})
        assert result == "list[string]"

    def test_array_no_items(self):
        result = _format_type({"type": "array"})
        assert result == "list[any]"

    def test_anyof_union(self):
        schema = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        assert _format_type(schema) == "string | integer"

    def test_anyof_nullable(self):
        schema = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        assert _format_type(schema) == "string"

    def test_anyof_deduplication(self):
        schema = {"anyOf": [{"type": "string"}, {"type": "string"}]}
        assert _format_type(schema) == "string"

    def test_anyof_with_ref(self):
        defs = {"Status": {"type": "string", "enum": ["active", "done"]}}
        schema = {"anyOf": [{"$ref": "#/$defs/Status"}, {"type": "null"}]}
        result = _format_type(schema, defs)
        assert result == "string ('active' | 'done')"

    def test_ref_resolved(self):
        defs = {"MyType": {"type": "integer"}}
        result = _format_type({"$ref": "#/$defs/MyType"}, defs)
        assert result == "integer"

    def test_ref_unresolved(self):
        result = _format_type({"$ref": "#/$defs/Missing"}, {})
        assert result == "any"

    def test_array_with_enum_items(self):
        defs = {"Color": {"type": "string", "enum": ["red", "blue"]}}
        schema = {"type": "array", "items": {"$ref": "#/$defs/Color"}}
        result = _format_type(schema, defs)
        assert result == "list[string ('red' | 'blue')]"

    def test_null_type(self):
        assert _format_type({"type": "null"}) == "null"
