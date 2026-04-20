"""Tests for almanak.gateway.database module.

Tests cover:
- _strip_schema_param URL parsing
"""

import pytest

from almanak.gateway.database import _strip_schema_param


class TestStripSchemaParam:
    def test_no_schema_param(self):
        url = "postgresql://user:pass@host:5432/db"
        clean, schema = _strip_schema_param(url)
        assert clean == url
        assert schema is None

    def test_schema_present(self):
        url = "postgresql://user:pass@host:5432/db?schema=myschema"
        clean, schema = _strip_schema_param(url)
        assert "schema" not in clean
        assert schema == "myschema"
        assert clean == "postgresql://user:pass@host:5432/db"

    def test_multiple_query_params(self):
        url = "postgresql://user:pass@host:5432/db?schema=myschema&sslmode=require"
        clean, schema = _strip_schema_param(url)
        assert schema == "myschema"
        assert "sslmode=require" in clean
        assert "schema" not in clean

    def test_empty_schema(self):
        url = "postgresql://user:pass@host:5432/db?schema="
        clean, schema = _strip_schema_param(url)
        # Empty value is normalized to None
        assert schema is None

    def test_preserves_other_params(self):
        url = "postgresql://user:pass@host:5432/db?timeout=30&schema=public&pool_size=5"
        clean, schema = _strip_schema_param(url)
        assert schema == "public"
        assert "timeout=30" in clean
        assert "pool_size=5" in clean
        assert "schema" not in clean
