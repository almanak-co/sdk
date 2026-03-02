"""Tests for centralized secret redaction (almanak.core.redaction)."""

from __future__ import annotations

import logging
import os
from unittest.mock import patch

import pytest

from almanak.core.redaction import (
    RedactingStream,
    RedactionFilter,
    _collect_secrets,
    _partial_reveal,
    install_redaction,
    mask_url,
    redact,
)


# ---------------------------------------------------------------------------
# _partial_reveal tests
# ---------------------------------------------------------------------------


class TestPartialReveal:
    def test_long_secret(self):
        assert _partial_reveal("QuiTw3JuH0VUc8CpUmacvhSIFIsSHuQZ") == "Qu***QZ"

    def test_hex_key(self):
        assert _partial_reveal("0xabcdef1234567890") == "0x***90"

    def test_exactly_5_chars(self):
        assert _partial_reveal("12345") == "12***45"

    def test_exactly_4_chars(self):
        assert _partial_reveal("1234") == "***"

    def test_3_chars(self):
        assert _partial_reveal("abc") == "***"

    def test_1_char(self):
        assert _partial_reveal("a") == "***"

    def test_empty(self):
        assert _partial_reveal("") == "***"


# ---------------------------------------------------------------------------
# _collect_secrets tests
# ---------------------------------------------------------------------------


class TestCollectSecrets:
    def test_collects_explicit_vars(self):
        env = {"ALCHEMY_API_KEY": "my_secret_key_12345"}
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        values = [s[0] for s in secrets]
        assert "my_secret_key_12345" in values

    def test_collects_suffix_pattern_vars(self):
        env = {"MY_CUSTOM_SECRET": "super_secret_value"}
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        values = [s[0] for s in secrets]
        assert "super_secret_value" in values

    def test_ignores_short_values(self):
        env = {"ALCHEMY_API_KEY": "short"}
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        values = [s[0] for s in secrets]
        assert "short" not in values

    def test_ignores_benign_values(self):
        env = {"SOME_TOKEN": "true"}
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        assert len(secrets) == 0

    def test_ignores_non_secret_vars(self):
        env = {"HOME": "/Users/nick", "PATH": "/usr/bin:/usr/local/bin"}
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        assert len(secrets) == 0

    def test_deduplicates_same_value(self):
        env = {
            "ALCHEMY_API_KEY": "same_value_1234",
            "MY_CUSTOM_KEY": "same_value_1234",
        }
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        values = [s[0] for s in secrets]
        assert values.count("same_value_1234") == 1

    def test_sorts_longest_first(self):
        env = {
            "SHORT_KEY": "abcdef",
            "LONG_SECRET": "abcdefghijklmnopqrstuvwxyz",
        }
        with patch.dict(os.environ, env, clear=True):
            secrets = _collect_secrets()
        if len(secrets) >= 2:
            assert len(secrets[0][0]) >= len(secrets[1][0])


# ---------------------------------------------------------------------------
# redact() tests
# ---------------------------------------------------------------------------


class TestRedact:
    def test_redacts_secret_in_message(self):
        env = {"ALCHEMY_API_KEY": "my_alchemy_key_abc123"}
        with patch.dict(os.environ, env, clear=True):
            # Force rebuild patterns
            from almanak.core import redaction
            redaction._rebuild()
            result = redact("Connecting to https://alchemy.com/v2/my_alchemy_key_abc123")
        assert "my_alchemy_key_abc123" not in result
        assert "my***23" in result

    def test_redacts_multiple_secrets(self):
        env = {
            "ALCHEMY_API_KEY": "alchemy_key_value",
            "ENSO_API_KEY": "enso_key_value_123",
        }
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()
            result = redact("Keys: alchemy_key_value and enso_key_value_123")
        assert "alchemy_key_value" not in result
        assert "enso_key_value_123" not in result

    def test_no_secrets_returns_unchanged(self):
        with patch.dict(os.environ, {}, clear=True):
            from almanak.core import redaction
            redaction._rebuild()
            msg = "This is a normal log message"
            assert redact(msg) == msg

    def test_message_without_secret_unchanged(self):
        env = {"ALCHEMY_API_KEY": "secret_key_12345"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()
            msg = "No secrets here"
            assert redact(msg) == msg

    def test_redacts_private_key(self):
        env = {"ALMANAK_PRIVATE_KEY": "deadbeefcafebabe1234567890abcdef"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()
            result = redact("Using key deadbeefcafebabe1234567890abcdef for signing")
        assert "deadbeefcafebabe1234567890abcdef" not in result
        assert "de***ef" in result


# ---------------------------------------------------------------------------
# mask_url() tests
# ---------------------------------------------------------------------------


class TestMaskUrl:
    def test_empty_url(self):
        assert mask_url("") == ""
        assert mask_url(None) is None

    def test_credentials_in_url(self):
        result = mask_url("https://user:password@host.com/path")
        assert "user:password" not in result
        assert "***@host.com" in result

    def test_query_param_api_key(self):
        result = mask_url("https://api.example.com/data?api_key=secret123&format=json")
        assert "secret123" not in result
        assert "api_key=***" in result
        assert "format=json" in result

    def test_long_path_segment(self):
        result = mask_url("https://arb-mainnet.g.alchemy.com/v2/QuiTw3JuH0VUc8CpUmacvhSIFIsSHuQZ")
        assert "QuiTw3JuH0VUc8CpUmacvhSIFIsSHuQZ" not in result
        assert "/***" in result

    def test_short_path_segment_preserved(self):
        result = mask_url("https://api.example.com/v2/data")
        assert result == "https://api.example.com/v2/data"

    def test_combined_with_env_redaction(self):
        env = {"ALCHEMY_API_KEY": "my_test_alchemy_key_xyz"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()
            result = mask_url("https://alchemy.com/v2/my_test_alchemy_key_xyz")
        assert "my_test_alchemy_key_xyz" not in result


# ---------------------------------------------------------------------------
# RedactionFilter tests
# ---------------------------------------------------------------------------


class TestRedactionFilter:
    def test_filters_msg_string(self):
        env = {"ALCHEMY_API_KEY": "filter_test_secret_key"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()

            f = RedactionFilter()
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="URL is https://alchemy.com/filter_test_secret_key",
                args=None,
                exc_info=None,
            )
            f.filter(record)
            assert "filter_test_secret_key" not in record.msg

    def test_filters_dict_args(self):
        env = {"ALCHEMY_API_KEY": "dict_arg_secret_val"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()

            f = RedactionFilter()
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="%(url)s",
                args=None,
                exc_info=None,
            )
            # Set dict args manually after construction (LogRecord __init__
            # doesn't handle raw dict args well in all Python versions).
            record.args = {"url": "https://alchemy.com/dict_arg_secret_val"}
            f.filter(record)
            assert "dict_arg_secret_val" not in record.args["url"]

    def test_filters_tuple_args(self):
        env = {"ALCHEMY_API_KEY": "tuple_arg_secret_v"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()

            f = RedactionFilter()
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="URL: %s port: %d",
                args=("https://alchemy.com/tuple_arg_secret_v", 8080),
                exc_info=None,
            )
            f.filter(record)
            assert "tuple_arg_secret_v" not in record.args[0]
            # Non-string args preserved
            assert record.args[1] == 8080

    def test_always_returns_true(self):
        """Filter should never suppress records, only redact content."""
        f = RedactionFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=None,
            exc_info=None,
        )
        assert f.filter(record) is True


# ---------------------------------------------------------------------------
# RedactingStream tests
# ---------------------------------------------------------------------------


class TestRedactingStream:
    def test_redacts_on_write(self):
        env = {"ALCHEMY_API_KEY": "stream_secret_value"}
        with patch.dict(os.environ, env, clear=True):
            from almanak.core import redaction
            redaction._rebuild()

            class MockStream:
                written = ""
                def write(self, msg):
                    self.written += msg
                    return len(msg)
                def flush(self):
                    pass

            mock = MockStream()
            stream = RedactingStream(mock)
            stream.write("Key is stream_secret_value here")
            assert "stream_secret_value" not in mock.written
            assert "st***ue" in mock.written

    def test_delegates_flush(self):
        class MockStream:
            flushed = False
            def write(self, msg):
                return len(msg)
            def flush(self):
                self.flushed = True

        mock = MockStream()
        stream = RedactingStream(mock)
        stream.flush()
        assert mock.flushed


# ---------------------------------------------------------------------------
# install_redaction() tests
# ---------------------------------------------------------------------------


class TestInstallRedaction:
    def test_installs_filter_on_root_logger(self):
        env = {"ALCHEMY_API_KEY": "install_test_key_1"}
        with patch.dict(os.environ, env, clear=True):
            root = logging.getLogger()
            # Remove any existing RedactionFilter
            root.filters = [f for f in root.filters if not isinstance(f, RedactionFilter)]
            from almanak.core import redaction
            redaction._installed = False

            install_redaction()

            assert any(isinstance(f, RedactionFilter) for f in root.filters)
            # Cleanup
            root.filters = [f for f in root.filters if not isinstance(f, RedactionFilter)]
            redaction._installed = False

    def test_idempotent(self):
        env = {"ALCHEMY_API_KEY": "idem_test_key_val"}
        with patch.dict(os.environ, env, clear=True):
            root = logging.getLogger()
            root.filters = [f for f in root.filters if not isinstance(f, RedactionFilter)]
            from almanak.core import redaction
            redaction._installed = False

            install_redaction()
            install_redaction()
            install_redaction()

            count = sum(1 for f in root.filters if isinstance(f, RedactionFilter))
            assert count == 1
            # Cleanup
            root.filters = [f for f in root.filters if not isinstance(f, RedactionFilter)]
            redaction._installed = False

    def test_disabled_via_env(self):
        env = {"ALMANAK_REDACT_SECRETS": "false", "ALCHEMY_API_KEY": "disabled_key_v"}
        with patch.dict(os.environ, env, clear=True):
            root = logging.getLogger()
            root.filters = [f for f in root.filters if not isinstance(f, RedactionFilter)]
            from almanak.core import redaction
            redaction._installed = False

            install_redaction()

            assert not any(isinstance(f, RedactionFilter) for f in root.filters)
            # Cleanup
            redaction._installed = False
