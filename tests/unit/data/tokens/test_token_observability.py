"""Tests for token resolution observability (structured logging and metrics emission)."""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.tokens.resolver import TokenResolver, _try_record_metric


class TestStructuredLogging:
    """Tests for structured log events emitted by TokenResolver."""

    def setup_method(self):
        """Reset singleton and use temp cache to avoid disk cache interference."""
        TokenResolver.reset_instance()
        self._temp_dir = tempfile.mkdtemp()
        self._cache_file = str(Path(self._temp_dir) / "test_cache.json")

    def teardown_method(self):
        """Reset singleton after each test."""
        TokenResolver.reset_instance()

    def _resolver(self):
        """Create a resolver with fresh temp cache."""
        return TokenResolver(cache_file=self._cache_file)

    def test_resolve_emits_token_resolved_log(self, caplog):
        """Successful resolution emits 'token_resolved' log with structured fields."""
        resolver = self._resolver()
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.tokens.resolver"):
            resolver.resolve("USDC", "arbitrum")

        resolved_logs = [r for r in caplog.records if "token_resolved" in r.getMessage()]
        assert len(resolved_logs) >= 1

    def test_resolve_log_has_structured_fields(self, caplog):
        """Resolution log includes token, chain, resolution_source, latency_ms in extra."""
        resolver = self._resolver()
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.tokens.resolver"):
            resolver.resolve("WETH", "ethereum")

        resolved_logs = [r for r in caplog.records if "token_resolved" in r.getMessage()]
        assert len(resolved_logs) >= 1
        log = resolved_logs[0]
        assert log.token == "WETH"
        assert log.chain == "ethereum"
        assert log.resolution_source in ("cache", "static")
        assert isinstance(log.latency_ms, float)
        assert log.latency_ms >= 0

    def test_cache_hit_emits_debug_log(self, caplog):
        """Cache hit emits 'token_cache_hit' debug log."""
        resolver = self._resolver()
        # First call populates cache
        resolver.resolve("USDC", "arbitrum")

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.tokens.resolver"):
            resolver.resolve("USDC", "arbitrum")

        cache_hit_logs = [r for r in caplog.records if "token_cache_hit" in r.getMessage()]
        assert len(cache_hit_logs) >= 1
        log = cache_hit_logs[0]
        assert log.token == "USDC"
        assert log.chain == "arbitrum"
        assert log.cache_type == "memory"

    def test_static_hit_emits_debug_log(self, caplog):
        """First resolution (static hit) emits 'token_cache_miss' log with resolved_via=static."""
        resolver = self._resolver()
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.tokens.resolver"):
            resolver.resolve("DAI", "ethereum")

        # First resolve should hit static registry (not cache)
        miss_logs = [r for r in caplog.records if "token_cache_miss" in r.getMessage()]
        assert len(miss_logs) >= 1
        log = miss_logs[0]
        assert log.token == "DAI"
        assert log.chain == "ethereum"
        assert log.resolved_via == "static"

    def test_error_emits_warning_log(self, caplog):
        """Resolution error emits 'token_resolution_error' warning."""
        resolver = self._resolver()
        with caplog.at_level(logging.WARNING, logger="almanak.framework.data.tokens.resolver"):
            with pytest.raises(Exception):
                resolver.resolve("NONEXISTENT_TOKEN_XYZ", "arbitrum")

        error_logs = [r for r in caplog.records if "token_resolution_error" in r.getMessage()]
        assert len(error_logs) >= 1
        log = error_logs[0]
        assert log.token == "NONEXISTENT_TOKEN_XYZ"
        assert log.chain == "arbitrum"
        assert log.error_type == "TokenNotFoundError"
        assert isinstance(log.latency_ms, float)

    def test_alias_resolution_emits_debug_log(self, caplog):
        """Alias resolution emits 'token_alias_resolved' debug log when symbol only in aliases."""
        from almanak.framework.data.tokens.defaults import SYMBOL_ALIASES

        resolver = self._resolver()
        # Remove USDC.E from static registry so the alias path is triggered
        if "arbitrum" in resolver._static_registry:
            resolver._static_registry["arbitrum"].pop("USDC.E", None)

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.data.tokens.resolver"):
            resolver.resolve("USDC.e", "arbitrum")

        alias_logs = [r for r in caplog.records if "token_alias_resolved" in r.getMessage()]
        assert len(alias_logs) >= 1
        log = alias_logs[0]
        assert log.token == "USDC.e"
        assert log.chain == "arbitrum"
        assert hasattr(log, "alias_address")


class TestMetricsEmission:
    """Tests for Prometheus metrics emission from TokenResolver."""

    def setup_method(self):
        """Reset singleton and use temp cache."""
        TokenResolver.reset_instance()
        self._temp_dir = tempfile.mkdtemp()
        self._cache_file = str(Path(self._temp_dir) / "test_cache.json")

    def teardown_method(self):
        """Reset singleton after each test."""
        TokenResolver.reset_instance()

    @patch("almanak.framework.data.tokens.resolver._try_record_metric")
    def test_resolve_emits_latency_metric(self, mock_metric):
        """Successful resolution emits latency metric."""
        resolver = TokenResolver(cache_file=self._cache_file)
        resolver.resolve("USDC", "arbitrum")

        latency_calls = [
            c for c in mock_metric.call_args_list if c[0][0] == "record_token_resolution_latency"
        ]
        assert len(latency_calls) >= 1
        call = latency_calls[0]
        assert call[0][1] == "arbitrum"  # chain
        assert call[0][2] in ("cache", "static")  # source
        assert isinstance(call[0][3], float)  # duration

    @patch("almanak.framework.data.tokens.resolver._try_record_metric")
    def test_cache_hit_emits_cache_hit_metric(self, mock_metric):
        """Cache hit emits cache_hit metric."""
        resolver = TokenResolver(cache_file=self._cache_file)
        resolver.resolve("USDC", "arbitrum")  # First call (static hit)
        mock_metric.reset_mock()

        resolver.resolve("USDC", "arbitrum")  # Second call (cache hit)

        cache_hit_calls = [
            c for c in mock_metric.call_args_list if c[0][0] == "record_token_resolution_cache_hit"
        ]
        assert len(cache_hit_calls) >= 1
        call = cache_hit_calls[0]
        assert call[0][1] == "arbitrum"  # chain
        assert call[0][2] == "memory"  # cache_type

    @patch("almanak.framework.data.tokens.resolver._try_record_metric")
    def test_static_hit_emits_static_metric(self, mock_metric):
        """Static registry hit emits cache_hit metric with 'static' type."""
        resolver = TokenResolver(cache_file=self._cache_file)
        resolver.resolve("WBTC", "ethereum")

        static_calls = [
            c
            for c in mock_metric.call_args_list
            if c[0][0] == "record_token_resolution_cache_hit" and len(c[0]) > 2 and c[0][2] == "static"
        ]
        assert len(static_calls) >= 1

    @patch("almanak.framework.data.tokens.resolver._try_record_metric")
    def test_error_emits_error_metric(self, mock_metric):
        """Resolution error emits error metric."""
        resolver = TokenResolver(cache_file=self._cache_file)
        with pytest.raises(Exception):
            resolver.resolve("DOESNOTEXIST99", "arbitrum")

        error_calls = [
            c for c in mock_metric.call_args_list if c[0][0] == "record_token_resolution_error"
        ]
        assert len(error_calls) >= 1
        call = error_calls[0]
        assert call[0][1] == "arbitrum"  # chain
        assert call[0][2] == "TokenNotFoundError"  # error_type

    @patch("almanak.framework.data.tokens.resolver._try_record_metric")
    def test_cache_miss_emits_miss_metric_on_not_found(self, mock_metric):
        """Token not found emits cache_miss metric."""
        resolver = TokenResolver(cache_file=self._cache_file)
        with pytest.raises(Exception):
            resolver.resolve("DOESNOTEXIST99", "arbitrum")

        miss_calls = [
            c for c in mock_metric.call_args_list if c[0][0] == "record_token_resolution_cache_miss"
        ]
        assert len(miss_calls) >= 1
        assert miss_calls[0][0][1] == "arbitrum"


class TestTryRecordMetric:
    """Tests for the _try_record_metric utility function."""

    def test_silently_ignores_import_error(self):
        """_try_record_metric does not raise when metrics module is unavailable."""
        # Test the real function with a mock that simulates ImportError
        with patch.dict("sys.modules", {"almanak.gateway": None, "almanak.gateway.metrics": None}):
            # Should not raise
            _try_record_metric("record_token_resolution_cache_hit", "arbitrum", "memory")

    def test_calls_metric_function_when_available(self):
        """_try_record_metric calls the metric function when module is available."""
        mock_func = MagicMock()
        mock_metrics = MagicMock()
        mock_metrics.record_token_resolution_cache_hit = mock_func

        with patch("almanak.gateway.metrics", mock_metrics):
            _try_record_metric("record_token_resolution_cache_hit", "arbitrum", "memory")

        mock_func.assert_called_once_with("arbitrum", "memory")

    def test_silently_ignores_missing_function(self):
        """_try_record_metric ignores if function name doesn't exist on metrics module."""
        mock_metrics = MagicMock(spec=[])  # Empty spec - no attributes

        with patch("almanak.gateway.metrics", mock_metrics):
            # Should not raise
            _try_record_metric("nonexistent_function", "arbitrum")
