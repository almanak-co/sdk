"""Tests for token resolution Prometheus metrics."""

from almanak.gateway.metrics import (
    GATEWAY_REGISTRY,
    TOKEN_RESOLUTION_CACHE_HIT,
    TOKEN_RESOLUTION_CACHE_MISS,
    TOKEN_RESOLUTION_ERROR,
    TOKEN_RESOLUTION_LATENCY,
    TOKEN_RESOLUTION_ONCHAIN_LOOKUP,
    record_token_resolution_cache_hit,
    record_token_resolution_cache_miss,
    record_token_resolution_error,
    record_token_resolution_latency,
    record_token_resolution_onchain_lookup,
)


class TestTokenResolutionMetrics:
    """Tests for token resolution Prometheus metrics and recording functions."""

    def test_record_cache_hit_memory(self):
        """Record cache hit increments counter for memory cache type."""
        initial = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="arbitrum", cache_type="memory")._value.get()
        record_token_resolution_cache_hit("arbitrum", "memory")
        new_value = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="arbitrum", cache_type="memory")._value.get()
        assert new_value == initial + 1

    def test_record_cache_hit_static(self):
        """Record cache hit increments counter for static cache type."""
        initial = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="ethereum", cache_type="static")._value.get()
        record_token_resolution_cache_hit("ethereum", "static")
        new_value = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="ethereum", cache_type="static")._value.get()
        assert new_value == initial + 1

    def test_record_cache_hit_disk(self):
        """Record cache hit increments counter for disk cache type."""
        initial = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="base", cache_type="disk")._value.get()
        record_token_resolution_cache_hit("base", "disk")
        new_value = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="base", cache_type="disk")._value.get()
        assert new_value == initial + 1

    def test_record_cache_miss(self):
        """Record cache miss increments counter."""
        initial = TOKEN_RESOLUTION_CACHE_MISS.labels(chain="optimism")._value.get()
        record_token_resolution_cache_miss("optimism")
        new_value = TOKEN_RESOLUTION_CACHE_MISS.labels(chain="optimism")._value.get()
        assert new_value == initial + 1

    def test_record_onchain_lookup_success(self):
        """Record on-chain lookup with success status."""
        initial = TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain="arbitrum", status="success")._value.get()
        record_token_resolution_onchain_lookup("arbitrum", "success")
        new_value = TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain="arbitrum", status="success")._value.get()
        assert new_value == initial + 1

    def test_record_onchain_lookup_not_found(self):
        """Record on-chain lookup with not_found status."""
        initial = TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain="ethereum", status="not_found")._value.get()
        record_token_resolution_onchain_lookup("ethereum", "not_found")
        new_value = TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain="ethereum", status="not_found")._value.get()
        assert new_value == initial + 1

    def test_record_onchain_lookup_timeout(self):
        """Record on-chain lookup with timeout status."""
        initial = TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain="base", status="timeout")._value.get()
        record_token_resolution_onchain_lookup("base", "timeout")
        new_value = TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain="base", status="timeout")._value.get()
        assert new_value == initial + 1

    def test_record_error(self):
        """Record token resolution error increments counter."""
        initial = TOKEN_RESOLUTION_ERROR.labels(chain="arbitrum", error_type="TokenNotFoundError")._value.get()
        record_token_resolution_error("arbitrum", "TokenNotFoundError")
        new_value = TOKEN_RESOLUTION_ERROR.labels(chain="arbitrum", error_type="TokenNotFoundError")._value.get()
        assert new_value == initial + 1

    def test_record_error_invalid_address(self):
        """Record InvalidTokenAddressError error type."""
        initial = TOKEN_RESOLUTION_ERROR.labels(
            chain="ethereum", error_type="InvalidTokenAddressError"
        )._value.get()
        record_token_resolution_error("ethereum", "InvalidTokenAddressError")
        new_value = TOKEN_RESOLUTION_ERROR.labels(
            chain="ethereum", error_type="InvalidTokenAddressError"
        )._value.get()
        assert new_value == initial + 1

    def test_record_latency(self):
        """Record token resolution latency updates histogram."""
        record_token_resolution_latency("arbitrum", "cache", 0.001)
        # Histogram observation should succeed without error
        # We can verify the sum increased
        sample = TOKEN_RESOLUTION_LATENCY.labels(chain="arbitrum", source="cache")
        assert sample._sum.get() > 0

    def test_record_latency_static(self):
        """Record static registry latency."""
        record_token_resolution_latency("ethereum", "static", 0.005)
        sample = TOKEN_RESOLUTION_LATENCY.labels(chain="ethereum", source="static")
        assert sample._sum.get() > 0

    def test_record_latency_onchain(self):
        """Record on-chain lookup latency."""
        record_token_resolution_latency("base", "on_chain", 0.5)
        sample = TOKEN_RESOLUTION_LATENCY.labels(chain="base", source="on_chain")
        assert sample._sum.get() > 0

    def test_metrics_registered_in_gateway_registry(self):
        """All token resolution metrics are in the GATEWAY_REGISTRY."""
        metric_names = {m.name for m in GATEWAY_REGISTRY.collect()}
        # Counter .name doesn't include _total suffix in prometheus_client
        assert "token_resolution_cache_hit" in metric_names
        assert "token_resolution_cache_miss" in metric_names
        assert "token_resolution_onchain_lookup" in metric_names
        assert "token_resolution_error" in metric_names
        assert "token_resolution_latency_seconds" in metric_names

    def test_multiple_chains_tracked_independently(self):
        """Different chains have independent counters."""
        arb_initial = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="arbitrum", cache_type="memory")._value.get()
        eth_initial = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="ethereum", cache_type="memory")._value.get()

        record_token_resolution_cache_hit("arbitrum", "memory")

        arb_new = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="arbitrum", cache_type="memory")._value.get()
        eth_new = TOKEN_RESOLUTION_CACHE_HIT.labels(chain="ethereum", cache_type="memory")._value.get()

        assert arb_new == arb_initial + 1
        assert eth_new == eth_initial  # Ethereum counter unchanged
