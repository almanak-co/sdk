"""Tests for manifest-driven provider routing in PerpBacktestAdapter (plan 023).

Verifies that ``_get_provider_for_protocol`` dispatches via
``FundingHistoryRegistry.canonical()`` — no protocol-name literals in the
adapter code.  Tests cover:

- ``"gmx"`` (alias) -> GMXFundingProvider
- ``"gmx_v2"`` (canonical) -> GMXFundingProvider
- ``"hyperliquid"`` -> HyperliquidFundingProvider
- unknown protocol -> None
- repeated calls return the same cached provider instance (``is`` identity)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from almanak.framework.backtesting.adapters.perp_adapter import (
    PerpBacktestAdapter,
    PerpBacktestConfig,
)


def _make_adapter(funding_rate_source: str = "historical") -> PerpBacktestAdapter:
    """Create a minimal adapter with historical funding enabled."""
    config = PerpBacktestConfig(
        strategy_type="perp",
        funding_rate_source=funding_rate_source,
        protocol="gmx",
        chain="arbitrum",
    )
    return PerpBacktestAdapter(config)


class TestGetProviderForProtocol:
    """_get_provider_for_protocol routes via FundingHistoryRegistry, not literals."""

    def test_gmx_alias_routes_to_gmx_provider(self) -> None:
        adapter = _make_adapter()
        mock_provider = MagicMock()
        with patch.object(adapter, "_ensure_gmx_provider", return_value=mock_provider) as mock_ensure:
            result = adapter._get_provider_for_protocol("gmx")
        mock_ensure.assert_called_once()
        assert result is mock_provider

    def test_gmx_v2_canonical_routes_to_gmx_provider(self) -> None:
        adapter = _make_adapter()
        mock_provider = MagicMock()
        with patch.object(adapter, "_ensure_gmx_provider", return_value=mock_provider) as mock_ensure:
            result = adapter._get_provider_for_protocol("gmx_v2")
        mock_ensure.assert_called_once()
        assert result is mock_provider

    def test_gmx_uppercase_routes_to_gmx_provider(self) -> None:
        """Case normalization is handled by FundingHistoryRegistry._normalize."""
        adapter = _make_adapter()
        mock_provider = MagicMock()
        with patch.object(adapter, "_ensure_gmx_provider", return_value=mock_provider) as mock_ensure:
            result = adapter._get_provider_for_protocol("GMX")
        mock_ensure.assert_called_once()
        assert result is mock_provider

    def test_hyperliquid_routes_to_hyperliquid_provider(self) -> None:
        adapter = _make_adapter()
        mock_provider = MagicMock()
        with patch.object(adapter, "_ensure_hyperliquid_provider", return_value=mock_provider) as mock_ensure:
            result = adapter._get_provider_for_protocol("hyperliquid")
        mock_ensure.assert_called_once()
        assert result is mock_provider

    def test_hyperliquid_uppercase_routes_correctly(self) -> None:
        adapter = _make_adapter()
        mock_provider = MagicMock()
        with patch.object(adapter, "_ensure_hyperliquid_provider", return_value=mock_provider) as mock_ensure:
            result = adapter._get_provider_for_protocol("HYPERLIQUID")
        mock_ensure.assert_called_once()
        assert result is mock_provider

    def test_unknown_protocol_returns_none(self) -> None:
        adapter = _make_adapter()
        result = adapter._get_provider_for_protocol("some_unknown_protocol")
        assert result is None

    def test_unknown_protocol_does_not_call_ensure_helpers(self) -> None:
        adapter = _make_adapter()
        with (
            patch.object(adapter, "_ensure_gmx_provider") as mock_gmx,
            patch.object(adapter, "_ensure_hyperliquid_provider") as mock_hl,
        ):
            result = adapter._get_provider_for_protocol("totally_unknown")
        mock_gmx.assert_not_called()
        mock_hl.assert_not_called()
        assert result is None


class TestProviderCaching:
    """Repeated calls to _ensure_* helpers return the same instance."""

    def test_gmx_provider_cached_across_calls(self) -> None:
        """Repeated _get_provider_for_protocol('gmx') returns the cached instance.

        Exercises the real caching path (``_ensure_gmx_provider`` is NOT
        patched): the first call constructs and caches the provider behind the
        ``_gmx_provider_initialized`` flag, the second must return the same
        object. Proves the full dispatch -> factory -> cache-slot chain caches,
        not just that a mock echoes itself.
        """
        adapter = _make_adapter()
        r1 = adapter._get_provider_for_protocol("gmx")
        r2 = adapter._get_provider_for_protocol("gmx")
        assert r1 is not None
        assert r1 is r2
        assert adapter._gmx_provider_initialized is True
        assert adapter._gmx_provider is r1

    def test_ensure_gmx_provider_returns_same_instance_after_init(self) -> None:
        """Once initialized, _ensure_gmx_provider must return the same instance."""
        adapter = _make_adapter()
        sentinel = MagicMock()
        adapter._gmx_provider = sentinel
        adapter._gmx_provider_initialized = True

        result1 = adapter._ensure_gmx_provider()
        result2 = adapter._ensure_gmx_provider()
        assert result1 is sentinel
        assert result2 is sentinel

    def test_ensure_hyperliquid_provider_returns_same_instance_after_init(self) -> None:
        """Once initialized, _ensure_hyperliquid_provider must return the same instance."""
        adapter = _make_adapter()
        sentinel = MagicMock()
        adapter._hyperliquid_provider = sentinel
        adapter._hyperliquid_provider_initialized = True

        result1 = adapter._ensure_hyperliquid_provider()
        result2 = adapter._ensure_hyperliquid_provider()
        assert result1 is sentinel
        assert result2 is sentinel

    def test_gmx_and_hyperliquid_initialized_flags_start_false(self) -> None:
        """Provider init flags start False (lazy initialization contract)."""
        adapter = _make_adapter()
        assert adapter._gmx_provider_initialized is False
        assert adapter._hyperliquid_provider_initialized is False
