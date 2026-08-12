"""Tests that derived funding constants match pinned historical content (plan 023).

Verifies that:
- ``SUPPORTED_MARKETS`` is derived from connector manifests and matches the
  historical literal that it replaced.
- The venue set declared in connectors is byte-parity with ``SUPPORTED_VENUES``
  (the three-way contract: enum members, parity test, and ``test_provider.py``
  len pin).
- ``FundingHistoryDecl`` field validation rejects malformed inputs.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest


def test_funding_rate_preserves_legacy_positional_construction() -> None:
    from almanak.framework.data.funding.models import FundingRate

    observed_at = datetime(2025, 1, 1, tzinfo=UTC)
    next_funding = observed_at + timedelta(hours=1)
    rate = FundingRate(
        "gmx_v2",
        "XMR-USD",
        Decimal("0.0001"),
        Decimal("0.0008"),
        Decimal("0.876"),
        observed_at,
        next_funding,
        Decimal("100"),
        Decimal("200"),
        Decimal("50"),
        Decimal("49"),
        False,
    )

    assert rate.timestamp == observed_at
    assert rate.next_funding_time == next_funding
    assert rate.open_interest_long == Decimal("100")
    assert rate.open_interest_short == Decimal("200")
    assert rate.mark_price == Decimal("50")
    assert rate.index_price == Decimal("49")
    assert rate.is_live_data is False
    assert rate.long_rate_hourly is None
    assert rate.short_rate_hourly is None
    assert rate.market_address is None


class TestSupportedMarketsDerivation:
    """SUPPORTED_MARKETS is derived from connector manifests; content must be stable."""

    # Static suggestions only. GMX is address-first and discovers its venue
    # catalogue dynamically, so it intentionally contributes no symbol list.
    _EXPECTED: dict[str, list[str]] = {
        "gmx_v2": [],
        "hyperliquid": [
            "ETH-USD",
            "BTC-USD",
            "ARB-USD",
            "LINK-USD",
            "SOL-USD",
            "DOGE-USD",
            "ATOM-USD",
            "APT-USD",
        ],
    }

    def test_derived_content_matches_pinned(self) -> None:
        from almanak.framework.data.funding.models import SUPPORTED_MARKETS

        assert SUPPORTED_MARKETS == self._EXPECTED

    def test_keys_match_supported_venues(self) -> None:
        from almanak.framework.data.funding.models import SUPPORTED_MARKETS, SUPPORTED_VENUES

        assert set(SUPPORTED_MARKETS.keys()) == set(SUPPORTED_VENUES)

    def test_all_values_are_lists(self) -> None:
        from almanak.framework.data.funding.models import SUPPORTED_MARKETS

        for venue, markets in SUPPORTED_MARKETS.items():
            assert isinstance(markets, list), f"{venue}: expected list, got {type(markets)}"

    def test_static_venue_keeps_eth_usd(self) -> None:
        from almanak.framework.data.funding.models import SUPPORTED_MARKETS

        assert "ETH-USD" in SUPPORTED_MARKETS["hyperliquid"]

    def test_import_via_package_init(self) -> None:
        """Ensure __init__.py lazy hook serves SUPPORTED_MARKETS correctly."""
        from almanak.framework.data.funding import SUPPORTED_MARKETS

        assert SUPPORTED_MARKETS == self._EXPECTED

    def test_not_a_module_level_literal(self) -> None:
        """SUPPORTED_MARKETS must NOT be a plain dict at module scope."""
        import almanak.framework.data.funding.models as models_mod

        # PEP 562: the attribute is NOT in the module __dict__ at import time
        # (it is served dynamically by __getattr__).
        assert "SUPPORTED_MARKETS" not in vars(models_mod), (
            "SUPPORTED_MARKETS should not be a module-level literal — it must be derived lazily via __getattr__"
        )


class TestVenueParity:
    """Venue set declared in connectors must match the Venue enum."""

    def test_declared_venues_match_enum(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
        from almanak.framework.data.funding.models import SUPPORTED_VENUES, Venue

        declared_venues = {FundingHistoryRegistry.venue_for(k) for k in FundingHistoryRegistry._venues()}
        enum_values = {v.value for v in Venue}
        assert declared_venues == enum_values, (
            f"Venue enum members {enum_values} != declared connector venues {declared_venues}. "
            "Add/remove both together per the venue registry contract in models.py."
        )
        assert set(SUPPORTED_VENUES) == enum_values


class TestFundingHistoryDeclValidation:
    """FundingHistoryDecl rejects malformed markets and provider references."""

    def test_valid_markets_accepted(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        decl = FundingHistoryDecl(
            venue="test_venue",
            markets=("ETH-USD", "BTC-USD", "DOGE-USD"),
        )
        assert decl.markets == ("ETH-USD", "BTC-USD", "DOGE-USD")

    def test_empty_markets_accepted(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        decl = FundingHistoryDecl(venue="test_venue")
        assert decl.markets == ()

    def test_dynamic_markets_rejects_static_list(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        with pytest.raises(ValueError, match="cannot be combined"):
            FundingHistoryDecl(venue="test_venue", dynamic_markets=True, markets=("ETH-USD",))

    def test_dynamic_markets_rejects_non_bool(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        with pytest.raises(ValueError, match="must be a bool"):
            FundingHistoryDecl(venue="test_venue", dynamic_markets=1)  # type: ignore[arg-type]

    def test_lowercase_market_rejected(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        with pytest.raises(ValueError, match="markets"):
            FundingHistoryDecl(venue="test_venue", markets=("eth-usd",))

    def test_non_usd_market_rejected(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        with pytest.raises(ValueError, match="markets"):
            FundingHistoryDecl(venue="test_venue", markets=("ETH-BTC",))

    def test_numeric_start_market_rejected(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        with pytest.raises(ValueError, match="markets"):
            FundingHistoryDecl(venue="test_venue", markets=("1INCH-USD",))

    def test_valid_backtest_provider_accepted(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl, ImportRef

        ref = ImportRef(module="almanak.connectors.gmx_v2.backtest_funding", attribute="GMXFundingProvider")
        decl = FundingHistoryDecl(venue="test_venue", backtest_provider=ref)
        assert decl.backtest_provider is ref

    def test_none_backtest_provider_accepted(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        decl = FundingHistoryDecl(venue="test_venue", backtest_provider=None)
        assert decl.backtest_provider is None

    def test_non_import_ref_backtest_provider_rejected(self) -> None:
        from almanak.connectors._connector_descriptor import FundingHistoryDecl

        with pytest.raises(ValueError, match="backtest_provider"):
            FundingHistoryDecl(venue="test_venue", backtest_provider="not_an_import_ref")  # type: ignore[arg-type]


class TestRegistryMarketsAccessor:
    """FundingHistoryRegistry.markets() returns manifest-declared markets."""

    def test_gmx_markets(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        assert FundingHistoryRegistry.markets("gmx_v2") == ()
        assert FundingHistoryRegistry.discovers_markets("gmx_v2") is True

    def test_static_and_unknown_venues_do_not_discover_markets(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        assert FundingHistoryRegistry.discovers_markets("hyperliquid") is False
        assert FundingHistoryRegistry.discovers_markets("unknown_proto") is False
        assert FundingHistoryRegistry.discovers_markets(None) is False

    def test_gmx_alias_markets(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        assert FundingHistoryRegistry.markets("gmx") == FundingHistoryRegistry.markets("gmx_v2")

    def test_hyperliquid_markets(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        markets = FundingHistoryRegistry.markets("hyperliquid")
        assert "ETH-USD" in markets
        assert "ATOM-USD" in markets
        assert len(markets) == 8

    def test_unknown_protocol_returns_empty(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        assert FundingHistoryRegistry.markets("unknown_proto") == ()

    def test_all_markets_shape(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        all_markets = FundingHistoryRegistry.all_markets()
        assert "gmx_v2" in all_markets
        assert "hyperliquid" in all_markets
        for _venue, mkt_list in all_markets.items():
            assert isinstance(mkt_list, list)


class TestRegistryBacktestProviderAccessor:
    """FundingHistoryRegistry.backtest_provider() loads manifest-declared providers."""

    def test_gmx_provider_class(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
        from almanak.connectors.gmx_v2.backtest_funding import GMXFundingProvider

        FundingHistoryRegistry.reset_cache()
        cls = FundingHistoryRegistry.backtest_provider("gmx_v2")
        assert cls is GMXFundingProvider

    def test_gmx_alias_provider_class(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        cls_canonical = FundingHistoryRegistry.backtest_provider("gmx_v2")
        FundingHistoryRegistry.reset_cache()
        cls_alias = FundingHistoryRegistry.backtest_provider("gmx")
        assert cls_canonical is cls_alias

    def test_hyperliquid_provider_class(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
        from almanak.connectors.hyperliquid.backtest_funding import HyperliquidFundingProvider

        FundingHistoryRegistry.reset_cache()
        cls = FundingHistoryRegistry.backtest_provider("hyperliquid")
        assert cls is HyperliquidFundingProvider

    def test_unknown_protocol_returns_none(self) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        assert FundingHistoryRegistry.backtest_provider("unknown_proto") is None

    def test_cached_class_identity(self) -> None:
        """Repeated calls return the same class object (no re-import)."""
        from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

        FundingHistoryRegistry.reset_cache()
        cls1 = FundingHistoryRegistry.backtest_provider("gmx_v2")
        cls2 = FundingHistoryRegistry.backtest_provider("gmx_v2")
        assert cls1 is cls2


def test_funding_history_decl_rejects_duplicate_markets() -> None:
    """Duplicate market symbols in a FundingHistoryDecl fail at declaration."""
    import pytest

    from almanak.connectors._connector_descriptor import FundingHistoryDecl

    with pytest.raises(ValueError, match="duplicates"):
        FundingHistoryDecl(venue="testvenue", markets=("ETH-USD", "ETH-USD"))
