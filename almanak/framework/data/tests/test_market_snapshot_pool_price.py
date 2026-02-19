"""Tests for MarketSnapshot pool price integration (US-009).

Tests cover:
- pool_price() method with mocked PoolReaderRegistry
- pool_price_by_pair() convenience method
- twap() method with mocked PriceAggregator
- lwap() method with mocked PriceAggregator
- Fail-closed behavior (EXECUTION_GRADE)
- DataEnvelope wrapping
- Default chain fallback
- Error handling and exception propagation
- DataRouter integration (optional routing)
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.market_snapshot import (
    MarketSnapshot,
    PoolPriceUnavailableError,
)
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.pools.aggregation import (
    AggregatedPrice,
    PoolContribution,
)
from almanak.framework.data.pools.reader import PoolPrice

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_meta(**overrides) -> DataMeta:
    defaults = {
        "source": "alchemy_rpc",
        "observed_at": datetime.now(UTC),
        "block_number": 19_000_000,
        "finality": "latest",
        "staleness_ms": 0,
        "latency_ms": 10,
        "confidence": 1.0,
        "cache_hit": False,
    }
    defaults.update(overrides)
    return DataMeta(**defaults)


def _make_pool_price(**overrides) -> PoolPrice:
    defaults = {
        "price": Decimal("1823.45"),
        "tick": 200123,
        "liquidity": 10**18,
        "fee_tier": 500,
        "block_number": 19_000_000,
        "timestamp": datetime.now(UTC),
        "pool_address": "0xpool1",
        "token0_decimals": 18,
        "token1_decimals": 6,
    }
    defaults.update(overrides)
    return PoolPrice(**defaults)


def _make_pool_price_envelope(**overrides) -> DataEnvelope[PoolPrice]:
    return DataEnvelope(
        value=_make_pool_price(**overrides),
        meta=_make_meta(),
        classification=DataClassification.EXECUTION_GRADE,
    )


def _make_aggregated_price(method: str = "twap", **overrides) -> AggregatedPrice:
    defaults = {
        "price": Decimal("1825.00"),
        "sources": [
            PoolContribution(
                pool_address="0xpool1",
                protocol="uniswap_v3",
                price=Decimal("1825.00"),
                weight=1.0,
                liquidity=10**18,
            )
        ],
        "block_range": (19_000_000, 19_000_000),
        "method": method,
        "window_seconds": 300 if method == "twap" else 0,
        "pool_count": 1,
    }
    defaults.update(overrides)
    return AggregatedPrice(**defaults)


def _make_aggregated_envelope(method: str = "twap", **overrides) -> DataEnvelope[AggregatedPrice]:
    return DataEnvelope(
        value=_make_aggregated_price(method, **overrides),
        meta=_make_meta(),
        classification=DataClassification.EXECUTION_GRADE,
    )


def _make_snapshot(
    chain: str = "arbitrum",
    pool_reader_registry=None,
    price_aggregator=None,
    data_router=None,
) -> MarketSnapshot:
    return MarketSnapshot(
        chain=chain,
        wallet_address="0x" + "a" * 40,
        pool_reader_registry=pool_reader_registry,
        price_aggregator=price_aggregator,
        data_router=data_router,
    )


# ===========================================================================
# pool_price() tests
# ===========================================================================


class TestPoolPrice:
    def test_pool_price_returns_envelope(self):
        """pool_price() returns DataEnvelope[PoolPrice] from registry reader."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        envelope = _make_pool_price_envelope()
        mock_reader.read_pool_price.return_value = envelope
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        result = snapshot.pool_price("0xpool1")

        assert result is envelope
        assert result.classification == DataClassification.EXECUTION_GRADE
        mock_reader.read_pool_price.assert_called_once_with("0xpool1", "arbitrum")

    def test_pool_price_uses_default_chain(self):
        """pool_price() uses snapshot's primary chain when none provided."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.read_pool_price.return_value = _make_pool_price_envelope()
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(chain="base", pool_reader_registry=registry)
        snapshot.pool_price("0xpool1")

        registry.protocols_for_chain.assert_called_with("base")
        mock_reader.read_pool_price.assert_called_once_with("0xpool1", "base")

    def test_pool_price_explicit_chain(self):
        """pool_price() uses explicit chain override."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.read_pool_price.return_value = _make_pool_price_envelope()
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(chain="arbitrum", pool_reader_registry=registry)
        snapshot.pool_price("0xpool1", chain="ethereum")

        registry.protocols_for_chain.assert_called_with("ethereum")

    def test_pool_price_no_registry_raises_value_error(self):
        """pool_price() raises ValueError when no pool reader registry configured."""
        snapshot = _make_snapshot()
        with pytest.raises(ValueError, match="No pool reader registry"):
            snapshot.pool_price("0xpool1")

    def test_pool_price_no_protocols_raises(self):
        """pool_price() raises PoolPriceUnavailableError when no protocols for chain."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = []

        snapshot = _make_snapshot(pool_reader_registry=registry)
        with pytest.raises(PoolPriceUnavailableError, match="No pool reader protocols"):
            snapshot.pool_price("0xpool1")

    def test_pool_price_tries_multiple_protocols(self):
        """pool_price() tries each protocol until one succeeds."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["aerodrome", "uniswap_v3"]
        reader_aero = MagicMock()
        reader_aero.read_pool_price.side_effect = DataUnavailableError("pool_price", "0xpool1", "fail")
        reader_uni = MagicMock()
        envelope = _make_pool_price_envelope()
        reader_uni.read_pool_price.return_value = envelope
        registry.get_reader.side_effect = lambda _chain, proto: reader_aero if proto == "aerodrome" else reader_uni

        snapshot = _make_snapshot(pool_reader_registry=registry)
        result = snapshot.pool_price("0xpool1")

        assert result is envelope

    def test_pool_price_all_protocols_fail(self):
        """pool_price() raises PoolPriceUnavailableError when all protocols fail."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.read_pool_price.side_effect = DataUnavailableError("pool_price", "0x", "fail")
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        with pytest.raises(PoolPriceUnavailableError, match="All protocols failed"):
            snapshot.pool_price("0xpool1")

    def test_pool_price_envelope_transparent_delegation(self):
        """DataEnvelope from pool_price supports transparent attribute delegation."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        envelope = _make_pool_price_envelope(price=Decimal("2000.50"))
        mock_reader.read_pool_price.return_value = envelope
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        result = snapshot.pool_price("0xpool1")

        # Transparent delegation to PoolPrice
        assert result.price == Decimal("2000.50")
        assert result.tick == 200123
        assert result.meta.source == "alchemy_rpc"


# ===========================================================================
# pool_price_by_pair() tests
# ===========================================================================


class TestPoolPriceByPair:
    def test_pool_price_by_pair_resolves_and_reads(self):
        """pool_price_by_pair() resolves pool address and reads price."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool_resolved"
        envelope = _make_pool_price_envelope()
        mock_reader.read_pool_price.return_value = envelope
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        result = snapshot.pool_price_by_pair("WETH", "USDC")

        assert result is envelope
        mock_reader.resolve_pool_address.assert_called_once_with("WETH", "USDC", "arbitrum", 3000)
        mock_reader.read_pool_price.assert_called_once_with("0xpool_resolved", "arbitrum")

    def test_pool_price_by_pair_specific_protocol(self):
        """pool_price_by_pair() uses specified protocol only."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader.read_pool_price.return_value = _make_pool_price_envelope()
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        snapshot.pool_price_by_pair("WETH", "USDC", protocol="aerodrome")

        registry.get_reader.assert_called_once_with("arbitrum", "aerodrome")

    def test_pool_price_by_pair_custom_fee_tier(self):
        """pool_price_by_pair() passes custom fee tier."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader.read_pool_price.return_value = _make_pool_price_envelope()
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        snapshot.pool_price_by_pair("WETH", "USDC", fee_tier=500)

        mock_reader.resolve_pool_address.assert_called_once_with("WETH", "USDC", "arbitrum", 500)

    def test_pool_price_by_pair_no_pool_found(self):
        """pool_price_by_pair() raises when no pool found for pair."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = None
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(pool_reader_registry=registry)
        with pytest.raises(PoolPriceUnavailableError, match="No pool found"):
            snapshot.pool_price_by_pair("ABC", "XYZ")

    def test_pool_price_by_pair_no_registry(self):
        """pool_price_by_pair() raises ValueError when no registry configured."""
        snapshot = _make_snapshot()
        with pytest.raises(ValueError, match="No pool reader registry"):
            snapshot.pool_price_by_pair("WETH", "USDC")

    def test_pool_price_by_pair_tries_multiple_protocols(self):
        """pool_price_by_pair() tries each protocol when protocol=None."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["aerodrome", "uniswap_v3"]
        reader_aero = MagicMock()
        reader_aero.resolve_pool_address.return_value = None  # no pool
        reader_uni = MagicMock()
        reader_uni.resolve_pool_address.return_value = "0xpool1"
        reader_uni.read_pool_price.return_value = _make_pool_price_envelope()
        registry.get_reader.side_effect = lambda _chain, proto: reader_aero if proto == "aerodrome" else reader_uni

        snapshot = _make_snapshot(pool_reader_registry=registry)
        result = snapshot.pool_price_by_pair("WETH", "USDC")

        assert result.value.pool_address == "0xpool1"

    def test_pool_price_by_pair_default_chain(self):
        """pool_price_by_pair() uses snapshot chain by default."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader.read_pool_price.return_value = _make_pool_price_envelope()
        registry.get_reader.return_value = mock_reader

        snapshot = _make_snapshot(chain="base", pool_reader_registry=registry)
        snapshot.pool_price_by_pair("WETH", "USDC")

        registry.protocols_for_chain.assert_called_with("base")


# ===========================================================================
# twap() tests
# ===========================================================================


class TestTWAP:
    def test_twap_returns_aggregated_envelope(self):
        """twap() returns DataEnvelope[AggregatedPrice] from PriceAggregator."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        envelope = _make_aggregated_envelope("twap")
        aggregator.twap.return_value = envelope

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)
        result = snapshot.twap("WETH/USDC")

        assert result is envelope
        assert result.classification == DataClassification.EXECUTION_GRADE
        aggregator.twap.assert_called_once_with(
            pool_address="0xpool1",
            chain="arbitrum",
            window_seconds=300,
            token0_decimals=18,
            token1_decimals=6,
            protocol="uniswap_v3",
        )

    def test_twap_custom_window(self):
        """twap() passes custom window_seconds."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        aggregator.twap.return_value = _make_aggregated_envelope("twap")

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)
        snapshot.twap("WETH/USDC", window_seconds=600)

        aggregator.twap.assert_called_once()
        call_kwargs = aggregator.twap.call_args
        assert call_kwargs.kwargs["window_seconds"] == 600

    def test_twap_explicit_pool_address(self):
        """twap() uses explicit pool_address when provided."""
        aggregator = MagicMock()
        aggregator.twap.return_value = _make_aggregated_envelope("twap")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        snapshot.twap("WETH/USDC", pool_address="0xexplicit")

        assert aggregator.twap.call_args.kwargs["pool_address"] == "0xexplicit"

    def test_twap_no_aggregator_raises(self):
        """twap() raises ValueError when no aggregator configured."""
        snapshot = _make_snapshot()
        with pytest.raises(ValueError, match="No price aggregator"):
            snapshot.twap("WETH/USDC")

    def test_twap_instrument_object(self):
        """twap() accepts Instrument objects."""
        from almanak.framework.data.models import Instrument

        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        aggregator.twap.return_value = _make_aggregated_envelope("twap")

        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)
        snapshot.twap(inst)

        # Should still resolve and call twap
        aggregator.twap.assert_called_once()

    def test_twap_native_token_canonicalization(self):
        """twap('ETH/USDC') canonicalizes to WETH/USDC."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        aggregator.twap.return_value = _make_aggregated_envelope("twap")

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)
        snapshot.twap("ETH/USDC")

        # resolve_pool_address should receive WETH (canonicalized)
        mock_reader.resolve_pool_address.assert_called_once()
        args = mock_reader.resolve_pool_address.call_args
        assert args[0][0] == "WETH"  # base canonicalized

    def test_twap_pool_not_found_raises(self):
        """twap() raises when pool cannot be resolved."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = None
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)
        with pytest.raises(PoolPriceUnavailableError, match="Cannot resolve pool"):
            snapshot.twap("ABC/XYZ")

    def test_twap_aggregator_error_wrapped(self):
        """twap() wraps aggregator exceptions in PoolPriceUnavailableError."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        aggregator.twap.side_effect = DataUnavailableError("twap", "0xpool1", "observe failed")

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)
        with pytest.raises(PoolPriceUnavailableError, match="TWAP calculation failed"):
            snapshot.twap("WETH/USDC")

    def test_twap_default_chain(self):
        """twap() uses snapshot's chain when none specified."""
        registry = MagicMock()
        mock_reader = MagicMock()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        aggregator.twap.return_value = _make_aggregated_envelope("twap")

        snapshot = _make_snapshot(chain="base", pool_reader_registry=registry, price_aggregator=aggregator)
        snapshot.twap("WETH/USDC")

        assert aggregator.twap.call_args.kwargs["chain"] == "base"


# ===========================================================================
# lwap() tests
# ===========================================================================


class TestLWAP:
    def test_lwap_returns_aggregated_envelope(self):
        """lwap() returns DataEnvelope[AggregatedPrice] from PriceAggregator."""
        aggregator = MagicMock()
        envelope = _make_aggregated_envelope("lwap")
        aggregator.lwap.return_value = envelope

        snapshot = _make_snapshot(price_aggregator=aggregator)
        result = snapshot.lwap("WETH/USDC")

        assert result is envelope
        assert result.classification == DataClassification.EXECUTION_GRADE
        aggregator.lwap.assert_called_once_with(
            token_a="WETH",
            token_b="USDC",
            chain="arbitrum",
            fee_tiers=None,
            protocols=None,
        )

    def test_lwap_custom_fee_tiers(self):
        """lwap() passes custom fee tiers to aggregator."""
        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        snapshot.lwap("WETH/USDC", fee_tiers=[500, 3000])

        assert aggregator.lwap.call_args.kwargs["fee_tiers"] == [500, 3000]

    def test_lwap_custom_protocols(self):
        """lwap() passes custom protocols to aggregator."""
        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        snapshot.lwap("WETH/USDC", protocols=["uniswap_v3"])

        assert aggregator.lwap.call_args.kwargs["protocols"] == ["uniswap_v3"]

    def test_lwap_no_aggregator_raises(self):
        """lwap() raises ValueError when no aggregator configured."""
        snapshot = _make_snapshot()
        with pytest.raises(ValueError, match="No price aggregator"):
            snapshot.lwap("WETH/USDC")

    def test_lwap_instrument_object(self):
        """lwap() accepts Instrument objects."""
        from almanak.framework.data.models import Instrument

        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        snapshot.lwap(inst)

        assert aggregator.lwap.call_args.kwargs["token_a"] == "WETH"
        assert aggregator.lwap.call_args.kwargs["token_b"] == "USDC"

    def test_lwap_native_token_canonicalization(self):
        """lwap('ETH/USDC') canonicalizes to WETH/USDC."""
        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        snapshot.lwap("ETH/USDC")

        assert aggregator.lwap.call_args.kwargs["token_a"] == "WETH"

    def test_lwap_error_wrapped(self):
        """lwap() wraps aggregator exceptions in PoolPriceUnavailableError."""
        aggregator = MagicMock()
        aggregator.lwap.side_effect = DataUnavailableError("lwap", "WETH/USDC", "no pools")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        with pytest.raises(PoolPriceUnavailableError, match="LWAP calculation failed"):
            snapshot.lwap("WETH/USDC")

    def test_lwap_default_chain(self):
        """lwap() uses snapshot's chain when none specified."""
        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(chain="base", price_aggregator=aggregator)
        snapshot.lwap("WETH/USDC")

        assert aggregator.lwap.call_args.kwargs["chain"] == "base"

    def test_lwap_explicit_chain_override(self):
        """lwap() uses explicit chain when provided."""
        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(chain="arbitrum", price_aggregator=aggregator)
        snapshot.lwap("WETH/USDC", chain="ethereum")

        assert aggregator.lwap.call_args.kwargs["chain"] == "ethereum"

    def test_lwap_single_token_defaults_to_usdc(self):
        """lwap('WETH') defaults quote to USDC via resolve_instrument."""
        aggregator = MagicMock()
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(price_aggregator=aggregator)
        snapshot.lwap("WETH")

        assert aggregator.lwap.call_args.kwargs["token_a"] == "WETH"
        assert aggregator.lwap.call_args.kwargs["token_b"] == "USDC"


# ===========================================================================
# PoolPriceUnavailableError tests
# ===========================================================================


class TestPoolPriceUnavailableError:
    def test_exception_attributes(self):
        """PoolPriceUnavailableError stores identifier and reason."""
        err = PoolPriceUnavailableError("0xpool1", "RPC timeout")
        assert err.identifier == "0xpool1"
        assert err.reason == "RPC timeout"
        assert "0xpool1" in str(err)
        assert "RPC timeout" in str(err)

    def test_is_market_snapshot_error(self):
        """PoolPriceUnavailableError is a MarketSnapshotError."""
        from almanak.framework.data.market_snapshot import MarketSnapshotError

        err = PoolPriceUnavailableError("0x", "fail")
        assert isinstance(err, MarketSnapshotError)


# ===========================================================================
# Integration / combined tests
# ===========================================================================


class TestIntegration:
    def test_all_methods_available_on_snapshot(self):
        """MarketSnapshot has all new pool price methods."""
        snapshot = _make_snapshot()
        assert hasattr(snapshot, "pool_price")
        assert hasattr(snapshot, "pool_price_by_pair")
        assert hasattr(snapshot, "twap")
        assert hasattr(snapshot, "lwap")
        assert callable(snapshot.pool_price)
        assert callable(snapshot.pool_price_by_pair)
        assert callable(snapshot.twap)
        assert callable(snapshot.lwap)

    def test_pool_price_unavailable_importable_from_data_init(self):
        """PoolPriceUnavailableError is importable from data package."""
        from almanak.framework.data import PoolPriceUnavailableError as Imported

        assert Imported is PoolPriceUnavailableError

    def test_snapshot_constructor_accepts_new_params(self):
        """MarketSnapshot constructor accepts pool_reader_registry, price_aggregator, data_router."""
        registry = MagicMock()
        aggregator = MagicMock()
        router = MagicMock()

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "b" * 40,
            pool_reader_registry=registry,
            price_aggregator=aggregator,
            data_router=router,
        )

        assert snapshot._pool_reader_registry is registry
        assert snapshot._price_aggregator is aggregator
        assert snapshot._data_router is router

    def test_envelope_is_execution_grade(self):
        """All pool price methods return EXECUTION_GRADE envelopes."""
        registry = MagicMock()
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        mock_reader = MagicMock()
        mock_reader.read_pool_price.return_value = _make_pool_price_envelope()
        mock_reader.resolve_pool_address.return_value = "0xpool1"
        mock_reader._get_pool_metadata.return_value = (18, 6, 500)
        registry.get_reader.return_value = mock_reader

        aggregator = MagicMock()
        aggregator.twap.return_value = _make_aggregated_envelope("twap")
        aggregator.lwap.return_value = _make_aggregated_envelope("lwap")

        snapshot = _make_snapshot(pool_reader_registry=registry, price_aggregator=aggregator)

        assert snapshot.pool_price("0xpool1").classification == DataClassification.EXECUTION_GRADE
        assert snapshot.pool_price_by_pair("WETH", "USDC").classification == DataClassification.EXECUTION_GRADE
        assert snapshot.twap("WETH/USDC").classification == DataClassification.EXECUTION_GRADE
        assert snapshot.lwap("WETH/USDC").classification == DataClassification.EXECUTION_GRADE
