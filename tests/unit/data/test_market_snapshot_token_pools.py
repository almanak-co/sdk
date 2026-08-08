"""``MarketSnapshot.token_pools`` — the strategy-facing venue-discovery accessor (VIB-6599).

The reader and the ranking rule are covered in
``tests/framework/data/test_pool_analytics_gateway_backed.py``; this module
covers the snapshot method's own behaviour — provider wiring, token
resolution, error translation, and the filter/sort it delegates.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.core.finality import DataFinality
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.market_snapshot import PoolAnalyticsUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.analytics import TokenPool, TokenPools
from almanak.framework.market import MarketSnapshot
from almanak.framework.runner.failure_kind import FailureKind, classify_failure

_WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def _pool(name: str, reserve: Decimal | None, *, dex_id: str = "uniswap_v3") -> TokenPool:
    return TokenPool(
        pool_address=f"0x{abs(hash(name)) % (16**40):040x}",
        dex_id=dex_id,
        name=name,
        reserve_usd=reserve,
        volume_24h_usd=None,
        base_token_address=_WETH_ARBITRUM,
        quote_token_address="",
    )


def _envelope(pools: tuple[TokenPool, ...], *, complete: bool = True, product_distinct: bool = True):
    from datetime import UTC, datetime

    return DataEnvelope(
        value=TokenPools(
            chain="arbitrum",
            token_address=_WETH_ARBITRUM,
            pools=pools,
            source="coingecko_onchain",
            complete=complete,
            product_distinct_dex_id=product_distinct,
        ),
        meta=DataMeta(
            source="coingecko_onchain",
            observed_at=datetime.now(UTC),
            finality=DataFinality.OFF_CHAIN,
            staleness_ms=0,
            latency_ms=0,
            confidence=0.85,
            cache_hit=False,
        ),
        classification=DataClassification.INFORMATIONAL,
    )


def _reader(envelope) -> MagicMock:
    reader = MagicMock()
    reader.list_token_pools.return_value = envelope
    return reader


def test_token_pools_without_a_reader_raises_value_error():
    """Unwired provider is a configuration error, not a data outage."""
    snapshot = MarketSnapshot(chain="arbitrum")

    with pytest.raises(ValueError, match="pool analytics reader"):
        snapshot.token_pools("WETH")


def test_token_pools_rejects_an_unresolvable_token_with_an_actionable_message():
    """Venue discovery is address-keyed, and the tail tokens it exists for are
    exactly the ones no static registry lists — so the error must say to pass
    an address rather than leave the caller guessing."""
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=_reader(_envelope(())))

    with pytest.raises(ValueError) as excinfo:
        snapshot.token_pools("NOT-A-REAL-TOKEN-SYMBOL-XYZ")

    assert "contract address" in str(excinfo.value)


def test_token_pools_sorts_deepest_first_with_unmeasured_last():
    """The reader preserves upstream RANK order; the snapshot is what sorts by depth."""
    reader = _reader(
        _envelope(
            (
                _pool("shallow", Decimal("10")),
                _pool("unknown", None),
                _pool("deep", Decimal("211219")),
                _pool("empty", Decimal("0")),
            )
        )
    )
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    result = snapshot.token_pools(_WETH_ARBITRUM).value

    assert [p.name for p in result.pools] == ["deep", "shallow", "empty", "unknown"]
    # Both caveats survive the re-wrap — dropping them would let a caller read a
    # truncated window as "the deepest venue that exists".
    assert result.complete is True
    assert result.product_distinct_dex_id is True
    assert result.source == "coingecko_onchain"

    # Address passed through untouched; chain defaults to the snapshot's own.
    kwargs = reader.list_token_pools.call_args.kwargs
    assert kwargs["token_address"] == _WETH_ARBITRUM
    assert kwargs["chain"] == "arbitrum"


def test_token_pools_min_liquidity_floor_drops_unmeasured_reserves():
    """"Unknown" does not satisfy ">= X" — Empty != Zero, so an unmeasured
    reserve is excluded by a floor rather than treated as 0 and sorted last."""
    reader = _reader(
        _envelope(
            (
                _pool("deep", Decimal("211219")),
                _pool("shallow", Decimal("10")),
                _pool("unknown", None),
            )
        )
    )
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    result = snapshot.token_pools(_WETH_ARBITRUM, min_liquidity_usd=100)

    assert [p.name for p in result.value.pools] == ["deep"]


def test_token_pools_explicit_chain_overrides_the_snapshot_chain():
    reader = _reader(_envelope(()))
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    snapshot.token_pools(_WETH_ARBITRUM, chain="Base")

    assert reader.list_token_pools.call_args.kwargs["chain"] == "base"


def test_token_pools_defaults_to_permitting_the_fallback_provider():
    """Default True is deliberate: the fallback engages only after the primary
    FAILS, so a configured gateway behaves identically — while a local gateway
    with no CoinGecko key gets an answer instead of nothing."""
    reader = _reader(_envelope(()))
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    snapshot.token_pools(_WETH_ARBITRUM)
    assert reader.list_token_pools.call_args.kwargs["allow_fallback_provider"] is True

    snapshot.token_pools(_WETH_ARBITRUM, allow_fallback_provider=False)
    assert reader.list_token_pools.call_args.kwargs["allow_fallback_provider"] is False


def test_token_pools_empty_venue_list_is_a_measured_answer():
    """"Nothing trades here" is the most decision-relevant answer this can give."""
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=_reader(_envelope(())))

    assert snapshot.token_pools(_WETH_ARBITRUM).value.pools == ()


def test_token_pools_zero_floor_means_no_floor_and_keeps_unmeasured():
    """CodeRabbit finding. ``0`` is the natural way to say "no floor", but as a
    Decimal it is a real threshold that unmeasured reserves fail (``None >= 0``
    is false by our own rule) — so it silently dropped the venues the docstring
    promises to keep. Non-positive now means no floor."""
    reader = _reader(_envelope((_pool("measured", Decimal("100")), _pool("unknown", None))))
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    for no_floor in (0, 0.0, Decimal("0"), -5):
        names = [p.name for p in snapshot.token_pools(_WETH_ARBITRUM, min_liquidity_usd=no_floor).value.pools]
        assert names == ["measured", "unknown"], f"{no_floor!r} should mean no floor"


def test_token_pools_rejects_a_non_finite_floor_before_the_round_trip():
    """CodeRabbit finding. A NaN floor made every Decimal comparison raise
    ``InvalidOperation`` from inside the sort key. Reject it at the boundary,
    naming the argument — and before spending a gateway call to find out."""
    reader = _reader(_envelope((_pool("measured", Decimal("100")),)))
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="min_liquidity_usd"):
            snapshot.token_pools(_WETH_ARBITRUM, min_liquidity_usd=bad)

    reader.list_token_pools.assert_not_called()


def test_token_pools_passes_a_solana_mint_through_with_its_case_intact():
    """CodeRabbit finding. Solana base58 is CASE-SENSITIVE — the shared
    ``_resolve_token_address`` lower-cases unconditionally, which turns
    ``EPjFWdd5…`` into a DIFFERENT address, and routes unregistered mints
    through the symbol registry so a valid address reads as unresolvable.
    Venue discovery for Solana tail tokens is exactly the case that breaks."""
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    reader = _reader(_envelope(()))
    snapshot = MarketSnapshot(chain="solana", pool_analytics_reader=reader)

    snapshot.token_pools(mint)

    sent = reader.list_token_pools.call_args.kwargs["token_address"]
    assert sent == mint, "Solana mint must reach the gateway byte-identical"
    assert sent != mint.lower()


def test_token_pools_wraps_reader_failure_for_the_hold_path():
    """A provider outage must classify as DATA_UNAVAILABLE so the runner HOLDs
    rather than crashing the iteration loop — the typed origin has to survive
    the wrap for ``classify_failure`` to walk ``__cause__``."""
    reader = MagicMock()
    reader.list_token_pools.side_effect = DataSourceUnavailable(
        source="pool_analytics", reason="providers exhausted"
    )
    snapshot = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)

    with pytest.raises(PoolAnalyticsUnavailableError) as excinfo:
        snapshot.token_pools(_WETH_ARBITRUM)

    assert isinstance(excinfo.value.__cause__, DataSourceUnavailable)
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE
