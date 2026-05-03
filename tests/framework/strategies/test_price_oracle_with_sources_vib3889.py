"""VIB-3889 — oracle source pass-through into ``price_inputs_json``.

Pre-VIB-3889 the dashboard "Oracle quotes used" expander rendered
every source as "unknown" — the strategy-side ``PriceData`` carried
no provenance, so the runner's flat ``{symbol: Decimal}`` dict
collapsed all source-tracking the aggregator did upstream. The ledger
writer's normaliser then defaulted ``oracle_source="unknown"``.

The fix exposes a ``with_sources=True`` opt-in on
``MarketSnapshot.get_price_oracle_dict``: when the runner asks for it,
the canonical AttemptNo17 §1.2 G12 nested shape comes back with each
provider's name. ``_merge_oracle_for_ledger`` calls this branch so
the ledger row carries real provenance.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.observability.ledger import build_ledger_entry
from almanak.framework.strategies.intent_strategy import MarketSnapshot
from almanak.framework.strategies.strategy_models import PriceData


def _market(prices: dict | None = None, cache: dict | None = None) -> MarketSnapshot:
    snap = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xwallet",
        timestamp=datetime.now(tz=UTC),
    )
    if prices:
        for k, v in prices.items():
            snap.set_price(k, v)
    if cache:
        snap._price_cache.update(cache)
    return snap


def test_with_sources_false_returns_flat_dict():
    """Default (legacy) path: flat ``{symbol: Decimal}`` — unchanged."""
    market = _market(
        prices={"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")},
    )
    result = market.get_price_oracle_dict(with_sources=False)
    assert result == {"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")}


def test_with_sources_true_returns_nested_shape_for_pre_populated_prices():
    """Pre-populated prices have no provider — labelled "preloaded"."""
    market = _market(prices={"WETH": Decimal("2301.69")})
    result = market.get_price_oracle_dict(with_sources=True)
    assert "WETH" in result
    assert result["WETH"]["price_usd"] == "2301.69"
    assert result["WETH"]["oracle_source"] == "preloaded"
    assert result["WETH"]["confidence"] == "HIGH"


def test_with_sources_true_propagates_cache_source():
    """Cached oracle calls preserve the provider name."""
    cached = {
        "WETH/USD": PriceData(
            price=Decimal("2301.69"),
            timestamp=datetime(2026, 5, 2, 11, 9, tzinfo=UTC),
            source="coingecko",
        ),
        "USDC/USD": PriceData(
            price=Decimal("1.0001"),
            timestamp=datetime(2026, 5, 2, 11, 9, tzinfo=UTC),
            source="chainlink",
        ),
    }
    market = _market(cache=cached)
    result = market.get_price_oracle_dict(with_sources=True)
    assert result["WETH"]["oracle_source"] == "coingecko"
    assert result["USDC"]["oracle_source"] == "chainlink"


def test_with_sources_true_falls_back_to_unknown_when_source_empty():
    """Cached entry without an explicit source → "unknown" (still better
    than the silent "unknown" default in the writer because at least
    it's per-token and surfaces in the dashboard expander)."""
    cached = {
        "WETH/USD": PriceData(price=Decimal("2301.69")),  # no source set
    }
    market = _market(cache=cached)
    result = market.get_price_oracle_dict(with_sources=True)
    assert result["WETH"]["oracle_source"] == "unknown"


# ──────────────────────────────────────────────────────────────────────────
# _infer_oracle_source — provider name extracted from callable identity
# ──────────────────────────────────────────────────────────────────────────


def test_infer_oracle_source_from_qualname():
    """The runner's price_oracle is typically a bound method like
    ``CoingeckoProvider.get_price`` — qualname lets us infer the source
    without changing the callable signature."""
    from almanak.framework.strategies.intent_strategy import _infer_oracle_source

    class CoingeckoProvider:
        def get_price(self, token, quote, chain):
            return Decimal("1")

    oracle = CoingeckoProvider().get_price
    assert _infer_oracle_source(oracle) == "coingecko"


def test_infer_oracle_source_from_module():
    """When qualname is generic, the module path can carry the hint
    (e.g. a free function ``almanak.framework.data.price.chainlink.fetch``)."""
    from almanak.framework.strategies.intent_strategy import _infer_oracle_source

    def _fetch(token, quote, chain):
        return Decimal("1")

    _fetch.__module__ = "almanak.framework.data.price.chainlink"
    assert _infer_oracle_source(_fetch) == "chainlink"


def test_infer_oracle_source_returns_empty_for_unknown_callable():
    """Lambda or anonymous oracle → empty string (callers default to
    "unknown" downstream — strictly better than pre-VIB-3889 always-
    "unknown" because at least named providers identify themselves)."""
    from almanak.framework.strategies.intent_strategy import _infer_oracle_source

    assert _infer_oracle_source(lambda token, quote: Decimal("1")) == ""


def test_infer_oracle_source_handles_functools_partial():
    """``functools.partial`` is the canonical way to bind chain to a
    free-function oracle — the helper unwraps ``.func`` to find the
    underlying provider."""
    import functools

    from almanak.framework.strategies.intent_strategy import _infer_oracle_source

    def _binance_get_price(token, quote, chain):
        return Decimal("1")

    bound = functools.partial(_binance_get_price, chain="arbitrum")
    assert _infer_oracle_source(bound) == "binance"


def test_intent_strategy_price_caches_inferred_source():
    """Integration: ``MarketSnapshot.price()`` writes a cache entry whose
    ``source`` matches the inferred provider name. Without this the
    runner's per-cycle oracle would still produce "unknown" downstream."""

    class CoingeckoProvider:
        def get_price(self, token: str, quote: str, chain: str) -> Decimal:
            return Decimal("2301.69")

    market = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xwallet",
        price_oracle=CoingeckoProvider().get_price,
        timestamp=datetime.now(tz=UTC),
    )
    # First read populates the cache.
    market.price("WETH")
    nested = market.get_price_oracle_dict(with_sources=True)
    assert nested["WETH"]["oracle_source"] == "coingecko"


def test_ledger_writer_preserves_nested_oracle_source_through_round_trip():
    """End-to-end: nested oracle dict → ledger writer → JSON roundtrip
    keeps ``oracle_source`` populated. This was the failure mode on the
    May 2 dashboard (every token rendered "unknown")."""
    nested = {
        "WETH": {
            "price_usd": "2301.69",
            "oracle_source": "coingecko",
            "fetched_at": "2026-05-02T11:09:00+00:00",
            "confidence": "HIGH",
        },
        "USDC": {
            "price_usd": "1.0001",
            "oracle_source": "chainlink",
            "fetched_at": "2026-05-02T11:09:00+00:00",
            "confidence": "HIGH",
        },
    }
    # Minimal intent / result — only the price_oracle path matters here.
    from types import SimpleNamespace

    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        protocol="uniswap_v3",
        chain="arbitrum",
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("2.0"),
    )
    result = SimpleNamespace(
        success=True,
        transaction_results=[],
        gas_cost_usd=Decimal("0.012"),
        swap_amounts=None,
    )

    entry = build_ledger_entry(
        strategy_id="s",
        cycle_id="c",
        intent=intent,
        result=result,
        chain="arbitrum",
        success=True,
        error=None,
        price_oracle=nested,
        pre_state=None,
        post_state=None,
    )

    decoded = json.loads(entry.price_inputs_json)
    assert decoded["WETH"]["oracle_source"] == "coingecko"
    assert decoded["USDC"]["oracle_source"] == "chainlink"
    # Sanity: prices propagated too.
    assert decoded["WETH"]["price_usd"] == "2301.69"
    assert decoded["USDC"]["price_usd"] == "1.0001"


def test_ledger_writer_legacy_flat_oracle_still_normalises_to_unknown():
    """Backwards compat: a market that doesn't (yet) support
    ``with_sources=True`` returns flat values; the ledger writer wraps
    them as ``oracle_source="unknown"``. Pre-VIB-3889 behaviour preserved."""
    flat = {"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")}
    from types import SimpleNamespace

    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        protocol="uniswap_v3",
        chain="arbitrum",
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("2.0"),
    )
    result = SimpleNamespace(
        success=True,
        transaction_results=[],
        gas_cost_usd=Decimal("0.012"),
        swap_amounts=None,
    )

    entry = build_ledger_entry(
        strategy_id="s",
        cycle_id="c",
        intent=intent,
        result=result,
        chain="arbitrum",
        success=True,
        error=None,
        price_oracle=flat,
        pre_state=None,
        post_state=None,
    )
    decoded = json.loads(entry.price_inputs_json)
    assert decoded["WETH"]["oracle_source"] == "unknown"
    assert decoded["WETH"]["price_usd"] == "2301.69"
