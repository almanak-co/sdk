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
from types import SimpleNamespace

from almanak.framework.data.interfaces import PriceResult
from almanak.framework.market import MarketSnapshot, PriceData
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.observability.ledger import build_ledger_entry


def _market(
    prices: dict | None = None,
    cache: dict | None = None,
    price_oracle: object | None = None,
) -> MarketSnapshot:
    strategy = SimpleNamespace(
        chain="arbitrum",
        wallet_address="0xwallet",
        _price_oracle=price_oracle,
    )
    snap = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        chain="arbitrum",
        runtime_surface="unit_test",
    )
    if prices:
        for k, v in prices.items():
            snap.set_price(k, v)
    if cache:
        for cache_key, price_data in cache.items():
            token_quote, _, chain = cache_key.partition("@")
            token, _, quote = token_quote.partition("/")
            snap.set_price_data(token, price_data, quote=quote or "USD", chain=chain or None)
    return snap


def test_with_sources_false_returns_flat_dict():
    """Default (legacy) path: flat ``{symbol: Decimal}`` — unchanged."""
    market = _market(
        prices={"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")},
    )
    result = market.get_price_oracle_dict(with_sources=False)
    assert result == {"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")}


def test_with_sources_true_marks_pre_populated_provenance_unavailable():
    """A scalar preload has a label, but no provider observation metadata."""
    market = _market(prices={"WETH": Decimal("2301.69")})
    result = market.get_price_oracle_dict(with_sources=True)
    assert "WETH" in result
    assert result["WETH"]["price_usd"] == "2301.69"
    assert result["WETH"]["oracle_source"] == "preloaded"
    assert result["WETH"]["confidence"] == "UNAVAILABLE"
    assert result["WETH"]["observed_at"] == ""
    assert result["WETH"]["raw_confidence"] is None
    assert result["WETH"]["stale"] is None


def test_with_sources_true_propagates_cache_source():
    """Cached oracle calls preserve the provider name."""
    cached = {
        "WETH/USD": PriceData(
            price=Decimal("2301.69"),
            timestamp=datetime(2026, 5, 2, 11, 9, tzinfo=UTC),
            source="coingecko",
            confidence="HIGH",
            raw_confidence=1.0,
            stale=False,
        ),
        "USDC/USD": PriceData(
            price=Decimal("1.0001"),
            timestamp=datetime(2026, 5, 2, 11, 9, tzinfo=UTC),
            source="chainlink",
            confidence="ESTIMATED",
            raw_confidence=0.9,
            stale=False,
        ),
    }
    market = _market(cache=cached)
    result = market.get_price_oracle_dict(with_sources=True)
    assert result["WETH"]["oracle_source"] == "coingecko"
    assert result["USDC"]["oracle_source"] == "chainlink"
    assert result["WETH"]["observed_at"] == "2026-05-02T11:09:00+00:00"
    assert result["WETH"]["raw_confidence"] == 1.0
    assert result["USDC"]["confidence"] == "ESTIMATED"


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


def test_scalar_oracle_does_not_infer_source_or_fabricate_confidence():
    """Callable identity is not data provenance, even when provider-named."""

    class CoingeckoProvider:
        def get_price(self, token: str, quote: str, chain: str) -> Decimal:
            return Decimal("2301.69")

    market = _market(price_oracle=CoingeckoProvider().get_price)
    # First read populates the cache.
    market.price("WETH")
    nested = market.get_price_oracle_dict(with_sources=True)
    assert nested["WETH"]["oracle_source"] == "unknown"
    assert nested["WETH"]["confidence"] == "UNAVAILABLE"
    assert nested["WETH"]["observed_at"] == ""


def test_aggregated_price_result_preserves_end_to_end_provenance():
    observed_at = datetime(2026, 5, 2, 11, 9, tzinfo=UTC)

    class Aggregator:
        async def get_aggregated_price(self, token, quote, *, chain=None):
            return PriceResult(
                price=Decimal("2301.69"),
                source="chainlink",
                timestamp=observed_at,
                confidence=0.91,
                stale=False,
            )

    market = _market(price_oracle=Aggregator())
    assert market.price("WETH") == Decimal("2301.69")
    entry = market.get_price_oracle_dict(with_sources=True)["WETH"]
    assert entry == {
        "price_usd": "2301.69",
        "oracle_source": "chainlink",
        "fetched_at": observed_at.isoformat(),
        "observed_at": observed_at.isoformat(),
        "confidence": "ESTIMATED",
        "raw_confidence": 0.91,
        "stale": False,
    }


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
        deployment_id="s",
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
        deployment_id="s",
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
    assert decoded["WETH"]["confidence"] == "UNAVAILABLE"
    assert decoded["WETH"]["observed_at"] == ""
