"""ALM-3191 — gateway price metadata is preserved, never inferred."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.data.interfaces import PriceResult
from almanak.framework.market import PriceData
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.portfolio.models import ValueConfidence


@pytest.mark.parametrize(
    ("raw_confidence", "stale", "expected"),
    [
        (1.0, False, ValueConfidence.HIGH),
        (0.9, False, ValueConfidence.ESTIMATED),
        (0.7, False, ValueConfidence.STALE),
        (0.95, True, ValueConfidence.STALE),
        (0.3, True, ValueConfidence.UNAVAILABLE),
    ],
)
def test_price_result_confidence_mapping_is_conservative(raw_confidence, stale, expected):
    result = PriceResult(
        price=Decimal("1"),
        source="gateway",
        timestamp=datetime(2026, 8, 14, tzinfo=UTC),
        confidence=raw_confidence,
        stale=stale,
    )

    data = PriceData.from_price_result(result)

    assert data.confidence is expected
    assert data.raw_confidence == raw_confidence
    assert data.stale is stale


def test_price_result_source_and_observation_are_not_restamped():
    observed_at = datetime(2026, 8, 14, 4, 5, 6, tzinfo=UTC)
    result = PriceResult(
        price=Decimal("3421.25"),
        source="coingecko",
        timestamp=observed_at,
        confidence=1.0,
    )

    data = PriceData.from_price_result(result)

    assert data.source == "coingecko"
    assert data.observed_at is observed_at
    assert data.to_oracle_entry()["observed_at"] == observed_at.isoformat()


def test_scalar_price_data_has_unavailable_provenance():
    entry = PriceData(price=Decimal("1")).to_oracle_entry()

    assert entry["oracle_source"] == "unknown"
    assert entry["observed_at"] == ""
    assert entry["confidence"] == "UNAVAILABLE"
    assert entry["raw_confidence"] is None
    assert entry["stale"] is None


def test_scalar_provider_result_keeps_price_with_unavailable_provenance():
    data = PriceData.from_price_result(Decimal("1.25"))

    assert data.price == Decimal("1.25")
    assert data.to_oracle_entry()["confidence"] == "UNAVAILABLE"


@pytest.mark.parametrize("confidence", [None, "unknown", float("nan"), 2.0])
def test_malformed_provider_confidence_degrades_to_unavailable(confidence):
    data = PriceData.from_price_result(SimpleNamespace(price=Decimal("1.25"), confidence=confidence))

    assert data.price == Decimal("1.25")
    assert data.raw_confidence is None
    assert data.to_oracle_entry()["confidence"] == "UNAVAILABLE"


def test_contradictory_metadata_cannot_export_high_confidence():
    data = PriceData(
        price=Decimal("1"),
        confidence=ValueConfidence.HIGH,
        stale=True,
        raw_confidence=0.4,
    )

    assert data.confidence is ValueConfidence.UNAVAILABLE
    assert data.to_oracle_entry()["confidence"] == "UNAVAILABLE"


def test_result_capability_supports_keyword_only_chain():
    observed_at = datetime(2026, 8, 14, 4, 5, 6, tzinfo=UTC)

    class KeywordOnlyOracle:
        def get_price_result(
            self,
            token: str,
            quote: str = "USD",
            *,
            chain: str | None = None,
        ) -> PriceResult:
            assert (token, quote, chain) == ("WETH", "USD", "arbitrum")
            return PriceResult(
                price=Decimal("3421.25"),
                source="keyword_only",
                timestamp=observed_at,
                confidence=1.0,
            )

    strategy = SimpleNamespace(
        chain="arbitrum",
        wallet_address="0xwallet",
        _price_oracle=KeywordOnlyOracle(),
    )
    market = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        chain="arbitrum",
        runtime_surface="unit_test",
    )

    assert market.price("WETH") == Decimal("3421.25")
    assert market.price_data("WETH").source == "keyword_only"


def test_result_capability_omits_chain_for_legacy_signature():
    observed_at = datetime(2026, 8, 14, 4, 5, 6, tzinfo=UTC)

    class LegacyOracle:
        def get_price_result(self, token: str, quote: str = "USD") -> PriceResult:
            assert (token, quote) == ("WETH", "USD")
            return PriceResult(
                price=Decimal("3421.25"),
                source="legacy",
                timestamp=observed_at,
                confidence=1.0,
            )

    strategy = SimpleNamespace(
        chain="arbitrum",
        wallet_address="0xwallet",
        _price_oracle=LegacyOracle(),
    )
    market = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        chain="arbitrum",
        runtime_surface="unit_test",
    )

    assert market.price("WETH") == Decimal("3421.25")
    assert market.price_data("WETH").source == "legacy"


def test_scalar_aggregator_result_keeps_price_without_inventing_provenance():
    class ScalarAggregator:
        def get_aggregated_price(
            self,
            token: str,
            quote: str = "USD",
            *,
            chain: str | None = None,
        ) -> Decimal:
            assert (token, quote, chain) == ("WETH", "USD", "arbitrum")
            return Decimal("3421.25")

    strategy = SimpleNamespace(
        chain="arbitrum",
        wallet_address="0xwallet",
        _price_oracle=ScalarAggregator(),
    )
    market = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        chain="arbitrum",
        runtime_surface="unit_test",
    )

    assert market.price("WETH") == Decimal("3421.25")
    entry = market.price_data("WETH").to_oracle_entry()
    assert entry["price_usd"] == "3421.25"
    assert entry["oracle_source"] == "unknown"
    assert entry["observed_at"] == ""
    assert entry["confidence"] == "UNAVAILABLE"
