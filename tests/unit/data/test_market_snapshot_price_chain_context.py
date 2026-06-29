from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.data.interfaces import PriceResult
from almanak.framework.market import MarketSnapshot
from almanak.framework.market.snapshot import _is_evm_address


def _price_result(price: str) -> PriceResult:
    return PriceResult(
        price=Decimal(price),
        source="test",
        timestamp=datetime.now(UTC),
        confidence=1.0,
        stale=False,
    )


class _ChainAwareOracle:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []

    async def get_aggregated_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        chain: str | None = None,
    ) -> PriceResult:
        self.calls.append((token, quote, chain))
        prices = {
            "ETH": "3000",
            "USDC": "1",
        }
        return _price_result(prices[token])


class _LegacyOracle:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def get_aggregated_price(self, token: str, quote: str = "USD") -> PriceResult:
        self.calls.append((token, quote))
        prices = {
            "ETH": "3000",
            "USDC": "1",
        }
        return _price_result(prices[token])


def test_price_and_prices_share_chain_aware_cache_contract() -> None:
    oracle = _ChainAwareOracle()
    snapshot = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x1234",
        price_oracle=oracle,
    )

    assert snapshot.price("ETH") == Decimal("3000")
    assert snapshot.prices(["ETH", "USDC"]) == {
        "ETH": Decimal("3000"),
        "USDC": Decimal("1"),
    }
    assert snapshot.price("USDC") == Decimal("1")
    assert oracle.calls == [
        ("ETH", "USD", "arbitrum"),
        ("USDC", "USD", "arbitrum"),
    ]


def test_prices_and_price_work_with_legacy_oracle_signature() -> None:
    oracle = _LegacyOracle()
    snapshot = MarketSnapshot(
        chain="base",
        wallet_address="0x1234",
        price_oracle=oracle,
    )

    assert snapshot.prices(["ETH"]) == {"ETH": Decimal("3000")}
    assert snapshot.price("ETH") == Decimal("3000")
    assert oracle.calls == [("ETH", "USD")]


def test_is_evm_address_rejects_non_string_tokens() -> None:
    assert _is_evm_address(("base", "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf")) is False
