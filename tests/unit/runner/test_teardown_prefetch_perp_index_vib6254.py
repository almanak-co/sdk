"""Regression coverage for GMX perp index prices in runner-driven teardown."""

from decimal import Decimal

from almanak.framework.intents.vocabulary import PerpCloseIntent
from almanak.framework.runner.runner_teardown import prefetch_teardown_prices


class _RecordingMarket:
    def __init__(self) -> None:
        self._chain = "avalanche"
        self.calls: list[tuple[str, str | None]] = []

    def price(self, token: str, chain: str | None = None) -> Decimal:
        self.calls.append((token, chain))
        return Decimal("1910.685")


def _close(*, market: str = "ETH/USD", chain: str = "avalanche") -> PerpCloseIntent:
    return PerpCloseIntent(
        protocol="gmx_v2",
        market=market,
        collateral_token="USDC",
        is_long=True,
        chain=chain,
    )


def test_runner_teardown_prefetches_perp_index_on_the_intent_chain() -> None:
    """The affected live-runner lane must warm ETH, not only USDC."""
    market = _RecordingMarket()

    prefetch_teardown_prices(market, [_close()])

    assert ("ETH", "avalanche") in market.calls
    assert ("USDC", None) in market.calls
    assert not any(symbol == "USD" for symbol, _chain in market.calls)


def test_runner_teardown_prices_each_perp_index_on_its_own_chain() -> None:
    """Multi-chain snapshots must not rely on an ambiguous plan-wide default."""
    market = _RecordingMarket()

    prefetch_teardown_prices(
        market,
        [
            _close(market="ETH/USD", chain="avalanche"),
            _close(market="BTC/USD", chain="arbitrum"),
        ],
    )

    assert ("ETH", "avalanche") in market.calls
    assert ("BTC", "arbitrum") in market.calls


def test_runner_teardown_uses_the_shared_perp_market_extractor(monkeypatch) -> None:
    """Pin delegation so the live-runner and manager lanes cannot drift again."""
    market = _RecordingMarket()

    monkeypatch.setattr(
        "almanak.framework.teardown.oracle_warmup.extract_warmable_token_chains",
        lambda intents, fallback_chain: {"SENTINEL": "avalanche"},
    )
    prefetch_teardown_prices(market, [_close()])

    assert ("SENTINEL", "avalanche") in market.calls


def test_runner_teardown_prefetches_index_from_serialized_perp_close_after_restart() -> None:
    """The persisted/resumed intent shape must warm the same GMX index token."""
    market = _RecordingMarket()
    serialized_close = {
        "type": "PERP_CLOSE",
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
        "chain": "avalanche",
        "protocol": "gmx_v2",
    }

    prefetch_teardown_prices(market, [serialized_close])

    assert ("ETH", "avalanche") in market.calls
