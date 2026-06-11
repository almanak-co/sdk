"""Tests for ``MarketSnapshot.invalidate_balance``.

Sequential execution lanes (the teardown staircase) mutate wallet balances
mid-snapshot: a REPAY consumes the debt token before a later ``amount="all"``
sweep resolves against the same snapshot. The memoized balance then
over-resolves by exactly the repaid amount and the sweep fails to compile
(found by the looping fixture's Anvil E2E run). ``invalidate_balance`` evicts
the snapshot-level memo so the next read re-queries the provider.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.market import MarketSnapshot
from almanak.framework.market.models import TokenBalance


class _Provider:
    """Callable balance provider whose value can change between reads."""

    def __init__(self, initial: Decimal) -> None:
        self.value = initial
        self.calls = 0

    def __call__(self, token: str) -> Decimal:
        self.calls += 1
        return self.value


def _snapshot(provider: _Provider) -> MarketSnapshot:
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x" + "11" * 20,
        balance_provider=provider,
    )


def test_invalidate_balance_forces_provider_requery() -> None:
    provider = _Provider(Decimal("50.356096"))
    snapshot = _snapshot(provider)

    first = snapshot.balance("USDT")
    assert first.balance == Decimal("50.356096")

    # The wallet changed (a REPAY consumed 1.556098) but the memo is stale.
    provider.value = Decimal("48.799998")
    cached = snapshot.balance("USDT")
    assert cached.balance == Decimal("50.356096"), "memoized read expected before invalidation"

    snapshot.invalidate_balance("USDT")
    fresh = snapshot.balance("USDT")
    assert fresh.balance == Decimal("48.799998")


def test_invalidate_balance_clears_prepopulated_value() -> None:
    provider = _Provider(Decimal("7"))
    snapshot = _snapshot(provider)
    snapshot.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("100"), balance_usd=Decimal("100")))

    assert snapshot.balance("USDC").balance == Decimal("100")
    snapshot.invalidate_balance("USDC")
    assert snapshot.balance("USDC").balance == Decimal("7")


def test_invalidate_balance_unknown_token_is_noop() -> None:
    provider = _Provider(Decimal("1"))
    snapshot = _snapshot(provider)
    snapshot.invalidate_balance("WETH")  # nothing cached — must not raise
    assert snapshot.balance("WETH").balance == Decimal("1")


def test_invalidate_balance_noop_without_provider() -> None:
    """Paper/dry-run snapshots have NO balance provider — simulated balances
    ARE the memo. Eviction must no-op, or every later read raises ValueError
    instead of serving the (correct, simulated) value."""
    snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x" + "11" * 20)
    snapshot.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("100"), balance_usd=Decimal("100")))

    snapshot.invalidate_balance("USDC")
    assert snapshot.balance("USDC").balance == Decimal("100")
