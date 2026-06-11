"""Regression tests for BUG-39 — zero-balance amount='all' swap teardown skip.

A HOLD-state strategy whose teardown logic unconditionally emits a
``swap_out`` intent (e.g. ``pancakeswap_rsi_bsc`` selling the base token
it never bought) used to mark the entire teardown FAILED — the inner
``balance is 0`` check inside ``_execute_intents.execute_at_slippage``
returned ``ExecutionAttempt(success=False)``, which the outer counter at
``failed += 1`` propagated into ``TeardownResult(success=False)``.

This file verifies that ``TeardownManager._execute_intents`` now
short-circuits at the outer-loop level via
``_zero_balance_swap_skip_reason`` and counts the zero-balance intent as
a no-op success — mirroring ``runner_teardown.execute_teardown_inline``'s
``if balance_value <= 0: continue`` branch.

The QA April 29 batch reported 11 BUG-39 incidents across 8 chain/
protocol combos; the most reproducible class (HOLD + amount='all'
swap-out) is what this fix targets. Phantom-position cases require
strategy-side fixes and are out of scope here.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.teardown.teardown_manager import _zero_balance_swap_skip_reason


def _market_with_balance(token: str, balance: Decimal | int | float):
    """Build a minimal market double whose ``.balance(<expected_token>)``
    returns the requested amount and whose ``.balance(<other>)`` raises.

    Token-aware on purpose: this lets the assertions verify that
    ``_zero_balance_swap_skip_reason`` actually queries the intent's
    ``from_token`` / ``token`` field, rather than relying on a mock that
    returns the same balance for any input.
    """

    bal = SimpleNamespace(balance=Decimal(str(balance)))

    def _lookup(arg_token: str):
        if arg_token != token:
            raise AssertionError(f"market.balance() called with {arg_token!r}; expected {token!r}")
        return bal

    market = MagicMock()
    market.balance = MagicMock(side_effect=_lookup)
    return market


class TestZeroBalanceSkipHelperObjectIntents:
    """Object-style swap intents (the live-runtime shape)."""

    def test_returns_skip_reason_when_swap_all_with_zero_balance(self):
        intent = SimpleNamespace(
            intent_type="SWAP",
            amount="all",
            from_token="WETH",
        )
        market = _market_with_balance("WETH", 0)
        reason = _zero_balance_swap_skip_reason(intent, market)
        assert reason is not None
        assert "WETH" in reason
        assert "0" in reason

    def test_returns_none_when_swap_all_with_positive_balance(self):
        intent = SimpleNamespace(
            intent_type="SWAP",
            amount="all",
            from_token="WETH",
        )
        market = _market_with_balance("WETH", Decimal("1.5"))
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_returns_none_when_amount_is_explicit_decimal(self):
        intent = SimpleNamespace(
            intent_type="SWAP",
            amount=Decimal("1.0"),
            from_token="WETH",
        )
        market = _market_with_balance("WETH", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_returns_none_when_market_is_none(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="WETH")
        assert _zero_balance_swap_skip_reason(intent, None) is None

    def test_falls_back_to_token_when_from_token_absent(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all", token="USDC")
        market = _market_with_balance("USDC", 0)
        reason = _zero_balance_swap_skip_reason(intent, market)
        assert reason is not None
        assert "USDC" in reason

    def test_returns_none_when_no_token_resolvable(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all")
        market = _market_with_balance("UNUSED", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None


class TestZeroBalanceSkipHelperNonSwapIntents:
    """Only SWAP intents may be short-circuited. Every other intent type
    (WITHDRAW/REPAY/LP_CLOSE/PERP_CLOSE/BRIDGE/...) resolves ``amount='all'``
    against a protocol or cross-chain balance, not the wallet — the compiler
    and the inner slippage manager handle those."""

    def test_withdraw_intent_not_skipped_even_when_wallet_balance_zero(self):
        intent = SimpleNamespace(
            intent_type="WITHDRAW",
            amount="all",
            token="aUSDC",
        )
        market = _market_with_balance("aUSDC", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_repay_intent_not_skipped_even_when_wallet_balance_zero(self):
        intent = SimpleNamespace(
            intent_type="REPAY",
            amount="all",
            token="USDC",
        )
        market = _market_with_balance("USDC", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_perp_close_not_skipped_even_when_wallet_balance_zero(self):
        """Regression: a PERP_CLOSE with amount='all' must reach the connector
        — perp position size lives in the perp contract, not the wallet."""
        intent = SimpleNamespace(
            intent_type="PERP_CLOSE",
            amount="all",
            from_token="USDC",
        )
        market = _market_with_balance("USDC", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_lp_close_not_skipped_even_when_wallet_balance_zero(self):
        intent = SimpleNamespace(
            intent_type="LP_CLOSE",
            amount="all",
            from_token="WETH",
        )
        market = _market_with_balance("WETH", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_bridge_not_skipped_even_when_wallet_balance_zero(self):
        intent = SimpleNamespace(
            intent_type="BRIDGE",
            amount="all",
            from_token="USDC",
        )
        market = _market_with_balance("USDC", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_withdraw_all_flag_short_circuits(self):
        intent = SimpleNamespace(
            intent_type="SWAP",  # type misclassified — flag still wins
            amount="all",
            from_token="WETH",
            withdraw_all=True,
        )
        market = _market_with_balance("WETH", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None


class TestZeroBalanceSkipHelperDictIntents:
    """Dict-shape intents (resumed-from-JSON path)."""

    def test_dict_swap_with_zero_balance_returns_reason(self):
        intent = {
            "intent_type": "SWAP",
            "amount": "all",
            "from_token": "USDT",
        }
        market = _market_with_balance("USDT", 0)
        reason = _zero_balance_swap_skip_reason(intent, market)
        assert reason is not None
        assert "USDT" in reason

    def test_dict_swap_with_positive_balance_returns_none(self):
        intent = {
            "intent_type": "SWAP",
            "amount": "all",
            "from_token": "USDT",
        }
        market = _market_with_balance("USDT", Decimal("100"))
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_dict_non_swap_amount_all_returns_none(self):
        """Regression: dict-shape non-SWAP intents must not be short-circuited."""
        intent = {
            "intent_type": "PERP_CLOSE",
            "amount": "all",
            "from_token": "USDC",
        }
        market = _market_with_balance("USDC", 0)
        assert _zero_balance_swap_skip_reason(intent, market) is None


class TestZeroBalanceSkipHelperMarketErrors:
    """Market lookup errors must not crash the pre-flight check — fall through
    to the inner check inside the slippage manager."""

    def test_market_balance_raises_returns_none(self):
        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="WETH")
        market = MagicMock()
        market.balance = MagicMock(side_effect=RuntimeError("token not registered"))
        assert _zero_balance_swap_skip_reason(intent, market) is None


class TestSkipReasonReadsLiveBalance:
    """PR #2726 (Codex finding) — the skip check must read the LIVE balance.

    Earlier intents in the same teardown sequence mutate the wallet after the
    MarketSnapshot was built: a staircase REPAY consumes the wallet's debt
    token before the residual sweep's skip check runs. With a stale positive
    memo the no-op skip never fires and the zero-balance sweep falls through
    to the slippage-escalation loop, failing a teardown whose risk is already
    removed (also the VIB-5049 duplicate-consolidation loop mechanism).
    """

    def test_stale_positive_memo_is_evicted_and_skip_fires(self):
        from almanak.framework.market import MarketSnapshot

        live = {"USDT": Decimal("48.8")}

        def provider(token: str) -> Decimal:
            return live.get(token, Decimal("0"))

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "11" * 20,
            balance_provider=provider,
        )
        assert market.balance("USDT").balance == Decimal("48.8")  # memoized

        # A REPAY consumed the whole wallet balance after the snapshot memo.
        live["USDT"] = Decimal("0")

        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="USDT", withdraw_all=False)
        reason = _zero_balance_swap_skip_reason(intent, market)
        assert reason is not None and "USDT" in reason

    def test_live_positive_balance_still_executes(self):
        from almanak.framework.market import MarketSnapshot

        live = {"USDT": Decimal("0")}

        def provider(token: str) -> Decimal:
            return live.get(token, Decimal("0"))

        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "11" * 20,
            balance_provider=provider,
        )
        assert market.balance("USDT").balance == Decimal("0")  # memoized zero

        # A WITHDRAW landed tokens in the wallet after the memo.
        live["USDT"] = Decimal("5")

        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="USDT", withdraw_all=False)
        assert _zero_balance_swap_skip_reason(intent, market) is None

    def test_market_without_invalidate_balance_still_works(self):
        market = _market_with_balance("USDT", 0)
        # Plain mock: getattr guard must tolerate any shape.
        del market.invalidate_balance  # force AttributeError path on MagicMock
        intent = SimpleNamespace(intent_type="SWAP", amount="all", from_token="USDT", withdraw_all=False)
        reason = _zero_balance_swap_skip_reason(intent, market)
        assert reason is not None
