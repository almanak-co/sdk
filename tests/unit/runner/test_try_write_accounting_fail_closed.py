"""Tests for VIB-3485: fail-closed semantics for _try_write_* accounting wrappers.

Verifies that in live mode, a store write failure propagates as
AccountingPersistenceError instead of being silently swallowed.

Covered wrappers (all three were previously best-effort in all modes):
- _try_write_pendle_lp_accounting   (LP_OPEN / LP_CLOSE)
- _try_write_pendle_pt_sell_accounting  (PT_SELL — pre-maturity sale)
- _try_write_pendle_pt_redeem_accounting  (PT_REDEEM — maturity redemption)

The already-hardened wrappers (_try_write_lending_accounting for BORROW/REPAY/DELEVERAGE
and _try_write_pendle_pt_buy_accounting for PT_BUY) are not re-tested here.

Test matrix for each wrapper:
1. Live mode + store raises → AccountingPersistenceError raised (fail-closed).
2. Paper mode + store raises → warning logged, no exception (best-effort).
3. Live mode + store returns False → AccountingPersistenceError raised.
4. Live mode + event builder returns None → no write attempted, no exception.
5. Paper mode + store returns False → save awaited, WARNING logged, no exception.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner
from almanak.framework.state.exceptions import AccountingPersistenceError


# =============================================================================
# Helpers
# =============================================================================


def _make_runner(*, paper_mode: bool = False, dry_run: bool = False) -> StrategyRunner:
    """Return a StrategyRunner whose mode is controlled by the kwargs.

    No paper_mode + no dry_run → LIVE (fail-closed).
    paper_mode=True → PAPER (best-effort).
    """
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
    )
    if paper_mode:
        # RunnerConfig doesn't have paper_mode; paper runners set it as an
        # attribute. Simulate via SimpleNamespace override approach used elsewhere.
        config = SimpleNamespace(  # type: ignore[assignment]
            default_interval_seconds=0,
            enable_state_persistence=False,
            enable_alerting=False,
            dry_run=False,
            paper_mode=True,
        )

    state_manager = MagicMock()
    # By default make save_accounting_event work; individual tests override it.
    state_manager.save_accounting_event = AsyncMock(return_value=True)

    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        config=config,
    )
    return runner


def _make_strategy(strategy_id: str = "test-strategy") -> MagicMock:
    s = MagicMock()
    s.strategy_id = strategy_id
    s.chain = "ethereum"
    s.wallet_address = "0x" + "ab" * 20
    s.deployment_id = strategy_id
    return s


def _make_intent(intent_type_value: str, protocol: str = "") -> MagicMock:
    intent = MagicMock()
    it = MagicMock()
    it.value = intent_type_value
    intent.intent_type = it
    intent.protocol = protocol
    return intent


# =============================================================================
# _try_write_pendle_lp_accounting
# =============================================================================


class TestPendleLpAccountingFailClosed:
    """_try_write_pendle_lp_accounting is fail-closed in live mode (VIB-3485)."""

    @pytest.mark.asyncio
    async def test_live_mode_store_raises_propagates(self) -> None:
        """In live mode, a store exception must propagate as AccountingPersistenceError."""
        runner = _make_runner()
        runner.state_manager.save_accounting_event = AsyncMock(
            side_effect=RuntimeError("DB connection lost")
        )
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "live"
        fake_event.event_type = "LP_OPEN"
        fake_event.market_id = "0xmarket"

        with patch(
            "almanak.framework.accounting.pendle_accounting.build_pendle_lp_accounting_event",
            return_value=fake_event,
        ):
            with pytest.raises(AccountingPersistenceError) as exc_info:
                await runner._try_write_pendle_lp_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-1"
                )

        assert exc_info.value.write_kind == "accounting"
        assert "LP" in exc_info.value.args[0] or "Pendle" in exc_info.value.args[0]

    @pytest.mark.asyncio
    async def test_live_mode_store_returns_false_propagates(self) -> None:
        """In live mode, a False return (unsupported backend) must raise."""
        runner = _make_runner()
        runner.state_manager.save_accounting_event = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = _make_intent("LP_CLOSE", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "live"
        fake_event.event_type = "LP_CLOSE"
        fake_event.market_id = "0xmarket"

        with patch(
            "almanak.framework.accounting.pendle_accounting.build_pendle_lp_accounting_event",
            return_value=fake_event,
        ):
            with pytest.raises(AccountingPersistenceError):
                await runner._try_write_pendle_lp_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-2"
                )

    @pytest.mark.asyncio
    async def test_paper_mode_store_raises_only_logs(self, caplog: Any) -> None:
        """In paper mode, a store exception must be swallowed — save attempted, WARNING logged."""
        import logging

        runner = _make_runner(paper_mode=True)
        runner.state_manager.save_accounting_event = AsyncMock(
            side_effect=RuntimeError("DB down")
        )
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "paper"
        fake_event.event_type = "LP_OPEN"
        fake_event.market_id = "0xmarket"

        with patch(
            "almanak.framework.accounting.pendle_accounting.build_pendle_lp_accounting_event",
            return_value=fake_event,
        ):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                # Must not raise — paper mode is best-effort.
                await runner._try_write_pendle_lp_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-3"
                )

        runner.state_manager.save_accounting_event.assert_awaited_once()
        assert any(r.levelno == logging.WARNING for r in caplog.records), (
            "Expected a WARNING log when store raises in paper mode"
        )

    @pytest.mark.asyncio
    async def test_paper_mode_write_false_warns(self, caplog: Any) -> None:
        """In paper mode, write() == False must emit a WARNING (not silently pass)."""
        import logging

        runner = _make_runner(paper_mode=True)
        runner.state_manager.save_accounting_event = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "paper"
        fake_event.event_type = "LP_OPEN"
        fake_event.market_id = "0xmarket"

        with patch(
            "almanak.framework.accounting.pendle_accounting.build_pendle_lp_accounting_event",
            return_value=fake_event,
        ):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                # Must not raise — paper mode is best-effort even when write() is False.
                await runner._try_write_pendle_lp_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-3b"
                )

        runner.state_manager.save_accounting_event.assert_awaited_once()
        assert any("not persisted" in r.message.lower() for r in caplog.records if r.levelno == logging.WARNING), (
            "Expected a WARNING about unsupported backend"
        )

    @pytest.mark.asyncio
    async def test_live_mode_event_builder_returns_none_no_exception(self) -> None:
        """When the event builder returns None, no write is attempted and no exception raised."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN", protocol="pendle")
        result = MagicMock()

        with patch(
            "almanak.framework.accounting.pendle_accounting.build_pendle_lp_accounting_event",
            return_value=None,
        ):
            # Must not raise, even in live mode.
            await runner._try_write_pendle_lp_accounting(
                strategy, intent, result, price_oracle=None, ledger_entry_id="lid-4"
            )

        # save_accounting_event must not have been called.
        runner.state_manager.save_accounting_event.assert_not_called()


# =============================================================================
# _try_write_pendle_pt_sell_accounting
# =============================================================================


class TestPendlePtSellAccountingFailClosed:
    """_try_write_pendle_pt_sell_accounting is fail-closed in live mode (VIB-3485)."""

    @pytest.mark.asyncio
    async def test_live_mode_store_raises_propagates(self) -> None:
        """In live mode, a store exception must propagate as AccountingPersistenceError."""
        runner = _make_runner()
        runner.state_manager.save_accounting_event = AsyncMock(
            side_effect=RuntimeError("write timeout")
        )
        strategy = _make_strategy()
        intent = _make_intent("SWAP", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "live"
        fake_event.pt_token = "PT-stETH-26DEC2025"
        fake_event.pt_price = 0.95

        with patch(
            "almanak.framework.accounting.pendle_pt_sell_accounting.build_pendle_pt_sell_accounting_event",
            return_value=fake_event,
        ):
            with pytest.raises(AccountingPersistenceError) as exc_info:
                await runner._try_write_pendle_pt_sell_accounting(
                    strategy, intent, result, ledger_entry_id="lid-5"
                )

        assert exc_info.value.write_kind == "accounting"

    @pytest.mark.asyncio
    async def test_live_mode_store_returns_false_propagates(self) -> None:
        """In live mode, False return (unsupported backend) must raise."""
        runner = _make_runner()
        runner.state_manager.save_accounting_event = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = _make_intent("SWAP", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "live"
        fake_event.pt_token = "PT-stETH-26DEC2025"
        fake_event.pt_price = 0.95

        with patch(
            "almanak.framework.accounting.pendle_pt_sell_accounting.build_pendle_pt_sell_accounting_event",
            return_value=fake_event,
        ):
            with pytest.raises(AccountingPersistenceError):
                await runner._try_write_pendle_pt_sell_accounting(
                    strategy, intent, result, ledger_entry_id="lid-6"
                )

    @pytest.mark.asyncio
    async def test_paper_mode_store_raises_only_logs(self, caplog: Any) -> None:
        """In paper mode, a store exception must be swallowed — save attempted, WARNING logged."""
        import logging

        runner = _make_runner(paper_mode=True)
        runner.state_manager.save_accounting_event = AsyncMock(
            side_effect=RuntimeError("DB down")
        )
        strategy = _make_strategy()
        intent = _make_intent("SWAP", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "paper"
        fake_event.pt_token = "PT-stETH-26DEC2025"
        fake_event.pt_price = 0.95

        with patch(
            "almanak.framework.accounting.pendle_pt_sell_accounting.build_pendle_pt_sell_accounting_event",
            return_value=fake_event,
        ):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                await runner._try_write_pendle_pt_sell_accounting(
                    strategy, intent, result, ledger_entry_id="lid-7"
                )

        runner.state_manager.save_accounting_event.assert_awaited_once()
        assert any(r.levelno == logging.WARNING for r in caplog.records), (
            "Expected a WARNING log when store raises in paper mode"
        )

    @pytest.mark.asyncio
    async def test_paper_mode_write_false_warns(self, caplog: Any) -> None:
        """In paper mode, write() == False must emit a WARNING (not silently pass)."""
        import logging

        runner = _make_runner(paper_mode=True)
        runner.state_manager.save_accounting_event = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = _make_intent("SWAP", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "paper"
        fake_event.event_type = "PT_SELL"
        fake_event.market_id = "0xmarket"
        fake_event.pt_token = "PT-stETH-26DEC2025"
        fake_event.pt_price = 0.95

        with patch(
            "almanak.framework.accounting.pendle_pt_sell_accounting.build_pendle_pt_sell_accounting_event",
            return_value=fake_event,
        ):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                await runner._try_write_pendle_pt_sell_accounting(
                    strategy, intent, result, ledger_entry_id="lid-7b"
                )

        runner.state_manager.save_accounting_event.assert_awaited_once()
        assert any("not persisted" in r.message.lower() for r in caplog.records if r.levelno == logging.WARNING), (
            "Expected a WARNING about unsupported backend"
        )

    @pytest.mark.asyncio
    async def test_live_mode_event_builder_returns_none_no_exception(self) -> None:
        """When the event builder returns None (e.g. not a PT sell), no exception is raised."""
        runner = _make_runner()
        strategy = _make_strategy()
        # SWAP but not a PT token — builder returns None.
        intent = _make_intent("SWAP", protocol="uniswap_v3")
        result = MagicMock()

        with patch(
            "almanak.framework.accounting.pendle_pt_sell_accounting.build_pendle_pt_sell_accounting_event",
            return_value=None,
        ):
            await runner._try_write_pendle_pt_sell_accounting(
                strategy, intent, result, ledger_entry_id="lid-8"
            )

        runner.state_manager.save_accounting_event.assert_not_called()


# =============================================================================
# _try_write_pendle_pt_redeem_accounting
# =============================================================================


class TestPendlePtRedeemAccountingFailClosed:
    """_try_write_pendle_pt_redeem_accounting is fail-closed in live mode (VIB-3485)."""

    @pytest.mark.asyncio
    async def test_live_mode_store_raises_propagates(self) -> None:
        """In live mode, a store exception must propagate as AccountingPersistenceError."""
        runner = _make_runner()
        runner.state_manager.save_accounting_event = AsyncMock(
            side_effect=RuntimeError("disk full")
        )
        strategy = _make_strategy()
        intent = _make_intent("WITHDRAW", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "live"
        fake_event.market_id = "0xmarket"
        fake_event.realized_yield_usd = 42.5

        with patch(
            "almanak.framework.accounting.pendle_redeem_accounting.build_pendle_pt_redeem_accounting_event",
            return_value=fake_event,
        ):
            with pytest.raises(AccountingPersistenceError) as exc_info:
                await runner._try_write_pendle_pt_redeem_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-9"
                )

        assert exc_info.value.write_kind == "accounting"
        assert "PT_REDEEM" in exc_info.value.args[0] or "Pendle" in exc_info.value.args[0]

    @pytest.mark.asyncio
    async def test_live_mode_store_returns_false_propagates(self) -> None:
        """In live mode, False return (unsupported backend) must raise."""
        runner = _make_runner()
        runner.state_manager.save_accounting_event = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = _make_intent("WITHDRAW", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "live"
        fake_event.market_id = "0xmarket"
        fake_event.realized_yield_usd = 42.5

        with patch(
            "almanak.framework.accounting.pendle_redeem_accounting.build_pendle_pt_redeem_accounting_event",
            return_value=fake_event,
        ):
            with pytest.raises(AccountingPersistenceError):
                await runner._try_write_pendle_pt_redeem_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-10"
                )

    @pytest.mark.asyncio
    async def test_paper_mode_store_raises_only_logs(self, caplog: Any) -> None:
        """In paper mode, a store exception must be swallowed — save attempted, WARNING logged."""
        import logging

        runner = _make_runner(paper_mode=True)
        runner.state_manager.save_accounting_event = AsyncMock(
            side_effect=RuntimeError("network error")
        )
        strategy = _make_strategy()
        intent = _make_intent("WITHDRAW", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "paper"
        fake_event.market_id = "0xmarket"
        fake_event.realized_yield_usd = 42.5

        with patch(
            "almanak.framework.accounting.pendle_redeem_accounting.build_pendle_pt_redeem_accounting_event",
            return_value=fake_event,
        ):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                await runner._try_write_pendle_pt_redeem_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-11"
                )

        runner.state_manager.save_accounting_event.assert_awaited_once()
        assert any(r.levelno == logging.WARNING for r in caplog.records), (
            "Expected a WARNING log when store raises in paper mode"
        )

    @pytest.mark.asyncio
    async def test_paper_mode_write_false_warns(self, caplog: Any) -> None:
        """In paper mode, write() == False must emit a WARNING (not silently pass)."""
        import logging

        runner = _make_runner(paper_mode=True)
        runner.state_manager.save_accounting_event = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = _make_intent("WITHDRAW", protocol="pendle")
        result = MagicMock()

        fake_event = MagicMock()
        fake_event.identity.execution_mode = "paper"
        fake_event.event_type = "PT_REDEEM"
        fake_event.market_id = "0xmarket"
        fake_event.realized_yield_usd = 42.5

        with patch(
            "almanak.framework.accounting.pendle_redeem_accounting.build_pendle_pt_redeem_accounting_event",
            return_value=fake_event,
        ):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                await runner._try_write_pendle_pt_redeem_accounting(
                    strategy, intent, result, price_oracle=None, ledger_entry_id="lid-11b"
                )

        runner.state_manager.save_accounting_event.assert_awaited_once()
        assert any("not persisted" in r.message.lower() for r in caplog.records if r.levelno == logging.WARNING), (
            "Expected a WARNING about unsupported backend"
        )

    @pytest.mark.asyncio
    async def test_live_mode_event_builder_returns_none_no_exception(self) -> None:
        """When builder returns None (no RedeemPY event), no exception is raised."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("WITHDRAW", protocol="pendle")
        result = MagicMock()

        with patch(
            "almanak.framework.accounting.pendle_redeem_accounting.build_pendle_pt_redeem_accounting_event",
            return_value=None,
        ):
            await runner._try_write_pendle_pt_redeem_accounting(
                strategy, intent, result, price_oracle=None, ledger_entry_id="lid-12"
            )

        runner.state_manager.save_accounting_event.assert_not_called()
