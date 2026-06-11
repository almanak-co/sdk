"""Tier-1 hardening tests for the accounting looping fixture.

Covers the D1 deliverable from ``docs/internal/AccountingStrats.md``:

* ``generate_teardown_intents`` delegates to the framework's HF-aware staircase
  (``generate_leverage_loop_teardown``) with ``consolidate_to`` = the starting
  collateral asset — the hand-rolled unconditional ``withdraw_all`` path is gone.
* Structural acceptance: with live debt on the fake market, the emitted intent
  list contains exactly one ``withdraw_all`` and it comes strictly AFTER the
  last repay; the final sweep lands in the starting asset.
* The HOLD-phase health-factor watchdog makes ``min_health_factor`` live:
  fires below the floor, holds when healthy, respects ``hf_watchdog_enabled``.
* Deleverage round selection (repay wallet debt > swap wallet collateral >
  HF-safe withdraw slice), the stuck path, and the recovery resync.

Construction mirrors ``test_lp_triple_phase_machine.py``: skip
``IntentStrategy.__init__`` (it wants runner scaffolding) and inject only the
attributes the code under test reads.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from strategies.accounting.looping.strategy import (
    PHASE_DELEVERAGING,
    PHASE_REBORROWED,
    AccountingQuantLoopingStrategy,
)


def _bare_strategy(**overrides: Any) -> AccountingQuantLoopingStrategy:
    obj = AccountingQuantLoopingStrategy.__new__(AccountingQuantLoopingStrategy)
    obj.protocol = "aave_v3"
    obj.collateral_token = "USDC"
    obj.borrow_token = "USDT"
    obj.starting_collateral_usd = Decimal("4")
    obj.target_ltv = Decimal("0.30")
    obj.swap_protocol = "uniswap_v3"
    obj.swap_slippage = Decimal("0.01")
    obj.min_health_factor = Decimal("1.5")
    obj.hf_watchdog_enabled = True
    obj._chain = "arbitrum"
    obj._deployment_id = "test"
    obj._phase = PHASE_REBORROWED
    obj._supplied_token_amount = Decimal("8")
    obj._borrowed_token_amount = Decimal("2.4")
    obj._first_borrowed_token_amount = Decimal("1.2")
    obj._second_borrowed_token_amount = Decimal("1.2")
    obj._last_swap_output = Decimal("0")
    obj._initial_balance_usd = Decimal("100")
    obj._initial_balance_token = Decimal("100")
    obj._deleverage_stuck_logged = False
    for key, value in overrides.items():
        setattr(obj, key, value)
    return obj


def _market(
    hf: str,
    *,
    collateral_usd: str = "8",
    debt_usd: str = "2.4",
    lltv: str = "0.78",
    balances: dict[str, Decimal] | None = None,
) -> SimpleNamespace:
    """Fake MarketSnapshot: 1 token == $1 so USD and token amounts coincide."""
    held = balances or {}

    def position_health(protocol: str, market_id: str, **kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            health_factor=Decimal(hf),
            collateral_value_usd=Decimal(collateral_usd),
            debt_value_usd=Decimal(debt_usd),
            lltv=Decimal(lltv),
        )

    def price(token: str) -> Decimal:
        return Decimal("1")

    def balance(token: str) -> SimpleNamespace:
        return SimpleNamespace(balance=held.get(token, Decimal("0")))

    return SimpleNamespace(position_health=position_health, price=price, balance=balance)


# ---------------------------------------------------------------------------
# Teardown delegation + structural acceptance
# ---------------------------------------------------------------------------


def test_teardown_delegates_to_staircase_with_starting_asset_sweep(monkeypatch) -> None:
    import almanak.framework.teardown.leverage_loop as leverage_loop

    captured: dict[str, Any] = {}

    def fake_helper(**kwargs: Any) -> list[str]:
        captured.update(kwargs)
        return ["SENTINEL"]

    monkeypatch.setattr(leverage_loop, "generate_leverage_loop_teardown", fake_helper)

    strat = _bare_strategy()
    market = _market("2.0")
    out = strat.generate_teardown_intents(mode=None, market=market)

    assert out == ["SENTINEL"]
    assert captured["market"] is market
    assert captured["protocol"] == "aave_v3"
    assert captured["collateral_token"] == "USDC"
    assert captured["borrow_token"] == "USDT"
    assert captured["chain"] == "arbitrum"
    assert captured["max_slippage"] == Decimal("0.01")
    assert captured["swap_protocol"] == "uniswap_v3"
    # The required acceptance criterion: the residual sweep lands in the
    # starting collateral asset, not the borrow token.
    assert captured["consolidate_to"] == "USDC"


def test_teardown_never_emits_withdraw_all_while_debt_remains() -> None:
    """Run the REAL helper against a debt-bearing fake position: exactly one
    withdraw_all, strictly after the last repay; final sweep -> starting asset.
    """
    strat = _bare_strategy()
    market = _market(
        "2.0",
        collateral_usd="8.24",
        debt_usd="2.42",
        balances={"USDT": Decimal("1.2")},  # liquid second borrow in the wallet
    )
    intents = strat.generate_teardown_intents(mode=None, market=market)

    kinds = [type(i).__name__ for i in intents]
    withdraw_all_indexes = [idx for idx, intent in enumerate(intents) if getattr(intent, "withdraw_all", False)]
    repay_indexes = [idx for idx, kind in enumerate(kinds) if kind == "RepayIntent"]

    assert repay_indexes, f"expected repays in {kinds}"
    assert len(withdraw_all_indexes) == 1, f"expected exactly one withdraw_all in {kinds}"
    assert withdraw_all_indexes[0] > max(repay_indexes), (
        f"withdraw_all at {withdraw_all_indexes[0]} must come after the last repay at {max(repay_indexes)}: {kinds}"
    )

    final = intents[-1]
    assert type(final).__name__ == "SwapIntent"
    assert final.from_token == "USDT" and final.to_token == "USDC"
    assert final.amount == "all"


# ---------------------------------------------------------------------------
# HOLD-phase health-factor watchdog
# ---------------------------------------------------------------------------


def test_watchdog_holds_when_healthy() -> None:
    strat = _bare_strategy()
    intent = strat.decide(_market("2.0"))
    assert intent.intent_type.value == "HOLD"
    assert strat._phase == PHASE_REBORROWED


def test_watchdog_disabled_never_enters_deleverage() -> None:
    strat = _bare_strategy(hf_watchdog_enabled=False)
    intent = strat.decide(_market("1.1"))
    assert intent.intent_type.value == "HOLD"
    assert strat._phase == PHASE_REBORROWED


def test_watchdog_health_unavailable_holds_without_phase_change() -> None:
    strat = _bare_strategy()
    market = _market("2.0")

    def broken(protocol: str, market_id: str, **kwargs: Any) -> SimpleNamespace:
        raise RuntimeError("gateway read failed")

    market.position_health = broken
    intent = strat.decide(market)
    assert intent.intent_type.value == "HOLD"
    assert strat._phase == PHASE_REBORROWED


def test_watchdog_enters_deleverage_below_floor_with_safe_withdraw() -> None:
    # Wallet holds nothing -> the first deleverage round is an HF-safe withdraw.
    strat = _bare_strategy()
    intent = strat.decide(_market("1.2"))

    assert strat._phase == PHASE_DELEVERAGING
    assert intent.intent_type.value == "WITHDRAW"
    assert not getattr(intent, "withdraw_all", False)
    # safe slice = collateral - floor*debt/lltv = 8 - 1.05*2.4/0.78 ~= 4.769;
    # needed = debt/(1-slippage) = 2.4/0.99 ~= 2.4242 -> withdraw min(...)
    assert Decimal("2.42") < intent.amount < Decimal("2.43")


def test_deleverage_repays_wallet_borrow_first_partial() -> None:
    strat = _bare_strategy(_phase=PHASE_DELEVERAGING)
    market = _market("1.2", balances={"USDT": Decimal("0.5")})
    intent = strat._handle_deleverage(market)

    assert intent.intent_type.value == "REPAY"
    assert not getattr(intent, "repay_full", False)
    # Partial repay just under the wallet balance (1% safety haircut).
    assert intent.amount == Decimal("0.5") * Decimal("0.99")


def test_deleverage_full_repay_when_wallet_covers_debt() -> None:
    strat = _bare_strategy(_phase=PHASE_DELEVERAGING)
    market = _market("1.2", balances={"USDT": Decimal("3.0")})
    intent = strat._handle_deleverage(market)

    assert intent.intent_type.value == "REPAY"
    assert getattr(intent, "repay_full", False)


def test_deleverage_swaps_wallet_collateral_when_no_borrow_held() -> None:
    strat = _bare_strategy(_phase=PHASE_DELEVERAGING)
    market = _market("1.2", balances={"USDC": Decimal("2.0")})
    intent = strat._handle_deleverage(market)

    assert intent.intent_type.value == "SWAP"
    assert intent.from_token == "USDC" and intent.to_token == "USDT"
    assert intent.amount == "all"


def test_deleverage_stuck_holds_in_phase_and_logs_once() -> None:
    strat = _bare_strategy(_phase=PHASE_DELEVERAGING)
    # safe slice = 8 - 1.05*6/0.78 < 0 -> nothing can be withdrawn safely.
    market = _market("1.02", debt_usd="6.0")

    first = strat._handle_deleverage(market)
    second = strat._handle_deleverage(market)

    assert first.intent_type.value == "HOLD"
    assert second.intent_type.value == "HOLD"
    assert strat._phase == PHASE_DELEVERAGING
    assert strat._deleverage_stuck_logged is True


def test_deleverage_recovery_resyncs_totals_and_resumes_hold() -> None:
    strat = _bare_strategy(_phase=PHASE_DELEVERAGING, _deleverage_stuck_logged=True)
    market = _market("1.8", collateral_usd="6.5", debt_usd="1.1")
    intent = strat._handle_deleverage(market)

    assert intent.intent_type.value == "HOLD"
    assert strat._phase == PHASE_REBORROWED
    assert strat._deleverage_stuck_logged is False
    # Intent-echo totals resynced from on-chain truth (price = $1).
    assert strat._supplied_token_amount == Decimal("6.5")
    assert strat._borrowed_token_amount == Decimal("1.1")


def test_deleveraging_phase_persists_round_trip() -> None:
    strat = _bare_strategy(_phase=PHASE_DELEVERAGING)
    state = strat.get_persistent_state()
    assert state["_phase"] == PHASE_DELEVERAGING

    restored = _bare_strategy()
    restored.load_persistent_state(state)
    assert restored._phase == PHASE_DELEVERAGING
