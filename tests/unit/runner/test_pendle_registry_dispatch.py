"""Runner-side Pendle registry dispatch tests (TD-03 / VIB-5461).

Drives ``StrategyRunner._maybe_save_ledger_with_registry_pendle`` by binding the
unbound method to a runner-shaped ``SimpleNamespace`` (same pattern as
``test_lending_registry_dispatch.py``) so each branch runs without the full boot
pipeline. Covers:

- PT buy (SWAP, to_token=PT-…) → open row; PT sell (SWAP, from_token=PT-…) and
  PT redeem (WITHDRAW, PT input leg) → closed row — kind='pt', symbol anchor.
- LP_OPEN → open row; LP_CLOSE → closed row — kind='lp', market anchor.
- Cutover-not-active and failed-on-chain-TX path-misses fall back (False).
- A non-PT Pendle swap (YT/SY) and an LP intent without a receipt market anchor
  fall back (Empty ≠ Zero — never a fabricated identity).
- All rows land in the isolated swap-primitive partition (Primitive.SWAP /
  swap / pendle@v1).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration.backfill import (
    physical_identity_hash_pendle,
    semantic_grouping_key_pendle,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.runner.strategy_runner import StrategyRunner

_dispatch = StrategyRunner._maybe_save_ledger_with_registry_pendle

CHAIN = "ethereum"
DEP = "dep:pendle1"
SYMBOL = "PT-wstETH-25JUN2026"


def _runner(*, cutover_active: bool = True) -> SimpleNamespace:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner.state_manager = SimpleNamespace()
    runner._cutover_complete_cache = {(Primitive.SWAP, "pendle")} if cutover_active else set()
    # Bind the real builders / classifiers from the class.
    runner._classify_pendle_registry = StrategyRunner._classify_pendle_registry.__get__(runner, SimpleNamespace)
    runner._pendle_lp_market_anchor = StrategyRunner._pendle_lp_market_anchor
    runner._build_pendle_registry_row = StrategyRunner._build_pendle_registry_row.__get__(runner, SimpleNamespace)
    runner._extract_block_number_from_result = StrategyRunner._extract_block_number_from_result
    return runner


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id=DEP, chain=CHAIN)


def _swap_intent(*, from_token: str = "", to_token: str = "", protocol: str = "pendle") -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        protocol=protocol,
        from_token=from_token,
        to_token=to_token,
        registry_handle=None,
    )


def _lp_intent(intent_type: str, *, protocol: str = "pendle") -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        protocol=protocol,
        registry_handle=None,
    )


def _result(
    *, success: bool = True, block: int = 42, lp_market: str | None = None, intent_type: str = ""
) -> SimpleNamespace:
    res = SimpleNamespace(
        success=success,
        transaction_receipt={"logs": [], "blockNumber": block},
        transaction_results=[],
        extracted_data=None,
    )
    if lp_market is not None:
        key = "lp_open_data" if intent_type == "LP_OPEN" else "lp_close_data"
        setattr(res, key, SimpleNamespace(market_address=lp_market))
    return res


def _entry() -> SimpleNamespace:
    return SimpleNamespace(tx_hash="0xdeadbeef")


async def _run(
    runner: SimpleNamespace,
    intent: SimpleNamespace,
    result: SimpleNamespace,
    intent_type: str,
    save_spy: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> bool:
    monkeypatch.setattr("almanak.framework.accounting.commit.save_ledger_and_registry", save_spy)
    return await _dispatch(
        runner,
        strategy=_strategy(),
        intent=intent,
        result=result,
        success=True,
        entry=_entry(),
        intent_type_str=intent_type,
    )


def _saved_row(save_spy: AsyncMock) -> RegistryRow:
    assert save_spy.await_count == 1
    return save_spy.await_args.kwargs["registry"]


# =============================================================================
# PT path
# =============================================================================


@pytest.mark.asyncio
async def test_pt_buy_opens_holding(monkeypatch) -> None:
    spy = AsyncMock()
    ok = await _run(_runner(), _swap_intent(to_token=SYMBOL), _result(), "SWAP", spy, monkeypatch)
    assert ok is True
    row = _saved_row(spy)
    assert row.primitive == Primitive.SWAP
    assert row.accounting_category == AccountingCategory.SWAP
    assert row.grouping_policy_version == "pendle@v1"
    assert row.status == "open"
    assert row.payload["kind"] == "pt"
    assert row.payload["market_id"] == SYMBOL.lower()
    assert row.payload["pt_symbol"] == SYMBOL.lower()
    assert row.payload["protocol"] == "pendle"
    assert "maturity_ts" not in row.payload  # maturity is intrinsic to the symbol anchor
    assert row.physical_identity_hash == physical_identity_hash_pendle(chain=CHAIN, anchor=SYMBOL.lower(), kind="pt")
    assert row.semantic_grouping_key == semantic_grouping_key_pendle(chain=CHAIN, anchor=SYMBOL.lower(), kind="pt")
    assert row.opened_at_block == 42
    assert row.opened_tx == "0xdeadbeef"
    assert row.closed_tx is None


@pytest.mark.asyncio
async def test_pt_sell_closes_holding(monkeypatch) -> None:
    spy = AsyncMock()
    ok = await _run(_runner(), _swap_intent(from_token=SYMBOL), _result(), "SWAP", spy, monkeypatch)
    assert ok is True
    row = _saved_row(spy)
    assert row.payload["kind"] == "pt"
    assert row.status == "closed"
    assert row.closed_at_block == 42
    assert row.closed_tx == "0xdeadbeef"
    # OPEN-side anchors are left to the UPSERT's preserve-on-conflict.
    assert row.opened_at_block is None
    assert row.opened_tx is None


@pytest.mark.asyncio
async def test_pt_redeem_withdraw_closes_holding(monkeypatch) -> None:
    from decimal import Decimal

    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg, PrimitiveMoneyLegs
    from almanak.framework.accounting.measured import MeasuredMoney

    legs = PrimitiveMoneyLegs(legs=[PrimitiveMoneyLeg.input(SYMBOL, MeasuredMoney.measured(Decimal("1")))])
    intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="WITHDRAW"),
        protocol="pendle",
        from_token="",
        to_token="",
        registry_handle=None,
    )
    result = _result()
    result.extracted_data = {"primitive_money_legs": legs}
    spy = AsyncMock()
    ok = await _run(_runner(), intent, result, "WITHDRAW", spy, monkeypatch)
    assert ok is True
    row = _saved_row(spy)
    assert row.payload["kind"] == "pt"
    assert row.payload["market_id"] == SYMBOL.lower()
    assert row.status == "closed"


@pytest.mark.asyncio
async def test_non_pt_pendle_swap_falls_back(monkeypatch) -> None:
    # A YT/SY swap (neither side is a PT-) is not a tracked PT holding → fall back.
    spy = AsyncMock()
    ok = await _run(
        _runner(), _swap_intent(from_token="SY-wstETH", to_token="WETH"), _result(), "SWAP", spy, monkeypatch
    )
    assert ok is False
    assert spy.await_count == 0


# =============================================================================
# LP path
# =============================================================================


@pytest.mark.asyncio
async def test_lp_open_opens_holding(monkeypatch) -> None:
    spy = AsyncMock()
    result = _result(lp_market="0xMARKET", intent_type="LP_OPEN")
    ok = await _run(_runner(), _lp_intent("LP_OPEN"), result, "LP_OPEN", spy, monkeypatch)
    assert ok is True
    row = _saved_row(spy)
    assert row.payload["kind"] == "lp"
    assert row.payload["market_id"] == "0xmarket"
    assert row.status == "open"
    assert row.physical_identity_hash == physical_identity_hash_pendle(chain=CHAIN, anchor="0xmarket", kind="lp")


@pytest.mark.asyncio
async def test_lp_close_closes_holding(monkeypatch) -> None:
    spy = AsyncMock()
    result = _result(lp_market="0xMARKET", intent_type="LP_CLOSE")
    ok = await _run(_runner(), _lp_intent("LP_CLOSE"), result, "LP_CLOSE", spy, monkeypatch)
    assert ok is True
    row = _saved_row(spy)
    assert row.payload["kind"] == "lp"
    assert row.status == "closed"


@pytest.mark.asyncio
async def test_lp_without_market_anchor_falls_back(monkeypatch) -> None:
    # No lp_open_data / market_address on the receipt → Empty ≠ Zero → fall back.
    spy = AsyncMock()
    ok = await _run(_runner(), _lp_intent("LP_OPEN"), _result(), "LP_OPEN", spy, monkeypatch)
    assert ok is False
    assert spy.await_count == 0


# =============================================================================
# Path-applicability gates
# =============================================================================


@pytest.mark.asyncio
async def test_cutover_not_active_falls_back(monkeypatch) -> None:
    spy = AsyncMock()
    ok = await _run(_runner(cutover_active=False), _swap_intent(to_token=SYMBOL), _result(), "SWAP", spy, monkeypatch)
    assert ok is False
    assert spy.await_count == 0


@pytest.mark.asyncio
async def test_failed_onchain_tx_not_recorded(monkeypatch) -> None:
    spy = AsyncMock()
    ok = await _run(_runner(), _swap_intent(to_token=SYMBOL), _result(success=False), "SWAP", spy, monkeypatch)
    assert ok is False
    assert spy.await_count == 0
