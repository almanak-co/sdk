"""Runner-side lending registry dispatch tests (TD-04 / VIB-5462).

Drives ``StrategyRunner._maybe_save_ledger_with_registry_lending`` by binding the
unbound method to a runner-shaped ``SimpleNamespace`` (same pattern as
``test_v4_registry_dispatch.py``) so each branch runs without the full boot
pipeline. Covers:

- SUPPLY → open collateral row; BORROW → open debt row (correct market_id + leg
  + protocol + Primitive.LENDING + lending@v1 grouping).
- WITHDRAW/REPAY with amount='all' → closed row; PARTIAL numeric WITHDRAW leaves
  the leg untouched (returns False → falls back to save_ledger_entry).
- Cutover-not-active and non-enabled-protocol path-misses fall back (False).
- A failed on-chain TX is NOT recorded (chain truth gate).
- The row SHAPE is protocol-agnostic — exercised with Spark via the same builder.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration.backfill import (
    physical_identity_hash_lending,
    semantic_grouping_key_lending,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.runner.strategy_runner import StrategyRunner

_dispatch = StrategyRunner._maybe_save_ledger_with_registry_lending
_build_row = StrategyRunner._build_lending_registry_row
_is_full_exit = StrategyRunner._lending_leg_is_fully_exited
_market_token = StrategyRunner._lending_intent_market_token

CHAIN = "arbitrum"
DEP = "dep:lending1"


def _runner(*, cutover_active: bool = True, save_spy: AsyncMock | None = None) -> SimpleNamespace:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner.state_manager = SimpleNamespace()
    runner._cutover_complete_cache = {(Primitive.LENDING, "lending")} if cutover_active else set()
    # Real bound builders/helpers from the class.
    runner._build_lending_registry_row = _build_row.__get__(runner, SimpleNamespace)
    runner._lending_leg_is_fully_exited = StrategyRunner._lending_leg_is_fully_exited
    runner._lending_intent_market_token = StrategyRunner._lending_intent_market_token
    runner._extract_block_number_from_result = StrategyRunner._extract_block_number_from_result
    return runner


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id=DEP, chain=CHAIN)


def _intent(
    intent_type: str,
    *,
    protocol: str = "aave_v3",
    token: str = "USDC",
    amount: Any = "100",
    market_id: Any = None,
    withdraw_all: bool = False,
    repay_full: bool = False,
) -> SimpleNamespace:
    # BORROW names its debt asset ``borrow_token``; the others use ``token``.
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        protocol=protocol,
        token=token,
        borrow_token=token,
        amount=amount,
        market_id=market_id,
        withdraw_all=withdraw_all,
        repay_full=repay_full,
        registry_handle=None,
    )


def _result(*, success: bool = True, block: int = 42) -> SimpleNamespace:
    # A receipt with (empty) logs so ``_extract_block_number_from_result`` can
    # read ``blockNumber`` without an LP topic match.
    return SimpleNamespace(
        success=success,
        transaction_receipt={"logs": [], "blockNumber": block},
        transaction_results=[],
    )


def _entry() -> SimpleNamespace:
    return SimpleNamespace(tx_hash="0xdeadbeef")


async def _run(
    runner: SimpleNamespace,
    intent: SimpleNamespace,
    result: SimpleNamespace,
    intent_type: str,
    save_spy: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    *,
    post_state: dict | None = None,
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
        post_state=post_state,
    )


# =============================================================================
# full-exit helper
# =============================================================================


def test_full_exit_explicit_flags_and_all_sentinel() -> None:
    assert _is_full_exit(intent=_intent("WITHDRAW", amount="all"), leg="collateral", post_state=None) is True
    assert _is_full_exit(intent=_intent("WITHDRAW", withdraw_all=True, amount="50"), leg="collateral", post_state=None) is True
    assert _is_full_exit(intent=_intent("REPAY", repay_full=True, amount="50"), leg="debt", post_state=None) is True
    # Numeric partial with no post-state → not full (bias-to-open).
    assert _is_full_exit(intent=_intent("WITHDRAW", amount="50"), leg="collateral", post_state=None) is False


def test_full_exit_from_post_state_residual_dust() -> None:
    # Teardown's snapshotted full withdraw: numeric amount, no flag, but the
    # post-state collateral residual is ~0 → CLOSE (same signal as position_events).
    closed = _is_full_exit(intent=_intent("WITHDRAW", amount="1000"), leg="collateral", post_state={"collateral_value_usd": "0"})
    assert closed is True
    # A meaningful residual → still open.
    still_open = _is_full_exit(intent=_intent("WITHDRAW", amount="1000"), leg="collateral", post_state={"collateral_value_usd": "500.0"})
    assert still_open is False
    # Debt leg reads debt_value_usd; protocol-keyed nesting is normalised.
    debt_closed = _is_full_exit(intent=_intent("REPAY", amount="1000"), leg="debt", post_state={"aave_v3": {"debt_value_usd": "0.005"}})
    assert debt_closed is True


def test_market_token_borrow_uses_borrow_token() -> None:
    assert _market_token(_intent("BORROW", token="DAI"), "BORROW") == "DAI"
    assert _market_token(_intent("SUPPLY", token="USDC"), "SUPPLY") == "USDC"


# =============================================================================
# OPEN side
# =============================================================================


@pytest.mark.asyncio
async def test_supply_writes_open_collateral_row(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    runner = _runner()
    ok = await _run(runner, _intent("SUPPLY", token="USDC"), _result(), "SUPPLY", save_spy, monkeypatch)
    assert ok is True
    save_spy.assert_awaited_once()
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert save_spy.await_args.kwargs["mode"] == "registry"
    assert row.primitive == Primitive.LENDING
    assert row.accounting_category == AccountingCategory.LENDING
    assert row.grouping_policy_version == "lending@v1"
    assert row.status == "open"
    assert row.payload["leg"] == "collateral"
    assert row.payload["market_id"] == "usdc"
    assert row.payload["protocol"] == "aave_v3"
    assert row.opened_at_block == 42
    assert row.opened_tx == "0xdeadbeef"
    assert row.closed_at_block is None
    assert row.physical_identity_hash == physical_identity_hash_lending(
        chain=CHAIN, protocol="aave_v3", market_id="usdc", leg="collateral"
    )
    assert row.semantic_grouping_key == semantic_grouping_key_lending(
        chain=CHAIN, protocol="aave_v3", market_id="usdc", leg="collateral"
    )


@pytest.mark.asyncio
async def test_borrow_writes_open_debt_row(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _intent("BORROW", token="DAI"), _result(), "BORROW", save_spy, monkeypatch)
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert row.payload["leg"] == "debt"
    assert row.payload["market_id"] == "dai"
    assert row.status == "open"


@pytest.mark.asyncio
async def test_supply_with_explicit_market_id_uses_it(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    intent = _intent("SUPPLY", token="USDC", market_id="0xMARKET")
    ok = await _run(_runner(), intent, _result(), "SUPPLY", save_spy, monkeypatch)
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert row.payload["market_id"] == "0xmarket"


# =============================================================================
# CLOSE side
# =============================================================================


@pytest.mark.asyncio
async def test_full_withdraw_writes_closed_row(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    intent = _intent("WITHDRAW", token="USDC", amount="all")
    ok = await _run(_runner(), intent, _result(block=99), "WITHDRAW", save_spy, monkeypatch)
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert row.status == "closed"
    assert row.closed_at_block == 99
    assert row.closed_tx == "0xdeadbeef"
    # OPEN-side anchors omitted on close — preserved by the ON CONFLICT UPSERT.
    assert row.opened_at_block is None
    assert row.opened_tx is None
    # Same physical identity as the matching SUPPLY → UPSERTs the open row closed.
    assert row.physical_identity_hash == physical_identity_hash_lending(
        chain=CHAIN, protocol="aave_v3", market_id="usdc", leg="collateral"
    )


@pytest.mark.asyncio
async def test_teardown_numeric_full_withdraw_closes_via_post_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """The teardown scenario: a numeric (snapshotted) full WITHDRAW with no
    flag, but post-state collateral residual ~0 → the row flips closed."""
    save_spy = AsyncMock()
    intent = _intent("WITHDRAW", token="USDC", amount="1000")
    ok = await _run(
        _runner(), intent, _result(block=77), "WITHDRAW", save_spy, monkeypatch,
        post_state={"collateral_value_usd": "0"},
    )
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert row.status == "closed"
    assert row.closed_at_block == 77


@pytest.mark.asyncio
async def test_partial_withdraw_leaves_leg_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    intent = _intent("WITHDRAW", token="USDC", amount="50")  # numeric partial
    # Post-state still shows meaningful collateral → leg stays open.
    ok = await _run(_runner(), intent, _result(), "WITHDRAW", save_spy, monkeypatch, post_state={"collateral_value_usd": "500"})
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_partial_repay_leaves_leg_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    intent = _intent("REPAY", token="DAI", amount="10")
    ok = await _run(_runner(), intent, _result(), "REPAY", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


# =============================================================================
# Path-miss gates
# =============================================================================


@pytest.mark.asyncio
async def test_cutover_not_active_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(cutover_active=False), _intent("SUPPLY"), _result(), "SUPPLY", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_enabled_protocol_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    intent = _intent("SUPPLY", protocol="compound_v3")
    ok = await _run(_runner(), intent, _result(), "SUPPLY", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_tx_not_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _intent("SUPPLY"), _result(success=False), "SUPPLY", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_asset_anchor_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    intent = _intent("SUPPLY", token="")
    ok = await _run(_runner(), intent, _result(), "SUPPLY", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


# =============================================================================
# Generalisability — the row builder is protocol-agnostic (Spark)
# =============================================================================


def test_build_row_generalises_to_spark() -> None:
    runner = _runner()
    pih = physical_identity_hash_lending(chain="ethereum", protocol="spark", market_id="dai", leg="debt")
    sgk = semantic_grouping_key_lending(chain="ethereum", protocol="spark", market_id="dai", leg="debt")
    row = runner._build_lending_registry_row(
        strategy=SimpleNamespace(deployment_id=DEP, chain="ethereum"),
        physical_identity_hash=pih,
        semantic_grouping_key=sgk,
        payload={"protocol": "spark", "market_id": "dai", "leg": "debt", "source": "runtime"},
        status="open",
        opened_at_block=10,
        opened_tx="0xabc",
        closed_at_block=None,
        closed_tx=None,
        handle=None,
    )
    assert isinstance(row, RegistryRow)
    assert row.primitive == Primitive.LENDING
    assert row.chain == "ethereum"
    assert row.payload["protocol"] == "spark"
    assert row.grouping_policy_version == "lending@v1"
