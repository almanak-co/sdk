"""Runner-side perp registry dispatch tests (TD-02 / VIB-5460).

Drives ``StrategyRunner._maybe_save_ledger_with_registry_perp`` by binding the
unbound method to a runner-shaped ``SimpleNamespace`` (same pattern as
``test_lending_registry_dispatch.py``) so each branch runs without the full boot
pipeline. Covers:

- PERP_OPEN → open row keyed on the venue position key, with market / collateral
  / direction / size + Primitive.PERP + perp@v1 grouping.
- Full PERP_CLOSE (no size_usd) → closed row with the SAME physical identity →
  UPSERTs the open row closed; a PARTIAL (sized) PERP_CLOSE leaves the position
  untouched (returns False → falls back to save_ledger_entry).
- Cutover-not-active / non-enabled-protocol / failed-TX / missing-position-key
  path-misses fall back (False).
- The row SHAPE is protocol-agnostic — exercised via the same builder.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration.backfill import (
    physical_identity_hash_perp,
    semantic_grouping_key_perp,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.runner.strategy_runner import StrategyRunner

_dispatch = StrategyRunner._maybe_save_ledger_with_registry_perp
_build_row = StrategyRunner._build_perp_registry_row
_is_full_exit = StrategyRunner._perp_close_is_full_exit
_position_key = StrategyRunner._perp_position_key

CHAIN = "arbitrum"
DEP = "dep:perp1"
KEY = "0xPositionKey"


def _runner(*, cutover_active: bool = True) -> SimpleNamespace:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner.state_manager = SimpleNamespace()
    runner._cutover_complete_cache = {(Primitive.PERP, "perp")} if cutover_active else set()
    runner._build_perp_registry_row = _build_row.__get__(runner, SimpleNamespace)
    runner._perp_close_is_full_exit = StrategyRunner._perp_close_is_full_exit
    runner._perp_position_key = StrategyRunner._perp_position_key
    runner._extract_block_number_from_result = StrategyRunner._extract_block_number_from_result
    return runner


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id=DEP, chain=CHAIN)


def _open_intent(
    *,
    protocol: str = "gmx_v2",
    market: str = "ETH/USD",
    collateral_token: str = "USDC",
    collateral_amount: Any = "5",
    size_usd: Any = "10",
    is_long: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="PERP_OPEN"),
        protocol=protocol,
        market=market,
        collateral_token=collateral_token,
        collateral_amount=collateral_amount,
        size_usd=size_usd,
        is_long=is_long,
        position_id=None,
        registry_handle=None,
    )


def _close_intent(
    *,
    protocol: str = "gmx_v2",
    market: str = "ETH/USD",
    collateral_token: str = "USDC",
    size_usd: Any = None,
    is_long: bool = True,
    position_id: Any = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="PERP_CLOSE"),
        protocol=protocol,
        market=market,
        collateral_token=collateral_token,
        size_usd=size_usd,
        # PerpCloseIntent.close_full_position == (size_usd is None).
        close_full_position=(size_usd is None),
        is_long=is_long,
        position_id=position_id,
        registry_handle=None,
    )


def _result(*, success: bool = True, block: int = 42, position_id: Any = KEY) -> SimpleNamespace:
    return SimpleNamespace(
        success=success,
        position_id=position_id,
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


# =============================================================================
# helpers
# =============================================================================


def test_full_exit_helper() -> None:
    assert _is_full_exit(_close_intent(size_usd=None)) is True
    assert _is_full_exit(_close_intent(size_usd="5")) is False


def test_position_key_prefers_result_then_intent() -> None:
    assert _position_key(_result(position_id="0xAAA"), _open_intent()) == "0xAAA"
    # Result missing → fall back to an explicit intent position_id.
    assert _position_key(_result(position_id=None), _close_intent(position_id="0xBBB")) == "0xBBB"
    # Neither → empty (no anchor).
    assert _position_key(_result(position_id=""), _close_intent(position_id=None)) == ""


# =============================================================================
# OPEN side
# =============================================================================


@pytest.mark.asyncio
async def test_perp_open_writes_open_row(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _open_intent(), _result(), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is True
    save_spy.assert_awaited_once()
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert save_spy.await_args.kwargs["mode"] == "registry"
    assert row.primitive == Primitive.PERP
    assert row.accounting_category == AccountingCategory.PERP
    assert row.grouping_policy_version == "perp@v1"
    assert row.status == "open"
    assert row.payload["position_id"] == KEY.lower()
    assert row.payload["market"] == "ETH/USD"
    assert row.payload["collateral_token"] == "USDC"
    assert row.payload["direction"] == "long"
    assert row.payload["size_usd"] == "10"
    assert row.payload["collateral_amount"] == "5"
    assert row.opened_at_block == 42
    assert row.opened_tx == "0xdeadbeef"
    assert row.closed_at_block is None
    assert row.physical_identity_hash == physical_identity_hash_perp(
        chain=CHAIN, protocol="gmx_v2", position_key=KEY
    )
    assert row.semantic_grouping_key == semantic_grouping_key_perp(
        chain=CHAIN, protocol="gmx_v2", position_key=KEY
    )


@pytest.mark.asyncio
async def test_perp_open_short_records_direction(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _open_intent(is_long=False), _result(), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert row.payload["direction"] == "short"


@pytest.mark.asyncio
async def test_perp_open_chained_collateral_not_persisted_as_number(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _open_intent(collateral_amount="all"), _result(), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    # "all" is a chained-amount sentinel — never persisted as a measured amount.
    assert "collateral_amount" not in row.payload


# =============================================================================
# CLOSE side
# =============================================================================


@pytest.mark.asyncio
async def test_full_perp_close_writes_closed_row(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _close_intent(size_usd=None), _result(block=99), "PERP_CLOSE", save_spy, monkeypatch)
    assert ok is True
    row: RegistryRow = save_spy.await_args.kwargs["registry"]
    assert row.status == "closed"
    assert row.closed_at_block == 99
    assert row.closed_tx == "0xdeadbeef"
    # OPEN-side anchors omitted on close — preserved by the ON CONFLICT UPSERT.
    assert row.opened_at_block is None
    assert row.opened_tx is None
    # Same physical identity as the matching PERP_OPEN → UPSERTs the open row closed.
    assert row.physical_identity_hash == physical_identity_hash_perp(
        chain=CHAIN, protocol="gmx_v2", position_key=KEY
    )


@pytest.mark.asyncio
async def test_partial_perp_close_leaves_position_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _close_intent(size_usd="5"), _result(), "PERP_CLOSE", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


# =============================================================================
# Path-miss gates
# =============================================================================


@pytest.mark.asyncio
async def test_cutover_not_active_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(cutover_active=False), _open_intent(), _result(), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_enabled_protocol_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _open_intent(protocol="hyperliquid"), _result(), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_tx_not_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _open_intent(), _result(success=False), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_position_key_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    save_spy = AsyncMock()
    ok = await _run(_runner(), _open_intent(), _result(position_id=None), "PERP_OPEN", save_spy, monkeypatch)
    assert ok is False
    save_spy.assert_not_awaited()


# =============================================================================
# row builder — protocol-agnostic shape
# =============================================================================


def test_build_row_shape() -> None:
    runner = _runner()
    pih = physical_identity_hash_perp(chain="avalanche", protocol="gmx_v2", position_key="0xkey")
    sgk = semantic_grouping_key_perp(chain="avalanche", protocol="gmx_v2", position_key="0xkey")
    row = runner._build_perp_registry_row(
        strategy=SimpleNamespace(deployment_id=DEP, chain="avalanche"),
        physical_identity_hash=pih,
        semantic_grouping_key=sgk,
        payload={"protocol": "gmx_v2", "position_id": "0xkey", "direction": "long", "source": "runtime"},
        status="open",
        opened_at_block=10,
        opened_tx="0xabc",
        closed_at_block=None,
        closed_tx=None,
        handle=None,
    )
    assert isinstance(row, RegistryRow)
    assert row.primitive == Primitive.PERP
    assert row.accounting_category == AccountingCategory.PERP
    assert row.chain == "avalanche"
    assert row.grouping_policy_version == "perp@v1"
