"""Runner-side V4 registry dispatch tests (VIB-4583).

Drives the ``StrategyRunner`` V4 helpers by binding the unbound methods to a
runner-shaped ``SimpleNamespace`` (same pattern as
``test_build_lp_close_registry_row.py``) so each branch runs without the full
boot pipeline. Covers:

- V4 OPEN / CLOSE row builders produce the V4 identity hash + ``chain:pool_id``
  grouping + ``Primitive.LP_V4``.
- The V4 close sources its tokenId from the close INTENT (V4 closes carry no
  receipt tokenId); a missing / zero intent ``position_id`` fails closed.
- The missing-PositionManager path: ``_maybe_save_ledger_with_registry_v4``
  skips the row, WARNs ``v4_registry_no_position_manager``, returns False, and
  NEVER raises.
- ``_build_registry_row`` stamps ``univ4_lp@v1`` for ``LP_V4``, ``univ3_lp@v1``
  otherwise.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.migration.backfill import (
    physical_identity_hash_univ4,
    semantic_grouping_key_univ4,
)
from almanak.framework.primitives.types import Primitive
from almanak.framework.runner.strategy_runner import StrategyRunner

_build_v4_open = StrategyRunner._build_lp_v4_open_registry_row
_build_v4_close = StrategyRunner._build_lp_v4_close_registry_row
_dispatch_v4 = StrategyRunner._maybe_save_ledger_with_registry_v4
_build_registry_row = StrategyRunner._build_registry_row
_v4_close_intent_token_id = StrategyRunner._v4_close_intent_token_id
_lookup_open_v4 = StrategyRunner._lookup_open_v4_registry_payload

CHAIN = "base"
PM = "0x7c5f5a4bbd8fd63184577525326123b519429bdc"
POOL_ID = "0x" + "ab" * 32
TOKEN_ID = 4242


def _stub_build_registry_row(
    *,
    strategy: Any,
    primitive: Any,
    physical_identity_hash: str,
    semantic_grouping_key: str,
    payload: dict,
    status: str,
    opened_at_block: int | None,
    opened_tx: str | None,
    closed_at_block: int | None,
    closed_tx: str | None,
    handle: str | None,
) -> SimpleNamespace:
    return SimpleNamespace(
        primitive=primitive,
        physical_identity_hash=physical_identity_hash,
        semantic_grouping_key=semantic_grouping_key,
        payload=payload,
        status=status,
        opened_at_block=opened_at_block,
        opened_tx=opened_tx,
        closed_at_block=closed_at_block,
        closed_tx=closed_tx,
        handle=handle,
    )


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id="dep:1", chain=CHAIN)


def _open_payload() -> dict[str, Any]:
    return {"token_id": str(TOKEN_ID), "pool_id": POOL_ID, "position_manager": PM}


def _close_payload() -> dict[str, Any]:
    return {"token_id": str(TOKEN_ID), "pool_id": POOL_ID, "position_manager": PM, "amount0_close": "1", "amount1_close": "2"}


# =============================================================================
# V4 OPEN row builder
# =============================================================================


def test_v4_open_builds_v4_identity_and_primitive() -> None:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._extract_block_number_from_result = MagicMock(return_value=123)
    runner._build_registry_row = _stub_build_registry_row
    parser = SimpleNamespace(extract_registry_payload_open=MagicMock(return_value=_open_payload()))

    out = _build_v4_open(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(registry_handle=None),
        result=SimpleNamespace(),
        entry=SimpleNamespace(tx_hash="0x" + "ab" * 32),
        chain=CHAIN,
        position_manager=PM,
        receipt={"logs": []},
        parser=parser,
        fee_tier=500,
    )
    assert out is not None
    row, payload, token_id = out
    assert token_id == TOKEN_ID
    assert row.primitive == Primitive.LP_V4
    assert row.physical_identity_hash == physical_identity_hash_univ4(chain=CHAIN, position_manager_addr=PM, token_id=TOKEN_ID)
    assert row.semantic_grouping_key == semantic_grouping_key_univ4(chain=CHAIN, pool_id=POOL_ID)
    assert row.status == "open"


def test_v4_open_returns_none_when_parser_refuses() -> None:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._extract_block_number_from_result = MagicMock(return_value=None)
    runner._build_registry_row = _stub_build_registry_row
    parser = SimpleNamespace(extract_registry_payload_open=MagicMock(return_value=None))
    out = _build_v4_open(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(registry_handle=None),
        result=SimpleNamespace(),
        entry=SimpleNamespace(tx_hash="0x"),
        chain=CHAIN,
        position_manager=PM,
        receipt={"logs": []},
        parser=parser,
        fee_tier=None,
    )
    assert out is None


# =============================================================================
# V4 CLOSE row builder — tokenId from intent
# =============================================================================


@pytest.mark.parametrize("raw,expected", [("4242", 4242), (4242, 4242), ("0", None), ("", None), (None, None), ("-1", None), ("x", None)])
def test_v4_close_intent_token_id_coercion(raw, expected) -> None:
    assert _v4_close_intent_token_id(SimpleNamespace(position_id=raw)) == expected


@pytest.mark.asyncio
async def test_v4_close_builds_row_from_intent_token_id() -> None:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._extract_block_number_from_result = MagicMock(return_value=999)
    runner._build_registry_row = _stub_build_registry_row
    runner._lookup_open_v4_registry_payload = AsyncMock(return_value=_open_payload())
    runner._v4_close_intent_token_id = _v4_close_intent_token_id
    parser = SimpleNamespace(extract_registry_payload_close=MagicMock(return_value=_close_payload()))

    out = await _build_v4_close(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(registry_handle=None, position_id=str(TOKEN_ID)),
        result=SimpleNamespace(),
        entry=SimpleNamespace(tx_hash="0x" + "cd" * 32),
        chain=CHAIN,
        position_manager=PM,
        receipt={"logs": []},
        parser=parser,
        fee_tier=None,
    )
    assert out is not None
    row, _payload, token_id = out
    assert token_id == TOKEN_ID
    assert row.primitive == Primitive.LP_V4
    assert row.status == "closed"
    # The OPEN-side lookup was keyed on the FULL V4 identity (tokenId + PM), not
    # tokenId alone (matches physical_identity_hash_univ4 = chain:PM:tokenId).
    runner._lookup_open_v4_registry_payload.assert_awaited_once()
    assert runner._lookup_open_v4_registry_payload.await_args.kwargs["token_id"] == TOKEN_ID
    assert runner._lookup_open_v4_registry_payload.await_args.kwargs["position_manager"] == PM


@pytest.mark.asyncio
async def test_v4_close_parser_refuse_with_open_match_builds_closed_row() -> None:
    """VIB-5409 layer 1: when the V4 close parser refuses (returns ``None``) but
    the runner matched the OPEN-side registry row, the builder must NOT return
    ``None`` (which would fall back to a registry-less ledger write and strand the
    OLD row at ``status='open'`` → VIB-5360 collision). It recovers the close
    identity from the OPEN payload and builds a ``status='closed'`` row keyed on
    the SAME physical identity, freeing the auto-mode group.
    """
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._extract_block_number_from_result = MagicMock(return_value=999)
    runner._build_registry_row = _stub_build_registry_row
    runner._lookup_open_v4_registry_payload = AsyncMock(return_value=_open_payload())
    runner._v4_close_intent_token_id = _v4_close_intent_token_id
    runner._build_v4_close_fallback_payload = StrategyRunner._build_v4_close_fallback_payload
    # Parser refuses — burn receipt yielded no usable close legs.
    parser = SimpleNamespace(extract_registry_payload_close=MagicMock(return_value=None))

    out = await _build_v4_close(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(registry_handle=None, position_id=str(TOKEN_ID)),
        result=SimpleNamespace(),
        entry=SimpleNamespace(tx_hash="0x" + "ef" * 32),
        chain=CHAIN,
        position_manager=PM,
        receipt={"logs": []},
        parser=parser,
        fee_tier=None,
    )
    assert out is not None  # no longer strands — the close row lands
    row, payload, token_id = out
    assert token_id == TOKEN_ID
    assert row.primitive == Primitive.LP_V4
    assert row.status == "closed"
    # Same physical identity as the OPEN row → the closed UPSERT frees the group.
    assert row.physical_identity_hash == physical_identity_hash_univ4(
        chain=CHAIN, position_manager_addr=PM, token_id=TOKEN_ID
    )
    assert row.semantic_grouping_key == semantic_grouping_key_univ4(chain=CHAIN, pool_id=POOL_ID)
    # Close legs stay UNMEASURED (Empty ≠ Zero) — degraded, not fabricated.
    assert "amount0_close" not in payload
    assert "amount1_close" not in payload


@pytest.mark.asyncio
async def test_v4_close_parser_refuse_without_open_match_returns_none() -> None:
    """VIB-5409 layer 1 boundary: parser refuses AND no OPEN row matched → there
    is no identity to recover, so the builder still returns ``None`` (fall back to
    ``save_ledger_entry``). There is no OPEN row to free, so no collision risk —
    this preserves the pre-fix behaviour for the genuinely-unknown case.
    """
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._extract_block_number_from_result = MagicMock(return_value=999)
    runner._build_registry_row = _stub_build_registry_row
    runner._lookup_open_v4_registry_payload = AsyncMock(return_value=None)
    runner._v4_close_intent_token_id = _v4_close_intent_token_id
    runner._build_v4_close_fallback_payload = StrategyRunner._build_v4_close_fallback_payload
    parser = SimpleNamespace(extract_registry_payload_close=MagicMock(return_value=None))

    out = await _build_v4_close(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(registry_handle=None, position_id=str(TOKEN_ID)),
        result=SimpleNamespace(),
        entry=SimpleNamespace(tx_hash="0x"),
        chain=CHAIN,
        position_manager=PM,
        receipt={"logs": []},
        parser=parser,
        fee_tier=None,
    )
    assert out is None


@pytest.mark.asyncio
async def test_v4_close_returns_none_without_intent_token_id() -> None:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._lookup_open_v4_registry_payload = AsyncMock(return_value=None)
    runner._v4_close_intent_token_id = _v4_close_intent_token_id
    parser = SimpleNamespace(extract_registry_payload_close=MagicMock(return_value=_close_payload()))
    out = await _build_v4_close(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(registry_handle=None, position_id=None),
        result=SimpleNamespace(),
        entry=SimpleNamespace(tx_hash="0x"),
        chain=CHAIN,
        position_manager=PM,
        receipt={"logs": []},
        parser=parser,
        fee_tier=None,
    )
    assert out is None
    # No OPEN lookup attempted — the intent had no usable tokenId.
    runner._lookup_open_v4_registry_payload.assert_not_awaited()


@pytest.mark.asyncio
async def test_v4_open_lookup_discriminates_by_position_manager() -> None:
    """Same tokenId on two V4 PositionManagers (one chain) → match the correct PM only.

    The V4 physical identity is ``chain:positionManager:tokenId``; the OPEN lookup
    must be as strong, or a CLOSE could merge the wrong OPEN payload (CodeRabbit).
    """
    other_pm = "0x" + "11" * 20
    rows = [
        {
            "payload": {"token_id": str(TOKEN_ID), "pool_id": POOL_ID, "position_manager": other_pm},
            "opened_at_block": 100,
            "opened_tx": "0xother",
        },
        {
            "payload": {"token_id": str(TOKEN_ID), "pool_id": POOL_ID, "position_manager": PM},
            "opened_at_block": 200,
            "opened_tx": "0xmine",
        },
    ]
    runner = SimpleNamespace(
        state_manager=SimpleNamespace(get_position_registry_open_rows=AsyncMock(return_value=rows))
    )

    out = await _lookup_open_v4(
        runner, deployment_id="dep:1", chain=CHAIN, token_id=TOKEN_ID, position_manager=PM
    )
    assert out is not None
    assert out["position_manager"] == PM
    assert out["opened_tx"] == "0xmine"  # the matching-PM row, not the same-tokenId other-PM row

    # A tokenId present only under a DIFFERENT PM → no match (never merge a wrong OPEN).
    miss = await _lookup_open_v4(
        runner, deployment_id="dep:1", chain=CHAIN, token_id=TOKEN_ID, position_manager="0x" + "22" * 20
    )
    assert miss is None


# =============================================================================
# Missing PositionManager — fail-closed skip, no raise, WARN
# =============================================================================


@pytest.mark.asyncio
async def test_v4_dispatch_missing_position_manager_skips_and_warns(caplog) -> None:
    """An unsupported V4 chain → chain resolution returns None → the dispatch
    returns False (fall back to ledger), emits v4_registry_no_position_manager,
    and NEVER raises (registry support is additive)."""
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain="zksync")
    # Boot guard cleared the V4 cutover...
    runner._cutover_complete_cache = {(Primitive.LP_V4, "lp_v4")}
    # ...but no PositionManager is known for zksync → resolver returns None.
    runner._registry_resolve_chain_and_nft_manager = MagicMock(return_value=None)

    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        result = await _dispatch_v4(
            runner,
            strategy=SimpleNamespace(deployment_id="dep:zk", chain="zksync"),
            intent=SimpleNamespace(protocol="uniswap_v4", position_id="1"),
            result=SimpleNamespace(success=True),
            entry=SimpleNamespace(tx_hash="0x"),
            intent_type_str="LP_OPEN",
            protocol="uniswap_v4",
        )
    assert result is False
    assert "v4_registry_no_position_manager" in " ".join(r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_v4_dispatch_inactive_cutover_returns_false() -> None:
    """Boot guard has NOT cleared the V4 cutover → dispatch is a no-op False."""
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    runner._cutover_complete_cache = set()  # V4 cutover not active
    result = await _dispatch_v4(
        runner,
        strategy=_strategy(),
        intent=SimpleNamespace(protocol="uniswap_v4", position_id="1"),
        result=SimpleNamespace(success=True),
        entry=SimpleNamespace(tx_hash="0x"),
        intent_type_str="LP_OPEN",
        protocol="uniswap_v4",
    )
    assert result is False


# =============================================================================
# _build_registry_row grouping-policy stamping
# =============================================================================


def test_build_registry_row_stamps_univ4_policy_for_lp_v4() -> None:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=CHAIN)
    row = _build_registry_row(
        runner,
        strategy=_strategy(),
        primitive=Primitive.LP_V4,
        physical_identity_hash="0xdead",
        semantic_grouping_key=f"{CHAIN}:{POOL_ID}",
        payload={"token_id": "1"},
        status="open",
        opened_at_block=1,
        opened_tx="0x",
        closed_at_block=None,
        closed_tx=None,
        handle=None,
    )
    assert row.grouping_policy_version == "univ4_lp@v1"
    assert row.primitive == Primitive.LP_V4


def test_build_registry_row_stamps_univ3_policy_for_lp() -> None:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain="arbitrum")
    row = _build_registry_row(
        runner,
        strategy=SimpleNamespace(deployment_id="dep:1", chain="arbitrum"),
        primitive=Primitive.LP,
        physical_identity_hash="0xbeef",
        semantic_grouping_key="arbitrum:0xpool",
        payload={"token_id": "1"},
        status="open",
        opened_at_block=1,
        opened_tx="0x",
        closed_at_block=None,
        closed_tx=None,
        handle=None,
    )
    assert row.grouping_policy_version == "univ3_lp@v1"
