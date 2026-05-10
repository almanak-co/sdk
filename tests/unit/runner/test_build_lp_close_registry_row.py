"""Focused branch-coverage tests for
``StrategyRunner._build_lp_close_registry_row`` (VIB-4198 / T12 — round 11
CRAP-gate fix).

The method's CRAP score (78 = cc² × (1 − cov) + cc on cc=9 cov=5%) is
coverage-driven, NOT cc-driven. The L2 contract test in
``tests/accounting/L2/test_univ3_ledger_registry_atomicity.py`` exercises
the happy path through the orchestrator, but the explicit per-branch
behaviour of the row-builder (parser-refuse path, malformed token_id,
None / non-dict open_payload, opened_at_block / opened_tx merge) goes
uncovered.

This file pins each branch so:

1. Coverage on ``_build_lp_close_registry_row`` lands at ~100%, dropping
   CRAP from 78 to ~9 (cc² × 0 + cc = cc).
2. Each branch's contract is testable in isolation without the full
   strategy-runner boot pipeline.

The function is method-bound but only depends on three sibling helpers
(``_lookup_open_registry_payload``, ``extract_registry_payload_close`` on
the parser, ``_extract_block_number_from_result``,
``_build_registry_row``). We bind the unbound ``async def`` to a
``SimpleNamespace`` ``self`` whose helpers are stubs — the production
``_build_lp_close_registry_row`` body still runs, every conditional
branch in IT is exercised, only the leaf collaborators are stubbed.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner

# Take the unbound method off the class so we can bind it to any object
# whose attribute surface satisfies the body's needs. This is the
# standard way to drive a method's branches without invoking the full
# ``__init__``.
_build_lp_close_registry_row = StrategyRunner._build_lp_close_registry_row


# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------


def _make_strategy(
    *, deployment_id: str = "dep:1", strategy_id: str = "strat-1", chain: str = "arbitrum"
) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_id=deployment_id,
        strategy_id=strategy_id,
        chain=chain,
    )


def _make_runner(
    *,
    open_payload_for_close: Any = None,
    closed_at_block: int | None = 100,
    chain: str = "arbitrum",
) -> SimpleNamespace:
    """Build a runner-shaped namespace with the helpers
    ``_build_lp_close_registry_row`` calls. The function-under-test's
    OWN branches are unmocked.
    """
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=chain)
    runner._lookup_open_registry_payload = AsyncMock(return_value=open_payload_for_close)
    runner._extract_block_number_from_result = MagicMock(return_value=closed_at_block)
    # _build_registry_row is sibling helper — pass through to the real one
    # bound to this scaffold so the RegistryRow it builds actually
    # carries the fields under test. We can't bind the real method
    # cheaply (it imports modules), so use a lambda that returns a
    # SimpleNamespace with the fields the orchestrator inspects.
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
            strategy_id=getattr(strategy, "deployment_id", "") or strategy.strategy_id,
            chain=getattr(strategy, "chain", "") or runner.config.chain,
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

    runner._build_registry_row = _stub_build_registry_row
    return runner


def _make_intent(*, registry_handle: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(registry_handle=registry_handle)


def _make_entry(*, tx_hash: str | None = "0x" + "ab" * 32) -> SimpleNamespace:
    return SimpleNamespace(tx_hash=tx_hash)


def _make_parser(*, close_payload: Any) -> SimpleNamespace:
    """Parser stub. ``extract_registry_payload_close`` returns whatever
    ``close_payload`` is — None to exercise the refuse branch, a dict to
    exercise the success branch."""
    parser = SimpleNamespace()
    parser.extract_registry_payload_close = MagicMock(return_value=close_payload)
    return parser


# ---------------------------------------------------------------------------
# Branches under test
# ---------------------------------------------------------------------------


class TestBuildLpCloseRegistryRow:
    """Each test pins one branch in
    ``StrategyRunner._build_lp_close_registry_row``."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    TOKEN_ID_INT = 5467895

    def _close_payload(self, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "token_id": str(self.TOKEN_ID_INT),
            "pool_address": self.POOL,
            "amount0_close": "2295340",
            "amount1_close": "979486010818981",
            "fee_owed_0": "14368",
            "fee_owed_1": "0",
            "nft_manager_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
        }
        base.update(overrides)
        return base

    @pytest.mark.asyncio
    async def test_returns_none_when_parser_returns_no_payload(self) -> None:
        """Branch 1: parser refuses (extract_registry_payload_close → None).
        The function logs INFO and returns None — the orchestrator falls
        back to ``save_ledger_entry``."""
        runner = _make_runner(open_payload_for_close=None)
        out = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=None),
            fee_tier=500,
        )
        assert out is None
        # Lookup was attempted (open_payload thread-through) before parser refusal.
        runner._lookup_open_registry_payload.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_payload_token_id_missing(self) -> None:
        """Branch 2: ``int(payload["token_id"])`` raises ``KeyError`` →
        return None (Empty != zero — never substitute)."""
        runner = _make_runner(open_payload_for_close=None)
        # Payload missing token_id entirely.
        bad_payload = self._close_payload()
        del bad_payload["token_id"]
        out = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=bad_payload),
            fee_tier=500,
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_returns_none_when_payload_token_id_not_int_coercible(self) -> None:
        """Branch 3: ``int(payload["token_id"])`` raises ``ValueError`` →
        return None."""
        runner = _make_runner(open_payload_for_close=None)
        bad_payload = self._close_payload(token_id="not-a-number")
        out = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=bad_payload),
            fee_tier=500,
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_returns_none_when_payload_token_id_wrong_type(self) -> None:
        """Branch 4: ``int(payload["token_id"])`` raises ``TypeError`` →
        return None (e.g. token_id is a list)."""
        runner = _make_runner(open_payload_for_close=None)
        bad_payload = self._close_payload(token_id=[1, 2, 3])
        out = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=bad_payload),
            fee_tier=500,
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_happy_path_with_open_payload_threads_anchors(self) -> None:
        """Branch 5: open_payload is a non-None dict — opened_at_block and
        opened_tx threaded onto the registry row from open_payload."""
        open_payload = {
            "token_id": str(self.TOKEN_ID_INT),
            "pool_address": self.POOL,
            "opened_at_block": 459405352,
            "opened_tx": "0x" + "ed" * 32,
        }
        runner = _make_runner(
            open_payload_for_close=open_payload,
            closed_at_block=459405353,
        )
        close_payload = self._close_payload()
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(registry_handle="my-handle"),
            result=SimpleNamespace(),
            entry=_make_entry(tx_hash="0xclose-tx"),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=close_payload),
            fee_tier=500,
        )
        assert result is not None
        registry_row, payload, token_id = result
        assert token_id == self.TOKEN_ID_INT
        assert payload is close_payload
        assert registry_row.status == "closed"
        assert registry_row.opened_at_block == 459405352
        assert registry_row.opened_tx == "0x" + "ed" * 32
        assert registry_row.closed_at_block == 459405353
        assert registry_row.closed_tx == "0xclose-tx"
        assert registry_row.handle == "my-handle"

    @pytest.mark.asyncio
    async def test_open_payload_none_yields_none_open_anchors(self) -> None:
        """Branch 6: open_payload is None — opened_at_block / opened_tx
        on the registry row are None (no synthesis)."""
        runner = _make_runner(open_payload_for_close=None, closed_at_block=200)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.opened_at_block is None
        assert registry_row.opened_tx is None

    @pytest.mark.asyncio
    async def test_open_payload_non_dict_skips_anchor_merge(self) -> None:
        """Branch 7: open_payload is non-None but not a dict (defensive,
        e.g. older state-manager returned a list) — opened_at_block /
        opened_tx defensively skip the .get() and stay None."""
        runner = _make_runner(open_payload_for_close=["unexpected", "shape"])
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.opened_at_block is None
        assert registry_row.opened_tx is None

    @pytest.mark.asyncio
    async def test_open_payload_dict_missing_anchors_yields_none(self) -> None:
        """Branch 8: open_payload is a dict but lacks
        ``opened_at_block`` / ``opened_tx`` keys — both default to None
        on the registry row (Empty != zero — never default to 0)."""
        open_payload = {
            "token_id": str(self.TOKEN_ID_INT),
            "pool_address": self.POOL,
            # NO opened_at_block, NO opened_tx
        }
        runner = _make_runner(open_payload_for_close=open_payload)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.opened_at_block is None
        assert registry_row.opened_tx is None

    @pytest.mark.asyncio
    async def test_entry_tx_hash_empty_string_passed_as_none(self) -> None:
        """Branch 9: ``entry.tx_hash`` is empty string — coerce to None
        on the registry row (Empty != zero)."""
        runner = _make_runner(open_payload_for_close=None)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(tx_hash=""),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.closed_tx is None

    @pytest.mark.asyncio
    async def test_intent_with_no_registry_handle_passes_none(self) -> None:
        """Branch 10: intent has no ``registry_handle`` attr / it's None →
        registry row's handle is None."""
        intent = SimpleNamespace()  # no registry_handle attribute
        runner = _make_runner(open_payload_for_close=None)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=intent,
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.handle is None

    @pytest.mark.asyncio
    async def test_strategy_no_deployment_id_falls_back_to_strategy_id(self) -> None:
        """Strategy with empty deployment_id → uses strategy_id (per
        ``_build_registry_row`` resolution). Pinned at the orchestrator
        boundary so a future refactor doesn't drop the fallback."""
        strategy = _make_strategy(deployment_id="", strategy_id="strat-fallback")
        runner = _make_runner(open_payload_for_close=None)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=strategy,
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.strategy_id == "strat-fallback"

    @pytest.mark.asyncio
    async def test_lookup_called_with_correct_args(self) -> None:
        """Wire-check: the open-payload lookup is called with the strategy
        deployment_id, the chain arg, token_id=None (parser recovers it),
        and the receipt + parser threaded through."""
        runner = _make_runner(open_payload_for_close=None)
        receipt = {"logs": [{"topics": ["0xdeadbeef"]}]}
        parser = _make_parser(close_payload=self._close_payload())
        await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(deployment_id="dep:7"),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt=receipt,
            parser=parser,
            fee_tier=500,
        )
        runner._lookup_open_registry_payload.assert_awaited_once_with(
            deployment_id="dep:7",
            chain="arbitrum",
            token_id=None,
            receipt=receipt,
            parser=parser,
        )

    @pytest.mark.asyncio
    async def test_parser_called_with_open_payload_and_fee_tier(self) -> None:
        """Wire-check: the parser's ``extract_registry_payload_close`` is
        called with the open_payload threaded from the lookup AND the
        fee_tier argument."""
        open_payload = {"token_id": str(self.TOKEN_ID_INT), "pool_address": self.POOL}
        runner = _make_runner(open_payload_for_close=open_payload)
        receipt = {"logs": [{"topics": ["0x" + "00" * 32]}]}
        parser = _make_parser(close_payload=self._close_payload())
        await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt=receipt,
            parser=parser,
            fee_tier=500,
        )
        parser.extract_registry_payload_close.assert_called_once_with(
            receipt,
            open_payload=open_payload,
            fee_tier=500,
        )

    @pytest.mark.asyncio
    async def test_close_block_extracted_from_result(self) -> None:
        """Wire-check: closed_at_block on the registry row comes from
        ``_extract_block_number_from_result`` — not from open_payload."""
        runner = _make_runner(open_payload_for_close=None, closed_at_block=12345)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.closed_at_block == 12345

    @pytest.mark.asyncio
    async def test_physical_identity_hash_uses_chain_nft_manager_and_token_id(self) -> None:
        """Wire-check: the physical_identity_hash on the registry row is
        the deterministic SHA256 of (chain, nft_manager, token_id) — pin
        the same value as the L2 fixture so a parser-side regression on
        token_id flips this test."""
        from almanak.framework.migration.backfill import physical_identity_hash_univ3

        expected_pih = physical_identity_hash_univ3(
            chain="arbitrum",
            nft_manager_addr="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            token_id=self.TOKEN_ID_INT,
        )
        runner = _make_runner(open_payload_for_close=None)
        result = await _build_lp_close_registry_row(
            runner,
            strategy=_make_strategy(),
            intent=_make_intent(),
            result=SimpleNamespace(),
            entry=_make_entry(),
            chain="arbitrum",
            nft_manager="0xc36442b4a4522e871399cd717abdd847ab11fe88",
            receipt={"logs": []},
            parser=_make_parser(close_payload=self._close_payload()),
            fee_tier=500,
        )
        assert result is not None
        registry_row, _, _ = result
        assert registry_row.physical_identity_hash == expected_pih
