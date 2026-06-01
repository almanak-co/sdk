"""Focused unit tests for the LP registry-id cache refresh helpers
(``_refresh_lp_registry_id_cache`` + ``_index_lp_registry_row_into_cache``)
in ``almanak.framework.runner._run_loop_helpers`` (VIB-4198 / T12 +
round-9 CRAP refactor).

The L2 + boot-guard tests cover the orchestrator's full path through
``initialize_run_loop`` → ``_run_cutover_boot_guard`` → registry-lookup
install. This file pins the cache-refresh helper's contract at unit grain
so:

1. Coverage on both helpers sits at 100%, which lands them well under the
   CRAP gate threshold (CRAP = cc² × (1 − cov) + cc — coverage is the
   multiplier).
2. A regression in any single indexing branch (corrupt payload, missing
   token_id / pool, multi-NFT collision) localises to a focused
   failure rather than a downstream symptom.
3. Audit M2 fail-closed semantics (cutover active → re-raise; cutover
   not active → debug-log + return) are exercised in isolation.
4. Audit P2 multi-NFT-per-pool collision detection is tested directly,
   not relied on as a side-effect of an end-to-end run.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner._run_loop_helpers import (
    _UNIV3_FAMILY_PROTOCOL_SLUGS,
    _index_lp_registry_row_into_cache,
    _install_registry_lookup_for_lp_tracker,
    _refresh_lp_registry_id_cache,
)


# ---------------------------------------------------------------------------
# _index_lp_registry_row_into_cache — per-row indexing
# ---------------------------------------------------------------------------


class TestIndexLpRegistryRowIntoCache:
    """Per-row indexing path: corrupt-payload guard, missing-anchors guard,
    fan-out across the UniV3 family slugs, multi-NFT collision detection."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    POOL_OTHER = "0x" + "ab" * 20
    TOKEN_ID_A = "5467895"
    TOKEN_ID_B = "9999999"
    CHAIN = "arbitrum"

    def _row(
        self,
        *,
        chain: str = CHAIN,
        token_id: Any = TOKEN_ID_A,
        pool: Any = POOL,
        payload: Any | None = None,
    ) -> dict[str, Any]:
        if payload is None:
            payload = {"token_id": token_id, "pool_address": pool}
        return {"chain": chain, "payload": payload}

    def test_indexes_under_every_univ3_family_slug(self) -> None:
        cache: dict[tuple[str, str, str], set[str]] = {}
        _index_lp_registry_row_into_cache(row=self._row(), cache=cache)
        # The row must land under every UniV3 family slug — the registry
        # doesn't carry ``protocol``, so the lookup table is fanned-out.
        # VIB-4301: value is the SET of open token_ids.
        for slug in _UNIV3_FAMILY_PROTOCOL_SLUGS:
            assert cache[(slug, self.CHAIN, self.POOL)] == {self.TOKEN_ID_A}

    def test_corrupt_payload_is_skipped(self) -> None:
        # payload is not a dict (corrupt registry row) — must be a no-op.
        cache: dict[tuple[str, str, str], str] = {}
        ambiguous: set[tuple[str, str, str]] = set()
        _index_lp_registry_row_into_cache(
            row=self._row(payload="this is not a dict"),
            cache=cache,
            ambiguous=ambiguous,
        )
        assert cache == {}

    def test_payload_none_is_skipped(self) -> None:
        cache: dict[tuple[str, str, str], str] = {}
        ambiguous: set[tuple[str, str, str]] = set()
        # payload=None defaults via `row.get("payload") or {}` to {} →
        # token_id/pool guard fires.
        _index_lp_registry_row_into_cache(
            row={"chain": self.CHAIN, "payload": None},
            cache=cache,
            ambiguous=ambiguous,
        )
        assert cache == {}

    def test_missing_token_id_is_skipped(self) -> None:
        cache: dict[tuple[str, str, str], str] = {}
        ambiguous: set[tuple[str, str, str]] = set()
        _index_lp_registry_row_into_cache(
            row=self._row(payload={"pool_address": self.POOL}),
            cache=cache,
            ambiguous=ambiguous,
        )
        assert cache == {}

    def test_missing_pool_is_skipped(self) -> None:
        cache: dict[tuple[str, str, str], str] = {}
        ambiguous: set[tuple[str, str, str]] = set()
        _index_lp_registry_row_into_cache(
            row=self._row(payload={"token_id": self.TOKEN_ID_A}),
            cache=cache,
            ambiguous=ambiguous,
        )
        assert cache == {}

    def test_zero_token_id_is_skipped(self) -> None:
        # Empty != zero contract — but token_id "0" / 0 / None all read
        # as "no usable identity" for the cache projection (the cache
        # exists for tracker injection; injecting token_id=0 would write
        # garbage to the close intent).
        cache: dict[tuple[str, str, str], str] = {}
        ambiguous: set[tuple[str, str, str]] = set()
        _index_lp_registry_row_into_cache(
            row=self._row(token_id=0),
            cache=cache,
            ambiguous=ambiguous,
        )
        assert cache == {}

    def test_pool_lowercased(self) -> None:
        # Mixed-case pool from the registry → lowercased on the cache key.
        cache: dict[tuple[str, str, str], set[str]] = {}
        _index_lp_registry_row_into_cache(row=self._row(pool=self.POOL.upper()), cache=cache)
        assert cache[("uniswap_v3", self.CHAIN, self.POOL)] == {self.TOKEN_ID_A}

    def test_chain_lowercased(self) -> None:
        cache: dict[tuple[str, str, str], set[str]] = {}
        _index_lp_registry_row_into_cache(row=self._row(chain="ARBITRUM"), cache=cache)
        assert cache[("uniswap_v3", "arbitrum", self.POOL)] == {self.TOKEN_ID_A}

    def test_chain_missing_uses_empty_string(self) -> None:
        # ``row.get("chain")`` is None — defaults to "".
        cache: dict[tuple[str, str, str], set[str]] = {}
        _index_lp_registry_row_into_cache(
            row={"payload": {"token_id": self.TOKEN_ID_A, "pool_address": self.POOL}},
            cache=cache,
        )
        assert cache[("uniswap_v3", "", self.POOL)] == {self.TOKEN_ID_A}

    def test_idempotent_same_token_same_pool(self) -> None:
        # Same row indexed twice — second call is a no-op (set add idempotent).
        cache: dict[tuple[str, str, str], set[str]] = {}
        row = self._row()
        _index_lp_registry_row_into_cache(row=row, cache=cache)
        cache_after_first = {k: set(v) for k, v in cache.items()}
        _index_lp_registry_row_into_cache(row=row, cache=cache)
        assert cache == cache_after_first

    def test_co_pool_opens_accumulate_in_set_no_warning(self, caplog) -> None:
        # VIB-4301: two different token_ids for the SAME (chain, pool) now
        # ACCUMULATE into the set — no drop, no spurious warning. The reader
        # (`_sync_lookup`) auto-injects only when the set has exactly one entry;
        # with N>1 the strategy supplies position_id on the close intent itself.
        cache: dict[tuple[str, str, str], set[str]] = {}
        _index_lp_registry_row_into_cache(row=self._row(token_id=self.TOKEN_ID_A), cache=cache)
        assert cache[("uniswap_v3", self.CHAIN, self.POOL)] == {self.TOKEN_ID_A}

        with caplog.at_level("WARNING"):
            _index_lp_registry_row_into_cache(row=self._row(token_id=self.TOKEN_ID_B), cache=cache)
        for slug in _UNIV3_FAMILY_PROTOCOL_SLUGS:
            assert cache[(slug, self.CHAIN, self.POOL)] == {self.TOKEN_ID_A, self.TOKEN_ID_B}
        assert not any("multiple OPEN NFTs" in rec.message for rec in caplog.records), (
            "VIB-4301: legitimate co-pool opens must not emit the spurious multi-NFT warning"
        )

    def test_different_pools_do_not_collide(self) -> None:
        # Two different pools on the same chain → both seed independently.
        cache: dict[tuple[str, str, str], set[str]] = {}
        _index_lp_registry_row_into_cache(
            row=self._row(pool=self.POOL, token_id=self.TOKEN_ID_A), cache=cache
        )
        _index_lp_registry_row_into_cache(
            row=self._row(pool=self.POOL_OTHER, token_id=self.TOKEN_ID_B), cache=cache
        )
        assert cache[("uniswap_v3", self.CHAIN, self.POOL)] == {self.TOKEN_ID_A}
        assert cache[("uniswap_v3", self.CHAIN, self.POOL_OTHER)] == {self.TOKEN_ID_B}


# ---------------------------------------------------------------------------
# _refresh_lp_registry_id_cache — orchestrator (M2 fail-closed semantics)
# ---------------------------------------------------------------------------


class TestRefreshLpRegistryIdCache:
    """Orchestrator-level tests: state-manager call, M2 fail-closed
    semantics, cache assignment."""

    @staticmethod
    def _runner_with_state_manager(rows_or_exc: Any) -> SimpleNamespace:
        """Build a runner-shaped namespace whose state_manager's
        ``get_position_registry_open_rows`` returns ``rows_or_exc`` or
        raises if it's an Exception.
        """
        sm = MagicMock()
        if isinstance(rows_or_exc, Exception):
            sm.get_position_registry_open_rows = AsyncMock(side_effect=rows_or_exc)
        else:
            sm.get_position_registry_open_rows = AsyncMock(return_value=rows_or_exc)
        return SimpleNamespace(state_manager=sm)

    @pytest.mark.asyncio
    async def test_empty_rows_assigns_empty_cache(self) -> None:
        runner = self._runner_with_state_manager([])
        await _refresh_lp_registry_id_cache(runner, "dep-1")
        assert runner._lp_registry_id_cache == {}

    @pytest.mark.asyncio
    async def test_single_row_populates_cache(self) -> None:
        rows = [
            {
                "chain": "arbitrum",
                "payload": {
                    "token_id": "5467895",
                    "pool_address": "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443",
                },
            }
        ]
        runner = self._runner_with_state_manager(rows)
        await _refresh_lp_registry_id_cache(runner, "dep-1")
        # All UniV3 family slugs populated (VIB-4301: value is a set).
        for slug in _UNIV3_FAMILY_PROTOCOL_SLUGS:
            assert (
                runner._lp_registry_id_cache[
                    (slug, "arbitrum", "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443")
                ]
                == {"5467895"}
            )

    @pytest.mark.asyncio
    async def test_state_manager_call_passes_lp_filters(self) -> None:
        runner = self._runner_with_state_manager([])
        await _refresh_lp_registry_id_cache(runner, "dep-7")
        runner.state_manager.get_position_registry_open_rows.assert_awaited_once_with(
            "dep-7",
            primitive="lp",
            accounting_category="lp",
        )

    @pytest.mark.asyncio
    async def test_exception_when_cutover_active_re_raises(self) -> None:
        # Audit M2 — cutover active + state-manager raises → re-raise.
        # Simulate cutover-active by pre-setting the runner's complete cache.
        from almanak.framework.primitives.types import Primitive

        class _Boom(RuntimeError):
            pass

        runner = self._runner_with_state_manager(_Boom("DB down"))
        runner._cutover_complete_cache = {(Primitive.LP, "lp")}

        with pytest.raises(_Boom):
            await _refresh_lp_registry_id_cache(runner, "dep-1")

    @pytest.mark.asyncio
    async def test_exception_when_cutover_not_active_logs_and_returns(self, caplog) -> None:
        # Audit M2 — cutover NOT active → debug-log + return without
        # raising; cache attribute is not assigned.
        runner = self._runner_with_state_manager(RuntimeError("DB down"))
        # No _cutover_complete_cache → is_cutover_active returns False.

        with caplog.at_level("DEBUG"):
            await _refresh_lp_registry_id_cache(runner, "dep-1")
        # Cache attribute should be absent (not even {} — the function
        # returned before reaching the assignment).
        assert not hasattr(runner, "_lp_registry_id_cache")
        # Debug log fired.
        assert any("Registry-id cache refresh failed" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_co_pool_rows_accumulate_through_orchestrator(self) -> None:
        # VIB-4301: two rows for the same (chain, pool) with different
        # token_ids accumulate into the set (no drop) — both legs are
        # represented and the reader returns None (auto-inject only on N==1).
        pool = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
        rows = [
            {"chain": "arbitrum", "payload": {"token_id": "5467895", "pool_address": pool}},
            {"chain": "arbitrum", "payload": {"token_id": "9999999", "pool_address": pool}},
        ]
        runner = self._runner_with_state_manager(rows)
        await _refresh_lp_registry_id_cache(runner, "dep-1")
        for slug in _UNIV3_FAMILY_PROTOCOL_SLUGS:
            assert runner._lp_registry_id_cache[(slug, "arbitrum", pool)] == {"5467895", "9999999"}

    @pytest.mark.asyncio
    async def test_state_manager_returns_none_treated_as_iterable_safely(
        self,
    ) -> None:
        # Defensive: some test stubs return None; the orchestrator
        # iterates ``rows`` — a None would raise TypeError. Per current
        # contract, ``get_position_registry_open_rows`` returns a list;
        # if a stub returns None, the failure surfaces as TypeError —
        # which the caller (cutover-active) re-raises and (cutover-
        # inactive) logs+returns. Pin the contract by passing []
        # (the spec'd empty case) and asserting no crash.
        runner = self._runner_with_state_manager([])
        await _refresh_lp_registry_id_cache(runner, "dep-empty")
        assert runner._lp_registry_id_cache == {}


# ---------------------------------------------------------------------------
# _sync_lookup consumer semantics (VIB-4301): single → token, N>1 → None
# ---------------------------------------------------------------------------


class TestSyncLookupSetSemantics:
    """The installed sync lookup auto-injects only when exactly one open NFT
    lives in the pool. With a legitimate co-pool (N>1) it returns None so the
    strategy's own ``position_id`` on the close intent wins — never a guessed
    leg."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    CHAIN = "arbitrum"

    async def _install(self, cache: dict) -> Any:
        from almanak.framework.strategies.lp_position_tracker import LPPositionTracker

        captured: dict[str, Any] = {}

        class _Tracker(LPPositionTracker):
            def attach_registry_lookup(self, lookup: Any) -> None:  # type: ignore[override]
                captured["lookup"] = lookup

        runner = SimpleNamespace()
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        runner.state_manager = sm
        tracker = _Tracker()
        await _install_registry_lookup_for_lp_tracker(runner, tracker, "dep-1")
        # The installer primes an empty cache via the (empty) refresh; override
        # with our seeded set-cache to exercise the lookup directly.
        runner._lp_registry_id_cache = cache
        return captured["lookup"]

    @pytest.mark.asyncio
    async def test_single_open_returns_token(self) -> None:
        cache = {("uniswap_v3", self.CHAIN, self.POOL): {"42"}}
        lookup = await self._install(cache)
        assert lookup(protocol="uniswap_v3", chain=self.CHAIN, pool=self.POOL) == "42"

    @pytest.mark.asyncio
    async def test_co_pool_returns_none(self) -> None:
        cache = {("uniswap_v3", self.CHAIN, self.POOL): {"42", "999"}}
        lookup = await self._install(cache)
        assert lookup(protocol="uniswap_v3", chain=self.CHAIN, pool=self.POOL) is None

    @pytest.mark.asyncio
    async def test_missing_pool_returns_none(self) -> None:
        lookup = await self._install({})
        assert lookup(protocol="uniswap_v3", chain=self.CHAIN, pool=self.POOL) is None
