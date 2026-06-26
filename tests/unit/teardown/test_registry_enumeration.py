"""VIB-5459 / TD-01 — teardown enumeration routed through position_registry.

These tests pin the WARM read-path cutover for the two cut-over LP primitives
(UniV3 ``primitive='lp'`` + UniV4 ``primitive='lp_v4'``):

* the registry read builds correct LP ``PositionInfo`` and degrades to
  "unavailable" (never "nothing open") on a backend without cutover storage;
* the reconcile is additive (union) — it never drops a strategy-reported
  position and re-derives forgotten ones from WARM;
* **restart determinism** — a fresh runner instance whose in-memory state was
  wiped (``get_open_positions`` returns empty) re-derives the identical open set
  from the registry, and two independent restarts agree.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.migration import CutoverStorageNotSupported
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.teardown.registry_enumeration import (
    read_open_lp_positions_from_registry,
    reconcile_lp_with_registry,
    resolve_open_positions_with_registry,
)

DEPLOYMENT_ID = "deployment:abc123def456"


def _v3_row(token_id: str = "555", pool: str = "0xPOOL") -> dict[str, Any]:
    return {
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": "lp",
        "status": "open",
        "payload": {
            "token_id": token_id,
            "pool_address": pool,
            "tick_lower": -100,
            "tick_upper": 100,
            "liquidity": "12345",
        },
    }


def _v4_row(token_id: str = "777", pool_id: str = "0xPOOLIDHASH") -> dict[str, Any]:
    return {
        "chain": "base",
        "primitive": "lp_v4",
        "accounting_category": "lp_v4",
        "status": "open",
        "payload": {"token_id": token_id, "pool_id": pool_id, "liquidity": "9999"},
    }


class _FakeRegistrySM:
    """Minimal registry-capable StateManager double.

    Returns the rows registered for the requested ``primitive``. Raises
    ``CutoverStorageNotSupported`` for primitives in ``unsupported`` so the
    hosted-pre-T19 degrade path can be exercised.
    """

    def __init__(
        self,
        rows_by_primitive: dict[str, list[dict[str, Any]]] | None = None,
        unsupported: set[str] | None = None,
    ) -> None:
        self._rows = rows_by_primitive or {}
        self._unsupported = unsupported or set()
        self.calls: list[tuple[str, str | None, str | None, str | None]] = []

    async def get_position_registry_open_rows(
        self,
        deployment_id: str,
        *,
        chain: str | None = None,
        primitive: str | None = None,
        accounting_category: str | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((deployment_id, chain, primitive, accounting_category))
        if primitive in self._unsupported:
            raise CutoverStorageNotSupported(f"{primitive} not on this backend")
        return list(self._rows.get(primitive or "", []))


class _FakeStrategy:
    """Duck-typed strategy: only what the enumeration path touches."""

    def __init__(self, summary: TeardownPositionSummary, state_manager: Any) -> None:
        self._summary = summary
        self._state_manager = state_manager
        self.deployment_id = DEPLOYMENT_ID

    def get_open_positions(self) -> TeardownPositionSummary:
        return self._summary


def _empty_summary() -> TeardownPositionSummary:
    return TeardownPositionSummary.empty(DEPLOYMENT_ID)


# ---------------------------------------------------------------------------
# read_open_lp_positions_from_registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_builds_v3_and_v4_positions() -> None:
    sm = _FakeRegistrySM({"lp": [_v3_row()], "lp_v4": [_v4_row()]})
    positions, available = await read_open_lp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True
    by_id = {p.position_id: p for p in positions}
    assert set(by_id) == {"555", "777"}
    assert by_id["555"].position_type == PositionType.LP
    # Label is the registry primitive (the framework must not invent a protocol
    # slug the registry payload does not carry).
    assert by_id["555"].protocol == "lp"
    assert by_id["555"].chain == "arbitrum"
    assert by_id["555"].details["pool"] == "0xPOOL"
    assert by_id["555"].details["source"] == "position_registry"
    assert by_id["777"].protocol == "lp_v4"
    assert by_id["777"].details["pool"] == "0xPOOLIDHASH"


@pytest.mark.asyncio
async def test_read_unavailable_when_no_state_manager() -> None:
    positions, available = await read_open_lp_positions_from_registry(state_manager=None, deployment_id=DEPLOYMENT_ID)
    assert positions == []
    assert available is False


@pytest.mark.asyncio
async def test_read_unavailable_when_backend_lacks_cutover_storage() -> None:
    sm = _FakeRegistrySM(unsupported={"lp", "lp_v4"})
    positions, available = await read_open_lp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    # Hosted pre-T19: degrade to legacy enumeration, NEVER "nothing open".
    assert positions == []
    assert available is False


@pytest.mark.asyncio
async def test_read_available_with_zero_rows_is_authoritative_empty() -> None:
    sm = _FakeRegistrySM({"lp": [], "lp_v4": []})
    positions, available = await read_open_lp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert positions == []
    assert available is True


@pytest.mark.asyncio
async def test_read_skips_row_without_token_id() -> None:
    bad = {"chain": "arbitrum", "primitive": "lp", "payload": {"pool_address": "0xP"}}
    sm = _FakeRegistrySM({"lp": [bad, _v3_row("888")]})
    positions, available = await read_open_lp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True
    assert [p.position_id for p in positions] == ["888"]


@pytest.mark.asyncio
async def test_read_empty_deployment_id_is_unavailable() -> None:
    sm = _FakeRegistrySM({"lp": [_v3_row()]})
    positions, available = await read_open_lp_positions_from_registry(state_manager=sm, deployment_id="  ")
    assert positions == []
    assert available is False


# ---------------------------------------------------------------------------
# reconcile_lp_with_registry — additive (union) semantics
# ---------------------------------------------------------------------------


def _lp(position_id: str, protocol: str = "uniswap_v3", value: str = "0", chain: str = "arbitrum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal(value),
    )


def _token(symbol: str = "USDC") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.TOKEN,
        position_id=symbol,
        chain="arbitrum",
        protocol="erc20",
        value_usd=Decimal("0"),
    )


def test_reconcile_unavailable_returns_strategy_summary_unchanged() -> None:
    strat = TeardownPositionSummary(deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("1")])
    out = reconcile_lp_with_registry(strategy_summary=strat, registry_positions=[_lp("2")], registry_available=False)
    assert out is strat  # unchanged identity — legacy degrade path


def test_reconcile_adds_registry_position_strategy_forgot() -> None:
    # Restart shape: strategy reports nothing, registry remembers an open LP.
    out = reconcile_lp_with_registry(
        strategy_summary=_empty_summary(),
        registry_positions=[_lp("999")],
        registry_available=True,
    )
    assert [p.position_id for p in out.positions] == ["999"]


def test_reconcile_dedupes_by_position_id() -> None:
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("42", value="100")]
    )
    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("42"), _lp("43")],
        registry_available=True,
    )
    ids = [p.position_id for p in out.positions]
    assert ids.count("42") == 1  # strategy's richer copy kept, not duplicated
    assert "43" in ids
    # The strategy's richer (valued) copy is the one retained.
    kept_42 = next(p for p in out.positions if p.position_id == "42")
    assert kept_42.value_usd == Decimal("100")


def test_reconcile_never_drops_strategy_positions() -> None:
    # Strategy reports an LP + a token the registry has no knowledge of; the
    # additive reconcile must keep BOTH even though the registry is authoritative
    # and returns a different open LP.
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("100"), _token("WETH")],
    )
    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("200")],
        registry_available=True,
    )
    assert {p.position_id for p in out.positions} == {"100", "WETH", "200"}


# ---------------------------------------------------------------------------
# Restart determinism — the headline acceptance criterion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restarted_runner_rederives_same_open_set_from_warm() -> None:
    """A restarted runner re-derives the identical open set from WARM.

    Simulate two independent restarts: each builds a *fresh* strategy whose
    in-memory enumeration is empty (state wiped), sharing only the durable
    registry. Both must resolve to exactly the registry's open LP set, and the
    two must agree (determinism).
    """
    registry_rows = {"lp": [_v3_row("321")], "lp_v4": [_v4_row("654")]}

    async def _resolve_after_restart() -> list[str]:
        sm = _FakeRegistrySM(registry_rows)  # WARM survives the restart
        strategy = _FakeStrategy(summary=_empty_summary(), state_manager=sm)  # HOT wiped
        summary = await resolve_open_positions_with_registry(strategy)
        return sorted(p.position_id for p in summary.positions)

    first = await _resolve_after_restart()
    second = await _resolve_after_restart()

    assert first == ["321", "654"]
    assert first == second  # deterministic across restarts


@pytest.mark.asyncio
async def test_resolve_unions_live_strategy_state_with_registry() -> None:
    # Strategy still tracks one LP (id 11); registry additionally remembers id 22.
    sm = _FakeRegistrySM({"lp": [_v3_row("22")], "lp_v4": []})
    strat = TeardownPositionSummary(deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("11")])
    strategy = _FakeStrategy(summary=strat, state_manager=sm)
    summary = await resolve_open_positions_with_registry(strategy)
    assert {p.position_id for p in summary.positions} == {"11", "22"}


@pytest.mark.asyncio
async def test_resolve_degrades_to_strategy_enumeration_without_registry() -> None:
    strat = TeardownPositionSummary(deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("11")])
    strategy = _FakeStrategy(summary=strat, state_manager=None)
    summary = await resolve_open_positions_with_registry(strategy)
    assert {p.position_id for p in summary.positions} == {"11"}


# ---------------------------------------------------------------------------
# Dedup key-namespace invariant — the union is only clean if BOTH sides key a
# cut-over LP by the bare NFT token_id (no pool-prefix / composite id).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedup_namespace_matches_bare_nft_token_id_univ3_and_v4() -> None:
    """`reconcile_lp_with_registry` dedups by `str(position_id)`, and the registry
    side keys by the bare NFT `token_id` (`payload['token_id']`). The union is
    only *clean* (no double-listing of the same open position) because the
    strategy's `get_open_positions()` ALSO keys a UniV3 / UniV4 LP by the bare
    NFT token id:

    - UniV3 demo (`uniswap_lp`, `primitive='lp'`):
      `PositionInfo(position_id=str(self._current_position_id))`, and
      `_current_position_id = str(result.position_id)` = the NFT token id from
      the receipt parser.
    - UniV4 demo (`uniswap_v4_hooks`, `primitive='lp_v4'`):
      `PositionInfo(position_id=self._current_position_id)`, same bare token id.

    This locks that invariant for BOTH primitives: when the strategy-reported
    position id equals the registry token id, the same position must NOT
    double-list; and if the namespaces ever diverge (a pool-prefixed / composite
    position id), the union stops deduping and double-lists — which this test
    makes visible rather than silently masking.
    """
    # Registry rows are keyed by the bare NFT token_id (V3 `lp`, V4 `lp_v4`).
    sm = _FakeRegistrySM({"lp": [_v3_row("555")], "lp_v4": [_v4_row("777")]})
    registry_positions, available = await read_open_lp_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True
    assert {p.position_id for p in registry_positions} == {"555", "777"}

    # MATCH: the strategy keys its V3 + V4 LP by the SAME bare NFT token id AND
    # the same chain as the registry (V3 row=arbitrum, V4 row=base) → the union
    # recognises them as the same position and adds nothing net-new.
    strat_match = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            _lp("555", protocol="uniswap_v3", chain="arbitrum"),
            _lp("777", protocol="uniswap_v4", chain="base"),
        ],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=strat_match,
        registry_positions=registry_positions,
        registry_available=True,
    )
    ids = [p.position_id for p in merged.positions]
    assert ids.count("555") == 1  # V3: deduped, not double-listed
    assert ids.count("777") == 1  # V4: deduped, not double-listed
    assert len(merged.positions) == 2  # registry adds nothing net-new

    # MISMATCH (canary): if the strategy ever keyed a cut-over LP by anything but
    # the bare NFT token id (pool-prefixed / composite), the bare-NFT registry
    # rows are NOT recognised as the same position and ARE appended — the same
    # open position double-lists. This asserts divergence is observable.
    strat_diverged = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            _lp("pool0xABC:555", protocol="uniswap_v3", chain="arbitrum"),
            _lp("v4#777", protocol="uniswap_v4", chain="base"),
        ],
    )
    merged_div = reconcile_lp_with_registry(
        strategy_summary=strat_diverged,
        registry_positions=registry_positions,
        registry_available=True,
    )
    assert sorted(p.position_id for p in merged_div.positions) == ["555", "777", "pool0xABC:555", "v4#777"]


@pytest.mark.asyncio
async def test_dedup_is_chain_scoped_cross_chain_token_id_not_suppressed() -> None:
    """Cross-chain non-suppression invariant (fund-safety).

    A bare NFT ``token_id`` is unique only WITHIN a chain, and a single
    deployment can span chains (the inline multi-chain teardown lane). So the
    union must dedupe on ``(chain, position_type, position_id)`` — keying on the
    bare token id alone would let a strategy-reported LP ``token_id=N`` on chain
    A SUPPRESS a registry-open LP ``token_id=N`` on chain B, under-reporting and
    stranding chain B's position.

    Here the strategy reports `token_id=555` on arbitrum; the registry holds an
    OPEN `token_id=555` on a DIFFERENT chain (base). The registry row MUST be
    appended (not suppressed), while the same-chain same-token-id case still
    dedupes.
    """
    # Registry: same token id (555) but on `base`, plus a same-chain dup (999).
    sm = _FakeRegistrySM(
        {
            "lp": [
                {"chain": "base", "primitive": "lp", "payload": {"token_id": "555", "pool_address": "0xB"}},
                {"chain": "arbitrum", "primitive": "lp", "payload": {"token_id": "999", "pool_address": "0xA"}},
            ],
            "lp_v4": [],
        }
    )
    registry_positions, available = await read_open_lp_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True

    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            _lp("555", chain="arbitrum"),  # same token id as the base registry row, different chain
            _lp("999", chain="arbitrum"),  # same token id AND chain as a registry row → must dedupe
        ],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=strat, registry_positions=registry_positions, registry_available=True
    )
    keys = sorted((p.chain, p.position_id) for p in merged.positions)
    # base:555 is net-new (cross-chain, not suppressed); arbitrum:555 kept;
    # arbitrum:999 deduped (same chain + token id) — appears once.
    assert keys == [("arbitrum", "555"), ("arbitrum", "999"), ("base", "555")]


def test_reconcile_preserves_strategy_summary_totals() -> None:
    """Appending registry rows must not clobber the strategy's explicit totals.

    `TeardownPositionSummary` recomputes `total_value_usd` / `has_liquidation_risk`
    from positions when they are omitted (== 0 / == False). Rebuilding the summary
    without carrying them forward would silently change safety/accounting
    semantics for a strategy that set them explicitly. Registry rows carry
    value_usd=0 and liquidation_risk=False, so they must add nothing to either.
    """
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("11", value="0")],
        total_value_usd=Decimal("1234.56"),  # explicit, != sum(positions)=0
        has_liquidation_risk=True,
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("22", chain="arbitrum")],  # net-new, value 0, no liq risk
        registry_available=True,
    )
    assert {p.position_id for p in merged.positions} == {"11", "22"}
    assert merged.total_value_usd == Decimal("1234.56")  # preserved, not recomputed to 0
    assert merged.has_liquidation_risk is True  # preserved, not recomputed to False
