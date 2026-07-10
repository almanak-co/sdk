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
    RegistryReadResult,
    read_open_lp_positions_detailed,
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


def _strat_lending(leg: PositionType, asset: str, chain: str = "arbitrum", protocol: str = "aave_v3") -> PositionInfo:
    """A strategy-emitted lending leg: position_id encodes the asset, details['asset']."""
    verb = "supply" if leg == PositionType.SUPPLY else "borrow"
    return PositionInfo(
        position_type=leg,
        position_id=f"aave-{verb}-{asset}-{chain}",
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("100"),
        details={"asset": asset},
    )


def _registry_lending(
    leg: PositionType, market_id: str, asset: str, chain: str = "arbitrum", protocol: str = "aave_v3"
) -> PositionInfo:
    """A registry-sourced lending leg: position_id is the market_id, details['asset_symbol']."""
    return PositionInfo(
        position_type=leg,
        position_id=market_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details={"source": "position_registry", "leg": "collateral", "market_id": market_id, "asset_symbol": asset},
    )


def test_reconcile_dedupes_lending_strategy_vs_registry_copies_VIB_5523() -> None:
    """Strategy + registry name the SAME lending leg with DIFFERENT position_id
    formats (strategy ``aave-supply-wstETH-arbitrum`` vs registry market_id
    ``wsteth``). The union must dedup to 2 (the strategy's richer copies), NOT 4
    — else the registry duplicates get flagged uncovered by completeness."""
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            _strat_lending(PositionType.SUPPLY, "wstETH"),
            _strat_lending(PositionType.BORROW, "USDC"),
        ],
    )
    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[
            _registry_lending(PositionType.SUPPLY, "wsteth", "wstETH"),
            _registry_lending(PositionType.BORROW, "usdc", "USDC"),
        ],
        registry_available=True,
    )
    assert len(out.positions) == 2  # deduped, not 4
    # The strategy's richer (valued, asset-keyed) copies are the retained ones.
    assert {p.position_id for p in out.positions} == {
        "aave-supply-wstETH-arbitrum",
        "aave-borrow-USDC-arbitrum",
    }


def test_reconcile_lending_keeps_distinct_isolated_markets_VIB_5523() -> None:
    """Two Morpho markets supplying the SAME asset are distinct positions — the
    bytes32 market_id (carried on both sides) must keep them separate, never
    merge them (under-counting = stranding a real position)."""
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="m-A",
                chain="ethereum",
                protocol="morpho_blue",
                value_usd=Decimal("100"),
                details={"asset": "wstETH", "market_id": "0xAAA"},
            )
        ],
    )
    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[
            _registry_lending(PositionType.SUPPLY, "0xBBB", "wstETH", chain="ethereum", protocol="morpho_blue"),
        ],
        registry_available=True,
    )
    # Distinct markets (0xAAA vs 0xBBB) → BOTH kept.
    assert len(out.positions) == 2


def test_reconcile_lending_market_id_zero_is_not_falsy_collapsed_VIB_5523() -> None:
    """Gemini MEDIUM (PR #3102): a legitimate integer ``market_id == 0`` must
    key on ``"0"``, not silently fall back to ``asset`` via ``market_id or ""``.
    A market-0 position is a DISTINCT identity from an asset-only position on the
    same asset/protocol/chain — collapsing them would strand a real position."""
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="market-zero",
                chain="ethereum",
                protocol="morpho_blue",
                value_usd=Decimal("100"),
                details={"asset": "wstETH", "market_id": 0},
            )
        ],
    )
    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[
            # Registry leg on the SAME asset but with NO market_id (asset-only).
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="asset-only",
                chain="ethereum",
                protocol="morpho_blue",
                value_usd=Decimal("50"),
                details={"asset": "wstETH"},
            ),
        ],
        registry_available=True,
    )
    # market_id=0 → discriminator "0"; asset-only → discriminator "wstETH".
    # Distinct identities → BOTH kept (the bug collapsed them to 1). Assert the
    # actual identities, not just the count: a count-only check would pass even
    # if reconciliation returned the wrong two positions (CodeRabbit MINOR).
    position_ids = {p.position_id for p in out.positions}
    assert position_ids == {"market-zero", "asset-only"}
    # And the discriminating detail survives: the market-0 leg keeps market_id 0.
    market_zero = next(p for p in out.positions if p.position_id == "market-zero")
    assert market_zero.details.get("market_id") == 0
    asset_only = next(p for p in out.positions if p.position_id == "asset-only")
    assert "market_id" not in asset_only.details


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
    """`reconcile_lp_with_registry` keys a cut-over LP by its source-independent
    identity (`_lp_identity`, VIB-5723): the resolved numeric NFT token id when
    one is recoverable (via `resolve_nft_token_id` — details keys first, then a
    numeric `position_id`), else the raw `position_id` string. The registry
    side keys by the bare NFT `token_id` (`payload['token_id']`). The union is
    *clean* (no double-listing of the same open position) when the strategy's
    `get_open_positions()` keys a UniV3 / UniV4 LP by the bare NFT token id —
    directly or via a `details` mirror:

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

    # MISMATCH (canary): if the strategy keys a cut-over LP by an id from which
    # NO numeric NFT token id is recoverable (opaque composite, no `details`
    # mirror — resolve_nft_token_id → None), the bare-NFT registry rows are NOT
    # recognised as the same position and ARE appended — the same open position
    # double-lists. This asserts divergence is observable rather than silently
    # masked. (A composite id WITH the bare id mirrored in `details` DOES
    # collapse since VIB-5723 — see the test_vib5723_* cases.)
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


# ---------------------------------------------------------------------------
# TD-05 (VIB-5463) — detailed read + chain-verify completeness wiring
# ---------------------------------------------------------------------------


class _RaisingRegistrySM:
    """Registry SM whose read RAISES a transient (non-cutover) fault."""

    async def get_position_registry_open_rows(
        self, deployment_id, *, chain=None, primitive=None, accounting_category=None
    ):
        raise RuntimeError("transient gateway fault")


class _VerifyStrategy:
    """Strategy double exposing the bits the completeness verifier touches."""

    def __init__(self, summary, state_manager, gateway_client=None):
        self._summary = summary
        self._state_manager = state_manager
        self._gateway_client = gateway_client
        self._gateway_network = ""
        self.deployment_id = DEPLOYMENT_ID

    def get_open_positions(self):
        return self._summary


@pytest.mark.asyncio
async def test_detailed_read_reports_failed_primitive() -> None:
    result = await read_open_lp_positions_detailed(state_manager=_RaisingRegistrySM(), deployment_id=DEPLOYMENT_ID)
    assert isinstance(result, RegistryReadResult)
    assert result.available is False
    # Both cut-over primitives failed transiently.
    assert set(result.failed_primitives) == {"lp", "lp_v4"}
    assert result.positions == []


@pytest.mark.asyncio
async def test_resolve_chain_verifies_known_lp_when_registry_read_failed(monkeypatch) -> None:
    # Registry read fails ⇒ the strategy-reported LP set is chain-verified
    # (no longer warn-only). The additive union is unchanged.
    verified: list[str] = []

    async def _verify(*, gateway_client, position, network=""):
        verified.append(str(position.position_id))
        return True

    monkeypatch.setattr("almanak.framework.teardown.live_position_reads.chain_verify_lp_open", _verify)
    strat = TeardownPositionSummary(deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("77")])
    strategy = _VerifyStrategy(summary=strat, state_manager=_RaisingRegistrySM(), gateway_client=object())
    out = await resolve_open_positions_with_registry(strategy)
    # Union preserved — verification never drops a position.
    assert {p.position_id for p in out.positions} == {"77"}
    assert verified == ["77"]


@pytest.mark.asyncio
async def test_resolve_flags_strategy_lp_absent_from_registry(monkeypatch) -> None:
    # Registry available (id 22) but strategy reports an LP (id 11) the registry
    # does NOT have AND chain confirms open ⇒ completeness signal fires; the
    # union still keeps both (no flip).
    seen: list[str] = []

    async def _verify(*, gateway_client, position, network=""):
        seen.append(str(position.position_id))
        return True

    monkeypatch.setattr("almanak.framework.teardown.live_position_reads.chain_verify_lp_open", _verify)
    sm = _FakeRegistrySM({"lp": [_v3_row("22")], "lp_v4": []})
    strat = TeardownPositionSummary(deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("11")])
    strategy = _VerifyStrategy(summary=strat, state_manager=sm, gateway_client=object())
    out = await resolve_open_positions_with_registry(strategy)
    assert {p.position_id for p in out.positions} == {"11", "22"}
    assert seen == ["11"]  # only the discrepancy (absent-from-registry) LP is verified


@pytest.mark.asyncio
async def test_resolve_skips_chain_verify_for_matched_positions(monkeypatch) -> None:
    # Strategy LP 22 matches a registry row 22 (same chain) and no read failed ⇒
    # ZERO chain reads (the common steady-state path stays cheap).
    calls: list[str] = []

    async def _verify(*, gateway_client, position, network=""):
        calls.append(str(position.position_id))
        return True

    monkeypatch.setattr("almanak.framework.teardown.live_position_reads.chain_verify_lp_open", _verify)
    sm = _FakeRegistrySM({"lp": [_v3_row("22")], "lp_v4": []})
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("22", chain="arbitrum")]
    )
    strategy = _VerifyStrategy(summary=strat, state_manager=sm, gateway_client=object())
    out = await resolve_open_positions_with_registry(strategy)
    assert {p.position_id for p in out.positions} == {"22"}
    assert calls == []  # matched + no failure ⇒ no chain read


@pytest.mark.asyncio
async def test_resolve_no_gateway_client_skips_verify_safely() -> None:
    # Registry read fails and there is no gateway client ⇒ no verify, union still
    # stands (must not raise).
    strat = TeardownPositionSummary(deployment_id=DEPLOYMENT_ID, timestamp=datetime.now(UTC), positions=[_lp("9")])
    strategy = _VerifyStrategy(summary=strat, state_manager=_RaisingRegistrySM(), gateway_client=None)
    out = await resolve_open_positions_with_registry(strategy)
    assert {p.position_id for p in out.positions} == {"9"}


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


# ---------------------------------------------------------------------------
# Lending cutover enumeration (TD-04 / VIB-5462)
# ---------------------------------------------------------------------------


def _lending_row(
    *,
    market_id: str = "usdc",
    leg: str = "collateral",
    protocol: str = "aave_v3",
    chain: str = "arbitrum",
    asset: str = "USDC",
) -> dict[str, Any]:
    return {
        "chain": chain,
        "primitive": "lending",
        "accounting_category": "lending",
        "status": "open",
        "payload": {"protocol": protocol, "market_id": market_id, "leg": leg, "asset": asset},
    }


@pytest.mark.asyncio
async def test_read_builds_lending_collateral_and_debt_positions() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_lending_positions_from_registry

    sm = _FakeRegistrySM(
        {"lending": [_lending_row(market_id="usdc", leg="collateral"), _lending_row(market_id="dai", leg="debt")]}
    )
    positions, available = await read_open_lending_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True
    by_id = {p.position_id: p for p in positions}
    assert set(by_id) == {"usdc", "dai"}
    # Collateral → SUPPLY (withdraw), debt → BORROW (repay) — teardown risk order.
    assert by_id["usdc"].position_type == PositionType.SUPPLY
    assert by_id["dai"].position_type == PositionType.BORROW
    assert by_id["usdc"].protocol == "aave_v3"
    assert by_id["usdc"].details["source"] == "position_registry"
    assert by_id["usdc"].details["leg"] == "collateral"


@pytest.mark.asyncio
async def test_read_lending_skips_row_without_market_id() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_lending_positions_from_registry

    bad = _lending_row()
    bad["payload"].pop("market_id")
    sm = _FakeRegistrySM({"lending": [bad]})
    positions, available = await read_open_lending_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True  # the read answered
    assert positions == []  # but the unusable row is not surfaced


@pytest.mark.asyncio
async def test_read_lending_unavailable_on_backend_without_cutover_storage() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_lending_positions_from_registry

    sm = _FakeRegistrySM({"lending": [_lending_row()]}, unsupported={"lending"})
    positions, available = await read_open_lending_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is False  # degrade — never "nothing open"
    assert positions == []


@pytest.mark.asyncio
async def test_read_lending_generalises_to_spark() -> None:
    """The enumeration is protocol-agnostic: a Spark row (non-Aave) flows through
    the SAME builder with no Aave-specific code (AC2)."""
    from almanak.framework.teardown.registry_enumeration import read_open_lending_positions_from_registry

    sm = _FakeRegistrySM(
        {"lending": [_lending_row(protocol="spark", market_id="dai", leg="debt", chain="ethereum", asset="DAI")]}
    )
    positions, available = await read_open_lending_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True
    assert len(positions) == 1
    assert positions[0].protocol == "spark"
    assert positions[0].position_type == PositionType.BORROW
    assert positions[0].position_id == "dai"


@pytest.mark.asyncio
async def test_resolve_restart_rederives_lending_legs_from_warm() -> None:
    """Wiped-state restart re-derives the open lending position (supply+borrow)
    from the durable registry — the AC4 restart-safe read for lending."""
    registry_rows = {
        "lp": [],
        "lp_v4": [],
        "lending": [_lending_row(market_id="usdc", leg="collateral"), _lending_row(market_id="dai", leg="debt")],
    }

    async def _resolve_after_restart() -> set[tuple[str, str]]:
        sm = _FakeRegistrySM(registry_rows)  # WARM survives the restart
        strategy = _FakeStrategy(summary=_empty_summary(), state_manager=sm)  # HOT wiped
        summary = await resolve_open_positions_with_registry(strategy)
        return {(str(p.position_type), p.position_id) for p in summary.positions}

    first = await _resolve_after_restart()
    second = await _resolve_after_restart()
    assert first == {(str(PositionType.SUPPLY), "usdc"), (str(PositionType.BORROW), "dai")}
    assert first == second  # deterministic across restarts


@pytest.mark.asyncio
async def test_resolve_unions_lp_and_lending_and_keeps_strategy_positions() -> None:
    """The union spans BOTH primitive streams and never drops a strategy-reported
    position (additive-union invariant)."""
    sm = _FakeRegistrySM({"lp": [_v3_row("99")], "lp_v4": [], "lending": [_lending_row(market_id="usdc")]})
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("11")],
    )
    strategy = _FakeStrategy(summary=strat, state_manager=sm)
    summary = await resolve_open_positions_with_registry(strategy)
    keys = {(str(p.position_type), p.position_id) for p in summary.positions}
    assert keys == {
        (str(PositionType.LP), "11"),  # strategy-reported — never dropped
        (str(PositionType.LP), "99"),  # registry LP
        (str(PositionType.SUPPLY), "usdc"),  # registry lending collateral
    }


# ---------------------------------------------------------------------------
# Pendle cutover enumeration (TD-03 / VIB-5461)
# ---------------------------------------------------------------------------


def _pendle_row(
    *,
    kind: str = "pt",
    market_id: str = "pt-wsteth-25jun2026",
    chain: str = "ethereum",
) -> dict[str, Any]:
    payload: dict[str, Any] = {"protocol": "pendle", "kind": kind, "market_id": market_id}
    if kind == "pt":
        payload["pt_symbol"] = market_id
    return {
        "chain": chain,
        "primitive": "swap",
        "accounting_category": "swap",
        "status": "open",
        "payload": payload,
    }


@pytest.mark.asyncio
async def test_read_builds_pendle_pt_and_lp_positions() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_pendle_positions_from_registry

    sm = _FakeRegistrySM(
        {
            "swap": [
                _pendle_row(kind="pt", market_id="pt-wsteth-25jun2026"),
                _pendle_row(kind="lp", market_id="0xmarket"),
            ]
        }
    )
    positions, available = await read_open_pendle_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True
    by_id = {p.position_id: p for p in positions}
    assert set(by_id) == {"pt-wsteth-25jun2026", "0xmarket"}
    # PT → TOKEN (swapped/redeemed last); LP → LP (closed via strategy LP_CLOSE).
    assert by_id["pt-wsteth-25jun2026"].position_type == PositionType.TOKEN
    assert by_id["0xmarket"].position_type == PositionType.LP
    assert by_id["pt-wsteth-25jun2026"].protocol == "pendle"
    assert by_id["pt-wsteth-25jun2026"].details["source"] == "position_registry"
    assert by_id["pt-wsteth-25jun2026"].details["kind"] == "pt"
    assert by_id["pt-wsteth-25jun2026"].details["asset_symbol"] == "pt-wsteth-25jun2026"
    # VIB-5590: a PT is a routing-required protocol-token — the registry enumeration
    # stamps ``protocol_routed_close`` so full_close routes its close SWAP through
    # the Pendle compiler (stamps the position's own protocol), not a generic DEX.
    assert by_id["pt-wsteth-25jun2026"].details["protocol_routed_close"] is True
    assert by_id["0xmarket"].details["kind"] == "lp"


@pytest.mark.asyncio
async def test_read_pendle_skips_row_without_market_id() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_pendle_positions_from_registry

    bad = _pendle_row()
    bad["payload"].pop("market_id")
    sm = _FakeRegistrySM({"swap": [bad]})
    positions, available = await read_open_pendle_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True  # the read answered
    assert positions == []  # but the unusable row is not surfaced


@pytest.mark.asyncio
async def test_read_pendle_skips_row_with_unknown_kind() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_pendle_positions_from_registry

    bad = _pendle_row()
    bad["payload"]["kind"] = "yt"  # not a tracked Pendle kind
    sm = _FakeRegistrySM({"swap": [bad, _pendle_row(kind="lp", market_id="0xmkt")]})
    positions, available = await read_open_pendle_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True
    assert [p.position_id for p in positions] == ["0xmkt"]


@pytest.mark.asyncio
async def test_read_pendle_unavailable_on_backend_without_cutover_storage() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_pendle_positions_from_registry

    sm = _FakeRegistrySM({"swap": [_pendle_row()]}, unsupported={"swap"})
    positions, available = await read_open_pendle_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is False  # degrade — never "nothing open"
    assert positions == []


@pytest.mark.asyncio
async def test_resolve_restart_rederives_pendle_holdings_from_warm() -> None:
    """Wiped-state restart re-derives the open Pendle holdings (PT + LP) from the
    durable registry — the headline restart-safe read for Pendle (AC2)."""
    registry_rows = {
        "lp": [],
        "lp_v4": [],
        "lending": [],
        "swap": [_pendle_row(kind="pt", market_id="pt-wsteth-25jun2026"), _pendle_row(kind="lp", market_id="0xmkt")],
    }

    async def _resolve_after_restart() -> set[tuple[str, str]]:
        sm = _FakeRegistrySM(registry_rows)  # WARM survives the restart
        strategy = _FakeStrategy(summary=_empty_summary(), state_manager=sm)  # HOT wiped
        summary = await resolve_open_positions_with_registry(strategy)
        return {(str(p.position_type), p.position_id) for p in summary.positions}

    first = await _resolve_after_restart()
    second = await _resolve_after_restart()
    assert first == {(str(PositionType.TOKEN), "pt-wsteth-25jun2026"), (str(PositionType.LP), "0xmkt")}
    assert first == second  # deterministic across restarts


@pytest.mark.asyncio
async def test_resolve_unions_pendle_with_lp_and_keeps_strategy_positions() -> None:
    """The union spans the Pendle stream too and never drops a strategy-reported
    position (additive-union invariant)."""
    sm = _FakeRegistrySM(
        {"lp": [_v3_row("99")], "lp_v4": [], "lending": [], "swap": [_pendle_row(kind="pt", market_id="pt-x")]}
    )
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("11")],
    )
    strategy = _FakeStrategy(summary=strat, state_manager=sm)
    summary = await resolve_open_positions_with_registry(strategy)
    keys = {(str(p.position_type), p.position_id) for p in summary.positions}
    assert keys == {
        (str(PositionType.LP), "11"),  # strategy-reported — never dropped
        (str(PositionType.LP), "99"),  # registry UniV3 LP
        (str(PositionType.TOKEN), "pt-x"),  # registry Pendle PT
    }


# ---------------------------------------------------------------------------
# Perp cutover enumeration (TD-02 / VIB-5460)
# ---------------------------------------------------------------------------


def _perp_row(
    *,
    position_id: str = "0xperpkey",
    protocol: str = "gmx_v2",
    chain: str = "arbitrum",
    market: str = "ETH/USD",
    collateral_token: str = "USDC",
    direction: str = "long",
    size_usd: str = "10",
) -> dict[str, Any]:
    return {
        "chain": chain,
        "primitive": "perp",
        "accounting_category": "perp",
        "status": "open",
        "payload": {
            "protocol": protocol,
            "position_id": position_id,
            "market": market,
            "collateral_token": collateral_token,
            "direction": direction,
            "size_usd": size_usd,
        },
    }


@pytest.mark.asyncio
async def test_read_builds_perp_positions() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_perp_positions_from_registry

    sm = _FakeRegistrySM({"perp": [_perp_row(position_id="0xaaa", market="ETH/USD")]})
    positions, available = await read_open_perp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True
    assert len(positions) == 1
    p = positions[0]
    assert p.position_type == PositionType.PERP
    assert p.position_id == "0xaaa"
    assert p.protocol == "gmx_v2"
    assert p.details["source"] == "position_registry"
    assert p.details["market"] == "ETH/USD"
    assert p.details["direction"] == "long"
    # Registry is an identity surface — never a valuation/risk surface.
    assert p.value_usd == Decimal("0")
    assert p.liquidation_risk is False


@pytest.mark.asyncio
async def test_read_perp_skips_row_without_position_id() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_perp_positions_from_registry

    bad = _perp_row()
    bad["payload"].pop("position_id")
    sm = _FakeRegistrySM({"perp": [bad]})
    positions, available = await read_open_perp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is True  # the read answered
    assert positions == []  # but the unusable row is not surfaced


@pytest.mark.asyncio
async def test_read_perp_unavailable_on_backend_without_cutover_storage() -> None:
    from almanak.framework.teardown.registry_enumeration import read_open_perp_positions_from_registry

    sm = _FakeRegistrySM({"perp": [_perp_row()]}, unsupported={"perp"})
    positions, available = await read_open_perp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)
    assert available is False  # degrade — never "nothing open"
    assert positions == []


@pytest.mark.asyncio
async def test_resolve_restart_rederives_perp_from_warm() -> None:
    """Wiped-state restart re-derives the open perp from the durable registry —
    the AC3 restart-safe read for perp."""
    registry_rows = {"lp": [], "lp_v4": [], "lending": [], "perp": [_perp_row(position_id="0xkey1")]}

    async def _resolve_after_restart() -> set[tuple[str, str]]:
        sm = _FakeRegistrySM(registry_rows)  # WARM survives the restart
        strategy = _FakeStrategy(summary=_empty_summary(), state_manager=sm)  # HOT wiped
        summary = await resolve_open_positions_with_registry(strategy)
        return {(str(p.position_type), p.position_id) for p in summary.positions}

    first = await _resolve_after_restart()
    second = await _resolve_after_restart()
    assert first == {(str(PositionType.PERP), "0xkey1")}
    assert first == second  # deterministic across restarts


@pytest.mark.asyncio
async def test_resolve_unions_lp_lending_perp_and_keeps_strategy_positions() -> None:
    """The union spans ALL THREE cut-over primitive streams and never drops a
    strategy-reported position (additive-union invariant)."""
    sm = _FakeRegistrySM(
        {
            "lp": [_v3_row("99")],
            "lp_v4": [],
            "lending": [_lending_row(market_id="usdc")],
            "perp": [_perp_row(position_id="0xpp")],
        }
    )
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("11")],
    )
    strategy = _FakeStrategy(summary=strat, state_manager=sm)
    summary = await resolve_open_positions_with_registry(strategy)
    keys = {(str(p.position_type), p.position_id) for p in summary.positions}
    assert keys == {
        (str(PositionType.LP), "11"),  # strategy-reported — never dropped
        (str(PositionType.LP), "99"),  # registry LP
        (str(PositionType.SUPPLY), "usdc"),  # registry lending collateral
        (str(PositionType.PERP), "0xpp"),  # registry perp
    }


# ---------------------------------------------------------------------------
# VIB-5723 — source-independent LP identity: the same physical NFT position
# must not double-count across enumeration sources (registry bare token id vs
# strategy composite position key). Field repro: DN-LP mainnet + Anvil runs
# reported positions_closed=2 for 1 physical LP (see the ticket and
# tests/reports/dnlp-mainnet-vib5670-proof.md Finding #4).
# ---------------------------------------------------------------------------


def _dnlp_strategy_lp(token_id: str = "5580510", chain: str = "arbitrum") -> PositionInfo:
    """The exact strategy-reported shape from the mainnet repro: composite
    ``position_id`` (framework position-key format) with the bare NFT id
    mirrored in ``details['position_id']``."""
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=f"uniswap_v3-WETH/USDC/500-{token_id}",
        chain=chain,
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"pool": "WETH/USDC/500", "position_id": token_id},
    )


@pytest.mark.asyncio
async def test_vib5723_composite_strategy_id_dedupes_against_registry_bare_token_id() -> None:
    """1 physical LP, two sources → 1 union entry (the strategy's richer copy)."""
    sm = _FakeRegistrySM({"lp": [_v3_row("5580510")], "lp_v4": []})
    registry_positions, available = await read_open_lp_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True

    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_dnlp_strategy_lp("5580510")],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=registry_positions,
        registry_available=True,
    )
    assert len(merged.positions) == 1
    # The strategy's copy (the one that can build closing intents) is retained.
    assert merged.positions[0].protocol == "uniswap_v3"


@pytest.mark.asyncio
async def test_vib5723_dedup_stays_chain_scoped_for_composite_ids() -> None:
    """Fund-safety guard: the NFT-identity collapse must NOT suppress the same
    token id on a DIFFERENT chain (registry row on base ≠ strategy LP on
    arbitrum) — the cross-chain non-suppression invariant survives the fix."""
    sm = _FakeRegistrySM(
        {
            "lp": [
                {
                    "chain": "base",
                    "primitive": "lp",
                    "accounting_category": "lp",
                    "status": "open",
                    "payload": {"token_id": "5580510", "pool_address": "0xB"},
                }
            ],
            "lp_v4": [],
        }
    )
    registry_positions, _ = await read_open_lp_positions_from_registry(state_manager=sm, deployment_id=DEPLOYMENT_ID)

    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_dnlp_strategy_lp("5580510", chain="arbitrum")],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=registry_positions,
        registry_available=True,
    )
    assert len(merged.positions) == 2  # base registry row appended, never suppressed


def test_vib5723_non_nft_ids_keep_raw_string_identity() -> None:
    """LP entries with no recoverable numeric NFT id (non-NFT venues, opaque
    ids) keep the raw ``position_id`` key — two distinct opaque ids never
    collapse, and an opaque id never matches a bare token id."""
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("lb-bins-25/26/27", protocol="traderjoe_v2")],
    )
    merged = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("42", protocol="lp")],
        registry_available=True,
    )
    assert {p.position_id for p in merged.positions} == {"lb-bins-25/26/27", "42"}


@pytest.mark.asyncio
async def test_vib5723_completeness_check_matches_composite_id_no_false_absent() -> None:
    """The completeness check must recognise the composite-id strategy LP as
    PRESENT in the registry (no chain read, no false "ABSENT" warning). Before
    the fix this logged "open on-chain but ABSENT from position_registry" for a
    position whose registry row existed."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from almanak.framework.teardown.registry_enumeration import _verify_lp_enumeration_completeness

    read = RegistryReadResult(
        positions=[
            PositionInfo(
                position_type=PositionType.LP,
                position_id="5580510",
                chain="arbitrum",
                protocol="lp",
                value_usd=Decimal("0"),
                details={"source": "position_registry"},
            )
        ],
        available=True,
        failed_primitives=(),
    )
    strategy = MagicMock()
    strategy._gateway_client = MagicMock()
    strategy._gateway_network = ""
    summary = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_dnlp_strategy_lp("5580510")],
    )
    with patch(
        "almanak.framework.teardown.live_position_reads.chain_verify_lp_open",
        new=AsyncMock(return_value=True),
    ) as verify:
        await _verify_lp_enumeration_completeness(strategy=strategy, strategy_summary=summary, read=read)
    # Matched via the source-independent identity → the discrepancy set is
    # empty → zero chain reads (and therefore no false ABSENT warning).
    verify.assert_not_called()
