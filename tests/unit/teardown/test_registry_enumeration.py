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
from almanak.framework.teardown import registry_enumeration as registry_enumeration_module
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.teardown.registry_enumeration import (
    RegistryReadResult,
    _merge_registry_lp_authority,
    _position_info_from_registry_row,
    read_open_lp_positions_detailed,
    read_open_lp_positions_from_registry,
    reconcile_lp_with_registry,
    resolve_open_positions_with_registry,
)
from tests.support.gmx_v2 import GMX_V2_TOKENS
from tests.unit.connectors.gmx_v2.market_fixtures import market_address

DEPLOYMENT_ID = "deployment:abc123def456"


# The Arbitrum UniV3 NonfungiblePositionManager. Present because the REAL
# producer always writes it: ``uniswap_v3/receipt_parser.py`` puts
# ``nft_manager_addr`` in every ``position_registry`` LP payload it emits, and
# refuses to emit a partial row. This fixture used to omit it, which made the
# whole registry-enumeration suite exercise a shape production never produces —
# and hid VIB-6730, where the manager-qualified key on the registry row and the
# unqualified key the strategy reports never intersected, so one NFT enumerated
# twice.
V3_NFT_MANAGER = "0xc36442b4a4522e871399cd717abdd847ab11fe88"


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
            "nft_manager_addr": V3_NFT_MANAGER,
        },
    }


# The Base UniV4 PositionManager. Same reason as ``V3_NFT_MANAGER``:
# ``uniswap_v4/receipt_parser.py`` writes ``position_manager`` into every LP
# payload it emits and refuses to emit the row without one.
V4_POSITION_MANAGER = "0x7c5f5a4bbd8fd63184577525326123b519429bdc"


def _v4_row(token_id: str = "777", pool_id: str = "0xPOOLIDHASH") -> dict[str, Any]:
    return {
        "chain": "base",
        "primitive": "lp_v4",
        "accounting_category": "lp_v4",
        "status": "open",
        "payload": {
            "token_id": token_id,
            "pool_id": pool_id,
            "liquidity": "9999",
            "position_manager": V4_POSITION_MANAGER,
        },
    }


def test_registry_row_preserves_exact_nft_manager_authority() -> None:
    row = _v3_row()
    row["payload"]["nft_manager_addr"] = "0x" + "ab" * 20

    position = _position_info_from_registry_row(row, primitive="lp")

    assert position is not None
    assert position.details["nft_manager_addr"] == "0x" + "ab" * 20


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


def _lp(
    position_id: str,
    protocol: str = "uniswap_v3",
    value: str = "0",
    chain: str = "arbitrum",
    *,
    nft_manager_addr: str | None = None,
) -> PositionInfo:
    details = {"nft_manager_addr": nft_manager_addr} if nft_manager_addr else {}
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal(value),
        details=details,
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


def test_reconcile_keeps_same_token_id_on_different_nft_managers() -> None:
    """An ERC-721 token ID is unique only within its manager contract."""
    current_manager = "0x" + "11" * 20
    legacy_manager = "0x" + "22" * 20
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("42", protocol="aerodrome_slipstream", nft_manager_addr=current_manager)],
    )

    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=legacy_manager)],
        registry_available=True,
    )

    assert len(out.positions) == 2
    assert {position.details["nft_manager_addr"] for position in out.positions} == {
        current_manager,
        legacy_manager,
    }


def test_reconcile_dedupes_same_token_id_on_same_nft_manager_case_insensitively() -> None:
    """The richer strategy copy still wins for one manager-qualified NFT."""
    manager = "0x" + "aB" * 20
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[
            _lp(
                "42",
                protocol="aerodrome_slipstream",
                value="100",
                nft_manager_addr=manager,
            )
        ],
    )

    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=manager.lower())],
        registry_available=True,
    )

    assert len(out.positions) == 1
    assert out.positions[0].value_usd == Decimal("100")


def test_reconcile_enriches_manager_less_strategy_lp_with_registry_authority_ALM_3428() -> None:
    """ALM-3428: a strategy that reports NO manager (the AlmanakCode-generated
    shape, not a hand-authored SDK demo) must still come out of the union able
    to name its reviewed manager on a multi-generation venue, or
    ``teardown_post_condition`` / the LP valuation reader refuse to certify a
    position that closed cleanly on-chain — confirmed live on Aerodrome
    Slipstream (PortfolioManager Experiment-35): LP_CLOSE executed and
    confirmed on-chain, but teardown still reported ``failed`` because the
    matching registry row (which DID carry the manager) was discarded instead
    of merged.
    """
    manager = "0x" + "11" * 20
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        # No nft_manager_addr at all — real AlmanakCode dynamic_lp output.
        positions=[_lp("42", protocol="aerodrome_slipstream", value="100")],
    )

    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=manager)],
        registry_available=True,
    )

    assert len(out.positions) == 1  # still one physical position, not two
    kept = out.positions[0]
    assert kept.value_usd == Decimal("100")  # strategy's own value preserved
    assert kept.protocol == "aerodrome_slipstream"  # strategy's own slug preserved, not "lp"
    assert kept.details["nft_manager_addr"] == manager  # registry's authority merged in, not dropped


def test_reconcile_manager_less_merge_never_overwrites_a_manager_the_strategy_already_named() -> None:
    """A strategy that DOES know its own manager keeps its own answer even if
    a (differently-cased, or stale) registry copy also matches."""
    strategy_manager = "0x" + "ab" * 20
    registry_manager = "0x" + "ab" * 20  # same address, different case below
    strat = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_lp("42", protocol="aerodrome_slipstream", value="100", nft_manager_addr=strategy_manager)],
    )

    out = reconcile_lp_with_registry(
        strategy_summary=strat,
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=registry_manager.upper())],
        registry_available=True,
    )

    assert len(out.positions) == 1
    assert out.positions[0].details["nft_manager_addr"] == strategy_manager  # unchanged, not overwritten


def test_registry_authority_does_not_add_a_higher_precedence_alias_to_strategy_authority() -> None:
    strategy = _lp("42")
    strategy.details = {"position_manager": "0x" + "ab" * 20}
    registry = _lp("42", nft_manager_addr="0x" + "cd" * 20)

    out = _merge_registry_lp_authority(strategy, registry)

    assert out is strategy
    assert out.details == {"position_manager": "0x" + "ab" * 20}


def test_registry_authority_replaces_a_whitespace_only_strategy_alias() -> None:
    strategy = _lp("42", protocol="aerodrome_slipstream")
    strategy.details = {"position_manager": " \t "}
    manager = "0x" + "ab" * 20

    out = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id=DEPLOYMENT_ID,
            timestamp=datetime.now(UTC),
            positions=[strategy],
        ),
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=manager)],
        registry_available=True,
    )

    assert len(out.positions) == 1
    assert out.positions[0].details["nft_manager_addr"] == manager
    assert registry_enumeration_module._lp_nft_parts(out.positions[0]) == ("42", manager)


def test_lp_nft_parts_skips_a_blank_higher_precedence_alias_for_a_valid_lower_one() -> None:
    position = _lp("42", protocol="aerodrome_slipstream")
    manager = "0x" + "ab" * 20
    position.details = {"nft_manager_addr": " \t ", "position_manager": manager}

    assert registry_enumeration_module._lp_nft_parts(position) == ("42", manager)


def test_conflicting_registry_manager_aliases_are_not_merged() -> None:
    strategy = _lp("42")
    registry = _lp("42", nft_manager_addr="0x" + "ab" * 20)
    registry.details["position_manager"] = "0x" + "cd" * 20

    out = _merge_registry_lp_authority(strategy, registry)

    assert out is strategy
    assert out.details == {}


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


def test_reconcile_deduplicates_strategy_and_registry_perp_by_full_economic_identity() -> None:
    """Synthetic HOT ids and venue WARM keys may name one aggregate perp."""
    market_address = "0x70d95587d40a2caf56bd97485ab3eec10bee6336"
    collateral_address = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    strategy_position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-ETH/USD-arbitrum",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("20"),
        details={
            "market": "ETH/USD",
            "market_address": market_address,
            "collateral_token": "USDC",
            "collateral_address": collateral_address,
            "is_long": True,
        },
    )
    registry_position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="0xvenuepositionkey",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": market_address,
            "collateral_token": collateral_address,
            "direction": "long",
            "source": "position_registry",
        },
    )
    summary = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[strategy_position],
    )

    merged = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=[registry_position],
        registry_available=True,
    )

    assert merged.positions == [strategy_position]


# Real Arbitrum GMX addresses (VIB-6287). The former fixtures — "0xeth",
# "0xusdc", "0xbtc", "0xusdt" — are not address-shaped, so the venue identity
# hook resolved NOTHING for either row and both fell through to the framework
# default. The test therefore passed with the venue mechanism disabled and
# discriminated on none of the five axes its name claims. Using catalogue
# addresses puts both rows through the real resolution path, so each axis now
# has to hold up against the mechanism that is actually shipping.
_ARB_ETH_MARKET = market_address("arbitrum", "ETH/USD")
_ARB_BTC_MARKET = market_address("arbitrum", "BTC/USD")
_ARB_USDC = GMX_V2_TOKENS["arbitrum"]["USDC"]
_ARB_USDT = GMX_V2_TOKENS["arbitrum"]["USDT"]
# An account, so the DERIVE path is exercised too and not just the semantic one.
_WALLET = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"


@pytest.mark.parametrize(
    ("dimension", "registry_value"),
    [
        ("chain", "avalanche"),
        ("protocol", "other_perp"),
        ("market", _ARB_BTC_MARKET),
        ("collateral", _ARB_USDT),
        ("direction", "short"),
    ],
)
def test_reconcile_never_deduplicates_distinct_perp_economic_identity(
    dimension: str,
    registry_value: str,
) -> None:
    """Every component of the perp identity independently prevents collapse.

    Negative control for VIB-6287: the alias-set union must never merge two
    genuinely distinct positions. Over-collapse is the strictly worse failure —
    a suppressed registry row is never closed and the funds strand silently,
    with no alarm — so each axis is asserted separately rather than in one
    all-different fixture that any single working axis would satisfy.
    """
    strategy_position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="eth-long",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("20"),
        details={"market": _ARB_ETH_MARKET, "collateral_token": _ARB_USDC, "is_long": True},
    )
    registry_position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="btc-long",
        chain=registry_value if dimension == "chain" else "arbitrum",
        protocol=registry_value if dimension == "protocol" else "gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": registry_value if dimension == "market" else _ARB_ETH_MARKET,
            "collateral_token": registry_value if dimension == "collateral" else _ARB_USDC,
            "direction": registry_value if dimension == "direction" else "long",
        },
    )
    summary = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[strategy_position],
    )

    merged = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=[registry_position],
        registry_available=True,
        wallet_for_chain=lambda _chain: _WALLET,
    )

    assert {p.position_id for p in merged.positions} == {"eth-long", "btc-long"}


def test_reconcile_collapses_the_same_perp_across_value_spaces() -> None:
    """The POSITIVE control the negative one above needs to be meaningful.

    Identical to the fixtures above except that the registry row names the same
    market and collateral by SYMBOL where the strategy row names them by
    ADDRESS. If this did not collapse, every parametrisation above would pass
    for the trivial reason that nothing ever collapses.
    """
    strategy_position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="eth-long",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("20"),
        details={"market": _ARB_ETH_MARKET, "collateral_token": _ARB_USDC, "is_long": True},
    )
    # The position_id is the REAL venue key these details derive for `_WALLET`
    # (verified against the mainnet run of record). A placeholder key here would make
    # the row internally inconsistent — naming one position by id and another by
    # attributes — which the hook now refuses to name at all, because emitting both
    # would bridge two distinct positions under the transitive closure (#3534 panel).
    registry_position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={"market": "ETH/USD", "collateral_token": "USDC", "direction": "long"},
    )
    summary = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[strategy_position],
    )

    merged = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=[registry_position],
        registry_available=True,
        wallet_for_chain=lambda _chain: _WALLET,
    )

    assert {p.position_id for p in merged.positions} == {"eth-long"}


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


# ---------------------------------------------------------------------------
# Alias identity must be closed TRANSITIVELY (VIB-6287, found by Codex on #3534)
# ---------------------------------------------------------------------------


def _alias_row(position_id: str) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=position_id,
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={},
    )


@pytest.mark.parametrize("order", [("A", "B"), ("B", "A")], ids=["bridge-last", "bridge-first"])
def test_a_bridge_row_collapses_a_key_only_row_into_a_tuple_only_row(monkeypatch, order):
    """ "Same iff intersecting" is not an equivalence until it is closed transitively.

    A row may carry several aliases, and a row carrying BOTH a venue key and a
    market/collateral/side tuple bridges a key-only row to a tuple-only row. The
    original greedy single pass could not use that bridge: the key-only row was
    appended before the bridge arrived, and the bridge was then discarded with
    its aliases, so ``key ~ sem`` was never learned and one physical position
    enumerated as two — VIB-6287's exact symptom, surviving inside its own fix.

    Both orderings are pinned because in THIS shape — a strategy row present —
    order genuinely cannot matter: ``seen`` holds ``{sem}`` before the loop, so the
    bridge always intersects and is always discarded. That is what makes "absorb
    the duplicate's aliases instead of dropping them" an insufficient fix: by then
    the key-only row is already appended.

    The RESTART shape is order-dependent and is pinned separately below; the
    registry read carries no ``ORDER BY``, so row order is a backend detail.

    Aliases are injected rather than derived so this tests the UNION algorithm
    and cannot pass or fail for a reason belonging to key derivation.
    """
    aliases = {
        "S": frozenset({("sem",)}),
        "A": frozenset({("key",)}),
        "B": frozenset({("key",), ("sem",)}),
    }
    monkeypatch.setattr(
        registry_enumeration_module,
        "_dedupe_keys",
        lambda position, wallet_for_chain=None, unambiguous_nft_ids=frozenset(): aliases[position.position_id],
    )

    out = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id="d", timestamp=datetime.now(UTC), positions=[_alias_row("S")]
        ),
        registry_positions=[_alias_row(x) for x in order],
        registry_available=True,
    )

    assert [p.position_id for p in out.positions] == ["S"], (
        "one physical position enumerated more than once — the bridge row's aliases "
        "were not used to join the key-only row to the strategy row"
    )


def test_unmeasured_rows_never_collapse_into_one_another():
    """The non-vacuity control: EMPTY is not a shared alias.

    An empty alias set means UNMEASURED. If the component build treated it as a
    linkable token, every unmeasured row across every venue would join one
    component and all but one would be suppressed — over-collapse, the silent
    strand. Each must survive as its own row.
    """
    monkeypatch_free_empty = frozenset()
    rows = [_alias_row("R1"), _alias_row("R2")]
    import unittest.mock as _mock

    with _mock.patch.object(
        registry_enumeration_module,
        "_dedupe_keys",
        lambda position, wallet_for_chain=None, unambiguous_nft_ids=frozenset(): monkeypatch_free_empty,
    ):
        out = reconcile_lp_with_registry(
            strategy_summary=TeardownPositionSummary(deployment_id="d", timestamp=datetime.now(UTC), positions=[]),
            registry_positions=rows,
            registry_available=True,
        )

    assert [p.position_id for p in out.positions] == ["R1", "R2"]


@pytest.mark.parametrize(
    "order",
    [("A", "B", "S"), ("B", "A", "S"), ("S", "A", "B")],
    ids=["key-bridge-sem", "bridge-key-sem", "sem-key-bridge"],
)
def test_the_restart_shape_is_order_independent(monkeypatch, order):
    """The shape the registry cutover exists for: the strategy reports NOTHING.

    With no strategy row, ``seen`` starts empty, so which row is appended first
    depends purely on iteration order — and the registry read carries **no
    ``ORDER BY``**, so that order is whatever the backend returns (rowid order on
    SQLite in practice, unspecified on Postgres and free to shift after updates or
    VACUUM). Under the old greedy pass the same three rows enumerated as 2, 1 or 2
    depending only on arrival order.

    One physical position must enumerate as one row under every permutation.
    """
    aliases = {
        "S": frozenset({("sem",)}),
        "A": frozenset({("key",)}),
        "B": frozenset({("key",), ("sem",)}),
    }
    monkeypatch.setattr(
        registry_enumeration_module,
        "_dedupe_keys",
        lambda position, wallet_for_chain=None, unambiguous_nft_ids=frozenset(): aliases[position.position_id],
    )

    out = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(deployment_id="d", timestamp=datetime.now(UTC), positions=[]),
        registry_positions=[_alias_row(x) for x in order],
        registry_available=True,
    )

    assert len(out.positions) == 1, (
        f"restart shape, arrival order {order}: one physical position enumerated as "
        f"{[p.position_id for p in out.positions]}"
    )


# ---------------------------------------------------------------------------
# VIB-6730 — the manager-qualified and bare NFT identity forms must bridge.
#
# Field repro (Arbitrum Anvil fork, 2026-08-19): a `uniswap_lp` run closed its
# LP successfully on-chain (NFT 5653574 burned, `ownerOf` reverts) and teardown
# still reported FAILED, latching the deployment out of new entries (VIB-5572).
# Enumeration had counted the ONE physical NFT twice —
# `positions=['5653574', '5653574']` — because the registry row carried
# `nft_manager_addr` (every V3-family receipt parser writes it) while the
# strategy's `get_open_positions()` did not (essentially none do). One copy had
# an on-chain post-condition and the other did not, so TD-15 refused to certify
# a close that had demonstrably happened.
#
# The pair below is the whole point: the bridge must collapse the same NFT
# across the two forms WITHOUT ever collapsing equal token ids that belong to
# two different manager authorities (Slipstream NPM generations).
# ---------------------------------------------------------------------------


def _unqualified_strategy_lp(token_id: str = "5653574", chain: str = "arbitrum") -> PositionInfo:
    """The exact strategy-reported shape from the VIB-6730 repro.

    `almanak/demo_strategies/uniswap_lp/strategy.py` reports the pool, the fee
    tier, the amounts and the token symbols — and no manager authority.
    """
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=token_id,
        chain=chain,
        protocol="uniswap_v3",
        value_usd=Decimal("62.5"),
        details={"pool": "0xPOOL", "fee_tier": 500, "token0": "WETH", "token1": "USDC"},
    )


@pytest.mark.asyncio
async def test_vib6730_manager_qualified_registry_row_dedupes_against_unqualified_strategy_lp() -> None:
    """1 physical NFT, manager on one side only → 1 union entry, not 2."""
    sm = _FakeRegistrySM({"lp": [_v3_row("5653574")], "lp_v4": []})
    registry_positions, available = await read_open_lp_positions_from_registry(
        state_manager=sm, deployment_id=DEPLOYMENT_ID
    )
    assert available is True
    assert registry_positions[0].details["nft_manager_addr"] == V3_NFT_MANAGER

    merged = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id=DEPLOYMENT_ID,
            timestamp=datetime.now(UTC),
            positions=[_unqualified_strategy_lp("5653574")],
        ),
        registry_positions=registry_positions,
        registry_available=True,
    )

    assert [p.position_id for p in merged.positions] == ["5653574"]
    # The strategy's richer copy survives — it is the one carrying the value and
    # the pool metadata the closing lane reads.
    assert merged.positions[0].protocol == "uniswap_v3"


def test_vib6730_equal_token_ids_under_two_managers_are_never_bridged() -> None:
    """The bridge must not buy dedup by merging two physical positions.

    Slipstream-style token ids are per-manager counters, so `(manager A, 42)`
    and `(manager B, 42)` are different NFTs. With the id ambiguous, no bare
    alias is emitted for it and every row stays distinct — over-split and loud,
    which is the only direction this module is allowed to fail in.
    """
    manager_a = "0x" + "aa" * 20
    manager_b = "0x" + "bb" * 20

    merged = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id=DEPLOYMENT_ID,
            timestamp=datetime.now(UTC),
            positions=[_lp("42", nft_manager_addr=manager_a), _unqualified_strategy_lp("42")],
        ),
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=manager_b)],
        registry_available=True,
    )

    assert len(merged.positions) == 3
    assert {str(p.details.get("nft_manager_addr") or "") for p in merged.positions} == {
        manager_a,
        manager_b,
        "",
    }


def test_vib6730_bridge_stays_chain_scoped() -> None:
    """Same token id, different chains → still two positions.

    The bare alias is only ever emitted alongside the row's own identity, and
    every key remains `(chain, position_type, ...)`-scoped, so bridging cannot
    let an Arbitrum NFT suppress a Base NFT that happens to share its number.
    """
    merged = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id=DEPLOYMENT_ID,
            timestamp=datetime.now(UTC),
            positions=[_unqualified_strategy_lp("42", chain="arbitrum")],
        ),
        registry_positions=[_lp("42", protocol="lp", chain="base", nft_manager_addr=V3_NFT_MANAGER)],
        registry_available=True,
    )

    assert len(merged.positions) == 2
    assert {p.chain for p in merged.positions} == {"arbitrum", "base"}


@pytest.mark.asyncio
async def test_vib6730_completeness_check_matches_manager_qualified_row_no_false_absent() -> None:
    """No false "ABSENT from position_registry" for a row that plainly exists.

    Before the fix this fired on every V3-family LP teardown: the registry key
    was `nft:<manager>:<id>` and the strategy key was `nft:<id>`, so the row was
    reported missing from the very registry that held it, and TD-06 recorded the
    registry as incomplete on a perfectly complete registry.
    """
    from unittest.mock import AsyncMock, MagicMock, patch

    from almanak.framework.teardown.registry_enumeration import _verify_lp_enumeration_completeness

    read = RegistryReadResult(
        positions=[_position_info_from_registry_row(_v3_row("5653574"), primitive="lp")],
        available=True,
        failed_primitives=(),
    )
    strategy = MagicMock()
    strategy._gateway_client = MagicMock()
    strategy._gateway_network = ""
    summary = TeardownPositionSummary(
        deployment_id=DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=[_unqualified_strategy_lp("5653574")],
    )
    with patch(
        "almanak.framework.teardown.live_position_reads.chain_verify_lp_open",
        new=AsyncMock(return_value=True),
    ) as verify:
        await _verify_lp_enumeration_completeness(strategy=strategy, strategy_summary=summary, read=read)

    # Matched → the discrepancy set is empty → zero chain reads and no warning.
    verify.assert_not_called()


@pytest.mark.asyncio
async def test_vib6730_registry_row_already_closed_still_enumerates_one_position() -> None:
    """The ticket's second lead, pinned as a non-cause.

    `_registry_open_keys` reads only `status='open'` rows, so it was proposed
    that a row already flipped to `closed` by the time the verify runs would
    make every on-chain-discovered position look absent. It does not: with no
    OPEN row the registry contributes nothing to the union (one position, from
    the strategy), and the ABSENT warning is additionally gated on the chain
    confirming the position still OPEN — which a closed position cannot do.
    """
    from unittest.mock import AsyncMock, MagicMock, patch

    from almanak.framework.teardown.registry_enumeration import _verify_lp_enumeration_completeness

    merged = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id=DEPLOYMENT_ID,
            timestamp=datetime.now(UTC),
            positions=[_unqualified_strategy_lp("5653574")],
        ),
        registry_positions=[],  # the row is `closed`, so the OPEN read returns nothing
        registry_available=True,
    )
    assert [p.position_id for p in merged.positions] == ["5653574"]

    strategy = MagicMock()
    strategy._gateway_client = MagicMock()
    strategy._gateway_network = ""
    with patch(
        "almanak.framework.teardown.live_position_reads.chain_verify_lp_open",
        new=AsyncMock(return_value=False),  # burned on-chain
    ):
        with patch.object(registry_enumeration_module.logger, "warning") as warn:
            await _verify_lp_enumeration_completeness(
                strategy=strategy,
                strategy_summary=merged,
                read=RegistryReadResult(positions=[], available=True, failed_primitives=()),
            )

    assert not [c for c in warn.call_args_list if "ABSENT" in str(c)]


def test_vib6735_complementary_source_rows_are_split_not_silently_merged() -> None:
    """The complementary-source hole is CLOSED: split loudly, never strand (VIB-6735).

    This test used to pin the opposite. While the manager was taken only from the
    producer, "at most one *observed* manager" was satisfied whenever the second
    authority never appeared, so a strategy row that is physically manager B's NFT
    merged with a registry row that is manager A's NFT of the same numeric id, and
    A was dropped from the enumeration — the strand direction, and the one
    direction this module refuses to fail in. Its docstring said the assertion had
    to be inverted to ``== 2`` when VIB-6735 landed rather than deleted. This is
    that inversion.

    :func:`_derived_lp_manager` now reconstructs the missing authority from
    ``(protocol, chain)`` using the same lookup that builds the registry row's own
    ``physical_identity_hash``. Here the strategy row declares ``uniswap_v3`` on
    ``arbitrum`` and resolves to the canonical NPM, which differs from ``manager_a``
    — so TWO managers are observed, the bridge switches off for that id, and the
    rows stay separate.

    The residual is now a loud double-count instead of a silent strand, which is
    the correct failure polarity: an operator sees two positions for one NFT, and
    both get closed. Nothing goes unclosed.
    """
    manager_a = "0x" + "aa" * 20

    merged = reconcile_lp_with_registry(
        strategy_summary=TeardownPositionSummary(
            deployment_id=DEPLOYMENT_ID,
            timestamp=datetime.now(UTC),
            positions=[_unqualified_strategy_lp("42")],  # physically manager B's NFT
        ),
        registry_positions=[_lp("42", protocol="lp", nft_manager_addr=manager_a)],
        registry_available=True,
    )

    # Two managers in play -> no bare alias -> both rows survive. Reverting
    # _derived_lp_manager collapses this to 1 and strands manager A's position.
    assert len(merged.positions) == 2


# ---------------------------------------------------------------------------
# VIB-6735: deriving the missing ERC-721 manager authority
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("protocol", "chain", "expected"),
    (
        ("uniswap_v3", "arbitrum", "0xc36442b4a4522e871399cd717abdd847ab11fe88"),
        # Sushi has its OWN manager on every chain. An earlier revision routed this
        # through a helper that falls through to the canonical Uniswap NPM and got
        # 0xc36442b4... here -- a false authority, which is worse than none.
        ("sushiswap_v3", "arbitrum", "0xf0cbce1942a68beb3d1b73f0dd86c8dcc363ef49"),
        ("sushiswap_v3", "ethereum", "0x2214a42d8e2a1d20635c2cb0664422c528b6a432"),
        ("uniswap_v4", "arbitrum", "0xd88f38f930b7952f2db2432cb002e7abbf3dd869"),
        ("pancakeswap_v3", "bsc", "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364"),
    ),
)
def test_vib6735_derived_manager_resolves_recognized_protocols(protocol: str, chain: str, expected: str) -> None:
    """A recognized protocol resolves to its real per-chain manager, lowercased.

    Lowercasing is load-bearing, not cosmetic: the underlying lookup returns the
    UniV3 NPM in mixed case (``0xC36442b4...``) while every producer and
    ``_lp_nft_parts`` store it lowercased. An un-normalised value would compare
    unequal to the identical address and silently re-split the very pair this
    derivation exists to join.
    """
    assert registry_enumeration_module._derived_lp_manager(protocol, chain) == expected


@pytest.mark.parametrize(
    "protocol",
    (
        "lp",  # the registry primitive, NOT a connector slug
        "lp_v4",
        "some_unlisted_v3_fork",
        "",
    ),
)
def test_vib6735_derived_manager_refuses_unrecognized_protocols(protocol: str) -> None:
    """Unrecognized protocols get NO authority — never a confidently wrong one.

    This is the whole safety argument for the derivation. The underlying
    ``_nft_manager_for_protocol_chain`` falls through to the canonical UniV3 NPM
    for ANY unrecognized protocol — measured: ``lp``, ``lp_v4`` and a nonsense slug
    all return ``0xC36442b4...``. Consuming that fallback would stamp a wrong
    manager on an unrecognized fork and manufacture a false identity, which is the
    failure this module exists to prevent. An empty result keeps the bare key and
    leaves ``_lp_bridge_tokens`` as the bounded fallback.
    """
    assert registry_enumeration_module._derived_lp_manager(protocol, "arbitrum") == ""
    # Guard the specific wrong answer, so a future refactor that "simplifies" the
    # allowlist away fails here rather than in a teardown.
    assert registry_enumeration_module._derived_lp_manager(protocol, "arbitrum") != (
        "0xc36442b4a4522e871399cd717abdd847ab11fe88"
    )


def test_vib6735_sushi_never_resolves_to_the_uniswap_manager() -> None:
    """A family member with its OWN manager must not inherit Uniswap's.

    SushiSwap V3 is in the UniV3 LP grouping family, so a membership-gated
    derivation lets it through -- but Sushi deploys its own NonfungiblePositionManager
    on every chain it supports. Resolving it through a helper that falls through to
    the canonical Uniswap NPM produced a FALSE authority: the Sushi row would fail to
    dedupe against its own registry row, and could alias an unrelated Uniswap NFT of
    the same token id and suppress it. Pinned per chain because the wrong answer was
    wrong on all of them.
    """
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3

    for chain, entry in SUSHISWAP_V3.items():
        expected = (entry.get("position_manager") or "").strip().lower()
        if not expected:
            continue
        derived = registry_enumeration_module._derived_lp_manager("sushiswap_v3", chain)
        assert derived == expected, f"{chain}: derived {derived!r}, connector says {expected!r}"
        assert derived != "0xc36442b4a4522e871399cd717abdd847ab11fe88", f"{chain}: inherited the Uniswap NPM"


def test_vib6735_derived_manager_returns_empty_for_unknown_chain() -> None:
    """A recognized protocol on a chain with no registered manager stays bare."""
    assert registry_enumeration_module._derived_lp_manager("uniswap_v3", "not_a_chain") == ""


def test_vib6735_derivation_qualifies_the_flagship_unqualified_strategy_row() -> None:
    """The VIB-6730 strategy row now carries the registry row's own authority.

    ``uniswap_lp`` reports no manager; the V3 receipt parser always writes
    ``0xc36442b4...``. After derivation both sides key identically, so the pair
    matches WITHOUT needing the bare-form bridge at all.
    """
    parts = registry_enumeration_module._lp_nft_parts(_unqualified_strategy_lp("5653574"))
    assert parts == ("5653574", "0xc36442b4a4522e871399cd717abdd847ab11fe88")

    registry_parts = registry_enumeration_module._lp_nft_parts(
        _lp("5653574", protocol="lp", nft_manager_addr="0xC36442b4a4522E871399CD717aBDD847Ab11FE88")
    )
    assert registry_parts == parts


def test_vib6735_producer_supplied_manager_still_wins_over_derivation() -> None:
    """An explicit authority is never overridden by the derived one."""
    explicit = "0x" + "bb" * 20
    row = _unqualified_strategy_lp("42")
    row.details["nft_manager_addr"] = explicit
    assert registry_enumeration_module._lp_nft_parts(row) == ("42", explicit)


def _fallback_chains(protocol: str) -> frozenset[str]:
    """Chains a protocol reaches only through the migration NPM view (every reviewed manager per chain)."""
    from almanak.connectors._strategy_base.address_registry import address_supported_chains
    from almanak.framework.migration.backfill import _NPM_ADDRESS_SETS_BY_PROTOCOL

    set_map = _NPM_ADDRESS_SETS_BY_PROTOCOL.get(protocol)
    if set_map:
        return frozenset(set_map)
    if set_map is not None:
        # Declared with no reviewed generation anywhere (velodrome_slipstream
        # rides the aerodrome table): the derivation is still checked on every
        # chain that table covers, where it must refuse.
        return frozenset(address_supported_chains("aerodrome") or frozenset())
    return frozenset()


def _reviewed_lp_managers(protocol: str, chain: str) -> frozenset[str]:
    """Every reviewed ERC-721 manager the connector publishes for (protocol, chain).

    More than one means the venue ships multiple generations and no single address
    is "the" authority — Aerodrome Slipstream on Base publishes a legacy and a
    current NPM, and new positions always mint under the current one.

    Deliberately consults the migration view that carries EVERY reviewed manager
    per chain, never a per-protocol singleton: a singleton reports one address
    where a multi-generation venue publishes two — which is exactly how the
    Aerodrome defect was produced, and an earlier version of this helper
    inherited the same blind spot and would have blessed it again.
    """
    from almanak.connectors._strategy_base.address_registry import addresses_for

    # The RECEIPT PARSER is the authority where one exists, because it is what
    # stamps ``nft_manager_addr`` onto the registry row this derivation must match.
    # They are not always the same source: uniswap_v3 on mantle is stamped
    # 0x218bf598... ("Agni Finance fork") by the parser, while the uniswap_v3
    # address table names 0x5911cb36... (the governance deployment). Comparing only
    # against the table made this guard demand the value that does NOT match the
    # registry row -- it would have ENFORCED the VIB-6730 wedge on mantle instead of
    # catching it.
    parser_map = _receipt_parser_managers(protocol)
    if parser_map.get(chain):
        return frozenset({str(parser_map[chain]).strip().lower()})

    from almanak.framework.migration.backfill import _NPM_ADDRESS_SETS_BY_PROTOCOL

    set_map = _NPM_ADDRESS_SETS_BY_PROTOCOL.get(protocol)
    if set_map is not None:
        return frozenset(str(manager).strip().lower() for manager in set_map.get(chain) or ())

    table = addresses_for(protocol, chain) or {}
    return frozenset(
        str(table[k]).strip().lower() for k in registry_enumeration_module._LP_MANAGER_CONTRACT_KINDS if table.get(k)
    )


def _receipt_parser_managers(protocol: str) -> dict[str, str]:
    """Per-chain manager map the protocol's receipt parser stamps registry rows with."""
    import importlib

    try:
        mod = importlib.import_module(f"almanak.connectors.{protocol}.receipt_parser")
    except Exception:  # noqa: BLE001 -- not every protocol names a connector module
        return {}
    return dict(getattr(mod, "POSITION_MANAGER_ADDRESSES", {}) or {})


@pytest.mark.parametrize("family", ("univ3", "univ4"))
def test_vib6735_derivation_agrees_with_the_connector_or_refuses(family: str) -> None:
    """The derivation must AGREE with the connector, or name nothing at all.

    Non-emptiness is the wrong property, and asserting it is how two wrong-value
    defects reached review. SushiSwap derived a non-empty address that was Uniswap's
    (VIB-6750). Aerodrome Slipstream derived a non-empty address that was the LEGACY
    NPM while every new position mints under the CURRENT one -- re-opening the very
    double-count VIB-6730 exists to fix, for a shipped connector, and an earlier
    version of this test asserted that legacy value as correct.

    The contract this pins instead:

    * connector publishes exactly ONE reviewed manager -> derivation must equal it;
    * connector publishes ZERO or MORE THAN ONE        -> derivation must be EMPTY.

    The second clause is the Aerodrome case. A venue shipping two reviewed
    generations has no single "the" authority for ``(protocol, chain)``, so any
    choice is a guess, and this module's own rule is that a false authority is worse
    than none: an empty result keeps the bare key and falls back to the bounded
    bridge, which is what correctly deduped Aerodrome before the derivation existed.

    Coverage is per protocol, not aggregate. An aggregate count passes while a
    protocol contributes zero pairs -- which is how the Slipstream forks were skipped
    silently, since they publish no address table under their own slug and were
    invisible to an address-table-only domain.
    """
    from almanak.connectors._strategy_base.address_registry import address_supported_chains
    from almanak.framework.intents.compiler_constants import (
        UNIV3_LP_GROUPING_PROTOCOLS,
        UNIV4_LP_GROUPING_PROTOCOLS,
    )

    protocols = UNIV3_LP_GROUPING_PROTOCOLS if family == "univ3" else UNIV4_LP_GROUPING_PROTOCOLS
    assert protocols, f"{family} family is empty -- the registry did not load"

    violations: list[str] = []
    per_protocol: dict[str, int] = {}
    for protocol in sorted(protocols):
        chains = set(address_supported_chains(protocol) or frozenset()) | set(_fallback_chains(protocol))
        per_protocol[protocol] = len(chains)
        for chain in sorted(chains):
            derived = registry_enumeration_module._derived_lp_manager(protocol, chain)
            managers = _reviewed_lp_managers(protocol, chain)
            if len(managers) == 1:
                expected = next(iter(managers))
                if derived != expected:
                    violations.append(
                        f"{protocol}/{chain}: derived {derived or '(empty)'}, connector publishes {expected}"
                    )
            elif derived:
                violations.append(
                    f"{protocol}/{chain}: derived {derived} while the connector publishes "
                    f"{len(managers)} reviewed managers -- no single authority exists, so this is a guess"
                )

    empty = [p for p, n in per_protocol.items() if n == 0]
    assert not empty, (
        f"{family}: these protocols contributed no (protocol, chain) pairs, so they are unguarded: {empty}"
    )
    assert not violations, f"{family}: derivation disagrees with the connector: {violations}"


def test_vib6730_no_slug_ever_derives_a_superseded_slipstream_generation() -> None:
    """No protocol slug may derive a NON-CURRENT Slipstream NPM. Ever.

    This pins the blocker directly, independently of which branch happens to
    produce the refusal today, because the branch is an accident of registration
    and the invariant is not.

    Measured on the shipped code, the two Aerodrome-family slugs refuse for
    DIFFERENT reasons: ``aerodrome_slipstream`` — the slug real Slipstream rows
    actually carry — declares a ``CL_POSITION_MANAGER`` role with one kind per
    reviewed generation on the shared ``aerodrome`` table, so it hits the
    >1-candidate refusal; Classic ``aerodrome`` publishes no manager kind at all,
    so it hits the older zero-candidate branch. Same safe output, different
    reason.

    That difference is a live regression path. Register an ``aerodrome_slipstream``
    table with a single LEGACY address and the derivation would return it, the
    guard in this file would pass it (one reviewed manager, derived equals it), and
    VIB-6730 would silently re-open for Slipstream: new positions mint under the
    CURRENT manager, so a legacy authority on the strategy arm splits the pair and
    the one physical NFT enumerates twice.

    Hence this test asserts the property that actually matters — the derived value
    is never a superseded generation — rather than the mechanism that currently
    delivers it.
    """
    from almanak.connectors.aerodrome.addresses import SLIPSTREAM_LP_DEPLOYMENTS

    offenders: list[str] = []
    checked = 0
    for chain, deployments in SLIPSTREAM_LP_DEPLOYMENTS.items():
        if len(deployments) < 2:
            continue  # single-generation chain: nothing to supersede
        # Tuple order carries no meaning; the generation name does.
        by_generation = {d.generation: d for d in deployments}
        current = str(by_generation["current"].position_manager).strip().lower()
        superseded = {str(d.position_manager).strip().lower() for d in deployments if d.generation != "current"}
        assert current not in superseded, f"{chain}: the current NPM is also published as a superseded generation"
        for slug in ("aerodrome", "aerodrome_slipstream", "velodrome_slipstream"):
            checked += 1
            derived = registry_enumeration_module._derived_lp_manager(slug, chain)
            if derived and derived in superseded:
                offenders.append(f"{slug}/{chain} -> {derived} (superseded; current is {current})")

    assert checked, "no multi-generation Slipstream chain examined — the guard would be vacuous"
    assert not offenders, (
        "derivation returned a SUPERSEDED Slipstream manager: "
        f"{offenders}. New positions mint under the current NPM, so a superseded "
        "authority splits the pair and re-opens the VIB-6730 double-count."
    )


def test_vib6752_alias_slug_resolves_to_the_canonical_connector_manager() -> None:
    """A strategy's declared slug must resolve even when it differs from the table key.

    ``agni_lp_mantle`` reports ``protocol="agni"`` — the slug the SDK routes Agni
    INTENTS under — while the address table registers ``agni_finance``. An
    un-normalised lookup returned nothing, so the row stayed bare, and a bare
    NFT-shaped row can be bridge-merged onto another manager's NFT of the same
    token id and strand it (VIB-6752).

    The row IS NFT-shaped despite its prefixed ``position_id``, which is the part
    that made this easy to miss: ``resolve_nft_token_id`` reads ``details`` FIRST
    and the strategy sets ``details["nft_id"]``. Checking ``position_id`` alone
    says "not NFT-shaped" and is wrong.

    Pinned per chain, and asserted to be a NO-OP for canonical slugs, because the
    normalisation must not perturb the venues the fork proof and the Aerodrome
    multi-generation refusal depend on.
    """
    from almanak.connectors._strategy_base.address_registry import addresses_for
    from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

    # The alias resolves, and it resolves to a manager the connector really publishes.
    derived = registry_enumeration_module._derived_lp_manager("agni", "mantle")
    assert derived, "the 'agni' slug must resolve — an unresolved NFT-shaped row is bridgeable"
    canonical = normalize_protocol("mantle", "agni")
    assert canonical == "agni_finance", f"alias seam changed: agni -> {canonical}"
    published = {
        str((addresses_for(canonical, "mantle") or {}).get(k, "")).strip().lower()
        for k in registry_enumeration_module._LP_MANAGER_CONTRACT_KINDS
    } - {""}
    assert derived in published, f"derived {derived} is not published by {canonical}: {sorted(published)}"

    # And it is a no-op everywhere else, including the multi-generation refusals.
    unchanged = {
        ("uniswap_v3", "arbitrum"),
        ("sushiswap_v3", "arbitrum"),
        ("uniswap_v4", "arbitrum"),
        ("pancakeswap_v3", "bsc"),
        ("camelot", "arbitrum"),
    }
    for proto, chain in sorted(unchanged):
        # VALUE equality, not truthiness. An earlier revision of this loop asserted
        # only that each pair still derived something truthy -- the exact property
        # this file's own docstrings condemn, since both wrong-value defects on this
        # PR (SushiSwap, Aerodrome) were non-empty. Normalisation could have changed
        # any of these five addresses and a truthiness check would have passed.
        assert normalize_protocol(chain, proto) == proto, (
            f"{proto}/{chain}: normalisation is not a no-op on this canonical slug"
        )
        expected = {
            str((addresses_for(proto, chain) or {}).get(k, "")).strip().lower()
            for k in registry_enumeration_module._LP_MANAGER_CONTRACT_KINDS
        } - {""}
        assert len(expected) == 1, f"{proto}/{chain}: fixture assumption changed, {sorted(expected)}"
        assert registry_enumeration_module._derived_lp_manager(proto, chain) == next(iter(expected)), (
            f"{proto}/{chain} must still resolve to the connector's own manager after normalisation"
        )
    for proto, chain in (("aerodrome", "base"), ("aerodrome_slipstream", "base")):
        assert registry_enumeration_module._derived_lp_manager(proto, chain) == "", (
            f"{proto}/{chain} must still REFUSE — normalisation must not defeat the multi-generation guard"
        )
