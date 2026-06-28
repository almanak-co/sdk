"""Tests for deployment-scoped on-chain LP auto-recovery — VIB-5138.

Teardown emits ``LP_CLOSE`` only when the strategy's ``_position_id`` survives.
On state desync (LP NFT live on-chain but ``_position_id`` lost — often after an
``AccountingPersistenceError`` on LP open) the signal-driven runner lane emits
only a token swap (or nothing), reports complete, and strands the open NFT.

The recovery scans the wallet for V3 NFTs but is SCOPED to the token ids
attributable to THIS deployment (``position_registry`` OPEN payload + the
``position_events`` LP OPEN log), so it can never close a sibling strategy's
live LP on a shared wallet (VIB-4976).

These tests lock in:

* the pure merge logic with deployment-ownership scoping;
* the runner-lane recovery helper (``runner_teardown._recover_orphaned_lp_intents``);
* the two adversarial scenarios:
  - shared-wallet: discovered NFT NOT attributable to this deployment → NOT recovered;
  - intents-present-with-incomplete: real intents + incomplete discovery for a
    deployment that owns an LP here → degraded/incomplete signal (not silent success).
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.runner_teardown import _recover_orphaned_lp_intents
from almanak.framework.teardown.lp_recovery import (
    DeploymentLpOwnership,
    LpDiscoveryResult,
    merge_discovered_lp,
    scope_discovered_for_plan_b,
    strategy_reports_lp,
)
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
)

DEPLOYMENT = "deployment:abc123456789"
CHAIN = "base"


def _lp_position(token_id: str, value_usd: str = "0") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=token_id,
        chain=CHAIN,
        protocol="uniswap_v3",
        value_usd=Decimal(value_usd),
        details={"discovered_on_chain": True, "value_usd_unknown": True},
    )


def _token_position() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="tok",
        chain=CHAIN,
        protocol="uniswap_v3",
        value_usd=Decimal("10"),
        details={"asset": "WETH"},
    )


def _summary(positions: list[PositionInfo]) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id=DEPLOYMENT,
        timestamp=dt.datetime.now(dt.UTC),
        positions=positions,
    )


# ──────────────────────────────────────────────────────────────────────────────
# scope_discovered_for_plan_b — Plan-B (--discover) attribution partition (VIB-5476)
# ──────────────────────────────────────────────────────────────────────────────


class TestScopeDiscoveredForPlanB:
    def test_partitions_owned_vs_unattributable(self):
        discovered = _summary([_lp_position("1"), _lp_position("2"), _lp_position("3")])
        ownership = DeploymentLpOwnership(token_ids=frozenset({"1", "3"}), had_lp_open=True, available=True)
        scope = scope_discovered_for_plan_b(discovered=discovered, ownership=ownership)
        assert {p.position_id for p in scope.owned} == {"1", "3"}
        assert {p.position_id for p in scope.unattributable} == {"2"}
        assert scope.ownership_available is True

    def test_unprovable_attribution_marks_everything_unattributable(self):
        discovered = _summary([_lp_position("1"), _lp_position("2")])
        ownership = DeploymentLpOwnership(token_ids=frozenset(), had_lp_open=False, available=False)
        scope = scope_discovered_for_plan_b(discovered=discovered, ownership=ownership)
        assert scope.owned == []
        assert {p.position_id for p in scope.unattributable} == {"1", "2"}
        assert scope.ownership_available is False

    def test_unavailable_fails_closed_even_with_stale_token_ids(self):
        # Defence-in-depth: a stale/inconsistent ownership object carrying
        # token_ids while available=False must NOT let any position through the
        # break-glass gate. Everything is unattributable (CodeRabbit VIB-5476 audit).
        discovered = _summary([_lp_position("1"), _lp_position("2")])
        ownership = DeploymentLpOwnership(token_ids=frozenset({"1", "2"}), had_lp_open=True, available=False)
        scope = scope_discovered_for_plan_b(discovered=discovered, ownership=ownership)
        assert scope.owned == []
        assert {p.position_id for p in scope.unattributable} == {"1", "2"}
        assert scope.ownership_available is False

    def test_non_lp_discovery_is_unattributable(self):
        discovered = _summary([_lp_position("1"), _token_position()])
        ownership = DeploymentLpOwnership(token_ids=frozenset({"1", "tok"}), had_lp_open=True, available=True)
        scope = scope_discovered_for_plan_b(discovered=discovered, ownership=ownership)
        # Only the LP token id matches as owned; the TOKEN position is never owned.
        assert {p.position_id for p in scope.owned} == {"1"}
        assert {p.position_id for p in scope.unattributable} == {"tok"}

    def test_none_summary_is_empty(self):
        ownership = DeploymentLpOwnership(token_ids=frozenset({"1"}), had_lp_open=True, available=True)
        scope = scope_discovered_for_plan_b(discovered=None, ownership=ownership)
        assert scope.owned == []
        assert scope.unattributable == []


def _owns(*token_ids: str, had_lp_open: bool | None = None, available: bool = True) -> DeploymentLpOwnership:
    ids = frozenset(token_ids)
    return DeploymentLpOwnership(
        token_ids=ids,
        had_lp_open=bool(ids) if had_lp_open is None else had_lp_open,
        available=available,
    )


# ---------------------------------------------------------------------------
# Pure merge logic — deployment-ownership scoping
# ---------------------------------------------------------------------------


def test_strategy_reports_lp_detects_lp() -> None:
    assert strategy_reports_lp(_summary([_lp_position("2359")])) is True
    assert strategy_reports_lp(_summary([_token_position()])) is False
    assert strategy_reports_lp(_summary([])) is False
    assert strategy_reports_lp(None) is False


def test_merge_recovers_only_deployment_owned_nft() -> None:
    """State missing + discovery finds one V3 NFT THIS deployment owns → LP_CLOSE."""
    discovery = LpDiscoveryResult(summary=_summary([_lp_position("2359")]))
    outcome = merge_discovered_lp(
        positions=_summary([_token_position()]),
        intents=[MagicMock(intent_type="SWAP")],
        discovery=discovery,
        ownership=_owns("2359"),
        mode=TeardownMode.SOFT,
    )
    assert outcome.recovered_count == 1
    assert outcome.incomplete is False
    assert len(outcome.intents) == 2
    lp_close = outcome.intents[-1]
    assert lp_close.position_id == "2359"
    assert lp_close.protocol == "uniswap_v3"
    assert lp_close.chain == CHAIN
    assert lp_close.collect_fees is True


def test_merge_does_not_close_other_strategys_nft_on_shared_wallet() -> None:
    """P0 fund-safety: discovered NFT NOT attributable to this deployment is
    NEVER recovered, even though the strategy reports no LP and the id is new."""
    discovery = LpDiscoveryResult(summary=_summary([_lp_position("100")]))
    outcome = merge_discovered_lp(
        positions=_summary([]),  # this deployment reports no LP
        intents=[MagicMock(intent_type="SWAP")],
        discovery=discovery,
        ownership=_owns(),  # owns NO token ids, no LP attribution
        mode=TeardownMode.SOFT,
    )
    assert outcome.recovered_count == 0
    assert len(outcome.intents) == 1  # no spurious LP_CLOSE on #100
    assert outcome.incomplete is False


def test_merge_emergency_mode_skips_fee_collection() -> None:
    discovery = LpDiscoveryResult(summary=_summary([_lp_position("777")]))
    outcome = merge_discovered_lp(
        positions=_summary([]),
        intents=[],
        discovery=discovery,
        ownership=_owns("777"),
        mode=TeardownMode.HARD,
    )
    assert outcome.intents[-1].collect_fees is False


def test_merge_dedupes_by_token_id() -> None:
    """Strategy-reported position + same discovered+owned token id → no duplicate."""
    discovery = LpDiscoveryResult(summary=_summary([_lp_position("2359")]))
    outcome = merge_discovered_lp(
        positions=_summary([_lp_position("2359")]),  # already known
        intents=[MagicMock(intent_type="LP_CLOSE")],
        discovery=discovery,
        ownership=_owns("2359"),
        mode=TeardownMode.SOFT,
    )
    assert outcome.recovered_count == 0
    assert len(outcome.intents) == 1


def test_merge_incomplete_fatal_only_when_deployment_owned_lp() -> None:
    """Incomplete scan + this deployment HELD an LP here → incomplete fatal."""
    discovery = LpDiscoveryResult(summary=_summary([]), incomplete=True, error="balanceOf unreadable")
    outcome = merge_discovered_lp(
        positions=_summary([]),
        intents=[],
        discovery=discovery,
        ownership=_owns("2359"),  # had_lp_open True
        mode=TeardownMode.SOFT,
    )
    assert outcome.incomplete is True
    assert outcome.warning and "INCOMPLETE" in outcome.warning


def test_merge_incomplete_benign_for_non_lp_deployment() -> None:
    """F2: incomplete scan + this deployment has NO LP here → WARNING, not fatal."""
    discovery = LpDiscoveryResult(summary=_summary([]), incomplete=True, error="unrelated NPM blip")
    outcome = merge_discovered_lp(
        positions=_summary([]),
        intents=[MagicMock(intent_type="REPAY")],
        discovery=discovery,
        ownership=_owns(had_lp_open=False),  # no LP attribution
        mode=TeardownMode.SOFT,
    )
    assert outcome.incomplete is False  # not fatal
    assert outcome.warning is None


# ---------------------------------------------------------------------------
# Runner-lane recovery helper (_recover_orphaned_lp_intents)
# ---------------------------------------------------------------------------


def _strategy(positions: TeardownPositionSummary) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = DEPLOYMENT
    strategy.chain = CHAIN
    strategy.wallet_address = "0x1111111111111111111111111111111111111111"
    strategy.get_open_positions.return_value = positions
    return strategy


def _runner_with_helpers(
    monkeypatch,
    *,
    discovery_result: LpDiscoveryResult | None,
    ownership: DeploymentLpOwnership,
    wired: bool = True,
) -> tuple[MagicMock, MagicMock]:
    import almanak.framework.teardown.runner_helpers as rh

    runner = MagicMock()
    helpers = MagicMock()
    helpers.has_lp_discovery = wired
    helpers.get_deployment_lp_ownership = AsyncMock(return_value=ownership)
    helpers.discover_lp_positions = (
        AsyncMock(return_value=discovery_result) if discovery_result is not None else AsyncMock()
    )
    monkeypatch.setattr(rh, "build_runner_helpers", lambda _runner: helpers)
    return runner, helpers


@pytest.mark.asyncio
async def test_recover_appends_lp_close_for_owned_nft(monkeypatch) -> None:
    """State missing → discovery finds an OWNED NFT → LP_CLOSE appended."""
    strategy = _strategy(_summary([]))
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([_lp_position("2359")])),
        ownership=_owns("2359"),
    )
    swap = MagicMock(intent_type="SWAP")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [swap], TeardownMode.SOFT)

    helpers.get_deployment_lp_ownership.assert_awaited_once()
    helpers.discover_lp_positions.assert_awaited_once()
    assert incomplete is False
    assert len(intents) == 2
    assert intents[-1].position_id == "2359"


@pytest.mark.asyncio
async def test_recover_shared_wallet_does_not_close_foreign_nft(monkeypatch) -> None:
    """P0: discovery finds a NON-owned NFT on a shared wallet → NOT closed."""
    strategy = _strategy(_summary([]))
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([_lp_position("100")])),
        ownership=_owns(),  # this deployment owns nothing
    )
    swap = MagicMock(intent_type="SWAP")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [swap], TeardownMode.SOFT)

    # no owned ids + no LP attribution → scan skipped entirely, no foreign close.
    helpers.discover_lp_positions.assert_not_awaited()
    assert intents == [swap]
    assert incomplete is False


@pytest.mark.asyncio
async def test_recover_no_op_when_strategy_reports_lp(monkeypatch) -> None:
    strategy = _strategy(_summary([_lp_position("2359")]))
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([_lp_position("2359")])),
        ownership=_owns("2359"),
    )
    existing = MagicMock(intent_type="LP_CLOSE")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [existing], TeardownMode.SOFT)

    helpers.get_deployment_lp_ownership.assert_not_awaited()
    helpers.discover_lp_positions.assert_not_awaited()
    assert intents == [existing]
    assert incomplete is False


@pytest.mark.asyncio
async def test_recover_skips_scan_for_non_lp_deployment(monkeypatch) -> None:
    """F2: a deployment with no LP attribution never triggers the gateway scan."""
    strategy = _strategy(_summary([]))
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([]), incomplete=True),
        ownership=_owns(had_lp_open=False),
    )
    repay = MagicMock(intent_type="REPAY")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [repay], TeardownMode.SOFT)

    helpers.discover_lp_positions.assert_not_awaited()
    assert intents == [repay]
    assert incomplete is False


@pytest.mark.asyncio
async def test_recover_incomplete_for_owned_lp_sets_flag(monkeypatch) -> None:
    """Owned LP + incomplete scan → incomplete flag + warning."""
    strategy = _strategy(_summary([]))
    runner, _ = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([]), incomplete=True, error="balanceOf unreadable"),
        ownership=_owns("2359"),
    )
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [], TeardownMode.SOFT)

    assert incomplete is True
    assert warning and "INCOMPLETE" in warning


@pytest.mark.asyncio
async def test_recover_unprovable_ownership_refuses(monkeypatch) -> None:
    """No attribution source readable → never close (ownership unprovable)."""
    strategy = _strategy(_summary([]))
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([_lp_position("2359")])),
        ownership=_owns("2359", available=False),
    )
    swap = MagicMock(intent_type="SWAP")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [swap], TeardownMode.SOFT)

    helpers.discover_lp_positions.assert_not_awaited()
    assert intents == [swap]
    assert incomplete is False


@pytest.mark.asyncio
async def test_recover_no_op_when_discovery_unwired(monkeypatch) -> None:
    strategy = _strategy(_summary([]))
    runner, _ = _runner_with_helpers(
        monkeypatch,
        discovery_result=None,
        ownership=_owns("2359"),
        wired=False,
    )
    swap = MagicMock(intent_type="SWAP")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [swap], TeardownMode.SOFT)
    assert intents == [swap]
    assert incomplete is False


@pytest.mark.asyncio
async def test_recover_discovery_error_for_owned_lp_degrades(monkeypatch) -> None:
    """Owned LP + discovery helper raises → degrade (incomplete), never silent."""
    import almanak.framework.teardown.runner_helpers as rh

    strategy = _strategy(_summary([]))
    runner = MagicMock()
    helpers = MagicMock()
    helpers.has_lp_discovery = True
    helpers.get_deployment_lp_ownership = AsyncMock(return_value=_owns("2359"))
    helpers.discover_lp_positions = AsyncMock(side_effect=RuntimeError("gateway down"))
    monkeypatch.setattr(rh, "build_runner_helpers", lambda _runner: helpers)

    swap = MagicMock(intent_type="SWAP")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [swap], TeardownMode.SOFT)

    assert intents == [swap]  # risk reduction still proceeds
    assert incomplete is True  # but not certified clean — owned LP unconfirmed
    assert warning


@pytest.mark.asyncio
async def test_recover_discovery_error_for_non_lp_deployment_benign(monkeypatch) -> None:
    """Defensive: discovery raises but the deployment has token_ids attribution
    yet had_lp_open is False off-chain — the owns-LP check still degrades; while a
    deployment with neither token_ids nor had_lp_open never reaches the scan.

    This exercises the exception-path branch that stays benign only when there is
    genuinely no LP attribution (token_ids empty AND had_lp_open False). Such a
    deployment is short-circuited BEFORE the scan, so reaching the raise with no
    attribution is not normally possible — we assert the helper never even calls
    discovery for it (no degrade, no crash)."""
    strategy = _strategy(_summary([]))
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([])),
        ownership=_owns(had_lp_open=False),  # no attribution
    )
    helpers.discover_lp_positions = AsyncMock(side_effect=RuntimeError("gateway down"))
    repay = MagicMock(intent_type="REPAY")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [repay], TeardownMode.SOFT)

    helpers.discover_lp_positions.assert_not_awaited()  # scan skipped before the raise
    assert intents == [repay]
    assert incomplete is False


def test_merge_none_positions_does_not_raise() -> None:
    """Gemini MEDIUM: ``positions=None`` (some strategies/mocks) must not raise —
    normalize to an empty summary; an owned discovered NFT is still recovered."""
    discovery = LpDiscoveryResult(summary=_summary([_lp_position("2359")]))
    outcome = merge_discovered_lp(
        positions=None,
        intents=[],
        discovery=discovery,
        ownership=_owns("2359"),
        mode=TeardownMode.SOFT,
    )
    assert outcome.recovered_count == 1
    assert outcome.intents[-1].position_id == "2359"
    # merged summary built from the empty normalization + the net-new owned NFT
    assert any(p.position_id == "2359" for p in outcome.positions.positions)


@pytest.mark.asyncio
async def test_recover_none_from_get_open_positions(monkeypatch) -> None:
    """Runner helper: a strategy returning None from get_open_positions() must not
    raise; discovery still scopes to owned ids and recovers the orphan."""
    strategy = _strategy(None)  # get_open_positions() -> None
    runner, helpers = _runner_with_helpers(
        monkeypatch,
        discovery_result=LpDiscoveryResult(summary=_summary([_lp_position("2359")])),
        ownership=_owns("2359"),
    )
    swap = MagicMock(intent_type="SWAP")
    intents, incomplete, warning = await _recover_orphaned_lp_intents(runner, strategy, [swap], TeardownMode.SOFT)

    assert incomplete is False
    assert len(intents) == 2
    assert intents[-1].position_id == "2359"


# ---------------------------------------------------------------------------
# Deployment-ownership read sources (_deployment_lp_ownership + helpers)
# VIB-5138: decomposed read sources, fully covered for the CRAP gate.
# ---------------------------------------------------------------------------


class _FakeStateManager:
    """Minimal state-manager double exposing only the two ownership reads.

    ``registry_rows`` / ``event_rows`` supply return values; ``registry_raises``
    / ``events_raise`` make the respective read raise (CutoverStorageNotSupported
    is simulated by a plain exception — the helper catches Exception broadly).
    Omitting a method entirely is covered by ``_NoReadsStateManager`` below.
    """

    def __init__(
        self,
        *,
        registry_rows: list | None = None,
        event_rows: list | None = None,
        registry_raises: bool = False,
        events_raise: bool = False,
    ) -> None:
        self._registry_rows = registry_rows or []
        self._event_rows = event_rows or []
        self._registry_raises = registry_raises
        self._events_raise = events_raise

    async def get_position_registry_open_rows(self, deployment_id, *, chain=None, primitive=None):
        if self._registry_raises:
            raise RuntimeError("CutoverStorageNotSupported (simulated)")
        return self._registry_rows

    def get_position_events_sync(self, deployment_id, *, position_type=None, event_type=None):
        if self._events_raise:
            raise RuntimeError("position_events read failed (simulated)")
        return self._event_rows


class _NoReadsStateManager:
    """State manager exposing neither read method (hasattr guards → both skip)."""


class _GatewayLikeStateManager:
    """Gateway-backed manager: async registry read + async filtered events read,
    NO sync ``get_position_events_sync`` (mirrors :class:`GatewayStateManager`).

    Pins the VIB-5476 audit fix — the Plan-B ``--discover`` lane (and hosted
    runners) read ``position_events`` via the async filtered accessor; without
    the dispatch the events fallback silently vanished on the gateway path.
    """

    def __init__(self, *, registry_rows: list | None = None, event_rows: list | None = None) -> None:
        self._registry_rows = registry_rows or []
        self._event_rows = event_rows or []

    async def get_position_registry_open_rows(self, deployment_id, *, chain=None, primitive=None):
        return self._registry_rows

    async def get_position_events_filtered(self, *, deployment_id, position_types):
        # Filtered API streams EVERY event type (OPEN + CLOSE); the reader must
        # keep only OPEN rows for token-id attribution.
        return self._event_rows


def _registry_row(token_id: str) -> dict:
    return {"payload": {"token_id": token_id}, "chain": CHAIN, "primitive": "lp", "status": "open"}


def _event_row(position_id: str, chain: str = CHAIN) -> dict:
    return {"position_id": position_id, "chain": chain, "position_type": "LP", "event_type": "OPEN"}


async def _ownership(sm) -> DeploymentLpOwnership:
    from almanak.framework.teardown.runner_helpers import _deployment_lp_ownership

    runner = MagicMock()
    runner.state_manager = sm
    strategy = MagicMock()
    strategy.deployment_id = DEPLOYMENT
    return await _deployment_lp_ownership(runner, strategy, CHAIN)


@pytest.mark.asyncio
async def test_ownership_unions_both_sources_deduped() -> None:
    """Registry OK + events OK → union deduped; available + had_lp_open True."""
    sm = _FakeStateManager(
        registry_rows=[_registry_row("100"), _registry_row("200")],
        event_rows=[_event_row("200"), _event_row("300")],  # 200 overlaps
    )
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset({"100", "200", "300"})
    assert owned.had_lp_open is True
    assert owned.available is True


@pytest.mark.asyncio
async def test_ownership_registry_raises_falls_back_to_events() -> None:
    """Registry raises (CutoverStorageNotSupported) → events still yield ids;
    available True via events."""
    sm = _FakeStateManager(registry_raises=True, event_rows=[_event_row("300")])
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset({"300"})
    assert owned.had_lp_open is True
    assert owned.available is True


@pytest.mark.asyncio
async def test_ownership_events_raise_registry_still_yields() -> None:
    """Events read raises → registry still yields ids; available True."""
    sm = _FakeStateManager(registry_rows=[_registry_row("100")], events_raise=True)
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset({"100"})
    assert owned.had_lp_open is True
    assert owned.available is True


@pytest.mark.asyncio
async def test_ownership_both_raise_fails_closed() -> None:
    """BOTH reads raise → available False, empty ids (fail-closed)."""
    sm = _FakeStateManager(registry_raises=True, events_raise=True)
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset()
    assert owned.had_lp_open is False
    assert owned.available is False


@pytest.mark.asyncio
async def test_ownership_had_lp_open_true_when_open_row_exists() -> None:
    """had_lp_open True when an open registry row exists, even if events empty."""
    sm = _FakeStateManager(registry_rows=[_registry_row("100")], event_rows=[])
    owned = await _ownership(sm)
    assert owned.had_lp_open is True
    assert "100" in owned.token_ids


@pytest.mark.asyncio
async def test_ownership_no_rows_available_but_no_lp() -> None:
    """Both reads succeed but return nothing → available True, had_lp_open False."""
    sm = _FakeStateManager(registry_rows=[], event_rows=[])
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset()
    assert owned.had_lp_open is False
    assert owned.available is True


@pytest.mark.asyncio
async def test_ownership_none_state_manager_fails_closed() -> None:
    """No state manager at all → both reads skip → available False."""
    owned = await _ownership(None)
    assert owned.available is False
    assert owned.token_ids == frozenset()


@pytest.mark.asyncio
async def test_ownership_state_manager_without_read_methods_fails_closed() -> None:
    """State manager exposing neither read method → both skip → available False."""
    owned = await _ownership(_NoReadsStateManager())
    assert owned.available is False
    assert owned.token_ids == frozenset()


@pytest.mark.asyncio
async def test_ownership_event_other_chain_excluded() -> None:
    """An OPEN event on a DIFFERENT chain is not counted toward this chain."""
    sm = _FakeStateManager(event_rows=[_event_row("999", chain="arbitrum"), _event_row("300", chain=CHAIN)])
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset({"300"})


@pytest.mark.asyncio
async def test_ownership_event_missing_chain_fails_closed() -> None:
    """VIB-5476: an OPEN event with a missing/empty chain is AMBIGUOUS, not owned.

    The Plan-B break-glass ``--discover`` lane must never authorise touching a
    position whose chain attribution is not provable. A row carrying the exact
    requested chain still matches; a row missing ``chain`` (or carrying a blank
    one) is excluded — fail closed.
    """
    sm = _FakeStateManager(
        event_rows=[
            {"position_id": "111", "position_type": "LP", "event_type": "OPEN"},  # chain key absent
            {"position_id": "222", "chain": "", "position_type": "LP", "event_type": "OPEN"},  # empty chain
            {"position_id": "333", "chain": "   ", "position_type": "LP", "event_type": "OPEN"},  # whitespace
            _event_row("444", chain=CHAIN),  # explicit matching chain still counts
        ]
    )
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset({"444"})


# ──────────────────────────────────────────────────────────────────────────────
# read_deployment_lp_ownership — gateway-backed events fallback (VIB-5476 audit)
# ──────────────────────────────────────────────────────────────────────────────


def _closed_event_row(position_id: str, chain: str = CHAIN) -> dict:
    return {"position_id": position_id, "chain": chain, "position_type": "LP", "event_type": "CLOSE"}


@pytest.mark.asyncio
async def test_gateway_lane_reads_events_via_async_filtered() -> None:
    """Gateway-backed SM (no sync accessor) must still surface position_events
    OPEN ids via the async filtered read — otherwise a pre-cutover deployment's
    OWN LP gets refused and the operator is pushed to --wallet-wide."""
    from almanak.framework.teardown.runner_helpers import read_deployment_lp_ownership

    sm = _GatewayLikeStateManager(
        registry_rows=[],  # pre-cutover: nothing in the registry
        event_rows=[_event_row("700"), _closed_event_row("800")],  # CLOSE must be ignored
    )
    owned = await read_deployment_lp_ownership(sm, DEPLOYMENT, CHAIN)
    assert owned.token_ids == frozenset({"700"})
    assert owned.had_lp_open is True
    assert owned.available is True


@pytest.mark.asyncio
async def test_gateway_lane_unions_registry_and_events() -> None:
    """Gateway lane unions the async registry rows with the async filtered events."""
    from almanak.framework.teardown.runner_helpers import read_deployment_lp_ownership

    sm = _GatewayLikeStateManager(
        registry_rows=[_registry_row("100")],
        event_rows=[_event_row("200"), _event_row("999", chain="arbitrum")],  # other-chain excluded
    )
    owned = await read_deployment_lp_ownership(sm, DEPLOYMENT, CHAIN)
    assert owned.token_ids == frozenset({"100", "200"})
    assert owned.available is True


@pytest.mark.asyncio
async def test_ownership_skips_rows_without_token_id() -> None:
    """Registry row with no token_id and event row with None position_id ignored."""
    sm = _FakeStateManager(
        registry_rows=[{"payload": {}, "chain": CHAIN}, {"not_a_payload": 1}],
        event_rows=[{"position_id": None, "chain": CHAIN}],
    )
    owned = await _ownership(sm)
    assert owned.token_ids == frozenset()
    assert owned.available is True  # reads completed, just no usable ids


# ---------------------------------------------------------------------------
# Bounded gateway-backed discovery wrapper (_discover_lp_for_teardown)
# VIB-5138: cover all four branches for the CRAP gate.
# ---------------------------------------------------------------------------


def _discovered_position(token_id: int = 2359):
    from almanak.framework.teardown.discovery import DiscoveredPosition

    return DiscoveredPosition(
        token_id=token_id,
        npm_address="0xnpm",
        chain=CHAIN,
        protocol="uniswap_v3",
        token0="0xtoken0",
        token1="0xtoken1",
        fee=3000,
        tick_lower=-100,
        tick_upper=100,
        liquidity=12345,
    )


def _discovery_strategy(*, deployment_id=DEPLOYMENT, chain=CHAIN, wallet="0x1111") -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = chain
    strategy.wallet_address = wallet
    return strategy


@pytest.mark.asyncio
async def test_discover_for_teardown_success(monkeypatch) -> None:
    """Happy path: scan returns positions → LpDiscoveryResult, incomplete False."""
    import almanak.framework.teardown.runner_helpers as rh

    async def _fake_scan(*, client, chain, wallet, strict):
        assert strict is True  # strict mode REQUIRED for recovery
        return [_discovered_position(2359)]

    monkeypatch.setattr("almanak.framework.teardown.discovery.discover_lp_positions", _fake_scan)
    runner = MagicMock()
    runner._get_gateway_client.return_value = MagicMock()

    result = await rh._discover_lp_for_teardown(runner, _discovery_strategy())
    assert result.incomplete is False
    assert any(p.position_id == "2359" for p in result.summary.positions)


@pytest.mark.asyncio
async def test_discover_for_teardown_missing_args_skips(monkeypatch) -> None:
    """Missing wallet → skip (no scan), incomplete False."""
    import almanak.framework.teardown.runner_helpers as rh

    called = {"scan": False}

    async def _fake_scan(**kwargs):
        called["scan"] = True
        return []

    monkeypatch.setattr("almanak.framework.teardown.discovery.discover_lp_positions", _fake_scan)
    runner = MagicMock()

    result = await rh._discover_lp_for_teardown(runner, _discovery_strategy(wallet=""))
    assert called["scan"] is False
    assert result.incomplete is False
    assert list(result.summary.positions) == []


@pytest.mark.asyncio
async def test_discover_for_teardown_incomplete_raises(monkeypatch) -> None:
    """Strict scan raises DiscoveryIncomplete → incomplete True with error."""
    import almanak.framework.teardown.runner_helpers as rh
    from almanak.framework.teardown.discovery import DiscoveryIncomplete

    async def _fake_scan(**kwargs):
        raise DiscoveryIncomplete(chain=CHAIN, npm="0xnpm", missing=[1, 2])

    monkeypatch.setattr("almanak.framework.teardown.discovery.discover_lp_positions", _fake_scan)
    runner = MagicMock()
    runner._get_gateway_client.return_value = MagicMock()

    result = await rh._discover_lp_for_teardown(runner, _discovery_strategy())
    assert result.incomplete is True
    assert result.error and "incomplete" in result.error.lower()


@pytest.mark.asyncio
async def test_discover_for_teardown_generic_error(monkeypatch) -> None:
    """Scan raises a generic exception → incomplete True (never raises out)."""
    import almanak.framework.teardown.runner_helpers as rh

    async def _fake_scan(**kwargs):
        raise RuntimeError("gateway down")

    monkeypatch.setattr("almanak.framework.teardown.discovery.discover_lp_positions", _fake_scan)
    runner = MagicMock()
    runner._get_gateway_client.return_value = MagicMock()

    result = await rh._discover_lp_for_teardown(runner, _discovery_strategy())
    assert result.incomplete is True
    assert result.error and "gateway down" in result.error
