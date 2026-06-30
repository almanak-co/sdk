"""Unit tests for boot-time on-chain strand detection (VIB-5419 / A2a).

Covers the three contract cases from the plan:

(a) DB record matches the on-chain position  -> no drift, boot proceeds.
(b) chain shows a lending/perp position the DB has no trace of -> HALT loudly
    (``OnChainStrandError``) in live mode; loud-continue in paper/dry_run.
(c) a connector with no registered boot reader (LP / vault) -> reported as
    drift-undetectable, NOT silently passed.

The detector reads on-chain positions through ``PositionDiscoveryService`` and
the DB through the ``StateManager`` read methods; both are faked here so the
test is pure (no chain, no DB).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.runner.boot_strand_detection import (
    OnChainStrandError,
    _classify_protocols,
    detect_boot_strands,
    enforce_no_boot_strands,
)
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation import position_discovery as pd_module
from almanak.framework.valuation.position_discovery import DiscoveryResult


class _FakeMetadata:
    def __init__(self, protocols):
        self.supported_protocols = list(protocols)


class _FakeStrategy:
    def __init__(self, *, chain, wallet, protocols, tracked_tokens):
        self.chain = chain
        self.wallet_address = wallet
        self.STRATEGY_METADATA = _FakeMetadata(protocols)
        self._tracked = list(tracked_tokens)

    def _get_tracked_tokens(self):
        return list(self._tracked)


class _FakeStateManager:
    """Returns canned ledger / accounting / registry rows."""

    def __init__(self, *, ledger=None, accounting=None, registry=None):
        self._ledger = ledger or []
        self._accounting = accounting or []
        self._registry = registry or []

    async def get_ledger_entries(self, deployment_id, limit=100):
        return list(self._ledger)

    def get_accounting_events_sync(self, deployment_id, position_key=None):
        # Sync read present on BOTH the local StateManager and the hosted
        # GatewayStateManager — the parity surface the detector reads through.
        return list(self._accounting)

    async def get_position_registry_open_rows(self, deployment_id, **kwargs):
        return list(self._registry)


class _FakeHostedStateManager:
    """Mimics the hosted GatewayStateManager: a MEASURED accounting read and NO
    ``get_ledger_entries`` (it carries the lending/perp trace via accounting only).

    ``measured=False`` models the fail-quiet absent/errored backend (or an old
    gateway) — an empty read that is NOT an authoritative zero (Empty != Zero).
    """

    def __init__(self, *, accounting=None, measured=True, registry=None):
        self._accounting = accounting or []
        self._measured = measured
        self._registry = registry or []

    def read_accounting_events_measured(self, deployment_id, position_key=None):
        return list(self._accounting), self._measured

    async def get_position_registry_open_rows(self, deployment_id, **kwargs):
        return list(self._registry)


class _FakeNoTraceStateManager:
    """A backend exposing NEITHER an accounting nor a ledger read surface."""

    async def get_position_registry_open_rows(self, deployment_id, **kwargs):
        return []


class _FakeRunner:
    def __init__(self, state_manager, *, live=True, gateway=object()):
        self.state_manager = state_manager
        self._live = live
        self._gateway = gateway

    def _get_gateway_client(self):
        return self._gateway

    def _is_live_mode(self):
        return self._live


def _ledger_row(protocol, chain):
    return SimpleNamespace(protocol=protocol, chain=chain)


def _supply_position(protocol, chain, symbol):
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id=f"{protocol}-supply-{symbol}-{chain}",
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details={"asset": symbol, "wallet_address": "0xwallet"},
    )


def _perp_position(protocol, chain, market):
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=f"{protocol}-{market}",
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details={"market": market},
    )


def _patch_discovery(monkeypatch, positions, errors=None):
    """Patch PositionDiscoveryService so discover() returns ``positions``."""

    class _FakeService:
        def __init__(self, gateway_client=None):
            self._gw = gateway_client

        def discover(self, config):
            return DiscoveryResult(positions=list(positions), errors=list(errors or []))

    monkeypatch.setattr(pd_module, "PositionDiscoveryService", _FakeService)


# --------------------------------------------------------------------------- #
# (c) coverage honesty — classification
# --------------------------------------------------------------------------- #


def test_classify_lp_only_is_undetectable():
    lending, perps, undetectable = _classify_protocols(["uniswap_v3"], ["WETH", "USDC"])
    assert lending == []
    assert perps == []
    assert [p for p, _ in undetectable] == ["uniswap_v3"]


def test_classify_lending_and_perps_scannable():
    lending, perps, undetectable = _classify_protocols(["aave_v3", "gmx_v2", "uniswap_v3"], ["WETH"])
    assert lending == ["aave_v3"]
    assert perps == ["gmx_v2"]
    assert [p for p, _ in undetectable] == ["uniswap_v3"]


def test_classify_perps_scannability_tracks_reader_registry():
    # Scannability is driven by the ACTUAL reader registry, not the broader
    # conceptual perp membership. ``pancakeswap_perps`` is a DEPRECATED ALIAS for
    # ``aster_perps`` (same venue) and resolves through PerpsReadRegistry.canonical
    # to a real reader, so it is correctly scannable — never falsely undetectable.
    lending, perps, undetectable = _classify_protocols(["pancakeswap_perps"], [])
    assert lending == []
    assert perps == ["pancakeswap_perps"]
    assert undetectable == []
    # A conceptually-perp name with NO reader and NO alias would canonicalize to
    # None and fall through to undetectable (future-proofing the honest-coverage
    # contract); no such protocol exists in the registry today.


def test_classify_lending_without_tracked_tokens_is_undetectable():
    lending, perps, undetectable = _classify_protocols(["aave_v3"], [])
    assert lending == []
    assert perps == []
    assert undetectable and undetectable[0][0] == "aave_v3"
    assert "no tracked tokens" in undetectable[0][1]


# --------------------------------------------------------------------------- #
# (a) DB matches chain -> no drift
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_db_matches_chain_no_drift(monkeypatch):
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager(ledger=[_ledger_row("aave_v3", "ethereum")])
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")

    assert not report.has_drift
    assert report.scanned_protocols == ["aave_v3"]
    # enforce must NOT raise even in live mode
    await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_db_match_via_accounting_event(monkeypatch):
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager(accounting=[{"protocol": "aave_v3", "chain": "ethereum"}])
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift


# --------------------------------------------------------------------------- #
# (b) chain has a position DB doesn't know -> HALT
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_lending_strand_halts_in_live_mode(monkeypatch):
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager()  # empty DB -> no trace -> strand
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert report.has_drift
    assert report.discrepancies[0].protocol == "aave_v3"
    assert report.discrepancies[0].position_type == "SUPPLY"

    with pytest.raises(OnChainStrandError) as exc:
        await enforce_no_boot_strands(runner, strategy, "deployment:abc")
    assert "STRAND detected" in str(exc.value)
    assert "aave_v3" in str(exc.value)


@pytest.mark.asyncio
async def test_perp_strand_halts_in_live_mode(monkeypatch):
    _patch_discovery(monkeypatch, [_perp_position("gmx_v2", "arbitrum", "ETH")])
    sm = _FakeStateManager()
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="arbitrum", wallet="0xwallet", protocols=["gmx_v2"], tracked_tokens=[])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert report.has_drift
    assert report.discrepancies[0].position_type == "PERP"

    with pytest.raises(OnChainStrandError):
        await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_strand_loud_continues_in_paper_mode(monkeypatch):
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager()
    runner = _FakeRunner(sm, live=False)  # paper / dry_run
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    # Must NOT raise — loud-continue — but the report still records the drift.
    report = await enforce_no_boot_strands(runner, strategy, "deployment:abc")
    assert report.has_drift


# --------------------------------------------------------------------------- #
# (c) undetectable primitive surfaces explicitly (not silently passed)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_lp_only_strategy_reports_undetectable_no_halt(monkeypatch):
    # LP discovery returns nothing at boot (no token ids); the LP protocol is
    # classified undetectable and must be surfaced, never treated as clean.
    _patch_discovery(monkeypatch, [])
    sm = _FakeStateManager()
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["uniswap_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert report.scanned_protocols == []  # nothing scannable
    assert [p for p, _ in report.undetectable] == ["uniswap_v3"]
    assert not report.has_drift  # cannot detect drift, so no false halt

    # enforce surfaces the undetectable primitive and does not raise.
    await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_mixed_scannable_and_undetectable(monkeypatch):
    # aave supply is a strand; uniswap LP is undetectable. Both must surface.
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager()
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(
        chain="ethereum",
        wallet="0xwallet",
        protocols=["aave_v3", "uniswap_v3"],
        tracked_tokens=["WETH"],
    )

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert report.scanned_protocols == ["aave_v3"]
    assert [p for p, _ in report.undetectable] == ["uniswap_v3"]
    assert report.has_drift


# --------------------------------------------------------------------------- #
# degrade-safely cases
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_no_gateway_client_skips_scan(monkeypatch):
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager()
    runner = _FakeRunner(sm, live=True, gateway=None)  # no gateway at boot
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift
    assert any("gateway client unavailable" in e for e in report.scan_errors)
    # No false halt when we couldn't read the chain.
    await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_no_protocols_declared_is_noop(monkeypatch):
    _patch_discovery(monkeypatch, [])
    sm = _FakeStateManager()
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=[], tracked_tokens=[])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert report.scanned_protocols == []
    assert report.undetectable == []
    assert not report.has_drift


# --------------------------------------------------------------------------- #
# Empty != Zero — an UNMEASURED authoritative trace must NOT false-halt (hosted)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_hosted_unmeasured_accounting_suppresses_halt(monkeypatch):
    # Hosted GatewayStateManager: accounting read is UNMEASURED (backend
    # absent/errored/old gateway → fail-quiet []). A discovered, fully-accounted
    # position must NOT be flagged as a strand — an unread trace is not proof of
    # absence (Empty != Zero). This is the core false-halt class the 3-auditor
    # sweep flagged.
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeHostedStateManager(accounting=[], measured=False)
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift  # suppressed: cannot assert a strand on an unread trace
    assert any("UNMEASURED" in e for e in report.scan_errors)

    # Live mode must NOT raise — no false brick of a healthy strategy.
    await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_hosted_measured_match_no_drift(monkeypatch):
    # Measured read returns the accounted position → known-set populated → no drift.
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeHostedStateManager(accounting=[{"protocol": "aave_v3", "chain": "ethereum"}], measured=True)
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift


@pytest.mark.asyncio
async def test_hosted_measured_empty_is_strand(monkeypatch):
    # MEASURED + empty == authoritative zero (Empty != Zero): the DB genuinely has
    # no trace → a real strand → HALT. This is the inverse of the unmeasured case.
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeHostedStateManager(accounting=[], measured=True)
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert report.has_drift
    with pytest.raises(OnChainStrandError):
        await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_no_db_read_surface_suppresses_halt(monkeypatch):
    # A backend exposing neither accounting nor ledger read → authoritative trace
    # incomplete → suppress the halt (no false brick), surfaced loudly.
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeNoTraceStateManager()
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift
    assert any("SUPPRESSED" in e for e in report.scan_errors)
    await enforce_no_boot_strands(runner, strategy, "deployment:abc")


# --------------------------------------------------------------------------- #
# protocol-alias canonicalization — a DB alias must not false-halt
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_perp_alias_gmx_vs_gmx_v2_no_false_halt(monkeypatch):
    # DB ledger row records the historical alias "gmx"; discovery emits the
    # canonical "gmx_v2". Both fold to gmx_v2 → match → no false halt.
    _patch_discovery(monkeypatch, [_perp_position("gmx_v2", "arbitrum", "ETH")])
    sm = _FakeStateManager(ledger=[_ledger_row("gmx", "arbitrum")])
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="arbitrum", wallet="0xwallet", protocols=["gmx_v2"], tracked_tokens=[])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift
    await enforce_no_boot_strands(runner, strategy, "deployment:abc")


@pytest.mark.asyncio
async def test_lending_alias_aave_vs_aave_v3_no_false_halt(monkeypatch):
    # DB accounting row spelled "aave"; discovery emits "aave_v3". Canonical match.
    _patch_discovery(monkeypatch, [_supply_position("aave_v3", "ethereum", "WETH")])
    sm = _FakeStateManager(accounting=[{"protocol": "aave", "chain": "ethereum"}])
    runner = _FakeRunner(sm, live=True)
    strategy = _FakeStrategy(chain="ethereum", wallet="0xwallet", protocols=["aave_v3"], tracked_tokens=["WETH"])

    report = await detect_boot_strands(runner, strategy, "deployment:abc")
    assert not report.has_drift
