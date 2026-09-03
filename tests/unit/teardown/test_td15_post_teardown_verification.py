"""TD-15 / VIB-5473 — fail-closed on-chain POST-teardown verification.

Pins :meth:`TeardownManager.verify_closure_against_chain`, the seam that
composes the TD-14 post-condition verdict with a FRESH POST-teardown Plan-A
reconciliation (TD-08) and the PRE-teardown reconciliation report. The contract:

* AC-(a) — a KNOWN position the chain STILL reports OPEN after every closing
  intent fired flips the teardown to ``all_closed=False`` + ``FAILED``. This
  covers the hook-less lending strand the post-condition path counts
  closed-by-execution (UNVERIFIED), which is the false-success class TD-14 alone
  could not see.
* AC-(b) — a position the PRE-teardown ledger believed open but the chain
  reported closed/unconfirmable (never-existed / stale enumeration) is never
  certified CHAIN_VERIFIED — it is lowered to UNVERIFIED.
* Inverted semantics — the check runs AFTER closure and NEVER raises: a
  reconciliation fault degrades to the incoming verification.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.teardown import live_position_reads
from almanak.framework.teardown.models import (
    ClosureVerification,
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
    VerificationStatus,
)
from almanak.framework.teardown.plan_a_reconciliation import (
    PositionReconciliation,
    ReconciliationReport,
    ReconciliationVerdict,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _mgr() -> TeardownManager:
    mgr = TeardownManager()
    # _teardown_gateway_client probes compiler/orchestrator for a client; give it
    # a non-None one so the LP read path is reached (the chain read itself is
    # monkeypatched per-test).
    mgr.compiler = SimpleNamespace(_gateway_client=object(), is_connected=True)
    return mgr


class _Strategy:
    deployment_id = "deployment:td15"
    _gateway_network = "arbitrum"
    wallet_address = "0xprimary"

    def get_wallet_for_chain(self, chain: str) -> str:
        return f"0x{chain}"


class _Health:
    def __init__(self, collateral_value_usd, debt_value_usd):
        self.collateral_value_usd = collateral_value_usd
        self.debt_value_usd = debt_value_usd
        self.health_factor = None


class _Market:
    def __init__(self, health):
        self._health = health

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        return self._health

    def price(self, token):  # pragma: no cover - amounts unused by the CHECK
        raise KeyError(token)


def _lp_position(position_id: str = "999") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry"},
    )


def _lending_position(leg: PositionType = PositionType.BORROW) -> PositionInfo:
    return PositionInfo(
        position_type=leg,
        position_id="0xmkt",
        chain="ethereum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry", "market_id": "0xmkt", "asset_symbol": "USDC"},
    )


def _summary(*positions: PositionInfo) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:td15", timestamp=datetime.now(UTC), positions=list(positions)
    )


def _verified(status: VerificationStatus = VerificationStatus.CHAIN_VERIFIED, total: int = 1) -> ClosureVerification:
    return ClosureVerification(
        all_closed=True,
        positions_total=total,
        positions_closed=total,
        has_position_breakdown=True,
        verification_status=status,
    )


# ---------------------------------------------------------------------------
# AC-(a): residual OPEN after teardown → FAILED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_residual_open_lp_flips_to_failed(monkeypatch, caplog):
    async def _still_open(*, gateway_client, position, network=""):
        return True  # chain says LP liquidity > 0 — STILL OPEN

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _still_open)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED
    assert out.positions_closed == 0
    assert any("STILL OPEN on-chain" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_residual_open_lending_hookless_flips_to_failed():
    """The new-value case: lending has no post-condition hook, so TD-14 reports
    UNVERIFIED (closed-by-execution). TD-15's lending chain read catches the
    still-open debt leg and fails closed."""
    market = _Market(_Health(Decimal("0"), Decimal("500")))  # debt still owed
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        # TD-14 would have produced UNVERIFIED here (no hook), all_closed=True.
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=market,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_residual_open_dominates_partial_clean(monkeypatch):
    """One residual-open position fails the teardown even if another closed."""
    market = _Market(_Health(Decimal("0"), Decimal("500")))  # lending still open

    async def _lp_closed(*, gateway_client, position, network=""):
        return False  # LP closed

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _lp_closed)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.UNVERIFIED, total=2),
        pre_execution_positions=_summary(_lp_position(), _lending_position(PositionType.BORROW)),
        market=market,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED
    assert out.positions_closed == 1  # the LP closed; the lending leg is residual


# ---------------------------------------------------------------------------
# VIB-5523 Bug B — POST-teardown read must be FRESH, not the cached pre-exec snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_exec_reads_fresh_snapshot_not_stale_market_VIB_5523():
    """The pre-execution snapshot memoizes ``position_health``. The POST-teardown
    reconciliation must read a FRESH snapshot — a debt leg zeroed on-chain after
    REPAY must NOT be reported CONFIRMED_OPEN off the stale pre-WITHDRAW value
    (the false-FAILED bug)."""
    stale_market = _Market(_Health(Decimal("500"), Decimal("500")))  # pre-teardown: OPEN
    fresh_market = _Market(_Health(Decimal("0"), Decimal("0")))  # post-teardown: CLOSED

    class _StrategyFresh(_Strategy):
        def create_market_snapshot(self):
            return fresh_market

    out = await _mgr().verify_closure_against_chain(
        _StrategyFresh(),
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=stale_market,  # reusing this would falsely report CONFIRMED_OPEN → FAILED
    )
    assert out.all_closed is True  # read fresh (closed) state, not the stale cache
    assert out.verification_status is VerificationStatus.UNVERIFIED  # NOT flipped to FAILED


@pytest.mark.asyncio
async def test_post_exec_falls_back_to_cache_eviction_when_no_fresh_snapshot_VIB_5523():
    """With no ``create_market_snapshot``, the verifier evicts the stale health
    memo on the reused snapshot so the post read still re-queries the chain."""
    evicted = {"called": False}

    class _CachingMarket(_Market):
        def invalidate_position_health(self, protocol=None, market_id=None):
            evicted["called"] = True
            self._health = _Health(Decimal("0"), Decimal("0"))  # live (closed) after eviction

    market = _CachingMarket(_Health(Decimal("0"), Decimal("500")))  # stale: debt still open
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),  # no create_market_snapshot → fallback path
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=market,
    )
    assert evicted["called"] is True
    assert out.all_closed is True  # post-eviction read is closed → no residual


@pytest.mark.asyncio
async def test_post_exec_fallback_also_evicts_balances_VIB_5523():
    """Gemini MEDIUM (PR #3102): the reused snapshot memoizes wallet balances as
    well as position health. The no-fresh-snapshot fallback must evict BOTH so a
    post-execution balance read reflects live (post-unwind) state, not the
    pre-unwind memo."""
    evicted = {"health": False, "balances": False}

    class _CachingMarket(_Market):
        def invalidate_position_health(self, protocol=None, market_id=None):
            evicted["health"] = True
            self._health = _Health(Decimal("0"), Decimal("0"))

        def invalidate_balances(self):
            evicted["balances"] = True

    market = _CachingMarket(_Health(Decimal("0"), Decimal("500")))
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),  # no create_market_snapshot → fallback path
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=market,
    )
    assert evicted["health"] is True
    assert evicted["balances"] is True
    assert out.all_closed is True


class _MemoizingMarket(_Market):
    """Models the REAL ``MarketSnapshot.position_health`` cache: keyed by
    ``(protocol, market_id)``, memoized after the FIRST call, never
    invalidated except by ``invalidate_position_health``. The ``_Market``
    fixture above re-evaluates ``self._health`` fresh on every call — it
    cannot model staleness at all. This class can, which is exactly what
    :func:`test_post_exec_memoized_strategy_snapshot_still_serves_stale_health_ALM_3473`
    below needs."""

    def __init__(self, health):
        super().__init__(health)
        self._cache: dict[tuple[str, str], Any] = {}

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        key = (protocol, market_id)
        if key not in self._cache:
            self._cache[key] = self._health
        return self._cache[key]


class _StrategyMemoized(_Strategy):
    """Faithfully mimics ``IntentStrategy``'s REAL per-iteration-token
    ``MarketSnapshot`` memo (VIB-4843) — not a hardcoded stand-in.
    ``create_market_snapshot()`` returns the cached instance until
    ``begin_market_snapshot_iteration()`` is called with a token different
    from the currently-stamped one (initially ``None`` — no runner has EVER
    stamped a token, matching the no-runner CLI ``teardown execute`` lane),
    exactly mirroring ``IntentStrategy.create_market_snapshot`` /
    ``begin_market_snapshot_iteration``'s real contract (``intent_strategy.py``).
    This is what makes the test below able to tell a real fix (which must
    call ``begin_market_snapshot_iteration`` with a genuinely new token
    before rebuilding) from a fake one (a fix that merely calls
    ``create_market_snapshot()`` again, unconditionally, would still hit this
    fake's memo and fail the test)."""

    def __init__(self, *, stale_instance: Any, fresh_builder: Callable[[], Any]) -> None:
        self._cached_market_snapshot: Any = stale_instance
        self._cached_market_snapshot_token: object | None = None
        self._fresh_builder = fresh_builder

    def begin_market_snapshot_iteration(self, token: object) -> None:
        if token is not None and token == self._cached_market_snapshot_token:
            return
        self._cached_market_snapshot = None
        self._cached_market_snapshot_token = token

    def create_market_snapshot(self) -> Any:
        if self._cached_market_snapshot is None:
            self._cached_market_snapshot = self._fresh_builder()
        return self._cached_market_snapshot


@pytest.mark.asyncio
async def test_post_exec_memoized_strategy_snapshot_still_serves_stale_health_ALM_3473():
    """ALM-3473 (sibling of VIB-5523 above): ``IntentStrategy.create_market_snapshot()``
    is ITSELF memoized per-iteration-token (VIB-4843) — only the live
    ``StrategyRunner`` ever rotates that token between teardown phases (via
    ``_begin_market_snapshot_iteration``). The no-runner ``almanak strat
    teardown execute`` CLI lane never rotates it, so a strategy's own
    ``create_market_snapshot()`` can hand back the EXACT SAME
    ``MarketSnapshot`` instance the PRE-teardown reconciliation already used
    — cache intact — silently defeating VIB-5523's "read fresh" fix from the
    INSIDE, not by omitting ``create_market_snapshot`` (that fallback path is
    already covered above) but by HAVING one that lies about freshness.

    Unlike ``test_post_exec_reads_fresh_snapshot_not_stale_market_VIB_5523``
    (which models ``create_market_snapshot()`` returning a genuinely
    DIFFERENT, already-clean object unconditionally — the case where the fix
    already works trivially), this models the REAL failure mode: a
    strategy whose "fresh" call is memoized JUST LIKE THE REAL
    ``IntentStrategy`` and only actually rebuilds when something calls
    ``begin_market_snapshot_iteration`` with a genuinely new token first —
    proving the FIX itself (not just a permissive test double).
    """
    stale_market = _MemoizingMarket(_Health(Decimal("500"), Decimal("500")))  # PRE-teardown: OPEN

    # Simulate the PRE-teardown reconciliation (TeardownManager.execute(),
    # ``_pre_teardown_reconciliation``) reading this market BEFORE the closing
    # intent fires — this is what seeds the cache with the pre-withdrawal
    # value in production.
    stale_market.position_health("aave_v3", "0xmkt")

    # The on-chain REPAY/WITHDRAW then genuinely succeeds. A truly fresh
    # rebuild would see this; the STALE instance's cache would not.
    fresh_market = _MemoizingMarket(_Health(Decimal("0"), Decimal("0")))

    strategy = _StrategyMemoized(stale_instance=stale_market, fresh_builder=lambda: fresh_market)

    out = await _mgr().verify_closure_against_chain(
        strategy,
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=stale_market,
    )
    assert out.all_closed is True, (
        "ALM-3473: _fresh_post_execution_market must not trust "
        "strategy.create_market_snapshot() as genuinely fresh on faith — it can "
        "return the SAME memoized instance whose position_health cache still "
        "holds the pre-withdrawal value on the no-runner teardown-execute CLI lane"
    )
    assert out.verification_status is not VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_post_exec_does_not_trust_creator_when_iteration_hook_missing():
    """A strategy shape without ``begin_market_snapshot_iteration`` at all (not
    the real ``IntentStrategy`` — a hand-rolled or legacy strategy that only
    implements ``create_market_snapshot``) must NOT have that method's output
    trusted as fresh — there is no way to know its memo, if any, was ever
    invalidated. Trusting it on faith is the exact original bug, reachable
    through this fallback door instead of the documented one. Must degrade to
    the eviction path instead, exactly like the "no create_market_snapshot at
    all" case already does."""
    evicted = {"called": False}

    class _CachingMarket(_Market):
        def invalidate_position_health(self, protocol=None, market_id=None):
            evicted["called"] = True
            self._health = _Health(Decimal("0"), Decimal("0"))

    stale_market = _CachingMarket(_Health(Decimal("0"), Decimal("500")))  # stale: debt still open

    class _StrategyNoIterationHook(_Strategy):
        # Deliberately HAS create_market_snapshot but NOT
        # begin_market_snapshot_iteration.
        def create_market_snapshot(self):
            return stale_market  # same stale instance — "looks" fresh, isn't

    out = await _mgr().verify_closure_against_chain(
        _StrategyNoIterationHook(),
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=stale_market,
    )
    assert evicted["called"] is True, "must fall back to cache eviction, not trust an unstamped creator() blindly"
    assert out.all_closed is True


@pytest.mark.asyncio
async def test_post_exec_does_not_trust_creator_when_iteration_hook_throws():
    """Same failure mode, different trigger: begin_market_snapshot_iteration
    EXISTS but raises. Must degrade to the eviction path exactly like the
    missing-hook case above — never fall through to trusting creator()
    just because it happened not to raise."""
    evicted = {"called": False}

    class _CachingMarket(_Market):
        def invalidate_position_health(self, protocol=None, market_id=None):
            evicted["called"] = True
            self._health = _Health(Decimal("0"), Decimal("0"))

    stale_market = _CachingMarket(_Health(Decimal("0"), Decimal("500")))

    class _StrategyBrokenIterationHook(_Strategy):
        def begin_market_snapshot_iteration(self, token):
            raise RuntimeError("broken override")

        def create_market_snapshot(self):
            return stale_market

    out = await _mgr().verify_closure_against_chain(
        _StrategyBrokenIterationHook(),
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=stale_market,
    )
    assert evicted["called"] is True, "must fall back to cache eviction when the iteration-token stamp raises"
    assert out.all_closed is True


# ---------------------------------------------------------------------------
# Clean close + confidence composition
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_close_stays_chain_verified(monkeypatch):
    async def _closed(*, gateway_client, position, network=""):
        return False  # chain confirms LP closed — the GOOD outcome

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _closed)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_unverifiable_post_does_not_lower_chain_verified(monkeypatch):
    """A burned LP NFT reads back as UNVERIFIABLE ('not found') post-close — that
    is the success signal, NOT a doubt. TD-14 already proved closure
    (CHAIN_VERIFIED); TD-15's coarser re-read must not drag it to UNVERIFIED."""

    async def _unknown(*, gateway_client, position, network=""):
        return None  # NFT not found on NPM — the burned-NFT case

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _unknown)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out.all_closed is True  # unknown ≠ open — does not fail
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED  # TD-14 proof preserved


# ---------------------------------------------------------------------------
# AC-(b): never-existed / stale enumeration is not certified CHAIN_VERIFIED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_teardown_divergence_blocks_false_chain_verified(monkeypatch):
    """A never-existed position: PRE-teardown the chain said CLOSED. POST-teardown
    it is (still) closed, so there is no residual — but the closure must NOT be
    certified CHAIN_VERIFIED off a stale enumeration."""

    async def _closed(*, gateway_client, position, network=""):
        return False

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _closed)
    pre = ReconciliationReport(
        deployment_id="deployment:td15",
        entries=(
            PositionReconciliation(
                "PositionType.LP", "999", "arbitrum", "uniswap_v3", ReconciliationVerdict.DIVERGED_CLOSED
            ),
        ),
    )
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
        pre_teardown_reconciliation=pre,
    )
    assert out.all_closed is True  # no residual risk — a never-existed position posed none
    assert out.verification_status is VerificationStatus.UNVERIFIED  # but never certified


# ---------------------------------------------------------------------------
# Inverted semantics: short-circuit + never-raise
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_failed_short_circuits_without_chain_read(monkeypatch):
    called = False

    async def _spy(*, gateway_client, position, network=""):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _spy)
    failing = ClosureVerification(
        all_closed=False,
        positions_total=1,
        positions_closed=0,
        has_position_breakdown=True,
        verification_status=VerificationStatus.FAILED,
    )
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=failing,
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out is failing  # unchanged — original residual error is the actionable one
    assert called is False  # no redundant chain read on the already-failed path


@pytest.mark.asyncio
async def test_reconciliation_fault_degrades_to_incoming(monkeypatch):
    from almanak.framework.teardown import teardown_manager as tm

    async def _boom(**_kwargs):
        raise RuntimeError("gateway exploded")

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _boom)
    incoming = _verified(VerificationStatus.CHAIN_VERIFIED)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=incoming,
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    # Never raises; degrades to the TD-14 verdict (the CHECK must not fault the lane).
    assert out is incoming


@pytest.mark.asyncio
async def test_empty_position_set_passes_through(monkeypatch):
    """No KNOWN positions ⇒ nothing to re-read ⇒ verification untouched."""
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED, total=0),
        pre_execution_positions=_summary(),
        market=None,
    )
    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED


# ---------------------------------------------------------------------------
# AC-(b) CLI lane: execute() computes a PRE-teardown reconciliation inline.
# The runner lane gets it from runner._teardown_reconciliation (TD-08); the CLI
# lane has no runner, so _pre_teardown_reconciliation reads chain BEFORE the
# closing intents fire. These cover the helper that closes the CLI-lane gap.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_teardown_reconciliation_reads_chain(monkeypatch):
    """The CLI-lane helper returns the PRE-teardown report from the chain read."""
    from almanak.framework.teardown import teardown_manager as tm

    sentinel = object()
    captured = {}

    async def _fake_reconcile(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _fake_reconcile)
    summary = _summary(_lp_position())
    out = await _mgr()._pre_teardown_reconciliation(_Strategy(), summary, market=None)
    # The helper threads the KNOWN set + the strategy's gateway network into the CHECK.
    assert out is sentinel
    assert captured["summary"] is summary
    assert captured["network"] == "arbitrum"
    assert captured["wallet_address"] == "0xprimary"
    assert captured["wallet_for_chain"]("avalanche") == "0xavalanche"


@pytest.mark.asyncio
async def test_post_teardown_reconciliation_threads_per_chain_wallet_resolver(monkeypatch):
    """The fresh POST CHECK must retain the same per-position wallet mapping."""
    from almanak.framework.teardown import teardown_manager as tm

    captured = {}

    async def _fake_reconcile(**kwargs):
        captured.update(kwargs)
        return ReconciliationReport(entries=[])

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _fake_reconcile)
    await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )

    assert captured["phase"] == "post"
    assert captured["wallet_address"] == "0xprimary"
    assert captured["wallet_for_chain"]("avalanche") == "0xavalanche"


@pytest.mark.asyncio
async def test_pre_teardown_reconciliation_fault_returns_none(monkeypatch):
    """A chain-read fault must NOT fault the teardown lane — returns None (no AC-(b) downgrade)."""
    from almanak.framework.teardown import teardown_manager as tm

    async def _boom(**_kwargs):
        raise RuntimeError("gateway exploded")

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _boom)
    out = await _mgr()._pre_teardown_reconciliation(_Strategy(), _summary(_lp_position()), market=None)
    assert out is None
