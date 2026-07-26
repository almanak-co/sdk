"""VIB-5952 — ``strat pnl`` must not print a confident-wrong headline for a
fully-closed perp-primitive deployment.

Grounded in the real sealed quant-user run
(``docs/internal/quant-user-runs/20260722-0913-gmx-perp-avalanche``, QU-4.4 /
SB-2, AUDIT_CONFIRMED). The frozen ``portfolio_metrics`` row for
``deployment:e32d997e1002`` after the GMX round trip:

    initial_value_usd = 6.000787492252604224764033304  (committed collateral,
                                                         seeded at iteration 1)
    total_value_usd   = 0                               (perp position closed)
    gas_spent_usd     = 0.001392646556706375488

With MEASURED-zero capital flows (the pre-settlement state the run-time
``strat-pnl-preteardown.json`` captured), the verbatim headline
``total - initial - deposits + withdrawals - gas`` =
``0 - 6.00... - 0 + 0 - 0.0014...`` ≈ **-$6.00**, even though the true wallet
round-trip PnL was ≈ **-$0.025** (the ~$6 of collateral was returned to the
wallet by GMX's keeper-executed settlement). The position-scoped
``total_value_usd`` (VIB-3614) collapsed to 0 on close while the lifecycle
baseline still reflects the committed collateral.

The correct behaviour (same as the VIB-4975 leveraged-lending and VIB-5788
swap-primitive *closed* states, and per the VIB-4976 ADR §7b which proved a
read-side wallet-baseline unsound) is to SUPPRESS the headline rather than print
the -$6.00. This test reproduces the corruption at the ``compute_pnl_breakdown``
level, then asserts suppression — and pins that the lending / swap paths and the
Empty≠Zero (unmeasured) rule are untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from almanak.framework.accounting.reporting.perp_class_fallback import (
    _ledger_has_successful_intent,
    _to_decimal_or_none,
    detect_closed_perp_primitive,
)
from almanak.framework.cli.strat_pnl import compute_pnl_breakdown, render_text
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionType

_BASE_TS = datetime(2026, 7, 22, 12, 19, 3, tzinfo=UTC)
_DEPLOYMENT_ID = "deployment:e32d997e1002"

# Exact frozen-DB values from the sealed 20260722-0913-gmx-perp-avalanche run.
_INITIAL = Decimal("6.000787492252604224764033304")
_GAS = Decimal("0.001392646556706375488")
_WALLET_AFTER = Decimal("7.908234838064839933399502275")


def _closed_metrics(
    total: Decimal | None = Decimal("0"),
    *,
    deposits: Decimal | None = Decimal("0"),
    withdrawals: Decimal | None = Decimal("0"),
) -> PortfolioMetrics:
    """Post-close metrics: committed collateral baseline, collapsed total.

    Default flows are MEASURED zero — the pre-settlement state in which the
    verbatim headline computes to the confident-wrong ≈ −initial (VIB-5866's
    unmeasured-flow guard does NOT catch a measured-zero flow).
    """
    return PortfolioMetrics(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS + timedelta(minutes=13),
        initial_value_usd=_INITIAL,
        total_value_usd=total,
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
        gas_spent_usd=_GAS,
    )


def _closed_snapshot(
    positions: list[PositionValue] | None = None,
    *,
    total: Decimal = Decimal("0"),
    wallet: Decimal = _WALLET_AFTER,
) -> PortfolioSnapshot:
    """Post-close snapshot: perp position closed, collateral back as wallet cash."""
    return PortfolioSnapshot(
        timestamp=_BASE_TS + timedelta(minutes=13),
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=total,
        available_cash_usd=wallet,
        value_confidence=ValueConfidence.HIGH,
        deployed_capital_usd=Decimal("0"),
        wallet_total_value_usd=wallet,
        positions=positions or [],
        wallet_balances=[],
        token_prices={},
        chain="avalanche",
        iteration_number=3,
        cycle_id="c41cab16-9929-4c8c-85be-eda3bf7db979",
    )


def _perp(
    ts: datetime,
    *,
    intent_type: str = "PERP_OPEN",
    success: bool = True,
) -> LedgerEntry:
    return LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=ts,
        intent_type=intent_type,
        token_in="USDC",
        amount_in="6",
        token_out="USDC",
        amount_out="6",
        gas_used=950847,
        gas_usd="0.000696",
        chain="avalanche",
        protocol="gmx_v2",
        success=success,
    )


def _round_trip_ledger() -> list[LedgerEntry]:
    """The real PERP_OPEN + PERP_CLOSE round trip."""
    return [
        _perp(_BASE_TS, intent_type="PERP_OPEN"),
        _perp(_BASE_TS + timedelta(minutes=13), intent_type="PERP_CLOSE"),
    ]


# ---------------------------------------------------------------------------
# The corruption reproduction: verbatim headline is ~ -$6.00, must be suppressed.
# ---------------------------------------------------------------------------


def test_verbatim_headline_would_be_confident_wrong_minus_6() -> None:
    """Anchor: the raw PortfolioMetrics headline is the false -$6.00 (pre-fix)."""
    metrics = _closed_metrics()
    # total - initial - deposits + withdrawals
    assert metrics.pnl_before_gas == Decimal("0") - _INITIAL  # ≈ -6.0008
    net = metrics.pnl_after_gas
    assert net is not None
    assert Decimal("-6.01") < net < Decimal("-6.00")  # ≈ -$6.00, the wrong number


def test_closed_perp_primitive_headline_is_suppressed() -> None:
    """After the fix, compute_pnl_breakdown suppresses the -$6.00 headline."""
    breakdown = compute_pnl_breakdown(
        deployment_id=_DEPLOYMENT_ID,
        metrics=_closed_metrics(),
        ledger_entries=_round_trip_ledger(),
        position_events=[],
        snapshot=_closed_snapshot(),
    )
    assert breakdown.headline_suppressed is True
    assert breakdown.headline_suppression_reason is not None
    assert "VIB-5952" in breakdown.headline_suppression_reason
    # Not leverage-adjusted — this is the perp path, not the lending path.
    assert breakdown.headline_leverage_adjusted is False
    # The raw numbers are retained additively (JSON payload keeps them, mirroring
    # VIB-5788), but the RENDERER must hide the confident-wrong −$6.00: the text
    # UI shows "Headline PnL: unavailable", never a Gross/Net PnL money line.
    text = render_text(breakdown)
    assert "Headline PnL:     unavailable" in text
    assert "Gross PnL:" not in text
    assert "Net PnL:" not in text
    assert "6.00" not in text


def test_detector_fires_on_frozen_run_values() -> None:
    """Unit-level: the pure detector fires on the exact frozen-DB shape."""
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(),
        _round_trip_ledger(),
        _closed_metrics(),
    )
    assert verdict.suppressed is True
    assert "perp-primitive" in verdict.reason


def test_close_only_ledger_still_fires() -> None:
    """A PERP_CLOSE alone (no recorded open) still proves the perp primitive."""
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(),
        [_perp(_BASE_TS, intent_type="PERP_CLOSE")],
        _closed_metrics(),
    )
    assert verdict.suppressed is True


# ---------------------------------------------------------------------------
# Empty≠Zero — the HARD LAW: unmeasured stays unmeasured, never fabricated.
# ---------------------------------------------------------------------------


def test_unmeasured_total_does_not_fire() -> None:
    """Empty≠Zero: an unmeasured total_value_usd leaves the headline to the
    upstream unmeasured path, not this suppression (no confident-wrong number
    exists to suppress)."""
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(),
        _round_trip_ledger(),
        _closed_metrics(total=None),
    )
    assert verdict.suppressed is False


def test_unmeasured_flows_leave_headline_unavailable_not_fabricated() -> None:
    """The frozen post-teardown DB shape: keeper-poisoned flows are unmeasured
    (None). The headline must be unavailable (Empty≠Zero) and NEVER a fabricated
    number or a −100% loss — whether via the upstream None path or this
    suppression. Either way: gross/net stay None."""
    breakdown = compute_pnl_breakdown(
        deployment_id=_DEPLOYMENT_ID,
        metrics=_closed_metrics(deposits=None, withdrawals=None),
        ledger_entries=_round_trip_ledger(),
        position_events=[],
        snapshot=_closed_snapshot(),
    )
    assert breakdown.gross_pnl_usd is None
    assert breakdown.net_pnl_usd is None
    # Must NEVER be the un-netted −initial ≈ −$6.00.
    assert breakdown.net_pnl_usd != -_INITIAL


# ---------------------------------------------------------------------------
# Negative cases — the detector must NOT over-fire.
# ---------------------------------------------------------------------------


def test_open_perp_position_is_not_suppressed() -> None:
    """A live PERP leg means the position is still open — headline must stand.

    Isolates rule 3 (live non-wallet position): metrics.total is collapsed to 0
    (rule 1 passes), the perp round trip is present with no borrow (rule 2
    passes), and the wallet retains value (rule 4 passes) — so the live PERP
    position is the ONLY thing preventing suppression. Mutation-tested: deleting
    the rule-3 guard flips this to ``suppressed=True`` and the test fails. The
    previous fixture used an *uncollapsed* total (``total=_INITIAL``), which made
    rule 1 short-circuit before rule 3 ran — the guard was never exercised
    (CodeRabbit VIB-5952 review).
    """
    open_pos = PositionValue(
        position_type=PositionType.PERP,
        protocol="gmx_v2",
        chain="avalanche",
        value_usd=Decimal("5.90"),
        label="gmx BTC long",
    )
    verdict = detect_closed_perp_primitive(
        _closed_snapshot([open_pos]),
        _round_trip_ledger(),
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_live_lp_value_remaining_is_not_suppressed() -> None:
    """A live LP leg means deployed value did NOT collapse to the wallet."""
    lp_pos = PositionValue(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        chain="avalanche",
        value_usd=Decimal("5.90"),
        label="LP",
    )
    verdict = detect_closed_perp_primitive(
        _closed_snapshot([lp_pos]),
        _round_trip_ledger(),
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_leveraged_deployment_is_left_to_the_lending_path() -> None:
    """A BORROW in the ledger routes to leveraged_lending, not this detector."""
    borrow = LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS,
        intent_type="BORROW",
        token_in="USDC",
        amount_in="1",
        token_out="USDC",
        amount_out="1",
        gas_used=50000,
        gas_usd="0.01",
        chain="avalanche",
        protocol="aave_v3",
        success=True,
    )
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(),
        [*_round_trip_ledger(), borrow],
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_genuine_wipe_to_zero_wallet_is_not_suppressed() -> None:
    """If the wallet holds nothing, the loss is real — do not suppress it."""
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(total=Decimal("0"), wallet=Decimal("0")),
        _round_trip_ledger(),
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_no_perp_in_ledger_does_not_fire() -> None:
    """No successful PERP intent -> not a perp-primitive deployment here."""
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(),
        [_perp(_BASE_TS, success=False)],
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_swap_only_ledger_is_left_to_the_swap_path() -> None:
    """A pure swap deployment (no PERP intent) is the swap detector's job."""
    swap = LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS,
        intent_type="SWAP",
        token_in="USDC",
        amount_in="6",
        token_out="WETH",
        amount_out="0.002",
        gas_used=150000,
        gas_usd="0.01",
        chain="avalanche",
        protocol="uniswap_v3",
        success=True,
    )
    verdict = detect_closed_perp_primitive(
        _closed_snapshot(),
        [swap],
        _closed_metrics(),
    )
    assert verdict.suppressed is False


# ---------------------------------------------------------------------------
# Sibling-path regression guards — lending / swap suppression untouched.
# ---------------------------------------------------------------------------


def test_lending_closed_state_still_suppressed_by_its_own_path() -> None:
    """A borrowed, torn-down loop is still suppressed by the lending path, and
    the perp detector leaves it alone (no double-handling)."""
    supply_gone_borrow = LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS,
        intent_type="BORROW",
        token_in="USDC",
        amount_in="3",
        token_out="USDC",
        amount_out="3",
        gas_used=60000,
        gas_usd="0.01",
        chain="avalanche",
        protocol="aave_v3",
        success=True,
    )
    breakdown = compute_pnl_breakdown(
        deployment_id=_DEPLOYMENT_ID,
        metrics=_closed_metrics(),
        ledger_entries=[supply_gone_borrow],
        position_events=[],
        snapshot=_closed_snapshot(),
    )
    # Suppressed — but by the LENDING reason (VIB-4975), not the perp reason.
    assert breakdown.headline_suppressed is True
    assert breakdown.headline_suppression_reason is not None
    assert "VIB-4975" in breakdown.headline_suppression_reason
    assert "VIB-5952" not in breakdown.headline_suppression_reason


def test_swap_closed_state_still_suppressed_by_its_own_path() -> None:
    """A pure swap close is still suppressed by the swap path (VIB-5788), and
    the perp detector no-ops because the headline was already scoped."""
    swap = LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS,
        intent_type="SWAP",
        token_in="USDC",
        amount_in="6",
        token_out="WETH",
        amount_out="0.002",
        gas_used=150000,
        gas_usd="0.01",
        chain="avalanche",
        protocol="uniswap_v3",
        success=True,
    )
    breakdown = compute_pnl_breakdown(
        deployment_id=_DEPLOYMENT_ID,
        metrics=_closed_metrics(),
        ledger_entries=[swap],
        position_events=[],
        snapshot=_closed_snapshot(),
    )
    assert breakdown.headline_suppressed is True
    assert breakdown.headline_suppression_reason is not None
    assert "VIB-5788" in breakdown.headline_suppression_reason
    assert "VIB-5952" not in breakdown.headline_suppression_reason


# ---------------------------------------------------------------------------
# Parsing-helper robustness (mirrors the VIB-5788 review follow-ups).
# ---------------------------------------------------------------------------


class TestParsingRobustness:
    def test_non_finite_decimal_is_unmeasured(self):
        assert _to_decimal_or_none("nan") is None
        assert _to_decimal_or_none("inf") is None
        assert _to_decimal_or_none(Decimal("NaN")) is None
        assert _to_decimal_or_none(Decimal("Infinity")) is None
        assert _to_decimal_or_none("6.0") == Decimal("6.0")

    def test_empty_string_is_unmeasured(self):
        # The frozen DB stores poisoned capital flows as the empty string.
        assert _to_decimal_or_none("") is None
        assert _to_decimal_or_none("   ") is None

    def test_non_string_intent_type_in_row_does_not_crash(self):
        from types import SimpleNamespace

        rows = [
            SimpleNamespace(success=True, intent_type=123),  # non-str: must not crash / match
            SimpleNamespace(success=True, intent_type="perp_open"),  # case-insensitive match
        ]
        assert _ledger_has_successful_intent(rows, frozenset({"PERP_OPEN"})) is True
        assert (
            _ledger_has_successful_intent([SimpleNamespace(success=True, intent_type=123)], frozenset({"PERP_OPEN"}))
            is False
        )

    def test_strict_success_identity(self):
        from types import SimpleNamespace

        # A truthy non-bool success must NOT count (Empty≠Zero at the read site).
        rows = [SimpleNamespace(success=1, intent_type="PERP_OPEN")]
        assert _ledger_has_successful_intent(rows, frozenset({"PERP_OPEN"})) is False
