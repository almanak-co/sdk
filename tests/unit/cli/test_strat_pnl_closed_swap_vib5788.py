"""VIB-5788 — ``strat pnl`` must not print a confident-wrong headline for a
fully-closed swap-primitive deployment.

Grounded in the real robinhood-rsi quant-user run
(``docs/internal/quant-user-runs/20260713-0947-robinhood-rsi``, DEV-6). The
frozen ``portfolio_metrics`` row for ``deployment:8ef869919b01`` after teardown:

    initial_value_usd = 2.50065881490955339716   (seeded at iteration 1 from the
                                                   position-scoped total_value_usd)
    total_value_usd   = 0                         (swap position closed to wallet)
    gas_spent_usd     = 0.029926009378049110

The verbatim headline is ``total - initial - deposits + withdrawals - gas`` =
``0 - 2.50065881... - 0 + 0 - 0.029926...`` ≈ **-$2.53**, even though the true
round-trip loss was ≈ **-$0.03** (the ~$2.50 of capital is sitting in the wallet
as USDG). The position-scoped ``total_value_usd`` (VIB-3614) collapsed to 0 on
close while the lifecycle baseline still reflects the deployed position.

The correct behaviour (same as the VIB-4975 leveraged-lending *closed* state, and
per the VIB-4976 ADR §7b which proved a read-side wallet-baseline unsound) is to
SUPPRESS the headline rather than print the -$2.53. This test reproduces the
corruption at the ``compute_pnl_breakdown`` level, then asserts suppression.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from almanak.framework.accounting.reporting.swap_class_fallback import (
    _ledger_has_successful_intent,
    _to_decimal_or_none,
    detect_closed_swap_primitive,
)
from almanak.framework.cli.strat_pnl import compute_pnl_breakdown
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionType

_BASE_TS = datetime(2026, 7, 13, 12, 54, 16, tzinfo=UTC)
_DEPLOYMENT_ID = "deployment:8ef869919b01"

# Exact DEV-6 frozen-DB values.
_INITIAL = Decimal("2.50065881490955339716")
_GAS = Decimal("0.029926009378049110")
_WALLET_AFTER = Decimal("3.847972619828172986636743156")


def _closed_metrics(total: Decimal = Decimal("0")) -> PortfolioMetrics:
    return PortfolioMetrics(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=_BASE_TS + timedelta(minutes=13),
        initial_value_usd=_INITIAL,
        total_value_usd=total,
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=_GAS,
    )


def _closed_snapshot(
    positions: list[PositionValue] | None = None,
    *,
    total: Decimal = Decimal("0"),
    wallet: Decimal = _WALLET_AFTER,
) -> PortfolioSnapshot:
    """Post-teardown snapshot: position closed, capital back as wallet cash."""
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
        chain="robinhood",
        iteration_number=9,
        cycle_id="teardown-td_03130046194f",
    )


def _swap(ts: datetime, *, token_in: str = "USDG", token_out: str = "WETH", success: bool = True) -> LedgerEntry:
    return LedgerEntry(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=ts,
        intent_type="SWAP",
        token_in=token_in,
        amount_in="2.5",
        token_out=token_out,
        amount_out="0.001412466428818898",
        gas_used=146492,
        gas_usd="0.016221780432416990",
        chain="robinhood",
        protocol="uniswap_v3",
        success=success,
    )


def _round_trip_ledger() -> list[LedgerEntry]:
    """The real BUY + teardown-SELL swap round trip."""
    return [
        _swap(_BASE_TS),  # entry BUY: USDG -> WETH
        _swap(_BASE_TS + timedelta(minutes=13), token_in="WETH", token_out="USDG"),  # teardown SELL
    ]


# ---------------------------------------------------------------------------
# The corruption reproduction: verbatim headline is ~ -$2.53, must be suppressed.
# ---------------------------------------------------------------------------


def test_verbatim_headline_would_be_confident_wrong_minus_2_53() -> None:
    """Anchor: the raw PortfolioMetrics headline is the false -$2.53 (pre-fix)."""
    metrics = _closed_metrics()
    # total - initial - deposits + withdrawals - gas
    assert metrics.pnl_before_gas == Decimal("0") - _INITIAL  # ≈ -2.5007
    net = metrics.pnl_after_gas
    assert net is not None
    assert Decimal("-2.54") < net < Decimal("-2.52")  # ≈ -$2.53, the wrong number


def test_closed_swap_primitive_headline_is_suppressed() -> None:
    """After the fix, compute_pnl_breakdown suppresses the -$2.53 headline."""
    breakdown = compute_pnl_breakdown(
        deployment_id=_DEPLOYMENT_ID,
        metrics=_closed_metrics(),
        ledger_entries=_round_trip_ledger(),
        position_events=[],
        snapshot=_closed_snapshot(),
    )
    assert breakdown.headline_suppressed is True
    assert breakdown.headline_suppression_reason is not None
    assert "VIB-5788" in breakdown.headline_suppression_reason
    # Not leverage-adjusted — this is the swap path, not the lending path.
    assert breakdown.headline_leverage_adjusted is False


def test_detector_fires_on_dev6_values() -> None:
    """Unit-level: the pure detector fires on the exact DEV-6 shape."""
    verdict = detect_closed_swap_primitive(
        _closed_snapshot(),
        _round_trip_ledger(),
        _closed_metrics(),
    )
    assert verdict.suppressed is True
    assert "swap-primitive" in verdict.reason


# ---------------------------------------------------------------------------
# Negative cases — the detector must NOT over-fire.
# ---------------------------------------------------------------------------


def test_open_swap_position_is_not_suppressed() -> None:
    """While the swap position is OPEN, total ~ initial — headline stands."""
    open_pos = PositionValue(
        position_type=PositionType.TOKEN,
        protocol="uniswap_v3",
        chain="robinhood",
        value_usd=_INITIAL,
        label="swap inventory WETH",
    )
    verdict = detect_closed_swap_primitive(
        _closed_snapshot([open_pos], total=_INITIAL, wallet=_WALLET_AFTER),
        _round_trip_ledger(),
        _closed_metrics(total=_INITIAL),
    )
    assert verdict.suppressed is False


def test_live_lp_value_remaining_is_not_suppressed() -> None:
    """A live LP leg means deployed value did NOT collapse to the wallet."""
    lp_pos = PositionValue(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        chain="robinhood",
        value_usd=Decimal("2.40"),
        label="LP",
    )
    # total in metrics still collapsed, but the snapshot proves live deployed value.
    verdict = detect_closed_swap_primitive(
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
        token_in="USDG",
        amount_in="1",
        token_out="USDG",
        amount_out="1",
        gas_used=50000,
        gas_usd="0.01",
        chain="robinhood",
        protocol="aave_v3",
        success=True,
    )
    verdict = detect_closed_swap_primitive(
        _closed_snapshot(),
        [*_round_trip_ledger(), borrow],
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_unmeasured_total_does_not_fire() -> None:
    """Empty!=Zero: an unmeasured total_value_usd leaves the headline to the
    upstream unmeasured path, not this suppression."""
    verdict = detect_closed_swap_primitive(
        _closed_snapshot(),
        _round_trip_ledger(),
        _closed_metrics(total=None),  # type: ignore[arg-type]
    )
    assert verdict.suppressed is False


def test_genuine_wipe_to_zero_wallet_is_not_suppressed() -> None:
    """If the wallet holds nothing, the loss is real — do not suppress it."""
    verdict = detect_closed_swap_primitive(
        _closed_snapshot(total=Decimal("0"), wallet=Decimal("0")),
        _round_trip_ledger(),
        _closed_metrics(),
    )
    assert verdict.suppressed is False


def test_no_swap_in_ledger_does_not_fire() -> None:
    """No successful SWAP -> not a swap-primitive deployment for this detector."""
    verdict = detect_closed_swap_primitive(
        _closed_snapshot(),
        [_swap(_BASE_TS, success=False)],
        _closed_metrics(),
    )
    assert verdict.suppressed is False


class TestParsingRobustnessVib5788Review:
    """CodeRabbit/gemini review follow-ups on the detector's parsing helpers."""

    def test_non_finite_decimal_is_unmeasured(self):
        # A NaN / Infinity would raise InvalidOperation the moment it is compared
        # downstream — it must be treated as unmeasured (None), not returned.
        assert _to_decimal_or_none("nan") is None
        assert _to_decimal_or_none("inf") is None
        assert _to_decimal_or_none(Decimal("NaN")) is None
        assert _to_decimal_or_none(Decimal("Infinity")) is None
        assert _to_decimal_or_none("2.5") == Decimal("2.5")

    def test_non_string_intent_type_in_row_does_not_crash(self):
        # A LEDGER ROW whose intent_type is a non-str (int/enum) must not raise
        # AttributeError at the `.upper()` comparison (line 419). Non-empty
        # ledger so the guard is passed and the comparison line is actually hit.
        from types import SimpleNamespace

        rows = [
            SimpleNamespace(success=True, intent_type=123),  # non-str: must not crash, must not match
            SimpleNamespace(success=True, intent_type="swap"),  # str: case-insensitive match
        ]
        assert _ledger_has_successful_intent(rows, "SWAP") is True
        # A ledger of ONLY the non-str row does not match (and does not crash).
        assert _ledger_has_successful_intent([SimpleNamespace(success=True, intent_type=123)], "SWAP") is False

    def test_non_string_intent_type_param_does_not_crash(self):
        # A non-str intent_type ARGUMENT must not raise AttributeError either.
        from types import SimpleNamespace

        rows = [SimpleNamespace(success=True, intent_type="SWAP")]
        assert _ledger_has_successful_intent(rows, 123) is False

    def test_suppression_fires_with_a_live_token_pseudo_position_present(self):
        # A closed swap leaves its proceeds in the wallet, surfaced as a
        # PositionType.TOKEN pseudo-position. That is NOT "live deployed value"
        # (Rule 3 skips wallet pseudo-positions), so suppression must still fire.
        # This exercises _snapshot_has_live_deployed_value's loop body + the
        # ptype == "TOKEN" skip, which the positions=[] cases never reach.
        token_pos = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="wallet",
            chain="robinhood",
            value_usd=Decimal("2.50"),
            label="swap inventory USDG",
        )
        verdict = detect_closed_swap_primitive(
            _closed_snapshot([token_pos]),
            _round_trip_ledger(),
            _closed_metrics(),
        )
        assert verdict.suppressed is True
        assert "swap-primitive" in verdict.reason
