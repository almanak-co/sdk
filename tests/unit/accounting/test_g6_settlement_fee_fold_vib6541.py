"""G6 folds the settlement-carried perp costs — VIB-6541.

Two defects in one cell, both measured on the sealed mainnet bundle
``20260804-2310-gmxrt-vib6522-5513``:

1. **G6 was never settlement-aware.** The dashboard's ``compute_reconciliation``
   has done per-component PERP_SETTLEMENT supersession since VIB-3872 WI-4, and
   that PR's own commit message logged the asymmetry as a known follow-up
   ("G6/G13 read the close's inline realized_pnl and are not settlement-aware").
   The consequence on a real GMX run: the perp receipt parser leaves
   ``realized_pnl_usd`` and the funding pair NULL on the PERP_CLOSE payload, so
   G6 failed with ``{'Σ_perp_usd_null_count': 1, 'Σ_funding_usd_null_count': 1}``
   while the measured values sat one row away on the keeper receipt.

2. **Σ_fees read 0 against $0.46 of measured fees.** Neither the keeper execution
   fee nor the position/borrowing fee reached any reconciliation bucket.

Both are read-path only — the write side already stored everything correctly.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import run_against_sqlite

_BUNDLE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "accounting"
    / "perp"
    / "vib6541_gmx_arb_mainnet.sqlite"
)

_KEEPER_FEE = Decimal("0.45634482836671465088928")
_POSITION_FEE = Decimal("0.00599868491185265436")


@pytest.fixture(scope="module")
def bundle_decomposition() -> dict:
    if not _BUNDLE.is_file():  # pragma: no cover - fixture is committed
        pytest.skip(f"missing frozen bundle fixture: {_BUNDLE}")
    report = run_against_sqlite(_BUNDLE, primitive="perp")
    return report.g6_decomposition


class TestTheSettlementCostsAreBooked:
    def test_the_keeper_fee_is_summed(self, bundle_decomposition):
        assert Decimal(bundle_decomposition["Σ_perp_keeper_fee_usd"]) == _KEEPER_FEE

    def test_the_trading_fee_is_summed(self, bundle_decomposition):
        assert Decimal(bundle_decomposition["Σ_perp_trading_fee_usd"]) == _POSITION_FEE

    def test_sigma_fees_carries_them_negated(self, bundle_decomposition):
        """NEGATIVE CONTROL — pre-fix this key read exactly ``0`` on this DB.

        The ticket's own words: "Accountant Test G6 independently confirms: Σ_fees
        contribution reads 0/null against fees present in settlement events."
        """
        assert Decimal(bundle_decomposition["Σ_fees_usd"]) == -(_KEEPER_FEE + _POSITION_FEE)

    def test_the_component_pnl_now_explains_the_run(self, bundle_decomposition):
        """Pre-fix Σ component was −$0.0615 on a run that lost ~$0.52.

        The wallet endpoint on this bundle is independently wrong for TWO reasons
        this ticket does not own, which is why this asserts the COMPONENT side
        rather than the cell verdict: a mid-run restart reset ``iteration_number``,
        so ``evaluate_cells``'s ``(iteration_number, timestamp)`` sort puts the
        teardown snapshot in the MIDDLE and the cell's terminal endpoint predates
        the close (VIB-6545); and both endpoints straddle an in-flight keeper-fee
        escrow, the condition G16 refuses to score and G6 has no guard for
        (VIB-6546). The component sum is the half VIB-6541 owns, and it is now
        right.
        """
        component = Decimal(bundle_decomposition["component_pnl_usd"])

        assert component < Decimal("-0.50")
        assert abs(component - Decimal("-0.5191")) < Decimal("0.02")


class TestSettlementSupersession:
    def test_the_null_buckets_are_gone(self, bundle_decomposition):
        """NEGATIVE CONTROL — pre-fix both of these read ``1`` and FAILed the cell."""
        assert bundle_decomposition["Σ_perp_usd_null_count"] == "0"
        assert bundle_decomposition["Σ_funding_usd_null_count"] == "0"

    def test_realized_pnl_comes_from_the_settlement(self, bundle_decomposition):
        """The PERP_CLOSE payload on this run carries ``realized_pnl_usd: null``;
        the value here can only have come from the keeper receipt."""
        assert Decimal(bundle_decomposition["Σ_perp_usd"]) == Decimal("0.004243951356245226952293017825")


class TestEmptyIsNotZero:
    """Synthetic DBs — the precedence rules the bundle happens not to exercise."""

    @staticmethod
    def _decomposition(tmp_path: Path, settlement_payload: dict | None, close_payload: dict) -> dict:
        db = tmp_path / "g6.sqlite"
        _build_minimal_perp_db(db, settlement_payload, close_payload)
        return run_against_sqlite(db, primitive="perp").g6_decomposition

    def test_an_unmeasured_settlement_field_falls_back_to_the_estimate(self, tmp_path):
        """A settlement whose ``realized_pnl_usd`` is null must NOT overwrite a
        measured estimate with a fabricated zero — the FIX-1 property of WI-4."""
        decomp = self._decomposition(
            tmp_path,
            _settlement_payload(realized_pnl_usd=None),
            _close_payload(realized_pnl_usd="7.5"),
        )

        assert Decimal(decomp["Σ_perp_usd"]) == Decimal("7.5")

    def test_a_non_executed_settlement_never_supersedes(self, tmp_path):
        """CANCELLED / FROZEN carry no measured fill economics."""
        decomp = self._decomposition(
            tmp_path,
            _settlement_payload(realized_pnl_usd="99", state="CANCELLED"),
            _close_payload(realized_pnl_usd="7.5"),
        )

        assert Decimal(decomp["Σ_perp_usd"]) == Decimal("7.5")

    def test_a_cancelled_order_owes_no_trading_fee_but_still_burned_keeper_gas(self, tmp_path):
        """The two costs are scoped differently ON PURPOSE.

        An unfilled order owes no position fee, so its absent fee is a correct
        absence and must not be counted as a measurement gap. The keeper consumed
        the escrow regardless of what the order did, so that fee is folded for
        every settlement state.
        """
        decomp = self._decomposition(
            tmp_path,
            _settlement_payload(state="CANCELLED", position_fee_usd=None, keeper_execution_fee_usd="0.2"),
            _close_payload(realized_pnl_usd="0"),
        )

        assert Decimal(decomp["Σ_perp_trading_fee_usd"]) == Decimal("0")
        assert decomp["Σ_perp_trading_fee_unmeasured_count"] == "0"
        assert Decimal(decomp["Σ_perp_keeper_fee_usd"]) == Decimal("0.2")

    def test_a_half_measured_trading_fee_is_unmeasured_in_total(self, tmp_path):
        """position present, borrowing absent → the bucket contributes nothing and
        SAYS SO. Booking the half we have is a wrong number, not a smaller one."""
        decomp = self._decomposition(
            tmp_path,
            _settlement_payload(borrowing_fee_usd=None),
            _close_payload(realized_pnl_usd="0"),
        )

        assert Decimal(decomp["Σ_perp_trading_fee_usd"]) == Decimal("0")
        assert decomp["Σ_perp_trading_fee_unmeasured_count"] == "1"


# ─── synthetic DB builder ────────────────────────────────────────────────────


def _settlement_payload(
    *,
    state: str = "EXECUTED",
    realized_pnl_usd: str | None = "5.0",
    position_fee_usd: str | None = "0.5",
    borrowing_fee_usd: str | None = "0",
    keeper_execution_fee_usd: str | None = "0.25",
) -> dict:
    payload: dict = {
        "event_type": "PERP_SETTLEMENT",
        "protocol": "gmx_v2",
        "position_key": "0x" + "cd" * 32,
        "submission_ledger_entry_id": "tl-close",
        "order_key": "0x" + "ab" * 32,
        "keeper_tx_hash": "0xkeeper",
        "settlement_state": state,
        "is_open": False,
        "is_long": True,
        "market": "0xmkt",
        "collateral_token": "0xusdc",
        "entry_price": None,
        "exit_price": "3000",
        "size_delta_usd": "2000",
        "collateral_delta_amount": "8000000",
        "price_impact_usd": "-0.1",
        "realized_pnl_usd": realized_pnl_usd,
        "funding_fee_usd": "0",
        "block_number": 123,
        "unavailable_reason": None,
        "confidence": "HIGH",
        "schema_version": 1,
        "formula_version": 1,
        "matching_policy_version": 1,
        "primitive_version": 2,
    }
    if position_fee_usd is not None:
        payload["position_fee_usd"] = position_fee_usd
    if borrowing_fee_usd is not None:
        payload["borrowing_fee_usd"] = borrowing_fee_usd
    if keeper_execution_fee_usd is not None:
        payload["keeper_execution_fee_usd"] = keeper_execution_fee_usd
    if state != "EXECUTED":
        # ``PerpSettlementEventPayload`` requires a stated reason on any
        # non-EXECUTED state; without it the payload fails validation and G6
        # short-circuits with an empty decomposition, which would make these
        # assertions unreachable rather than false.
        payload["unavailable_reason"] = "order cancelled by keeper"
    return payload


def _close_payload(*, realized_pnl_usd: str | None) -> dict:
    return {
        "event_type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "position_key": "perp:arbitrum:gmx_v2:wallet:ETH-USDC",
        "market": "ETH-USDC",
        "is_long": True,
        "size": "0.04",
        "exit_price": "2625.0",
        "close_fee_usd": "0.5",
        "price_impact_usd": "0.1",
        "funding_paid_usd": "0",
        "funding_received_usd": "0",
        "realized_pnl_usd": realized_pnl_usd,
        "confidence": "HIGH",
        "schema_version": 1,
        "formula_version": 1,
        "matching_policy_version": 1,
        "primitive_version": 2,
    }


def _build_minimal_perp_db(db_path: Path, settlement_payload: dict | None, close_payload: dict) -> None:
    """The smallest DB ``_cell_g6_reconciliation`` will score: two priced
    snapshots, one gas-bearing ledger row, a PERP_CLOSE and its settlement."""
    conn = sqlite3.connect(str(db_path))
    dep = "deployment:g6test"
    try:
        conn.executescript(
            """
            CREATE TABLE transaction_ledger (
                id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT, timestamp TEXT,
                intent_type TEXT, chain TEXT, protocol TEXT, tx_hash TEXT, gas_usd TEXT,
                success INTEGER, price_inputs_json TEXT);
            CREATE TABLE accounting_events (
                id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT, execution_mode TEXT,
                timestamp TEXT, chain TEXT, protocol TEXT, wallet_address TEXT, event_type TEXT,
                position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT, confidence TEXT,
                payload_json TEXT, schema_version INTEGER);
            CREATE TABLE position_events (
                id TEXT PRIMARY KEY, deployment_id TEXT, cycle_id TEXT, timestamp TEXT,
                position_id TEXT, position_type TEXT, event_type TEXT, chain TEXT,
                protocol TEXT, value_usd TEXT, tx_hash TEXT, ledger_entry_id TEXT);
            CREATE TABLE portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT, cycle_id TEXT,
                execution_mode TEXT, timestamp TEXT, iteration_number INTEGER,
                total_value_usd TEXT, available_cash_usd TEXT, deployed_capital_usd TEXT,
                value_confidence TEXT, positions_json TEXT, token_prices_json TEXT,
                wallet_balances_json TEXT, chain TEXT, created_at TEXT);
            CREATE TABLE portfolio_metrics (
                deployment_id TEXT PRIMARY KEY, initial_value_usd TEXT, initial_timestamp TEXT,
                deposits_usd TEXT, withdrawals_usd TEXT, gas_spent_usd TEXT,
                total_value_usd TEXT, positions_json TEXT, cycle_id TEXT,
                execution_mode TEXT, is_complete INTEGER, updated_at TEXT);
            CREATE TABLE position_state_snapshots (
                id TEXT PRIMARY KEY, deployment_id TEXT, timestamp TEXT);
            CREATE TABLE position_registry (
                physical_identity_hash TEXT PRIMARY KEY, deployment_id TEXT, primitive TEXT,
                status TEXT, closed_tx TEXT);
            """
        )
        conn.execute(
            "INSERT INTO transaction_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "tl-close",
                dep,
                "c1",
                "2026-05-09T00:01:00+00:00",
                "PERP_CLOSE",
                "arbitrum",
                "gmx_v2",
                "0xt",
                "0",
                1,
                "{}",
            ),
        )
        rows = [("ae-close", "PERP_CLOSE", "tl-close", close_payload)]
        if settlement_payload is not None:
            rows.append(("ae-settle", "PERP_SETTLEMENT", "tl-close", settlement_payload))
        for row_id, event_type, ledger_id, payload in rows:
            conn.execute(
                "INSERT INTO accounting_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row_id,
                    dep,
                    "c1",
                    "paper",
                    "2026-05-09T00:01:00+00:00",
                    "arbitrum",
                    "gmx_v2",
                    "0xwallet",
                    event_type,
                    "pk",
                    ledger_id,
                    "0xt",
                    "HIGH",
                    json.dumps(payload),
                    1,
                ),
            )
        for i, (ts, total) in enumerate(
            (("2026-05-09T00:00:00+00:00", "100"), ("2026-05-09T00:02:00+00:00", "100")), start=1
        ):
            conn.execute(
                "INSERT INTO portfolio_snapshots "
                "(deployment_id, cycle_id, execution_mode, timestamp, iteration_number, total_value_usd,"
                " available_cash_usd, deployed_capital_usd, value_confidence, positions_json,"
                " token_prices_json, wallet_balances_json, chain, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (dep, "c1", "paper", ts, i, total, "0", "0", "HIGH", "[]", "{}", "[]", "arbitrum", ts),
            )
        conn.execute(
            "INSERT INTO portfolio_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (dep, "100", "2026-05-09T00:00:00+00:00", "0", "0", "0", "100", "[]", "c1", "paper", 1, "x"),
        )
        conn.commit()
    finally:
        conn.close()
