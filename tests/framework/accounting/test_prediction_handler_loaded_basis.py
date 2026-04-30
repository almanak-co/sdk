"""Loaded-cost-basis tests for the prediction-market accounting handler (VIB-3710).

The headline pUSD basis recorded by VIB-3707 was systematically optimistic
because (a) first-trade approval/wrap MATIC was unattributed and (b) operator
fees set at match time weren't part of the signed order. This file covers
the VIB-3710 fix:

  a. BUY with no setup txs (allowances pre-set, no wrap), fee_pusd present
     -> basis row reflects only fees beyond cost.
  b. BUY with 5 approval txs + 1 wrap tx -> basis row reflects all 6 gas
     costs summed.
  c. BUY then SELL with fees and gas -> realized_pnl =
     proceeds - (sold/total) * fully_loaded_basis.
  d. BUY then REDEEM with fees -> realized_pnl includes fees in cost.
  e. Average-up: BUY then BUY -> both gas/fees accumulate.
  f. Missing matic_price: gas_cost_native recorded, gas_cost_usd is None,
     warning logged on the enricher; the handler tolerates the missing USD
     value and folds only fees + cost into the basis (gas_cost_usd = 0).

No live chain calls, no SQLite, no gateway.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.prediction_handler import handle_prediction
from almanak.framework.accounting.models import PredictionEventType

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_DEPLOYMENT_ID = "dep-pred-loaded"
_STRATEGY_ID = "strat-pred-loaded"
_CYCLE_ID = "cycle-pred-loaded-1"
_WALLET = "0xabcdef1234567890abcdef1234567890abcdef12"
_CHAIN = "polygon"
_PROTOCOL = "polymarket"
_MARKET_ID = "will-eth-double-by-eoy"
_TX_HASH = "0xfeedface5678"


def _position_key(market_id: str = _MARKET_ID, outcome: str = "YES") -> str:
    return f"prediction:{_PROTOCOL}:{_CHAIN}:{_WALLET.lower()}:{market_id}:{outcome}"


def _make_outbox_row(intent_type: str) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": str(uuid.uuid4()),
        "deployment_id": _DEPLOYMENT_ID,
        "strategy_id": _STRATEGY_ID,
        "cycle_id": _CYCLE_ID,
        "intent_type": intent_type,
        "wallet_address": _WALLET,
        "position_key": _position_key(),
        "market_id": _MARKET_ID,
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _extracted_data_json(extra: dict[str, Any]) -> str:
    return json.dumps({k: (str(v) if isinstance(v, Decimal) else v) for k, v in extra.items()})


def _make_ledger_row(intent_type: str, *, extracted_data_json: str = "", gas_usd: str = "0") -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "strategy_id": _STRATEGY_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": "USDC",
        "amount_in": "0",
        "token_out": "",
        "amount_out": "0",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": gas_usd,
        "tx_hash": _TX_HASH,
        "chain": _CHAIN,
        "protocol": _PROTOCOL,
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": "",
        "pre_state_json": "",
        "post_state_json": "",
    }


def _buy_event(
    *,
    shares: str,
    cost_basis: str,
    gas_cost_usd: str | None = None,
    fee_pusd: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    extracted: dict[str, Any] = {
        "outcome_tokens_received": shares,
        "cost_basis": cost_basis,
        "market_id": _MARKET_ID,
    }
    if gas_cost_usd is not None:
        extracted["gas_cost_usd"] = gas_cost_usd
    if fee_pusd is not None:
        extracted["fee_pusd"] = fee_pusd
    return (
        _make_outbox_row("PREDICTION_BUY"),
        _make_ledger_row("PREDICTION_BUY", extracted_data_json=_extracted_data_json(extracted)),
    )


def _sell_event(*, shares: str, proceeds: str, fee_pusd: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    extracted: dict[str, Any] = {
        "outcome_tokens_sold": shares,
        "proceeds": proceeds,
        "market_id": _MARKET_ID,
    }
    if fee_pusd is not None:
        extracted["fee_pusd"] = fee_pusd
    return (
        _make_outbox_row("PREDICTION_SELL"),
        _make_ledger_row("PREDICTION_SELL", extracted_data_json=_extracted_data_json(extracted)),
    )


def _redeem_event(*, shares: str, payout: str) -> tuple[dict[str, Any], dict[str, Any]]:
    extracted = {
        "redemption_amount": shares,
        "payout": payout,
        "market_id": _MARKET_ID,
    }
    return (
        _make_outbox_row("PREDICTION_REDEEM"),
        _make_ledger_row("PREDICTION_REDEEM", extracted_data_json=_extracted_data_json(extracted)),
    )


# ──────────────────────────────────────────────────────────────────────────────
# (a) BUY with no setup txs (allowances pre-set), fee only
# ──────────────────────────────────────────────────────────────────────────────


class TestBuyWithFeeOnlyNoGas:
    """Allowances were already in place from a prior trade and no wrap was
    needed for this BUY -> the gateway records zero setup_txs -> gas_cost_usd
    arrives as 0 (or omitted). The fee remains the only "loaded extra"."""

    def test_buy_records_fee_in_loaded_extras(self) -> None:
        basis = FIFOBasisStore()
        outbox, ledger = _buy_event(
            shares="10",
            cost_basis="5.00",
            gas_cost_usd="0",
            fee_pusd="0.05",
        )
        ev = handle_prediction(outbox, ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_OPEN
        # position_basis_after is the headline pUSD spent only — VIB-3710
        # tracks loaded extras separately so the existing event payload shape
        # stays backward-compatible.
        assert ev.position_basis_after == Decimal("5.00")
        # The basis store's loaded_extras accumulator should equal the fee.
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0.05")


# ──────────────────────────────────────────────────────────────────────────────
# (b) BUY with 5 approval txs + 1 wrap tx — all 6 gas costs summed
# ──────────────────────────────────────────────────────────────────────────────


class TestBuyWithSetupTxGas:
    def test_basis_row_reflects_all_six_gas_costs(self) -> None:
        # The enricher's _extract_offchain_prediction_costs has already
        # summed the 6 setup_tx total_cost_wei values and converted via the
        # MATIC USD price into gas_cost_usd. The handler reads the summed
        # USD value directly. We plug the realistic post-enricher number:
        #  - 5 approvals @ 60_000 gas * 50 gwei = 0.003 MATIC each = 0.015 MATIC
        #  - 1 wrap @ 150_000 gas * 50 gwei = 0.0075 MATIC
        #  - total: 0.0225 MATIC * $0.80 MATIC = $0.018
        basis = FIFOBasisStore()
        outbox, ledger = _buy_event(
            shares="10",
            cost_basis="5.00",
            gas_cost_usd="0.018",
            fee_pusd="0.05",
        )
        ev = handle_prediction(outbox, ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_OPEN
        assert ev.position_basis_after == Decimal("5.00")
        # loaded_extras = gas + fees = 0.018 + 0.05 = 0.068
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0.068")


# ──────────────────────────────────────────────────────────────────────────────
# (c) BUY then SELL — realized PnL uses fully-loaded basis proportionally
# ──────────────────────────────────────────────────────────────────────────────


class TestBuyThenSellWithLoadedBasis:
    def test_full_sell_realized_pnl_subtracts_full_loaded_basis(self) -> None:
        basis = FIFOBasisStore()
        # BUY 10 @ $0.50 = $5 cost basis, $0.018 gas, $0.05 fee
        # fully_loaded_basis = 5.00 + 0.018 + 0.05 = 5.068
        handle_prediction(
            *_buy_event(shares="10", cost_basis="5.00", gas_cost_usd="0.018", fee_pusd="0.05"),
            basis,
        )
        # SELL all 10 @ $0.55 = $5.50 gross proceeds, $0.10 fee on the sell.
        # VIB-3710 (CodeRabbit thread 4): SELL-side fee_pusd is now subtracted
        # from proceeds BEFORE realized PnL is computed, so the bookkeeping
        # matches what the wallet actually received. The gross proceeds are
        # still preserved on the event payload (``usd_delta``) for audit trail.
        # net_proceeds = 5.50 - 0.10 = 5.40
        # realized = net_proceeds - (10/10) * fully_loaded_basis
        #          = 5.40 - 5.068 = 0.332
        sell_outbox, sell_ledger = _sell_event(shares="10", proceeds="5.50", fee_pusd="0.10")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_CLOSE
        assert ev.realized_pnl_usd == Decimal("0.332")
        # Audit trail: ``usd_delta`` keeps the gross proceeds, NOT net.
        assert ev.usd_delta == Decimal("5.50")
        # Position closed -> aggregate dropped.
        assert basis.get_prediction_position(_DEPLOYMENT_ID, _position_key()) is None
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0")

    def test_partial_sell_consumes_loaded_extras_proportionally(self) -> None:
        basis = FIFOBasisStore()
        # BUY 10 @ $0.50 = $5.00 basis, $0.10 gas, $0.05 fee
        # loaded_extras = 0.15. fully_loaded_basis = 5.15.
        handle_prediction(
            *_buy_event(shares="10", cost_basis="5.00", gas_cost_usd="0.10", fee_pusd="0.05"),
            basis,
        )
        # SELL 4 @ $0.55 = $2.20 proceeds.
        # cost_consumed = (4/10) * 5.15 = 2.06
        # realized = 2.20 - 2.06 = 0.14
        sell_outbox, sell_ledger = _sell_event(shares="4", proceeds="2.20")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_REDUCE
        assert ev.realized_pnl_usd == Decimal("0.14")
        # Remaining: 6 shares, basis 5.00 - 2.00 = 3.00, extras 0.15 - 0.06 = 0.09
        residual = basis.get_prediction_position(_DEPLOYMENT_ID, _position_key())
        assert residual is not None
        assert residual[0] == Decimal("6")
        assert residual[1] == Decimal("3.00")
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0.09")


# ──────────────────────────────────────────────────────────────────────────────
# (c.5) SELL-side fee_pusd is subtracted from realized PnL (CodeRabbit thread 4)
# ──────────────────────────────────────────────────────────────────────────────


class TestSellFeePusdDeductedFromRealizedPnl:
    """Focused regression for the VIB-3710 / CodeRabbit thread 4 fix.

    Pre-fix the SELL-side ``fee_pusd`` extracted by the result enricher was
    landed on the event but NEVER subtracted from realized PnL — proceeds
    flowed straight into ``match_prediction_sell``. That overstated realized
    PnL by exactly ``fee_pusd``. This test isolates the fix from the loaded
    BUY-side basis path so a regression here is unambiguous.
    """

    def test_sell_fee_pusd_deducted_from_realized_pnl(self) -> None:
        basis = FIFOBasisStore()
        # BUY 10 @ $0.50 = $5.00 basis. NO BUY-side gas/fee extras: this
        # isolates the SELL-fee subtraction from the loaded-basis arithmetic.
        handle_prediction(
            *_buy_event(shares="10", cost_basis="5.00"),
            basis,
        )
        # SELL 10 @ $0.60 = $6.00 gross proceeds, $0.05 SELL-side fee.
        # net_proceeds = 6.00 - 0.05 = 5.95
        # realized = net_proceeds - basis_consumed = 5.95 - 5.00 = 0.95
        sell_outbox, sell_ledger = _sell_event(shares="10", proceeds="6.00", fee_pusd="0.05")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_CLOSE
        assert ev.realized_pnl_usd == Decimal("0.95")
        # Gross proceeds preserved on the event for audit traceability.
        assert ev.usd_delta == Decimal("6.00")

    def test_sell_with_zero_fee_matches_no_fee_path(self) -> None:
        """fee_pusd == "0" -> identical PnL to no fee_pusd at all (both -> 0)."""
        basis_a = FIFOBasisStore()
        basis_b = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis_a)
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis_b)

        ev_a = handle_prediction(*_sell_event(shares="10", proceeds="6.00"), basis_a)
        ev_b = handle_prediction(*_sell_event(shares="10", proceeds="6.00", fee_pusd="0"), basis_b)
        assert ev_a is not None and ev_b is not None
        assert ev_a.realized_pnl_usd == ev_b.realized_pnl_usd == Decimal("1.00")

    def test_sell_with_negative_fee_clamped_to_zero(self) -> None:
        """A buggy upstream sending a negative fee must NOT inflate PnL."""
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)
        sell_outbox, sell_ledger = _sell_event(shares="10", proceeds="6.00", fee_pusd="-0.10")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert ev is not None
        # Negative fee clamped to 0 -> realized PnL unchanged from no-fee case.
        assert ev.realized_pnl_usd == Decimal("1.00")


# ──────────────────────────────────────────────────────────────────────────────
# (d) BUY then REDEEM — realized PnL includes fees + gas in cost
# ──────────────────────────────────────────────────────────────────────────────


class TestBuyThenRedeemWithLoadedBasis:
    def test_winning_redeem_includes_fees_and_gas_in_cost(self) -> None:
        basis = FIFOBasisStore()
        # BUY 10 @ $0.40 = $4.00 basis, $0.02 gas, $0.04 fee
        # fully_loaded_basis = 4.06
        handle_prediction(
            *_buy_event(shares="10", cost_basis="4.00", gas_cost_usd="0.02", fee_pusd="0.04"),
            basis,
        )
        # REDEEM: position resolves YES -> payout = 10 * $1.00 = $10.00
        # realized = 10.00 - 4.06 = 5.94
        ev = handle_prediction(*_redeem_event(shares="10", payout="10.00"), basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_REDEEM
        assert ev.realized_pnl_usd == Decimal("5.94")


# ──────────────────────────────────────────────────────────────────────────────
# (e) Average-up: BUY then BUY — gas/fees accumulate alongside basis
# ──────────────────────────────────────────────────────────────────────────────


class TestAverageUpAccumulatesLoadedExtras:
    def test_two_buys_sum_extras_into_aggregate(self) -> None:
        basis = FIFOBasisStore()
        # First BUY: 5 shares @ $0.40, $0.018 gas, $0.05 fee
        ev1 = handle_prediction(
            *_buy_event(shares="5", cost_basis="2.00", gas_cost_usd="0.018", fee_pusd="0.05"),
            basis,
        )
        assert ev1 is not None
        assert ev1.event_type == PredictionEventType.PREDICTION_OPEN
        assert ev1.position_basis_after == Decimal("2.00")

        # Second BUY: 5 shares @ $0.50, $0 gas (allowances done), $0.04 fee
        ev2 = handle_prediction(
            *_buy_event(shares="5", cost_basis="2.50", gas_cost_usd="0", fee_pusd="0.04"),
            basis,
        )
        assert ev2 is not None
        assert ev2.event_type == PredictionEventType.PREDICTION_INCREASE
        assert ev2.position_size_after == Decimal("10")
        assert ev2.position_basis_after == Decimal("4.50")

        # loaded_extras accumulates: 0.018 + 0.05 + 0 + 0.04 = 0.108
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0.108")

        # SELL all 10 @ $0.60 = $6.00 proceeds
        # fully_loaded_basis = 4.50 + 0.108 = 4.608
        # realized = 6.00 - 4.608 = 1.392
        sell_ev = handle_prediction(*_sell_event(shares="10", proceeds="6.00"), basis)
        assert sell_ev is not None
        assert sell_ev.event_type == PredictionEventType.PREDICTION_CLOSE
        assert sell_ev.realized_pnl_usd == Decimal("1.392")


# ──────────────────────────────────────────────────────────────────────────────
# (f) Missing matic_price: gas_cost_usd absent -> handler folds 0 USD gas
# ──────────────────────────────────────────────────────────────────────────────


class TestMissingMaticPrice:
    """The result enricher records gas_cost_native_wei always, but
    gas_cost_usd is omitted when MATIC USD price is unavailable. The handler
    must tolerate that (treat as 0 USD gas) without crashing — under-attributing
    cost is far safer than fabricating a USD figure from nothing."""

    def test_buy_with_missing_gas_cost_usd_does_not_crash(self) -> None:
        basis = FIFOBasisStore()
        # extracted_data has fee_pusd but NOT gas_cost_usd (and no
        # gas_cost_native_wei either — we test the handler's read here, not
        # the enricher; the enricher test covers the warning path).
        outbox, ledger = _buy_event(shares="10", cost_basis="5.00", fee_pusd="0.05")
        ev = handle_prediction(outbox, ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_OPEN
        assert ev.position_basis_after == Decimal("5.00")
        # Only fee was loaded — gas was unavailable, so loaded_extras = 0.05.
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0.05")

    def test_buy_with_negative_gas_cost_usd_clamped_to_zero(self) -> None:
        # Defensive: a buggy upstream measurement that sends a negative
        # gas_cost_usd must NOT silently subtract from realized PnL.
        basis = FIFOBasisStore()
        outbox, ledger = _buy_event(
            shares="10",
            cost_basis="5.00",
            gas_cost_usd="-1.00",  # bogus
            fee_pusd="0.05",
        )
        ev = handle_prediction(outbox, ledger, basis)
        assert ev is not None
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0.05")


# ──────────────────────────────────────────────────────────────────────────────
# Backward-compat: old extracted_data without VIB-3710 fields still works
# ──────────────────────────────────────────────────────────────────────────────


class TestBackwardCompat:
    def test_buy_without_any_loaded_extras_keys_records_zero_extras(self) -> None:
        basis = FIFOBasisStore()
        outbox, ledger = _buy_event(shares="5", cost_basis="2.50")
        ev = handle_prediction(outbox, ledger, basis)
        assert ev is not None
        assert ev.position_basis_after == Decimal("2.50")
        assert basis.get_prediction_loaded_extras(_DEPLOYMENT_ID, _position_key()) == Decimal("0")

        # Sell behaves identically to pre-VIB-3710 when no extras loaded.
        sell_outbox, sell_ledger = _sell_event(shares="5", proceeds="3.00")
        sev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert sev is not None
        assert sev.realized_pnl_usd == Decimal("0.50")


@pytest.mark.parametrize(
    "shares,cost_basis,gas,fee,proceeds,expected_pnl",
    [
        # Plain (no extras): 10 @ 0.50 -> sold @ 0.55 -> 0.50 realized
        ("10", "5.00", None, None, "5.50", "0.50"),
        # Fees only: same trade, $0.10 fee on BUY -> 0.40 realized
        ("10", "5.00", "0", "0.10", "5.50", "0.40"),
        # Gas only: $0.20 gas -> 0.30 realized
        ("10", "5.00", "0.20", "0", "5.50", "0.30"),
        # Gas + fees: $0.10 gas + $0.10 fee -> 0.30 realized
        ("10", "5.00", "0.10", "0.10", "5.50", "0.30"),
    ],
)
def test_realized_pnl_parametric(
    shares: str,
    cost_basis: str,
    gas: str | None,
    fee: str | None,
    proceeds: str,
    expected_pnl: str,
) -> None:
    """Parametric coverage of the loaded-basis subtraction across combinations."""
    basis = FIFOBasisStore()
    handle_prediction(
        *_buy_event(shares=shares, cost_basis=cost_basis, gas_cost_usd=gas, fee_pusd=fee),
        basis,
    )
    sell_outbox, sell_ledger = _sell_event(shares=shares, proceeds=proceeds)
    ev = handle_prediction(sell_outbox, sell_ledger, basis)
    assert ev is not None
    assert ev.realized_pnl_usd == Decimal(expected_pnl)
