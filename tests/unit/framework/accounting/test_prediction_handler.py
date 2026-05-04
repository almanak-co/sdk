"""Unit + integration tests for the prediction-market accounting handler (VIB-3707).

Covers:
- BUY -> SELL (full close) -> realized PnL, basis row deleted
- BUY -> partial SELL -> proportional realized PnL, remaining basis correct
- BUY -> partial SELL -> SELL rest -> total realized PnL across two disposals
- BUY -> REDEEM -> realized PnL = payout - basis, position closed
- BUY -> BUY (averaging up) -> weighted-average aggregate
- SELL with no prior basis -> warning logged, realized_pnl=None, no crash
- Classifier routes PREDICTION_BUY/SELL/REDEEM to AccountingCategory.PREDICTION
- Processor _dispatch integration: PREDICTION_BUY ledger row produces a
  PredictionAccountingEvent

No live chain calls, no SQLite, no gateway.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.prediction_handler import handle_prediction
from almanak.framework.accounting.classifier import AccountingCategory, classify
from almanak.framework.accounting.models import (
    AccountingConfidence,
    PredictionAccountingEvent,
    PredictionEventType,
)
from almanak.framework.accounting.processor import AccountingProcessor

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_DEPLOYMENT_ID = "dep-pred-test"
_STRATEGY_ID = "strat-pred-test"
_CYCLE_ID = "cycle-pred-1"
_WALLET = "0xabcdef1234567890abcdef1234567890abcdef12"
_CHAIN = "polygon"
_PROTOCOL = "polymarket"
_MARKET_ID = "will-bitcoin-exceed-100000"
_TX_HASH = "0xfeedface5678"


def _position_key(market_id: str = _MARKET_ID, outcome: str = "YES", protocol: str = _PROTOCOL) -> str:
    return f"prediction:{protocol}:{_CHAIN}:{_WALLET.lower()}:{market_id}:{outcome}"


def _make_outbox_row(
    intent_type: str,
    *,
    wallet_address: str = _WALLET,
    market_id: str = _MARKET_ID,
    outcome: str = "YES",
    position_key: str | None = None,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": str(uuid.uuid4()),
        "deployment_id": _DEPLOYMENT_ID,
        "strategy_id": _STRATEGY_ID,
        "cycle_id": _CYCLE_ID,
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key
        if position_key is not None
        else _position_key(market_id=market_id, outcome=outcome),
        "market_id": market_id,
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _extracted_data_json(extra: dict[str, Any]) -> str:
    """Serialize the extracted_data payload the way ledger.py does (no _type tags
    needed for plain Decimals/strings the prediction handler reads).
    """
    return json.dumps({k: (str(v) if isinstance(v, Decimal) else v) for k, v in extra.items()})


def _make_ledger_row(
    intent_type: str,
    *,
    extracted_data_json: str = "",
    protocol: str = _PROTOCOL,
    chain: str = _CHAIN,
    tx_hash: str = _TX_HASH,
    gas_usd: str = "0",
    ledger_entry_id: str | None = None,
) -> dict[str, Any]:
    lid = ledger_entry_id or str(uuid.uuid4())
    return {
        "id": lid,
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
        "tx_hash": tx_hash,
        "chain": chain,
        "protocol": protocol,
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": "",
        "pre_state_json": "",
        "post_state_json": "",
    }


def _buy_event(shares: str, cost_basis: str) -> tuple[dict[str, Any], dict[str, Any]]:
    extracted = {
        "outcome_tokens_received": shares,
        "cost_basis": cost_basis,
        "market_id": _MARKET_ID,
    }
    return (
        _make_outbox_row("PREDICTION_BUY"),
        _make_ledger_row("PREDICTION_BUY", extracted_data_json=_extracted_data_json(extracted)),
    )


def _sell_event(shares: str, proceeds: str) -> tuple[dict[str, Any], dict[str, Any]]:
    extracted = {
        "outcome_tokens_sold": shares,
        "proceeds": proceeds,
        "market_id": _MARKET_ID,
    }
    return (
        _make_outbox_row("PREDICTION_SELL"),
        _make_ledger_row("PREDICTION_SELL", extracted_data_json=_extracted_data_json(extracted)),
    )


def _redeem_event(shares: str, payout: str) -> tuple[dict[str, Any], dict[str, Any]]:
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
# (g) Classifier wiring
# ──────────────────────────────────────────────────────────────────────────────


class TestClassifier:
    @pytest.mark.parametrize(
        "intent_type",
        ["PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"],
    )
    def test_prediction_intent_routes_to_prediction_category(self, intent_type: str) -> None:
        assert classify(intent_type) == AccountingCategory.PREDICTION
        # Also case-insensitive — the classifier upper()s its input
        assert classify(intent_type.lower()) == AccountingCategory.PREDICTION
        # Protocol does not matter for prediction routing
        assert classify(intent_type, protocol="polymarket") == AccountingCategory.PREDICTION
        assert classify(intent_type, protocol="any-future-prediction-protocol") == AccountingCategory.PREDICTION

    def test_unknown_prediction_intent_falls_through(self) -> None:
        # Defensive: unrecognised PREDICTION_* type is NOT auto-routed
        assert classify("PREDICTION_RANDOM") == AccountingCategory.NO_ACCOUNTING
        assert classify("PREDICTION_FOO") == AccountingCategory.NO_ACCOUNTING


# ──────────────────────────────────────────────────────────────────────────────
# (a) BUY -> SELL (full close)
# ──────────────────────────────────────────────────────────────────────────────


class TestBuyThenFullSell:
    def test_buy_then_full_sell_realizes_profit(self) -> None:
        """BUY 5 @ $0.50 (basis $2.50), SELL 5 @ $0.60 (proceeds $3.00) -> realized $0.50."""
        basis = FIFOBasisStore()

        buy_outbox, buy_ledger = _buy_event(shares="5", cost_basis="2.50")
        buy_event = handle_prediction(buy_outbox, buy_ledger, basis)
        assert buy_event is not None
        assert buy_event.event_type == PredictionEventType.PREDICTION_OPEN
        assert buy_event.position_size_after == Decimal("5")
        assert buy_event.position_basis_after == Decimal("2.50")
        assert buy_event.realized_pnl_usd is None  # BUY has no realized PnL

        sell_outbox, sell_ledger = _sell_event(shares="5", proceeds="3.00")
        sell_event = handle_prediction(sell_outbox, sell_ledger, basis)
        assert sell_event is not None
        assert sell_event.event_type == PredictionEventType.PREDICTION_CLOSE
        assert sell_event.realized_pnl_usd == Decimal("0.50")
        # Position fully closed -> aggregate is zeroed.
        assert sell_event.position_size_after == Decimal("0")
        assert sell_event.position_basis_after == Decimal("0")

        # Basis row deleted
        assert basis.get_prediction_position(_DEPLOYMENT_ID, _position_key()) is None

    def test_buy_then_full_sell_realizes_loss(self) -> None:
        """BUY 10 @ $0.60 -> SELL 10 @ $0.40 -> realized $-2.00."""
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="6.00"), basis)

        sell_outbox, sell_ledger = _sell_event(shares="10", proceeds="4.00")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert ev is not None
        assert ev.realized_pnl_usd == Decimal("-2.00")
        assert ev.event_type == PredictionEventType.PREDICTION_CLOSE


# ──────────────────────────────────────────────────────────────────────────────
# (b) BUY -> partial SELL
# ──────────────────────────────────────────────────────────────────────────────


class TestPartialSell:
    def test_buy_then_partial_sell(self) -> None:
        """BUY 10 @ $0.50 (basis $5.00), SELL 3 @ $0.55 (proceeds $1.65)
        -> realized = 1.65 - (3/10)*5 = 0.15. Remaining: size=7, basis=$3.50.
        """
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)

        sell_outbox, sell_ledger = _sell_event(shares="3", proceeds="1.65")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_REDUCE
        assert ev.realized_pnl_usd == Decimal("0.15")
        assert ev.position_size_after == Decimal("7")
        assert ev.position_basis_after == Decimal("3.50")

        # Underlying aggregate matches event snapshot
        prior = basis.get_prediction_position(_DEPLOYMENT_ID, _position_key())
        assert prior == (Decimal("7"), Decimal("3.50"))


# ──────────────────────────────────────────────────────────────────────────────
# (c) BUY -> partial SELL -> SELL rest
# ──────────────────────────────────────────────────────────────────────────────


class TestPartialThenFullSell:
    def test_two_disposals_total_realized(self) -> None:
        """Continuation of (b): SELL remaining 7 @ $0.55 -> realized $0.35.
        Total realized across both disposals = 0.15 + 0.35 = 0.50.
        """
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)
        first = handle_prediction(*_sell_event(shares="3", proceeds="1.65"), basis)
        assert first is not None
        assert first.realized_pnl_usd == Decimal("0.15")

        second = handle_prediction(*_sell_event(shares="7", proceeds="3.85"), basis)
        assert second is not None
        assert second.event_type == PredictionEventType.PREDICTION_CLOSE
        assert second.realized_pnl_usd == Decimal("0.35")

        total_realized = first.realized_pnl_usd + second.realized_pnl_usd
        assert total_realized == Decimal("0.50")
        # Basis row deleted after final SELL
        assert basis.get_prediction_position(_DEPLOYMENT_ID, _position_key()) is None


# ──────────────────────────────────────────────────────────────────────────────
# (d) BUY -> REDEEM
# ──────────────────────────────────────────────────────────────────────────────


class TestRedeem:
    def test_buy_then_winning_redeem(self) -> None:
        """BUY 10 @ $0.50 (basis $5.00), REDEEM payout $10 (winning YES @ $1)
        -> realized = $5.00. Basis row deleted.
        """
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)

        red_outbox, red_ledger = _redeem_event(shares="10", payout="10.00")
        ev = handle_prediction(red_outbox, red_ledger, basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_REDEEM
        assert ev.realized_pnl_usd == Decimal("5.00")
        assert ev.position_size_after == Decimal("0")
        assert ev.position_basis_after == Decimal("0")
        # Always closes
        assert basis.get_prediction_position(_DEPLOYMENT_ID, _position_key()) is None

    def test_losing_redeem(self) -> None:
        """Losing position -> payout $0 -> realized = -basis."""
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="5", cost_basis="2.50"), basis)
        ev = handle_prediction(*_redeem_event(shares="5", payout="0"), basis)
        assert ev is not None
        assert ev.realized_pnl_usd == Decimal("-2.50")
        assert ev.event_type == PredictionEventType.PREDICTION_REDEEM


# ──────────────────────────────────────────────────────────────────────────────
# (e) BUY -> BUY (averaging up)
# ──────────────────────────────────────────────────────────────────────────────


class TestAveragingUp:
    def test_two_buys_combine_via_weighted_average(self) -> None:
        """BUY 5 @ $0.50, BUY 5 @ $0.60 -> aggregate size=10, basis=$5.50."""
        basis = FIFOBasisStore()
        first = handle_prediction(*_buy_event(shares="5", cost_basis="2.50"), basis)
        assert first is not None
        assert first.event_type == PredictionEventType.PREDICTION_OPEN
        assert first.position_size_after == Decimal("5")
        assert first.position_basis_after == Decimal("2.50")

        second = handle_prediction(*_buy_event(shares="5", cost_basis="3.00"), basis)
        assert second is not None
        assert second.event_type == PredictionEventType.PREDICTION_INCREASE
        assert second.position_size_after == Decimal("10")
        assert second.position_basis_after == Decimal("5.50")

        prior = basis.get_prediction_position(_DEPLOYMENT_ID, _position_key())
        assert prior == (Decimal("10"), Decimal("5.50"))

    def test_average_up_then_partial_sell_uses_blended_basis(self) -> None:
        """After averaging up to size=10, basis=$5.50, SELL 4 @ $0.65
        -> cost_consumed = (4/10)*5.50 = 2.20; proceeds = 2.60; realized = 0.40.
        Remaining: size=6, basis=$3.30.
        """
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="5", cost_basis="2.50"), basis)
        handle_prediction(*_buy_event(shares="5", cost_basis="3.00"), basis)

        ev = handle_prediction(*_sell_event(shares="4", proceeds="2.60"), basis)
        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_REDUCE
        assert ev.realized_pnl_usd == Decimal("0.40")
        assert ev.position_size_after == Decimal("6")
        assert ev.position_basis_after == Decimal("3.30")


# ──────────────────────────────────────────────────────────────────────────────
# (f) SELL with no prior basis
# ──────────────────────────────────────────────────────────────────────────────


class TestSellWithoutPriorBasis:
    def test_sell_with_no_basis_logs_warning_and_returns_event(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        basis = FIFOBasisStore()
        sell_outbox, sell_ledger = _sell_event(shares="3", proceeds="1.65")

        with caplog.at_level(logging.WARNING):
            ev = handle_prediction(sell_outbox, sell_ledger, basis)

        assert ev is not None, "must not crash"
        assert ev.realized_pnl_usd is None, "must not fabricate $0 basis from missing record"
        assert ev.confidence == AccountingConfidence.UNAVAILABLE
        assert "no recorded basis" in ev.unavailable_reason.lower()
        # WARNING was emitted
        assert any("no recorded basis" in rec.message.lower() for rec in caplog.records)

    def test_redeem_with_no_basis_does_not_crash(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        basis = FIFOBasisStore()
        red_outbox, red_ledger = _redeem_event(shares="10", payout="10.00")

        with caplog.at_level(logging.WARNING):
            ev = handle_prediction(red_outbox, red_ledger, basis)

        assert ev is not None
        assert ev.realized_pnl_usd is None
        assert ev.event_type == PredictionEventType.PREDICTION_REDEEM


# ──────────────────────────────────────────────────────────────────────────────
# Outcome isolation: positions on YES vs NO do not collide
# ──────────────────────────────────────────────────────────────────────────────


class TestOutcomeIsolation:
    def test_yes_and_no_positions_are_isolated(self) -> None:
        basis = FIFOBasisStore()
        # Buy YES
        yes_outbox, yes_ledger = _buy_event(shares="5", cost_basis="2.50")
        handle_prediction(yes_outbox, yes_ledger, basis)
        # Buy NO (same market, different outcome)
        no_outbox = _make_outbox_row("PREDICTION_BUY", outcome="NO")
        no_ledger = _make_ledger_row(
            "PREDICTION_BUY",
            extracted_data_json=_extracted_data_json(
                {
                    "outcome_tokens_received": "8",
                    "cost_basis": "3.20",
                    "market_id": _MARKET_ID,
                }
            ),
        )
        handle_prediction(no_outbox, no_ledger, basis)

        yes_pos = basis.get_prediction_position(_DEPLOYMENT_ID, _position_key(outcome="YES"))
        no_pos = basis.get_prediction_position(_DEPLOYMENT_ID, _position_key(outcome="NO"))
        assert yes_pos == (Decimal("5"), Decimal("2.50"))
        assert no_pos == (Decimal("8"), Decimal("3.20"))


# ──────────────────────────────────────────────────────────────────────────────
# Reconstruction safety — events round-trip into FIFOBasisStore on restart
# ──────────────────────────────────────────────────────────────────────────────


class TestReconstruction:
    def test_open_then_increase_replay_restores_aggregate(self) -> None:
        """After two BUYs, reconstruct_from_events on the persisted snapshot
        rebuilds the aggregate exactly.
        """
        basis = FIFOBasisStore()
        first = handle_prediction(*_buy_event(shares="5", cost_basis="2.50"), basis)
        second = handle_prediction(*_buy_event(shares="5", cost_basis="3.00"), basis)
        assert first is not None and second is not None

        # Simulate persistence + reconstruction via a fresh store.
        events = [
            _row_for_reconstruction(first),
            _row_for_reconstruction(second),
        ]
        fresh = FIFOBasisStore()
        replayed = fresh.reconstruct_from_events(events)
        assert replayed == 2

        prior = fresh.get_prediction_position(_DEPLOYMENT_ID, _position_key())
        assert prior == (Decimal("10"), Decimal("5.50"))

    def test_close_event_drops_row_during_replay(self) -> None:
        basis = FIFOBasisStore()
        buy = handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)
        sell = handle_prediction(*_sell_event(shares="10", proceeds="6.00"), basis)
        assert buy is not None and sell is not None

        events = [_row_for_reconstruction(buy), _row_for_reconstruction(sell)]
        fresh = FIFOBasisStore()
        fresh.reconstruct_from_events(events)
        # Closed -> row absent
        assert fresh.get_prediction_position(_DEPLOYMENT_ID, _position_key()) is None


def _row_for_reconstruction(event: PredictionAccountingEvent) -> dict[str, Any]:
    """Build the row shape reconstruct_from_events expects from a built event."""
    return {
        "event_type": event.event_type.value,
        "deployment_id": event.identity.deployment_id,
        "position_key": event.position_key,
        "timestamp": event.identity.timestamp.isoformat(),
        "ledger_entry_id": event.identity.ledger_entry_id,
        "payload_json": event.to_payload_json(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Payload roundtrip
# ──────────────────────────────────────────────────────────────────────────────


class TestPayloadRoundtrip:
    def test_to_from_payload_preserves_fields(self) -> None:
        basis = FIFOBasisStore()
        ev = handle_prediction(*_buy_event(shares="5", cost_basis="2.50"), basis)
        assert ev is not None

        payload = ev.to_payload_json()
        restored = PredictionAccountingEvent.from_payload_json(ev.identity, payload)
        assert restored.event_type == PredictionEventType.PREDICTION_OPEN
        assert restored.market_id == _MARKET_ID
        assert restored.outcome == "YES"
        assert restored.intent_type == "PREDICTION_BUY"
        assert restored.shares_delta == Decimal("5")
        assert restored.usd_delta == Decimal("2.50")
        assert restored.position_size_after == Decimal("5")
        assert restored.position_basis_after == Decimal("2.50")
        assert restored.realized_pnl_usd is None


# ──────────────────────────────────────────────────────────────────────────────
# (h) Processor dispatch integration
# ──────────────────────────────────────────────────────────────────────────────


def _make_mock_store(
    outbox_row: dict | None = None,
    ledger_row: dict | None = None,
    already_written: bool = False,
) -> MagicMock:
    store = MagicMock()
    store.get_outbox_by_ledger_id = MagicMock(return_value=outbox_row)
    store.get_outbox_pending = MagicMock(return_value=[outbox_row] if outbox_row else [])
    store.update_outbox_entry = MagicMock()
    store.has_accounting_events_for_ledger = MagicMock(return_value=already_written)
    store.get_ledger_entry_by_id = MagicMock(return_value=ledger_row)
    store.save_accounting_event = AsyncMock(return_value=True)
    return store


class TestProcessorDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_prediction_buy_writes_event(self) -> None:
        """drain_one on a PREDICTION_BUY outbox row produces a PredictionAccountingEvent."""
        outbox, ledger = _buy_event(shares="5", cost_basis="2.50")
        led_id = ledger["id"]
        outbox["ledger_entry_id"] = led_id
        store = _make_mock_store(outbox_row=outbox, ledger_row=ledger, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT_ID)

        result = await proc.drain_one(led_id)

        assert result is True, "drain_one must return True for PREDICTION_BUY"
        store.save_accounting_event.assert_awaited_once()
        written = store.save_accounting_event.call_args[0][0]
        assert isinstance(written, PredictionAccountingEvent)
        assert written.event_type == PredictionEventType.PREDICTION_OPEN
        assert written.market_id == _MARKET_ID
        assert written.outcome == "YES"
        assert written.position_size_after == Decimal("5")
        assert written.position_basis_after == Decimal("2.50")

    @pytest.mark.asyncio
    async def test_dispatch_prediction_sell_writes_event_after_buy(self) -> None:
        """End-to-end: BUY -> drain -> SELL -> drain; second event has realized_pnl."""
        shared_basis = FIFOBasisStore()

        # First, drain a BUY
        buy_outbox, buy_ledger = _buy_event(shares="10", cost_basis="5.00")
        buy_led_id = buy_ledger["id"]
        buy_outbox["ledger_entry_id"] = buy_led_id
        store_buy = _make_mock_store(outbox_row=buy_outbox, ledger_row=buy_ledger, already_written=False)
        proc = AccountingProcessor(state_manager=store_buy, basis_store=shared_basis, deployment_id=_DEPLOYMENT_ID)
        assert await proc.drain_one(buy_led_id) is True

        # Now drain a SELL on the same processor (in-memory basis state preserved)
        sell_outbox, sell_ledger = _sell_event(shares="10", proceeds="6.00")
        sell_led_id = sell_ledger["id"]
        sell_outbox["ledger_entry_id"] = sell_led_id
        store_sell = _make_mock_store(outbox_row=sell_outbox, ledger_row=sell_ledger, already_written=False)
        # Re-bind processor to the new mock store (basis_store stays in scope)
        proc._state_manager = store_sell
        proc._writer._store = store_sell  # AccountingWriter holds its own store ref
        assert await proc.drain_one(sell_led_id) is True

        store_sell.save_accounting_event.assert_awaited_once()
        sell_written = store_sell.save_accounting_event.call_args[0][0]
        assert isinstance(sell_written, PredictionAccountingEvent)
        assert sell_written.event_type == PredictionEventType.PREDICTION_CLOSE
        assert sell_written.realized_pnl_usd == Decimal("1.00")  # 6.00 - 5.00


# ──────────────────────────────────────────────────────────────────────────────
# CodeRabbit thread 3 (round 2): empty position_key short-circuits to UNAVAILABLE
# without touching the basis store.
#
# Pre-fix the handler would fall through to record_prediction_buy /
# match_prediction_sell with ``position_key=""`` — every malformed event would
# collapse into the same shared empty-key aggregate and a future SELL keyed
# the same way would book bogus realized PnL against the polluted bucket.
# ──────────────────────────────────────────────────────────────────────────────
def _make_basis_spy() -> MagicMock:
    """A FIFOBasisStore mock that records every method call so we can prove
    the basis store was NOT touched on the empty-position-key path."""
    spy = MagicMock(spec=FIFOBasisStore)
    # Default returns chosen so an accidental call wouldn't crash the test —
    # the assertion that matters is ``not_called`` on the mutating methods.
    spy.get_prediction_position.return_value = None
    spy.record_prediction_buy.return_value = (Decimal("0"), Decimal("0"), True)
    spy.match_prediction_sell.return_value = (None, Decimal("0"), Decimal("0"), True)
    return spy


def _outbox_with_no_key(intent_type: str) -> dict[str, Any]:
    """Outbox row missing position_key AND with empty market_id + outcome.
    Wallet is also empty so _build_position_key cannot derive a key from
    the (market_id, outcome, wallet) trio either."""
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": str(uuid.uuid4()),
        "deployment_id": _DEPLOYMENT_ID,
        "strategy_id": _STRATEGY_ID,
        "cycle_id": _CYCLE_ID,
        "intent_type": intent_type,
        "wallet_address": "",  # missing wallet
        "position_key": "",  # missing position_key
        "market_id": "",  # missing market_id
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _ledger_no_market(intent_type: str, extracted: dict[str, Any]) -> dict[str, Any]:
    """Ledger row whose extracted_data has NO market_id either, so the handler's
    market_id resolution falls all the way through to ""."""
    payload = dict(extracted)
    payload.pop("market_id", None)  # no market_id anywhere
    return _make_ledger_row(intent_type, extracted_data_json=_extracted_data_json(payload))


class TestEmptyPositionKeyShortCircuit:
    """Empty position_key on BUY / SELL / REDEEM must emit an UNAVAILABLE event
    and never touch the basis store (otherwise unrelated malformed events
    would collapse into the same empty-key aggregate and corrupt PnL)."""

    def test_buy_with_empty_position_key_returns_unavailable(self) -> None:
        spy = _make_basis_spy()
        outbox = _outbox_with_no_key("PREDICTION_BUY")
        ledger = _ledger_no_market(
            "PREDICTION_BUY",
            {"outcome_tokens_received": "5", "cost_basis": "2.50"},
        )

        ev = handle_prediction(outbox, ledger, spy)

        assert ev is not None
        assert ev.position_key == ""
        assert ev.confidence == AccountingConfidence.UNAVAILABLE
        assert "missing position_key" in ev.unavailable_reason
        assert ev.event_type == PredictionEventType.PREDICTION_OPEN
        assert ev.realized_pnl_usd is None
        # CRITICAL: the basis store was never touched on the empty-key path.
        spy.record_prediction_buy.assert_not_called()
        spy.match_prediction_sell.assert_not_called()
        spy.get_prediction_position.assert_not_called()

    def test_sell_with_empty_position_key_returns_unavailable(self) -> None:
        spy = _make_basis_spy()
        outbox = _outbox_with_no_key("PREDICTION_SELL")
        ledger = _ledger_no_market(
            "PREDICTION_SELL",
            {"outcome_tokens_sold": "3", "proceeds": "1.65"},
        )

        ev = handle_prediction(outbox, ledger, spy)

        assert ev is not None
        assert ev.position_key == ""
        assert ev.confidence == AccountingConfidence.UNAVAILABLE
        assert "missing position_key" in ev.unavailable_reason
        # Disposal short-circuit -> PREDICTION_CLOSE on SELL, never REDUCE.
        assert ev.event_type == PredictionEventType.PREDICTION_CLOSE
        assert ev.realized_pnl_usd is None
        # Basis store untouched — the lookup-then-mutate path was avoided
        # entirely so an unrelated SELL cannot consume basis from a bucket
        # populated by an unrelated BUY.
        spy.get_prediction_position.assert_not_called()
        spy.match_prediction_sell.assert_not_called()
        spy.record_prediction_buy.assert_not_called()

    def test_redeem_with_empty_position_key_returns_unavailable(self) -> None:
        spy = _make_basis_spy()
        outbox = _outbox_with_no_key("PREDICTION_REDEEM")
        ledger = _ledger_no_market(
            "PREDICTION_REDEEM",
            {"redemption_amount": "10", "payout": "10.00"},
        )

        ev = handle_prediction(outbox, ledger, spy)

        assert ev is not None
        assert ev.position_key == ""
        assert ev.confidence == AccountingConfidence.UNAVAILABLE
        assert "missing position_key" in ev.unavailable_reason
        assert ev.event_type == PredictionEventType.PREDICTION_REDEEM
        assert ev.realized_pnl_usd is None
        spy.get_prediction_position.assert_not_called()
        spy.match_prediction_sell.assert_not_called()
        spy.record_prediction_buy.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# CodeRabbit thread 4 (round 2): negative gross proceeds/payout never reach
# match_prediction_sell.
#
# Pre-fix a malformed enrichment payload sending ``proceeds=-5`` would flow
# straight into match_prediction_sell and book a synthetic loss against the
# live aggregate. The BUY branch already clamps negative ``cost_basis`` to
# UNAVAILABLE; SELL/REDEEM now mirrors that contract — gross proceeds < 0
# emits an UNAVAILABLE event, leaves the aggregate intact, and never consumes
# basis.
# ──────────────────────────────────────────────────────────────────────────────


class TestNegativeProceedsRejected:
    def test_sell_with_negative_proceeds_short_circuits_without_consuming_basis(self) -> None:
        # Real basis store with prior position so we can prove the aggregate
        # is left intact.
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)
        prior_size, prior_basis = basis.get_prediction_position(_DEPLOYMENT_ID, _position_key())
        assert (prior_size, prior_basis) == (Decimal("10"), Decimal("5.00"))

        # Now spy on match_prediction_sell to assert it is NEVER called.
        # Use the spy alongside the real store by calling the handler with the
        # spy and pre-seeding the spy's get_prediction_position response so the
        # ``prior is None`` short-circuit doesn't hide the real concern.
        spy = MagicMock(spec=FIFOBasisStore)
        spy.get_prediction_position.return_value = (prior_size, prior_basis)
        spy.match_prediction_sell.return_value = (
            None,
            Decimal("0"),
            Decimal("0"),
            True,
        )
        spy.record_prediction_buy.return_value = (Decimal("0"), Decimal("0"), True)

        sell_outbox, sell_ledger = _sell_event(shares="5", proceeds="-5.0")
        ev = handle_prediction(sell_outbox, sell_ledger, spy)

        assert ev is not None
        assert ev.confidence == AccountingConfidence.UNAVAILABLE
        assert ev.realized_pnl_usd is None
        assert "invalid" in ev.unavailable_reason or "proceeds" in ev.unavailable_reason
        # The aggregate snapshot on the event is the prior, untouched aggregate.
        assert ev.position_size_after == prior_size
        assert ev.position_basis_after == prior_basis
        # CRITICAL: match_prediction_sell was never invoked — the negative
        # proceeds did not reach the basis store mutation path.
        spy.match_prediction_sell.assert_not_called()

    def test_redeem_with_negative_payout_short_circuits_without_consuming_basis(self) -> None:
        spy = MagicMock(spec=FIFOBasisStore)
        spy.get_prediction_position.return_value = (Decimal("10"), Decimal("4.00"))
        spy.match_prediction_sell.return_value = (
            None,
            Decimal("0"),
            Decimal("0"),
            True,
        )
        spy.record_prediction_buy.return_value = (Decimal("0"), Decimal("0"), True)

        red_outbox, red_ledger = _redeem_event(shares="10", payout="-2.0")
        ev = handle_prediction(red_outbox, red_ledger, spy)

        assert ev is not None
        assert ev.confidence == AccountingConfidence.UNAVAILABLE
        assert ev.realized_pnl_usd is None
        assert ev.event_type == PredictionEventType.PREDICTION_REDEEM
        # Prior aggregate preserved on the event payload.
        assert ev.position_size_after == Decimal("10")
        assert ev.position_basis_after == Decimal("4.00")
        # CRITICAL: match_prediction_sell never reached.
        spy.match_prediction_sell.assert_not_called()

    def test_sell_with_zero_proceeds_still_processes_normally(self) -> None:
        """Regression guard: zero proceeds is a real (worthless) sale — it
        must still consume basis and book the realized loss. Only NEGATIVE
        gross is rejected."""
        basis = FIFOBasisStore()
        handle_prediction(*_buy_event(shares="10", cost_basis="5.00"), basis)
        sell_outbox, sell_ledger = _sell_event(shares="10", proceeds="0")
        ev = handle_prediction(sell_outbox, sell_ledger, basis)

        assert ev is not None
        assert ev.event_type == PredictionEventType.PREDICTION_CLOSE
        # Basis was 5.00, gross proceeds 0 -> realized = -5.00.
        assert ev.realized_pnl_usd == Decimal("-5.00")
        assert ev.confidence == AccountingConfidence.HIGH
