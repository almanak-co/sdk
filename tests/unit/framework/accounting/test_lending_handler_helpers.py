"""Unit tests for the lending_handler helpers extracted from handle_lending.

Covers each helper directly plus end-to-end branches that had no coverage:
unmeasurable amounts, absent basis store, empty wallet pool key, the
bounded-interest WITHDRAW fallback, and partial/invalid state snapshots.
No live DB, no gateway, no network.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.lending_handler import (
    _apply_fifo_lots,
    _confidence_for_post_state,
    _extract_snapshots,
    _FifoContext,
    _parse_gas_usd,
    _resolve_asset_with_fallback,
    _resolve_position_key_with_fallback,
    _resolve_row_ids,
    _resolve_timestamp,
    _snapshot_from_state,
    _split_withdraw_deltas,
    _swap_wallet_key_for,
    handle_lending,
)
from almanak.framework.accounting.models import AccountingConfidence


def _make_outbox_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    wallet_address: str = "0xwallet",
    position_key: str = "lending:arbitrum:aave_v3:0xwallet:usdc",
    market_id: str = "",
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": market_id,
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _make_ledger_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    extracted_data_json: str = "",
    price_inputs_json: str = "",
    tx_hash: str = "0xdeadbeef",
    token_in: str = "USDC",
    **overrides: Any,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": ledger_entry_id,
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": "100",
        "token_out": "",
        "amount_out": "",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": tx_hash,
        "chain": "arbitrum",
        "protocol": "aave_v3",
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": "",
    }
    row.update(overrides)
    return row


def _mock_resolver(decimals: int = 6) -> MagicMock:
    token_info = MagicMock()
    token_info.decimals = decimals
    resolver = MagicMock()
    resolver.resolve.return_value = token_info
    return resolver


def _usdc_price_json() -> str:
    return json.dumps({"USDC": "1.0"})


# ──────────────────────────────────────────────────────────────────────────────
# _resolve_timestamp
# ──────────────────────────────────────────────────────────────────────────────


class TestResolveTimestamp:
    def test_valid_iso_passthrough(self) -> None:
        ts = _resolve_timestamp("2026-05-01T12:00:00+00:00")
        assert ts == datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)

    def test_z_suffix_normalised(self) -> None:
        ts = _resolve_timestamp("2026-05-01T12:00:00Z")
        assert ts == datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)

    def test_missing_timestamp_falls_back_to_now(self) -> None:
        before = datetime.now(UTC)
        ts = _resolve_timestamp(None)
        assert before <= ts <= datetime.now(UTC)

    def test_invalid_string_falls_back_to_now(self) -> None:
        before = datetime.now(UTC)
        ts = _resolve_timestamp("not-a-timestamp")
        assert before <= ts <= datetime.now(UTC)


# ──────────────────────────────────────────────────────────────────────────────
# _parse_gas_usd
# ──────────────────────────────────────────────────────────────────────────────


class TestParseGasUsd:
    def test_valid_gas(self) -> None:
        assert _parse_gas_usd({"gas_usd": "0.42"}) == Decimal("0.42")

    def test_missing_gas_is_none(self) -> None:
        assert _parse_gas_usd({}) is None

    def test_invalid_gas_is_none_not_raise(self) -> None:
        assert _parse_gas_usd({"gas_usd": "n/a"}) is None


# ──────────────────────────────────────────────────────────────────────────────
# _resolve_asset_with_fallback / _resolve_position_key_with_fallback
# ──────────────────────────────────────────────────────────────────────────────


class TestResolveAssetAndPositionKey:
    def test_extracted_token_wins(self) -> None:
        assert _resolve_asset_with_fallback({"borrow_token": "WETH"}, {"token_in": "USDC"}) == "WETH"

    def test_token_in_fallback_uppercased(self) -> None:
        assert _resolve_asset_with_fallback({}, {"token_in": "usdc"}) == "USDC"

    def test_unknown_when_nothing_known(self) -> None:
        assert _resolve_asset_with_fallback({}, {"token_in": ""}) == "UNKNOWN"

    def test_existing_position_key_kept(self) -> None:
        outbox = {"position_key": "pk-keep", "market_id": "m1"}
        assert (
            _resolve_position_key_with_fallback(outbox, "aave_v3", "arbitrum", "0xw", "USDC", "pk-keep")
            == "pk-keep"
        )

    def test_empty_position_key_derived_with_market(self) -> None:
        outbox = {"position_key": "", "market_id": "Market123"}
        key = _resolve_position_key_with_fallback(outbox, "aave_v3", "arbitrum", "0xWallet", "USDC", "")
        assert key.startswith("lending:arbitrum:aave_v3:0xwallet:")
        assert "market123" in key
        assert key.endswith(":usdc")


# ──────────────────────────────────────────────────────────────────────────────
# _swap_wallet_key_for
# ──────────────────────────────────────────────────────────────────────────────


class TestSwapWalletKey:
    def test_canonical_key(self) -> None:
        assert _swap_wallet_key_for("Arbitrum", "0xWallet") == "swap:arbitrum:0xwallet"

    def test_missing_chain_or_wallet_is_empty(self) -> None:
        assert _swap_wallet_key_for("", "0xwallet") == ""
        assert _swap_wallet_key_for("arbitrum", "") == ""
        assert _swap_wallet_key_for("  ", "  ") == ""


# ──────────────────────────────────────────────────────────────────────────────
# _resolve_row_ids
# ──────────────────────────────────────────────────────────────────────────────


class TestResolveRowIds:
    def test_ledger_first_outbox_fallback(self) -> None:
        ledger = _make_ledger_row("led-1")
        ledger["deployment_id"] = ""
        outbox = _make_outbox_row("led-1")
        outbox["deployment_id"] = "dep-fallback"
        ids = _resolve_row_ids(ledger, outbox)
        assert ids.deployment_id == "dep-fallback"
        assert ids.cycle_id == "cycle-1"
        assert ids.chain == "arbitrum"

    def test_invalid_execution_mode_raises_before_fifo(self) -> None:
        ledger = _make_ledger_row("led-1", execution_mode="livve")
        outbox = _make_outbox_row("led-1")
        with pytest.raises(ValueError, match="invalid run mode"):
            _resolve_row_ids(ledger, outbox)


# ──────────────────────────────────────────────────────────────────────────────
# _apply_fifo_lots no-op lanes
# ──────────────────────────────────────────────────────────────────────────────


def _fifo_ctx(**overrides: Any) -> _FifoContext:
    kwargs: dict[str, Any] = {
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "position_key": "lending:arbitrum:aave_v3:0xwallet:usdc",
        "asset": "USDC",
        "amount_human": Decimal("100"),
        "price_oracle": {"USDC": Decimal("1.0")},
        "timestamp": datetime.now(UTC),
        "tx_hash": "0xabc",
        "ledger_entry_id": "led-1",
        "swap_wallet_key": "swap:arbitrum:0xwallet",
    }
    kwargs.update(overrides)
    return _FifoContext(**kwargs)


class TestApplyFifoLotsNoop:
    def test_none_ctx_yields_empty_deltas(self) -> None:
        deltas = _apply_fifo_lots("BORROW", None, FIFOBasisStore())
        assert deltas.principal_delta_usd is None
        assert deltas.interest_delta_usd is None

    def test_none_basis_store_yields_empty_deltas(self) -> None:
        store = MagicMock(spec=FIFOBasisStore)
        deltas = _apply_fifo_lots("BORROW", _fifo_ctx(), None)
        assert deltas.principal_delta_usd is None
        assert deltas.interest_delta_usd is None
        store.record_borrow.assert_not_called()

    def test_unknown_intent_yields_empty_deltas(self) -> None:
        store = MagicMock(spec=FIFOBasisStore)
        deltas = _apply_fifo_lots("SWAP", _fifo_ctx(), store)
        assert deltas.principal_delta_usd is None
        assert deltas.interest_delta_usd is None
        store.record_borrow.assert_not_called()
        store.match_repay.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# _snapshot_from_state / _extract_snapshots / _confidence_for_post_state
# ──────────────────────────────────────────────────────────────────────────────


class TestSnapshots:
    def test_none_state_yields_all_none(self) -> None:
        assert _snapshot_from_state(None, log_msg="t") == (None, None, None, None)

    def test_invalid_field_keeps_earlier_fields(self) -> None:
        # Mirrors the pre-refactor single-try blocks: collateral parsed before
        # the failing debt field keeps its value; later fields stay None.
        collateral, debt, net, hf = _snapshot_from_state(
            {"collateral_usd": "500", "debt_usd": "bogus", "health_factor": "2.0"},
            log_msg="t",
        )
        assert collateral == Decimal("500")
        assert debt is None
        assert net is None
        assert hf is None

    def test_partial_state_does_not_fabricate_net_equity(self) -> None:
        collateral, debt, net, hf = _snapshot_from_state({"collateral_usd": "500"}, log_msg="t")
        assert collateral == Decimal("500")
        assert debt is None
        assert net is None
        assert hf is None

    def test_liquidation_threshold_scaled(self) -> None:
        snapshots = _extract_snapshots(None, {"collateral_usd": "1", "liquidation_threshold_bps": 8000})
        assert snapshots.liquidation_threshold == Decimal("0.8")

    def test_invalid_liquidation_threshold_keeps_snapshot(self) -> None:
        snapshots = _extract_snapshots(
            None, {"collateral_usd": "100", "health_factor": "2.0", "liquidation_threshold_bps": "bogus"}
        )
        assert snapshots.collateral_after == Decimal("100")
        assert snapshots.hf_after == Decimal("2.0")
        assert snapshots.liquidation_threshold is None

    def test_confidence_high_on_collateral_only(self) -> None:
        snapshots = _extract_snapshots(None, {"collateral_usd": "100"})
        confidence, reason = _confidence_for_post_state(snapshots)
        assert confidence == AccountingConfidence.HIGH
        assert reason == ""

    def test_confidence_estimated_when_empty(self) -> None:
        snapshots = _extract_snapshots(None, None)
        confidence, reason = _confidence_for_post_state(snapshots)
        assert confidence == AccountingConfidence.ESTIMATED
        assert "post_state_json" in reason


# ──────────────────────────────────────────────────────────────────────────────
# _split_withdraw_deltas bounded-interest guard
# ──────────────────────────────────────────────────────────────────────────────


class TestSplitWithdrawDeltas:
    def test_trustworthy_split(self) -> None:
        match = MagicMock()
        match.unmatched_amount = Decimal("0")
        match.repaid_principal = Decimal("100")
        match.interest_or_yield = Decimal("0.5")
        ctx = _fifo_ctx(amount_human=Decimal("100.5"))
        deltas = _split_withdraw_deltas(ctx, match, Decimal("100.5"))
        assert deltas.principal_delta_usd == Decimal("100")
        assert deltas.interest_delta_usd == Decimal("0.5")

    def test_implausible_interest_falls_back_to_total(self) -> None:
        match = MagicMock()
        match.unmatched_amount = Decimal("0")
        match.repaid_principal = Decimal("100")
        match.interest_or_yield = Decimal("150")
        ctx = _fifo_ctx(amount_human=Decimal("250"))
        deltas = _split_withdraw_deltas(ctx, match, Decimal("250"))
        assert deltas.principal_delta_usd == Decimal("250")
        assert deltas.interest_delta_usd is None


# ──────────────────────────────────────────────────────────────────────────────
# End-to-end uncovered branches through handle_lending
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingUncoveredBranches:
    def test_none_basis_store_yields_event_without_deltas(self) -> None:
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="BORROW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="BORROW",
            extracted_data_json=json.dumps({"borrow_amount": 100_000_000}),
            price_inputs_json=_usdc_price_json(),
        )
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, None)
        assert event is not None
        assert event.amount_token == Decimal("100")
        assert event.principal_delta_usd is None
        assert event.interest_delta_usd is None

    def test_unresolvable_asset_yields_none_amount(self) -> None:
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=json.dumps({"supply_amount": 100_000_000}),
            price_inputs_json=_usdc_price_json(),
        )
        resolver = _mock_resolver(6)
        resolver.resolve.return_value = None
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=resolver):
            event = handle_lending(outbox, ledger, FIFOBasisStore())
        assert event is not None
        assert event.amount_token is None
        assert event.principal_delta_usd is None
        assert event.interest_delta_usd is None

    def test_empty_wallet_still_records_borrow_lot(self) -> None:
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="BORROW", wallet_address="")
        ledger = _make_ledger_row(
            led_id,
            intent_type="BORROW",
            extracted_data_json=json.dumps({"borrow_amount": 100_000_000}),
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)
        assert event is not None
        assert event.principal_delta_usd == Decimal("100")
        assert not [k for k in basis._lots if ":swap:" in k]

    def test_withdraw_large_unbounded_interest_falls_back(self) -> None:
        supply_id = str(uuid.uuid4())
        pk = "lending:arbitrum:aave_v3:0xwallet:usdc"
        supply_outbox = _make_outbox_row(supply_id, intent_type="SUPPLY", position_key=pk)
        supply_ledger = _make_ledger_row(
            supply_id,
            intent_type="SUPPLY",
            extracted_data_json=json.dumps({"supply_amount": 100_000_000}),
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(supply_outbox, supply_ledger, basis)

        withdraw_id = str(uuid.uuid4())
        withdraw_outbox = _make_outbox_row(withdraw_id, intent_type="WITHDRAW", position_key=pk)
        withdraw_ledger = _make_ledger_row(
            withdraw_id,
            intent_type="WITHDRAW",
            extracted_data_json=json.dumps({"withdraw_amount": 250_000_000}),
            price_inputs_json=_usdc_price_json(),
        )
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(withdraw_outbox, withdraw_ledger, basis)
        assert event is not None
        assert event.interest_delta_usd is None
        assert event.principal_delta_usd == Decimal("250")

    def test_invalid_gas_string_yields_none_gas(self) -> None:
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=json.dumps({"supply_amount": 100_000_000}),
            price_inputs_json=_usdc_price_json(),
            gas_usd="not-a-number",
        )
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, FIFOBasisStore())
        assert event is not None
        assert event.gas_usd is None

    def test_position_key_derived_when_outbox_empty(self) -> None:
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY", position_key="", market_id="MarketXYZ")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=json.dumps({"supply_amount": 100_000_000}),
            price_inputs_json=_usdc_price_json(),
        )
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, FIFOBasisStore())
        assert event is not None
        assert "marketxyz" in event.position_key
