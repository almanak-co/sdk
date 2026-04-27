"""Tests for FIFOBasisStore.reconstruct_from_events (VIB-3484, VIB-3468).

Covers:
- BORROW → REPAY reconstruction across a simulated restart
- PT_BUY → PT_REDEEM reconstruction
- PT_BUY → PT_SELL (partial) → PT_REDEEM reconstruction
- Idempotency: calling reconstruct_from_events twice gives same result
- Malformed / unknown event types are skipped without error
- amount_token field round-trips through LendingAccountingEvent payload
- source_ledger_entry_id stored on lots and propagated during reconstruction
- Policy v1 events (missing amount_token) log WARNING and are skipped
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.models import (
    AccountingConfidence,
    LendingAccountingEvent,
    LendingEventType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DECIMALS_18 = Decimal(10**18)


def _lending_row(
    event_type: str,
    deployment_id: str,
    position_key: str,
    asset: str,
    amount_token: Decimal,
    timestamp: str = "2026-04-27T10:00:00+00:00",
) -> dict:
    payload = {
        "event_type": event_type,
        "position_key": position_key,
        "market_id": "test-market",
        "asset": asset,
        "amount_token": str(amount_token),
        "confidence": "HIGH",
        "unavailable_reason": "",
        "schema_version": 1,
        # fill required nullable fields
        "collateral_value_before_usd": None,
        "collateral_value_after_usd": None,
        "debt_value_before_usd": None,
        "debt_value_after_usd": None,
        "net_equity_before_usd": None,
        "net_equity_after_usd": None,
        "health_factor_before": None,
        "health_factor_after": None,
        "liquidation_threshold": None,
        "lltv": None,
        "supply_apr_bps": None,
        "borrow_apr_bps": None,
        "principal_delta_usd": None,
        "interest_delta_usd": None,
        "gas_usd": None,
    }
    return {
        "event_type": event_type,
        "deployment_id": deployment_id,
        "position_key": position_key,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


def _pt_row(
    event_type: str,
    deployment_id: str,
    position_key: str,
    pt_token: str,
    pt_amount_human: Decimal,
    sy_amount_human: Decimal,
    timestamp: str = "2026-04-27T10:00:00+00:00",
) -> dict:
    payload = {
        "event_type": event_type,
        "position_key": position_key,
        "market_id": "pendle-market",
        "pt_token": pt_token,
        # stored as raw integers (18-decimal scale)
        "pt_amount": str(int(pt_amount_human * _DECIMALS_18)),
        "sy_amount": str(int(sy_amount_human * _DECIMALS_18)),
        "confidence": "HIGH",
        "unavailable_reason": "",
        "schema_version": 1,
        "implied_apr_bps": None,
        "days_to_maturity": None,
        "realized_yield_usd": None,
        "basis_lot_id": None,
        "maturity_timestamp": None,
        "pt_price": None,
    }
    return {
        "event_type": event_type,
        "deployment_id": deployment_id,
        "position_key": position_key,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


# ---------------------------------------------------------------------------
# Tests: lending BORROW / REPAY
# ---------------------------------------------------------------------------


class TestLendingReconstruction:
    def test_borrow_repay_interest_correct_after_restart(self):
        dep = "dep-1"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _lending_row("BORROW", dep, pk, "USDC", Decimal("1000")),
            # simulate restart — store cleared by reconstruct
        ]
        store = FIFOBasisStore()
        # Simulate first session: BORROW recorded but store lost on restart
        # Reconstruct from durable events
        replayed = store.reconstruct_from_events(events)
        assert replayed == 1

        # REPAY 1004.20 USDC — should compute interest = 4.20
        result = store.match_repay(dep, pk, "USDC", Decimal("1004.20"))
        assert result.unmatched_amount == Decimal("0")
        assert result.repaid_principal == Decimal("1000")
        assert result.interest_or_yield == pytest.approx(Decimal("4.20"), abs=Decimal("0.001"))

    def test_repay_without_borrow_still_unmatched_after_reconstruct(self):
        store = FIFOBasisStore()
        store.reconstruct_from_events([])  # no events
        result = store.match_repay("dep", "pk", "USDC", Decimal("500"))
        assert result.unmatched_amount == Decimal("500")
        assert result.interest_or_yield == Decimal("0")

    def test_partial_repay_leaves_remaining_lot(self):
        dep = "dep-2"
        pk = "lending:arb:aave_v3:0xwallet:WETH"
        events = [
            _lending_row("BORROW", dep, pk, "WETH", Decimal("1.0")),
            _lending_row("REPAY", dep, pk, "WETH", Decimal("0.4"),
                         timestamp="2026-04-27T11:00:00+00:00"),
        ]
        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        # Remaining principal in the lot should be 0.6 WETH
        result = store.match_repay(dep, pk, "WETH", Decimal("0.6"))
        assert result.repaid_principal == pytest.approx(Decimal("0.6"), abs=Decimal("0.0001"))
        assert result.interest_or_yield == Decimal("0")

    def test_idempotent_reconstruction(self):
        dep = "dep-3"
        pk = "lending:arb:aave_v3:0xwallet:DAI"
        events = [_lending_row("BORROW", dep, pk, "DAI", Decimal("500"))]
        store = FIFOBasisStore()
        store.reconstruct_from_events(events)
        store.reconstruct_from_events(events)  # second call resets and replays
        result = store.match_repay(dep, pk, "DAI", Decimal("502"))
        assert result.repaid_principal == Decimal("500")
        assert result.interest_or_yield == pytest.approx(Decimal("2"), abs=Decimal("0.001"))

    def test_malformed_row_skipped(self):
        events = [
            {"event_type": "BORROW", "deployment_id": "d", "position_key": "p",
             "timestamp": "bad", "payload_json": "{not valid json"}
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 0

    def test_unknown_event_type_skipped(self):
        dep = "dep-x"
        pk = "pk-x"
        events = [
            _lending_row("FUTURE_EVENT_TYPE_V99", dep, pk, "USDC", Decimal("100")),
            _lending_row("BORROW", dep, pk, "USDC", Decimal("200")),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 1  # only BORROW counted


# ---------------------------------------------------------------------------
# Tests: Pendle PT_BUY / PT_SELL / PT_REDEEM
# ---------------------------------------------------------------------------


class TestPendleReconstruction:
    def test_pt_buy_redeem_yield_correct_after_restart(self):
        dep = "dep-pendle"
        pk = "pendle:arb:pendle:0xwallet"
        pt_token = "0xPT_wstETH_25JUN2026"
        events = [
            _pt_row("PT_BUY", dep, pk, pt_token,
                    pt_amount_human=Decimal("1000"),
                    sy_amount_human=Decimal("950")),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 1

        result = store.match_pt_redeem(dep, pk, pt_token,
                                       pt_redeemed=Decimal("1000"),
                                       sy_received=Decimal("1000"))
        assert result.repaid_principal == pytest.approx(Decimal("950"), abs=Decimal("0.01"))
        assert result.interest_or_yield == pytest.approx(Decimal("50"), abs=Decimal("0.01"))

    def test_pt_sell_reduces_lot_before_redeem(self):
        dep = "dep-pendle-2"
        pk = "pendle:arb:pendle:0xwallet2"
        pt_token = "0xPT_USDC_30SEP2026"
        events = [
            _pt_row("PT_BUY", dep, pk, pt_token,
                    pt_amount_human=Decimal("1000"), sy_amount_human=Decimal("920")),
            _pt_row("PT_SELL", dep, pk, pt_token,
                    pt_amount_human=Decimal("400"), sy_amount_human=Decimal("400"),
                    timestamp="2026-04-27T12:00:00+00:00"),
        ]
        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        # Only 600 PT remain; redeem 600 — original cost prorated 920 * 600/1000 = 552
        result = store.match_pt_redeem(dep, pk, pt_token,
                                       pt_redeemed=Decimal("600"),
                                       sy_received=Decimal("600"))
        assert result.repaid_principal == pytest.approx(Decimal("552"), abs=Decimal("1"))
        assert result.interest_or_yield == pytest.approx(Decimal("48"), abs=Decimal("1"))


# ---------------------------------------------------------------------------
# Tests: amount_token round-trip through LendingAccountingEvent payload
# ---------------------------------------------------------------------------


class TestAmountTokenPayloadRoundTrip:
    def _make_identity(self):
        from datetime import UTC, datetime

        from almanak.framework.accounting.models import AccountingIdentity

        return AccountingIdentity(
            id="test-id",
            deployment_id="dep",
            strategy_id="strat",
            cycle_id="cycle",
            execution_mode="live",
            timestamp=datetime(2026, 4, 27, tzinfo=UTC),
            chain="arbitrum",
            protocol="aave_v3",
            wallet_address="0xwallet",
            tx_hash="0xtx",
            ledger_entry_id="ledger-1",
        )

    def test_amount_token_serialized_and_deserialized(self):
        from almanak.framework.accounting.models import AccountingIdentity, LendingAccountingEvent

        identity = self._make_identity()
        event = LendingAccountingEvent(
            identity=identity,
            event_type=LendingEventType.BORROW,
            position_key="pk",
            market_id="m",
            asset="USDC",
            collateral_value_before_usd=None,
            collateral_value_after_usd=None,
            debt_value_before_usd=None,
            debt_value_after_usd=None,
            net_equity_before_usd=None,
            net_equity_after_usd=None,
            health_factor_before=None,
            health_factor_after=None,
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=500,
            principal_delta_usd=Decimal("1000"),
            interest_delta_usd=None,
            gas_usd=Decimal("2.50"),
            amount_token=Decimal("1000.5"),
            confidence=AccountingConfidence.HIGH,
        )
        payload = event.to_payload_json()
        parsed = json.loads(payload)
        assert parsed["amount_token"] == "1000.5"

        # Round-trip via from_payload_json
        restored = LendingAccountingEvent.from_payload_json(identity, payload)
        assert restored.amount_token == Decimal("1000.5")

    def test_amount_token_none_round_trips(self):
        from almanak.framework.accounting.models import AccountingIdentity, LendingAccountingEvent

        identity = self._make_identity()
        event = LendingAccountingEvent(
            identity=identity,
            event_type=LendingEventType.SUPPLY,
            position_key="pk",
            market_id="m",
            asset="WETH",
            collateral_value_before_usd=None,
            collateral_value_after_usd=None,
            debt_value_before_usd=None,
            debt_value_after_usd=None,
            net_equity_before_usd=None,
            net_equity_after_usd=None,
            health_factor_before=None,
            health_factor_after=None,
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=None,
            principal_delta_usd=Decimal("3000"),
            interest_delta_usd=None,
            gas_usd=None,
            amount_token=None,
            confidence=AccountingConfidence.ESTIMATED,
        )
        payload = event.to_payload_json()
        parsed = json.loads(payload)
        assert parsed["amount_token"] is None

        restored = LendingAccountingEvent.from_payload_json(identity, payload)
        assert restored.amount_token is None

    def test_old_payload_without_amount_token_deserializes_as_none(self):
        """Payloads written before VIB-3484 lack amount_token — must not crash."""
        from datetime import UTC, datetime

        from almanak.framework.accounting.models import AccountingIdentity, LendingAccountingEvent

        identity = AccountingIdentity(
            id="old-id", deployment_id="d", strategy_id="s", cycle_id="c",
            execution_mode="live", timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            chain="arbitrum", protocol="aave_v3", wallet_address="0xw",
            tx_hash="0xt", ledger_entry_id="l",
        )
        old_payload = json.dumps({
            "event_type": "BORROW",
            "position_key": "pk",
            "market_id": "m",
            "asset": "USDC",
            "collateral_value_before_usd": None,
            "collateral_value_after_usd": None,
            "debt_value_before_usd": None,
            "debt_value_after_usd": None,
            "net_equity_before_usd": None,
            "net_equity_after_usd": None,
            "health_factor_before": None,
            "health_factor_after": "1.5",
            "liquidation_threshold": None,
            "lltv": None,
            "supply_apr_bps": None,
            "borrow_apr_bps": 300,
            "principal_delta_usd": "1000",
            "interest_delta_usd": None,
            "gas_usd": "1.5",
            # no "amount_token" key
            "confidence": "HIGH",
            "unavailable_reason": "",
            "schema_version": 1,
        })
        event = LendingAccountingEvent.from_payload_json(identity, old_payload)
        assert event.amount_token is None
        assert event.health_factor_after == Decimal("1.5")


# ---------------------------------------------------------------------------
# Tests: source_ledger_entry_id (VIB-3468)
# ---------------------------------------------------------------------------


class TestSourceLedgerEntryId:
    def test_record_borrow_stores_source_ledger_entry_id(self):
        store = FIFOBasisStore()
        store.record_borrow(
            deployment_id="dep",
            position_key="pk",
            token="USDC",
            principal_amount=Decimal("1000"),
            source_ledger_entry_id="ledger-abc-123",
        )
        lots = store._lots["dep:pk:usdc"]
        assert len(lots) == 1
        assert lots[0]["source_ledger_entry_id"] == "ledger-abc-123"

    def test_record_pt_buy_stores_source_ledger_entry_id(self):
        store = FIFOBasisStore()
        store.record_pt_buy(
            deployment_id="dep",
            position_key="pk",
            pt_token="PT-wstETH",
            pt_amount=Decimal("500"),
            sy_cost=Decimal("480"),
            source_ledger_entry_id="ledger-pt-999",
        )
        lots = store._lots["dep:pk:pt-wsteth"]
        assert len(lots) == 1
        assert lots[0]["source_ledger_entry_id"] == "ledger-pt-999"

    def test_source_ledger_entry_id_none_when_not_provided(self):
        store = FIFOBasisStore()
        store.record_borrow(
            deployment_id="dep",
            position_key="pk",
            token="DAI",
            principal_amount=Decimal("100"),
        )
        lots = store._lots["dep:pk:dai"]
        assert lots[0]["source_ledger_entry_id"] is None

    def test_reconstruct_propagates_source_ledger_entry_id(self):
        dep = "dep-recon"
        pk = "lending:eth:aave_v3:0xwallet:USDC"
        row = _lending_row("BORROW", dep, pk, "USDC", Decimal("2000"))
        row["ledger_entry_id"] = "ledger-xyz-456"

        store = FIFOBasisStore()
        store.reconstruct_from_events([row])

        lots = store._lots[f"{dep}:{pk}:usdc"]
        assert len(lots) == 1
        assert lots[0]["source_ledger_entry_id"] == "ledger-xyz-456"

    def test_reconstruct_pt_buy_propagates_source_ledger_entry_id(self):
        dep = "dep-pendle-recon"
        pk = "pendle:arb:pendle:0xwallet"
        pt_token = "0xPT_wstETH_30JUN2028"
        row = _pt_row("PT_BUY", dep, pk, pt_token,
                      pt_amount_human=Decimal("300"),
                      sy_amount_human=Decimal("285"))
        row["ledger_entry_id"] = "ledger-pt-buy-777"

        store = FIFOBasisStore()
        store.reconstruct_from_events([row])

        lots = store._lots[f"{dep}:{pk}:{pt_token.lower()}"]
        assert len(lots) == 1
        assert lots[0]["source_ledger_entry_id"] == "ledger-pt-buy-777"

    def test_empty_ledger_entry_id_coalesces_to_none(self):
        dep = "dep-empty"
        pk = "pk-empty"
        row = _lending_row("BORROW", dep, pk, "USDC", Decimal("500"))
        row["ledger_entry_id"] = ""  # proto default for missing string field

        store = FIFOBasisStore()
        store.reconstruct_from_events([row])

        lots = store._lots[f"{dep}:{pk}:usdc"]
        assert lots[0]["source_ledger_entry_id"] is None


# ---------------------------------------------------------------------------
# Tests: policy v1 event warning (VIB-3468)
# ---------------------------------------------------------------------------


class TestPolicyV1Warning:
    def _borrow_row_without_amount_token(
        self,
        deployment_id: str,
        position_key: str,
        asset: str,
    ) -> dict:
        """Simulate a pre-VIB-3484 BORROW event payload lacking amount_token."""
        payload = {
            "event_type": "BORROW",
            "position_key": position_key,
            "market_id": "test-market",
            "asset": asset,
            # amount_token intentionally absent — policy v1 fingerprint
            "confidence": "HIGH",
            "unavailable_reason": "",
            "schema_version": 1,
            "collateral_value_before_usd": None,
            "collateral_value_after_usd": None,
            "debt_value_before_usd": None,
            "debt_value_after_usd": None,
            "net_equity_before_usd": None,
            "net_equity_after_usd": None,
            "health_factor_before": None,
            "health_factor_after": None,
            "liquidation_threshold": None,
            "lltv": None,
            "supply_apr_bps": None,
            "borrow_apr_bps": 500,
            "principal_delta_usd": None,
            "interest_delta_usd": None,
            "gas_usd": None,
        }
        return {
            "event_type": "BORROW",
            "deployment_id": deployment_id,
            "position_key": position_key,
            "timestamp": "2025-01-01T00:00:00+00:00",
            "payload_json": json.dumps(payload),
        }

    def test_policy_v1_borrow_logs_warning_and_skips(self, caplog):
        dep = "dep-v1"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        row = self._borrow_row_without_amount_token(dep, pk, "USDC")

        store = FIFOBasisStore()
        import logging

        with caplog.at_level(logging.WARNING, logger="almanak.framework.accounting.basis"):
            replayed = store.reconstruct_from_events([row])

        assert replayed == 0
        # Aggregated summary warning emitted at end of reconstruction
        assert any("policy-v1" in r.message for r in caplog.records)
        assert any("FIFO store may be incomplete" in r.message for r in caplog.records)
        # Store must be empty — the lot was not recorded
        assert store._lots == {}

    def test_policy_v1_borrow_does_not_block_subsequent_v2_events(self, caplog):
        dep = "dep-v1-mixed"
        pk = "lending:arb:aave_v3:0xwallet:WETH"
        events = [
            self._borrow_row_without_amount_token(dep, pk, "WETH"),  # skipped (v1)
            _lending_row("BORROW", dep, pk, "WETH", Decimal("2.0"),
                         timestamp="2026-01-01T00:00:00+00:00"),  # recorded (v2)
        ]

        store = FIFOBasisStore()
        import logging

        with caplog.at_level(logging.WARNING, logger="almanak.framework.accounting.basis"):
            replayed = store.reconstruct_from_events(events)

        assert replayed == 1
        result = store.match_repay(dep, pk, "WETH", Decimal("2.1"))
        assert result.repaid_principal == pytest.approx(Decimal("2.0"), abs=Decimal("0.001"))
        assert result.interest_or_yield == pytest.approx(Decimal("0.1"), abs=Decimal("0.001"))

    def test_policy_v1_repay_logs_warning_and_skips(self, caplog):
        dep = "dep-v1-repay"
        pk = "lending:eth:compound:0xwallet:DAI"
        # First a valid BORROW
        borrow = _lending_row("BORROW", dep, pk, "DAI", Decimal("500"))
        # Then a policy v1 REPAY missing amount_token
        repay_payload = json.dumps({
            "event_type": "REPAY",
            "position_key": pk,
            "market_id": "test",
            "asset": "DAI",
            # amount_token absent
            "confidence": "HIGH",
            "unavailable_reason": "",
            "schema_version": 1,
            "collateral_value_before_usd": None,
            "collateral_value_after_usd": None,
            "debt_value_before_usd": None,
            "debt_value_after_usd": None,
            "net_equity_before_usd": None,
            "net_equity_after_usd": None,
            "health_factor_before": None,
            "health_factor_after": None,
            "liquidation_threshold": None,
            "lltv": None,
            "supply_apr_bps": None,
            "borrow_apr_bps": None,
            "principal_delta_usd": None,
            "interest_delta_usd": None,
            "gas_usd": None,
        })
        repay = {
            "event_type": "REPAY",
            "deployment_id": dep,
            "position_key": pk,
            "timestamp": "2025-06-01T00:00:00+00:00",
            "payload_json": repay_payload,
        }

        store = FIFOBasisStore()
        import logging

        with caplog.at_level(logging.WARNING, logger="almanak.framework.accounting.basis"):
            replayed = store.reconstruct_from_events([borrow, repay])

        # BORROW replayed; REPAY skipped (v1)
        assert replayed == 1
        assert any("policy-v1" in r.message for r in caplog.records)
        # Lot still has full principal since REPAY was skipped
        result = store.match_repay(dep, pk, "DAI", Decimal("502"))
        assert result.repaid_principal == pytest.approx(Decimal("500"), abs=Decimal("0.001"))
