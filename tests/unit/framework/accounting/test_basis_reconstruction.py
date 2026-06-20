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
        # Stored as HUMAN decimals — the uniform PT payload convention
        # (VIB-4988 v4: PT_BUY/PT_SELL/PT_REDEEM all human; replay reads human
        # directly, no /1e18).
        "pt_amount": str(pt_amount_human),
        "sy_amount": str(sy_amount_human),
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
            deployment_id="strat",
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
            id="old-id", deployment_id="s", cycle_id="c",
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


# ---------------------------------------------------------------------------
# VIB-3964 — wallet basis pool: BORROW credits, REPAY debits, the swap-key
# pool. Without this, a SWAP that disposes a borrowed token returns a
# null realized PnL and breaks G6 looping reconciliation.
# ---------------------------------------------------------------------------


def _lending_row_v2(
    event_type: str,
    deployment_id: str,
    position_key: str,
    asset: str,
    amount_token: Decimal,
    amount_usd: Decimal | None = None,
    chain: str = "arbitrum",
    wallet: str = "0xwallet",
    timestamp: str = "2026-04-27T10:00:00+00:00",
) -> dict:
    """Lending row that includes chain + wallet_address top-level fields.

    These two columns are what reconstruct_from_events reads to derive the
    swap-key the BORROW / WITHDRAW credit was minted under (VIB-3964).
    """
    payload: dict = {
        "event_type": event_type,
        "position_key": position_key,
        "market_id": "test-market",
        "asset": asset,
        "amount_token": str(amount_token),
        "amount": str(amount_token),
        "confidence": "HIGH",
        "unavailable_reason": "",
        "schema_version": 1,
    }
    if amount_usd is not None:
        payload["borrowed_amount_usd"] = str(amount_usd)
        payload["amount_usd"] = str(amount_usd)
        payload["principal_delta_usd"] = str(amount_usd)
    return {
        "event_type": event_type,
        "deployment_id": deployment_id,
        "position_key": position_key,
        "chain": chain,
        "wallet_address": wallet,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


class TestWalletBasisRoundTrip:
    """VIB-3964 — BORROW → SWAP → REPAY closes the wallet basis pool cleanly.

    A constant-price round-trip should produce realized PnL of zero (within a
    slippage epsilon). Pre-VIB-3964 this returned None because the SWAP
    disposing the borrowed token had no swap-key acquisition lot to consume.
    """

    def test_borrow_swap_repay_round_trip_basis_closes_to_zero(self):
        dep = "dep-loop-1"
        chain = "arbitrum"
        wallet = "0xwallet"
        lending_pk = f"lending:{chain}:aave_v3:{wallet}:USDT"
        swap_pk = f"swap:{chain}:{wallet}"

        # 1. BORROW 2 USDT @ $1.00 = $2.00 obligation
        borrow_row = _lending_row_v2(
            "BORROW",
            dep,
            lending_pk,
            "USDT",
            Decimal("2"),
            amount_usd=Decimal("2.00"),
            chain=chain,
            wallet=wallet,
        )
        store = FIFOBasisStore()
        store.reconstruct_from_events([borrow_row])

        # The wallet pool now holds the borrowed USDT.
        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep,
            position_key=swap_pk,
            token="USDT",
            amount=Decimal("2"),
        )
        assert unmatched == Decimal("0")
        assert cost_consumed == pytest.approx(Decimal("2.00"), abs=Decimal("0.001"))

        # 2. The lending-key BORROW lot is independent: REPAY of 2 USDT
        # consumes the principal from the lending key (no interest accrued
        # in this constant-price test).
        repay_result = store.match_repay(dep, lending_pk, "USDT", Decimal("2"))
        assert repay_result.unmatched_amount == Decimal("0")
        assert repay_result.repaid_principal == pytest.approx(Decimal("2"), abs=Decimal("0.001"))
        assert repay_result.interest_or_yield == Decimal("0")

    def test_borrow_credits_swap_key_with_basis_for_disposal(self):
        """The minimum unit-level reproduction of the G6 RED path.

        Pre-VIB-3964: match_swap_disposal returned (None, amount) for a
        borrowed token because no swap-key lot existed.
        """
        dep = "dep-min"
        chain = "arbitrum"
        wallet = "0xwallet"
        lending_pk = f"lending:{chain}:aave_v3:{wallet}:USDT"
        swap_pk = f"swap:{chain}:{wallet}"

        store = FIFOBasisStore()
        store.reconstruct_from_events(
            [_lending_row_v2("BORROW", dep, lending_pk, "USDT", Decimal("2"), Decimal("2.00"), chain, wallet)]
        )

        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep,
            position_key=swap_pk,
            token="USDT",
            amount=Decimal("2"),
        )
        assert cost_consumed is not None, "BORROW must credit a swap-key basis lot for disposal"
        assert unmatched == Decimal("0")

    def test_withdraw_credits_swap_key_with_basis(self):
        """A WITHDRAW returns collateral to the wallet — also needs a basis lot."""
        dep = "dep-w"
        chain = "arbitrum"
        wallet = "0xwallet"
        lending_pk = f"lending:{chain}:aave_v3:{wallet}:USDC"
        swap_pk = f"swap:{chain}:{wallet}"

        store = FIFOBasisStore()
        store.reconstruct_from_events(
            [_lending_row_v2("WITHDRAW", dep, lending_pk, "USDC", Decimal("4"), Decimal("4.00"), chain, wallet)]
        )

        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep,
            position_key=swap_pk,
            token="USDC",
            amount=Decimal("4"),
        )
        assert cost_consumed is not None
        assert unmatched == Decimal("0")
        assert cost_consumed == pytest.approx(Decimal("4.00"), abs=Decimal("0.001"))

    def test_supply_drains_swap_key_to_keep_pool_consistent(self):
        """SUPPLY moves a token from the wallet to the lending pool.

        Without disposal, a phantom acquisition lot would survive the SUPPLY
        and poison a later WITHDRAW-then-SWAP attribution.
        """
        dep = "dep-s"
        chain = "arbitrum"
        wallet = "0xwallet"
        lending_pk = f"lending:{chain}:aave_v3:{wallet}:USDC"
        swap_pk = f"swap:{chain}:{wallet}"

        store = FIFOBasisStore()
        # Mint a wallet USDC lot via WITHDRAW, then SUPPLY it back; the pool
        # must end empty.
        store.reconstruct_from_events(
            [
                _lending_row_v2(
                    "WITHDRAW", dep, lending_pk, "USDC", Decimal("4"), Decimal("4.00"),
                    chain, wallet, timestamp="2026-04-27T10:00:00+00:00",
                ),
                _lending_row_v2(
                    "SUPPLY", dep, lending_pk, "USDC", Decimal("4"), Decimal("4.00"),
                    chain, wallet, timestamp="2026-04-27T10:01:00+00:00",
                ),
            ]
        )

        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep,
            position_key=swap_pk,
            token="USDC",
            amount=Decimal("1"),
        )
        # Pool drained by SUPPLY → only the empty lot remains → all 1 USDC
        # disposal is unmatched. The matcher returns ``Decimal("0")`` (not
        # None) for cost_consumed because the lot existed and had a
        # ``cost_usd`` field (just remaining=0); ``_unmatched > 0`` is the
        # signal swap_handler.py uses to leave realized_pnl_usd null.
        assert unmatched == Decimal("1")
        assert cost_consumed == Decimal("0")


# ---------------------------------------------------------------------------
# VIB-4078 — coverage for per-event-type replay helpers (SWAP, PT_REDEEM,
# PREDICTION_*, REPAY non-positive log path, WITHDRAW USD fallback,
# matching_policy_version pinning).
# ---------------------------------------------------------------------------


def _swap_row(
    deployment_id: str,
    position_key: str,
    chain: str,
    wallet: str,
    token_in: str,
    token_out: str,
    amount_in: Decimal,
    amount_out: Decimal,
    amount_out_usd: Decimal | None,
    timestamp: str = "2026-04-27T10:00:00+00:00",
) -> dict:
    payload = {
        "event_type": "SWAP",
        "position_key": position_key,
        "swap_position_key": f"swap:{chain}:{wallet}",
        "token_in": token_in,
        "token_out": token_out,
        "amount_in": str(amount_in),
        "amount_out": str(amount_out),
        "amount_out_usd": None if amount_out_usd is None else str(amount_out_usd),
    }
    return {
        "event_type": "SWAP",
        "deployment_id": deployment_id,
        "position_key": position_key,
        "chain": chain,
        "wallet_address": wallet,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


def _prediction_row(
    event_type: str,
    deployment_id: str,
    position_key: str,
    size_after: Decimal,
    basis_after: Decimal,
    timestamp: str = "2026-04-27T10:00:00+00:00",
) -> dict:
    payload = {
        "event_type": event_type,
        "position_key": position_key,
        "position_size_after": str(size_after),
        "position_basis_after": str(basis_after),
    }
    return {
        "event_type": event_type,
        "deployment_id": deployment_id,
        "position_key": position_key,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


class TestVIB4078ReplayHelpers:
    def test_swap_event_credits_token_out_acquisition_lot(self):
        """SWAP replay must mint a token_out acquisition lot under the swap-key
        so a follow-up disposal computes realized PnL against the correct cost.
        """
        dep = "dep-swap-recon"
        chain = "arbitrum"
        wallet = "0xwallet"
        swap_pk = f"swap:{chain}:{wallet}"
        events = [
            _swap_row(
                dep, swap_pk, chain, wallet,
                token_in="USDT", token_out="USDC",
                amount_in=Decimal("100"), amount_out=Decimal("99.5"),
                amount_out_usd=Decimal("99.50"),
            ),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 1

        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep, position_key=swap_pk, token="USDC", amount=Decimal("99.5"),
        )
        assert unmatched == Decimal("0")
        assert cost_consumed == pytest.approx(Decimal("99.50"), abs=Decimal("0.001"))

    def test_pt_redeem_event_falls_back_to_sy_amount_when_pt_amount_missing(self):
        """PT_REDEEM events written when py_redeemed was missing fall back to
        sy_amount (see build_pendle_pt_redeem_accounting_event). Replay must
        mirror that fallback or PT redemptions written under that path are
        silently dropped on restart.
        """
        dep = "dep-redeem"
        pk = "pendle:arb:pendle:0xwallet"
        pt_token = "0xPT_FALLBACK"
        # Seed an open PT lot so the redeem can match.
        buy = _pt_row("PT_BUY", dep, pk, pt_token,
                      pt_amount_human=Decimal("100"), sy_amount_human=Decimal("95"))
        # PT_REDEEM with pt_amount missing — builder fell back to sy_amount;
        # values stored in human-decimal (NOT 18-decimal), unlike PT_BUY/PT_SELL.
        redeem_payload = json.dumps({
            "event_type": "PT_REDEEM",
            "position_key": pk,
            "pt_token": pt_token,
            "pt_amount": None,
            "sy_amount": "100",
        })
        redeem = {
            "event_type": "PT_REDEEM",
            "deployment_id": dep,
            "position_key": pk,
            "timestamp": "2026-04-27T11:00:00+00:00",
            "payload_json": redeem_payload,
        }
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events([buy, redeem])
        assert replayed == 2
        # Lot fully consumed via sy_amount fallback — remaining_pt should be 0.
        lots = store._lots[f"{dep}:{pk}:{pt_token.lower()}"]
        assert lots[0]["remaining_pt"] == Decimal("0")

    def test_prediction_open_snapshot_replay_assigns_aggregate_directly(self):
        """PREDICTION_* events store post-trade snapshots — replay assigns the
        snapshot row directly (latest event wins for a given key).
        """
        dep = "dep-pred"
        pk = "pred:polymarket:m1:YES"
        events = [
            _prediction_row("PREDICTION_OPEN", dep, pk,
                            size_after=Decimal("1000"), basis_after=Decimal("0.55")),
            _prediction_row("PREDICTION_INCREASE", dep, pk,
                            size_after=Decimal("1500"), basis_after=Decimal("0.83"),
                            timestamp="2026-04-27T11:00:00+00:00"),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 2
        size, basis = store.get_prediction_position(dep, pk)
        # Latest event wins — second snapshot overrides the first.
        assert size == Decimal("1500")
        assert basis == Decimal("0.83")

    def test_repay_with_non_positive_amount_token_logs_debug_and_skips(self, caplog):
        """A REPAY with present-but-zero amount_token is a v2 extraction bug,
        not a v1 schema gap — skipped with DEBUG log, not the v1 WARNING summary.
        """
        import logging

        dep = "dep-zero-repay"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        # Seed a borrow so the position exists.
        borrow = _lending_row("BORROW", dep, pk, "USDC", Decimal("100"))
        # REPAY with amount_token = "0" — present, parsed, non-positive.
        repay = _lending_row("REPAY", dep, pk, "USDC", Decimal("0"),
                             timestamp="2026-04-27T11:00:00+00:00")
        store = FIFOBasisStore()
        with caplog.at_level(logging.DEBUG, logger="almanak.framework.accounting.basis"):
            replayed = store.reconstruct_from_events([borrow, repay])
        # Only BORROW counts — REPAY skipped silently (no v1 summary warning).
        assert replayed == 1
        assert not any("policy-v1" in r.message for r in caplog.records)
        assert any(
            "non-positive amount_token" in r.message and "REPAY" in r.message
            for r in caplog.records
        )

    def test_withdraw_amount_usd_falls_back_to_principal_plus_interest(self):
        """When ``amount_usd`` is absent, replay reconstructs the wallet-basis
        cost from ``principal_delta_usd + interest_delta_usd`` (CodeRabbit
        2026-05-04 — must match the live writer's full-withdraw-USD basis).
        """
        dep = "dep-w-fallback"
        chain = "arbitrum"
        wallet = "0xwallet"
        lending_pk = f"lending:{chain}:aave_v3:{wallet}:USDC"
        swap_pk = f"swap:{chain}:{wallet}"
        # Build a WITHDRAW row WITHOUT amount_usd, but WITH the split fields.
        payload = {
            "event_type": "WITHDRAW",
            "position_key": lending_pk,
            "asset": "USDC",
            "amount_token": "10",
            # amount_usd intentionally absent — exercise the fallback.
            "principal_delta_usd": "9.50",
            "interest_delta_usd": "0.50",
        }
        row = {
            "event_type": "WITHDRAW",
            "deployment_id": dep,
            "position_key": lending_pk,
            "chain": chain,
            "wallet_address": wallet,
            "timestamp": "2026-04-27T10:00:00+00:00",
            "payload_json": json.dumps(payload),
        }
        store = FIFOBasisStore()
        store.reconstruct_from_events([row])

        # Disposing the withdrawn token should consume basis = 9.50 + 0.50 = 10.00.
        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep, position_key=swap_pk, token="USDC", amount=Decimal("10"),
        )
        assert unmatched == Decimal("0")
        assert cost_consumed == pytest.approx(Decimal("10.00"), abs=Decimal("0.001"))

    def test_match_repay_after_reconstruct_pins_matching_policy_version(self):
        """Per CLAUDE.md: matching_policy_version is stamped on every typed
        event. After reconstruction the basis store must continue to surface
        the current MATCHING_POLICY_VERSION on match results so downstream
        consumers can detect a policy change between sessions.
        """
        from almanak.framework.accounting.basis import MATCHING_POLICY_VERSION

        dep = "dep-policy"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [_lending_row("BORROW", dep, pk, "USDC", Decimal("1000"))]
        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        result = store.match_repay(dep, pk, "USDC", Decimal("1004"))
        assert result.matching_policy_version == MATCHING_POLICY_VERSION
        assert result.matching_policy_version >= 3  # bumped by VIB-3964


# ---------------------------------------------------------------------------
# VIB-4487 audit Fold B — retroactive FIFO-key healing on replay
# ---------------------------------------------------------------------------

# Real Base mainnet WETH address (present in the static registry → resolves
# offline at boot via the resolver fast path).
_WETH_BASE = "0x4200000000000000000000000000000000000006"


class TestSwapFifoKeyHealingOnReplay:
    """VIB-4487 audit Fold B: an OLD address-keyed SWAP acquisition payload
    (written by a pre-fix address-emitting connector) must reconcile against
    a NEW symbol-keyed disposal after a runner restart. The replay path
    canonicalizes the persisted token to its symbol on read, so the lot keys
    under the canonical identity and the upgrade transition window vanishes.

    Reuses the module-level ``_swap_row`` helper (signature: deployment_id,
    position_key, chain, wallet, token_in, token_out, amount_in, amount_out,
    amount_out_usd) — ``chain`` is a TOP-LEVEL row column, which is where
    ``_row_context`` reads it for the Fold-B canonicalization.
    """

    def test_old_address_keyed_acquisition_matches_new_symbol_disposal(self):
        dep = "dep-heal"
        chain = "base"
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        swap_pk = f"swap:{chain}:{wallet}"

        events = [
            # OLD acquisition: 1 WETH bought with 2000 USDC, token_out
            # persisted as the raw ADDRESS (pre-VIB-4487 address-emitting
            # connector), cost $2000.
            _swap_row(
                dep, swap_pk, chain, wallet,
                token_in="USDC", token_out=_WETH_BASE,  # address — pre-fix shape
                amount_in=Decimal("2000"), amount_out=Decimal("1"),
                amount_out_usd=Decimal("2000"),
            ),
        ]

        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        # After replay the acquisition lot must be keyed under canonical
        # "WETH". A symbol-keyed disposal of 1 WETH fully matches it.
        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep,
            position_key=swap_pk,
            token="WETH",  # canonical symbol, as a post-fix disposal emits
            amount=Decimal("1"),
        )
        assert unmatched == Decimal("0")  # fully reconciled — no orphan
        assert cost_consumed == Decimal("2000")

    def test_without_healing_address_lot_would_orphan_symbol_disposal(self):
        """Control: prove the heal is doing the work. Replaying the SAME old
        acquisition but with NO chain on the row means the address cannot be
        resolved, so the lot stays address-keyed and a symbol-keyed disposal
        orphans (unmatched). This is exactly the pre-fix corruption — the
        chain column on the row is what enables the heal.
        """
        dep = "dep-heal-control"
        chain = ""  # no chain → resolver no-ops → address-keyed lot
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        # _swap_row builds the payload swap_position_key from chain+wallet, so
        # with an empty chain it is ``swap::<wallet>``; use the SAME key for
        # the disposal so the ONLY thing that can mismatch is the token
        # identity (address-keyed lot vs symbol disposal) — that isolates the
        # heal as the variable under test.
        swap_pk = f"swap:{chain}:{wallet}"

        events = [
            _swap_row(
                dep, swap_pk, chain, wallet,
                token_in="USDC", token_out=_WETH_BASE,
                amount_in=Decimal("2000"), amount_out=Decimal("1"),
                amount_out_usd=Decimal("2000"),
            ),
        ]

        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        cost_consumed, unmatched = store.match_swap_disposal(
            deployment_id=dep,
            position_key=swap_pk,
            token="WETH",
            amount=Decimal("1"),
        )
        # No lot under "WETH" → entire disposal unmatched.
        assert cost_consumed is None
        assert unmatched == Decimal("1")


# ---------------------------------------------------------------------------
# Tests: prediction loaded_extras survives restart (#2146)
# ---------------------------------------------------------------------------


def _prediction_snapshot_row(
    *,
    event_type: str,
    deployment_id: str,
    position_key: str,
    position_size_after: Decimal,
    position_basis_after: Decimal,
    position_loaded_extras_after: Decimal,
    drop_extras_field: bool = False,
    timestamp: str = "2026-04-27T10:00:00+00:00",
) -> dict:
    """Build a reconstruct_from_events row from a serialized PredictionAccountingEvent.

    Routes through the real ``to_payload_json`` so the test exercises model
    serialization + replay deserialization together. ``drop_extras_field``
    simulates a legacy (pre-#2146) payload that never carried the new field.
    """
    from datetime import datetime

    from almanak.framework.accounting.models import (
        AccountingIdentity,
        PredictionAccountingEvent,
        PredictionEventType,
    )

    identity = AccountingIdentity(
        id=f"evt-{event_type}",
        deployment_id=deployment_id,
        cycle_id="c1",
        execution_mode="paper",
        timestamp=datetime.fromisoformat(timestamp),
        chain="polygon",
        protocol="polymarket",
        wallet_address="0xwallet",
        tx_hash="",
        ledger_entry_id="",
    )
    event = PredictionAccountingEvent(
        identity=identity,
        event_type=PredictionEventType(event_type),
        position_key=position_key,
        market_id="m1",
        outcome="YES",
        intent_type="PREDICTION_BUY",
        shares_delta=Decimal("0"),
        usd_delta=Decimal("0"),
        realized_pnl_usd=None,
        position_size_after=position_size_after,
        position_basis_after=position_basis_after,
        position_loaded_extras_after=position_loaded_extras_after,
        confidence=AccountingConfidence.HIGH,
    )
    payload = json.loads(event.to_payload_json())
    if drop_extras_field:
        payload.pop("position_loaded_extras_after", None)
    return {
        "event_type": event_type,
        "deployment_id": deployment_id,
        "position_key": position_key,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


class TestPredictionLoadedExtrasReconstruction:
    """#2146: the VIB-3710 loaded-extras accumulator must survive a runner
    restart so a cross-restart SELL/REDEEM prices realized PnL against the
    fully-loaded basis, not bare basis.
    """

    DEP = "deployment:test"
    PK = "prediction:polymarket:polygon:0xwallet:m1:YES"

    def test_full_close_after_restart_matches_single_process(self) -> None:
        # Single-process baseline: BUY (basis 50 + extras 3), then full sell.
        single = FIFOBasisStore()
        single.record_prediction_buy(
            deployment_id=self.DEP,
            position_key=self.PK,
            shares=Decimal("100"),
            cost_basis_usd=Decimal("50"),
            gas_cost_usd=Decimal("2"),
            fee_pusd=Decimal("1"),
        )
        realized_single, _, _, _ = single.match_prediction_sell(
            deployment_id=self.DEP, position_key=self.PK, shares_sold=Decimal("100"), proceeds_usd=Decimal("60")
        )
        # 60 - (50 + 3) = 7.
        assert realized_single == Decimal("7")

        # Restart: reconstruct from the post-BUY snapshot, then full sell.
        restarted = FIFOBasisStore()
        restarted.reconstruct_from_events(
            [
                _prediction_snapshot_row(
                    event_type="PREDICTION_OPEN",
                    deployment_id=self.DEP,
                    position_key=self.PK,
                    position_size_after=Decimal("100"),
                    position_basis_after=Decimal("50"),
                    position_loaded_extras_after=Decimal("3"),
                )
            ]
        )
        assert restarted.get_prediction_loaded_extras(self.DEP, self.PK) == Decimal("3")
        realized_restarted, _, _, _ = restarted.match_prediction_sell(
            deployment_id=self.DEP, position_key=self.PK, shares_sold=Decimal("100"), proceeds_usd=Decimal("60")
        )
        assert realized_restarted == realized_single == Decimal("7")

    def test_partial_reduce_then_restart_then_close_matches_single_process(self) -> None:
        # Single-process: BUY, partial REDUCE, then CLOSE the remainder.
        single = FIFOBasisStore()
        single.record_prediction_buy(
            deployment_id=self.DEP,
            position_key=self.PK,
            shares=Decimal("100"),
            cost_basis_usd=Decimal("50"),
            gas_cost_usd=Decimal("2"),
            fee_pusd=Decimal("1"),
        )
        single.match_prediction_sell(
            deployment_id=self.DEP, position_key=self.PK, shares_sold=Decimal("40"), proceeds_usd=Decimal("30")
        )
        # Residual after REDUCE: size 60, basis 30, extras 1.8.
        realized_close_single, _, _, is_close = single.match_prediction_sell(
            deployment_id=self.DEP, position_key=self.PK, shares_sold=Decimal("60"), proceeds_usd=Decimal("40")
        )
        # 40 - (30 + 1.8) = 8.2.
        assert is_close is True
        assert realized_close_single == Decimal("8.2")

        # Restart from the post-REDUCE snapshot, then CLOSE the remainder.
        restarted = FIFOBasisStore()
        restarted.reconstruct_from_events(
            [
                _prediction_snapshot_row(
                    event_type="PREDICTION_REDUCE",
                    deployment_id=self.DEP,
                    position_key=self.PK,
                    position_size_after=Decimal("60"),
                    position_basis_after=Decimal("30"),
                    position_loaded_extras_after=Decimal("1.8"),
                )
            ]
        )
        assert restarted.get_prediction_loaded_extras(self.DEP, self.PK) == Decimal("1.8")
        realized_close_restarted, _, _, _ = restarted.match_prediction_sell(
            deployment_id=self.DEP, position_key=self.PK, shares_sold=Decimal("60"), proceeds_usd=Decimal("40")
        )
        assert realized_close_restarted == realized_close_single == Decimal("8.2")

    def test_legacy_payload_without_extras_field_defaults_to_zero(self) -> None:
        # A pre-#2146 payload (no position_loaded_extras_after) must still
        # reconstruct — extras default to 0, preserving the old arithmetic
        # rather than crashing.
        restarted = FIFOBasisStore()
        restarted.reconstruct_from_events(
            [
                _prediction_snapshot_row(
                    event_type="PREDICTION_OPEN",
                    deployment_id=self.DEP,
                    position_key=self.PK,
                    position_size_after=Decimal("100"),
                    position_basis_after=Decimal("50"),
                    position_loaded_extras_after=Decimal("3"),
                    drop_extras_field=True,
                )
            ]
        )
        assert restarted.get_prediction_loaded_extras(self.DEP, self.PK) == Decimal("0")
        realized, _, _, _ = restarted.match_prediction_sell(
            deployment_id=self.DEP, position_key=self.PK, shares_sold=Decimal("100"), proceeds_usd=Decimal("60")
        )
        # Bare basis (50), extras treated as 0: 60 - 50 = 10.
        assert realized == Decimal("10")
