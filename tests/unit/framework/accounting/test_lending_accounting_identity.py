"""Tests for deterministic identity.id generation in lending_accounting.py."""

from __future__ import annotations

from unittest.mock import MagicMock


def _make_basis_store():
    from almanak.framework.accounting.basis import FIFOBasisStore

    return FIFOBasisStore()


def _make_supply_intent():
    intent = MagicMock()
    it = MagicMock()
    it.value = "SUPPLY"
    intent.intent_type = it
    intent.protocol = "aave_v3"
    intent.pool = "0xabc000"
    intent.token = "USDC"
    intent.borrow_token = None
    intent.market_id = None
    return intent


def _make_result(tx_hash: str = "0xdeadbeef12345678"):
    result = MagicMock()
    result.tx_hash = tx_hash
    result.extracted_data = {}
    result.total_gas_cost_wei = None
    result.transaction_results = []
    return result


def _call(tx_hash: str = "0xdeadbeef12345678", intent_type: str = "SUPPLY"):
    from almanak.framework.accounting.lending_accounting import build_lending_accounting_event

    intent = _make_supply_intent()
    intent.intent_type.value = intent_type
    result = _make_result(tx_hash)
    return build_lending_accounting_event(
        intent=intent,
        result=result,
        deployment_id="strat-1",
        cycle_id="cycle-001",
        execution_mode="paper",
        chain="arbitrum",
        wallet_address="0xwallet",
        gateway_client=None,
        basis_store=_make_basis_store(),
        price_oracle=None,
        ledger_entry_id="led-001",
    )


class TestLendingAccountingIdentity:

    def test_identity_id_is_deterministic(self):
        """Same inputs produce the same identity.id on repeated calls (uuid5, not uuid4)."""
        ev1 = _call()
        ev2 = _call()
        assert ev1 is not None and ev2 is not None
        assert ev1.identity.id == ev2.identity.id

    def test_identity_id_differs_by_tx_hash(self):
        """Different tx_hash produces a different identity.id."""
        ev1 = _call(tx_hash="0xaaa111")
        ev2 = _call(tx_hash="0xbbb222")
        assert ev1 is not None and ev2 is not None
        assert ev1.identity.id != ev2.identity.id

    def test_identity_id_differs_by_intent_type(self):
        """SUPPLY and BORROW produce different IDs for the same tx_hash."""
        ev1 = _call(intent_type="SUPPLY")
        ev2 = _call(intent_type="BORROW")
        assert ev1 is not None and ev2 is not None
        assert ev1.identity.id != ev2.identity.id

    def test_identity_id_is_valid_uuid(self):
        """identity.id must be a parseable UUID string (gateway validation)."""
        import uuid

        ev = _call()
        assert ev is not None
        parsed = uuid.UUID(ev.identity.id)
        assert str(parsed) == ev.identity.id
