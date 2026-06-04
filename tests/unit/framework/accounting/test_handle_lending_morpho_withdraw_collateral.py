"""VIB-4635 — LIVE writer (handle_lending) must consume
``withdraw_collateral_amount`` for Morpho Blue collateral withdrawals.

A Morpho ``WithdrawIntent`` for the collateral leg routes through
``withdrawCollateral(...)`` and emits ``WithdrawCollateral`` (not the
loan-side ``Withdraw``). The live writer routes through
``_extract_amount_human`` in ``category_handlers/lending_handler.py``, whose
per-intent primary key for WITHDRAW is ``withdraw_amount`` — absent on a
WithdrawCollateral receipt. Without the ``withdraw_collateral_amount``
fallback, the typed event recorded ``amount_token=None`` even though the
amount is known exactly on-chain (Empty ≠ Zero ≠ None).

WITHDRAW-side mirror of test_handle_lending_morpho_supply_collateral.py.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.lending_handler import handle_lending
from almanak.framework.accounting.models import LendingEventType

_WALLET = "0xa11ce0000000000000000000000000000000a11ce"
_MARKET_ID = "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41"
_DEPLOYMENT_ID = "strat-1"
_CYCLE_ID = "cycle-1"


def _make_outbox_row(
    ledger_entry_id: str,
    intent_type: str = "WITHDRAW",
    position_key: str = "lending:ethereum:morpho_blue:0xa11ce:wsteth",
    market_id: str = _MARKET_ID,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "intent_type": intent_type,
        "wallet_address": _WALLET,
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
    *,
    intent_type: str = "WITHDRAW",
    protocol: str = "morpho_blue",
    chain: str = "ethereum",
    extracted_data_json: str = "",
    price_inputs_json: str = "",
    token_in: str = "wstETH",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": "0.2",
        "token_out": "",
        "amount_out": "",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": "0xdeadbeef",
        "chain": chain,
        "protocol": protocol,
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": "",
    }


def _mock_resolver(decimals: int = 18) -> MagicMock:
    token_info = MagicMock()
    token_info.decimals = decimals
    resolver = MagicMock()
    resolver.resolve.return_value = token_info
    return resolver


def _wsteth_price_json() -> str:
    return json.dumps({"wstETH": "3500.0", "WSTETH": "3500.0"})


class TestHandleLendingMorphoWithdrawCollateralFallback:
    """VIB-4635 live-writer leg — handle_lending must consume
    withdraw_collateral_amount for Morpho collateral WITHDRAW."""

    WITHDRAWN_RAW = 200_000_000_000_000_000  # 0.2 wstETH (18 decimals)
    WITHDRAWN_HUMAN = Decimal("0.2")
    WITHDRAWN_USD = WITHDRAWN_HUMAN * Decimal("3500")  # = $700

    def test_withdraw_collateral_amount_yields_amount_token(self) -> None:
        """Morpho WITHDRAW intent with only withdraw_collateral_amount in
        extracted_data must still produce a non-None amount_token."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"withdraw_collateral_amount": self.WITHDRAWN_RAW})
        outbox = _make_outbox_row(led_id, intent_type="WITHDRAW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_wsteth_price_json(),
        )
        basis = FIFOBasisStore()

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver(18),
        ):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None, "Writer must produce a typed event for WITHDRAW"
        assert event.event_type == LendingEventType.WITHDRAW
        assert event.asset == "WSTETH"
        assert event.amount_token == self.WITHDRAWN_HUMAN, (
            "VIB-4635 live-writer regression: handle_lending's _extract_amount_human "
            "must fall back to 'withdraw_collateral_amount' for Morpho WITHDRAW "
            "intents (which emit WithdrawCollateral on-chain). "
            f"Got amount_token={event.amount_token!r}, expected {self.WITHDRAWN_HUMAN!r}."
        )

    def test_withdraw_collateral_amount_yields_principal_delta_usd(self) -> None:
        """With no Layer-5 SUPPLY lot the WITHDRAW degrades principal to the
        total measured withdrawal — a measured, positive leg (never a
        fabricated 0); interest stays None."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"withdraw_collateral_amount": self.WITHDRAWN_RAW})
        outbox = _make_outbox_row(led_id, intent_type="WITHDRAW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_wsteth_price_json(),
        )
        basis = FIFOBasisStore()

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver(18),
        ):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.principal_delta_usd == self.WITHDRAWN_USD, (
            "Unmatched WITHDRAW principal must equal amount_token × price. "
            f"Got {event.principal_delta_usd!r}, expected {self.WITHDRAWN_USD!r}."
        )
        assert event.interest_delta_usd is None, (
            "Unmatched WITHDRAW (no SUPPLY lot) must leave interest None — never 0."
        )

    # ─── Regression guards ──────────────────────────────────────────────────

    def test_withdraw_amount_still_drives_writer_for_loan_side_path(self) -> None:
        """Loan-side ``Withdraw`` events must continue to work — the fallback
        adds, it does not replace."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"withdraw_amount": 50_000_000_000_000_000})  # 0.05
        outbox = _make_outbox_row(led_id, intent_type="WITHDRAW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_wsteth_price_json(),
        )
        basis = FIFOBasisStore()

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver(18),
        ):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.amount_token == Decimal("0.05")

    def test_withdraw_amount_wins_over_withdraw_collateral_when_both_present(self) -> None:
        """Precedence guard: existing withdraw_amount must take priority when
        both fields are present. The new fallback only applies when the
        primary key is None."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps(
            {
                "withdraw_amount": 25_000_000_000_000_000,  # 0.025 — wins
                "withdraw_collateral_amount": self.WITHDRAWN_RAW,  # 0.2 — ignored
            }
        )
        outbox = _make_outbox_row(led_id, intent_type="WITHDRAW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_wsteth_price_json(),
        )
        basis = FIFOBasisStore()

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver(18),
        ):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.amount_token == Decimal("0.025"), (
            "withdraw_amount must win over withdraw_collateral_amount when both "
            f"are present. Got {event.amount_token!r}."
        )
