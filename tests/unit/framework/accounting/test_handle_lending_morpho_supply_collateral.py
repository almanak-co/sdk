"""Morpho Blue F2 part C (MorphoMay15 §6.2) — LIVE writer (handle_lending)
must also consume ``supply_collateral_amount``.

Part A: enricher overlay (test_result_enricher_morpho_supply_collateral.py).
Part B: replay-path writer build_lending_accounting_event
        (test_morpho_supply_collateral_amount_writer.py).
Part C (here): live-path writer handle_lending.

The live writer routes through ``_extract_amount_human`` in
``category_handlers/lending_handler.py`` which has the same fallback shape
as the replay path: a per-intent-type primary key (``supply_amount`` for
SUPPLY) and nothing else. Without a ``supply_collateral_amount`` fallback,
a Morpho SUPPLY intent emitting ``SupplyCollateral`` produces
``amount_token=None`` / ``principal_delta_usd=None`` on the typed event
written by the live drain path too.

RED until A6 extends the SUPPLY lookup. Symmetric to A4 on the replay path.
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


# ──────────────────────────────────────────────────────────────────────────────
# Helpers — mirror the shapes used in test_lending_accounting.py
# ──────────────────────────────────────────────────────────────────────────────


_WALLET = "0xa11ce0000000000000000000000000000000a11ce"
_MARKET_ID = "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41"
_DEPLOYMENT_ID = "dep-1"
_DEPLOYMENT_ID = "strat-1"
_CYCLE_ID = "cycle-1"


def _make_outbox_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    position_key: str = "lending:ethereum:morpho_blue:0xa11ce:wsteth",
    market_id: str = _MARKET_ID,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": _DEPLOYMENT_ID,
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
    intent_type: str = "SUPPLY",
    protocol: str = "morpho_blue",
    chain: str = "ethereum",
    extracted_data_json: str = "",
    price_inputs_json: str = "",
    token_in: str = "wstETH",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
        "deployment_id": _DEPLOYMENT_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": "0.0175",
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


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingMorphoSupplyCollateralFallback:
    """F2 live-writer leg — handle_lending must consume supply_collateral_amount."""

    SUPPLIED_RAW = 17_500_000_000_000_000  # 0.0175 wstETH (18 decimals)
    SUPPLIED_HUMAN = Decimal("0.0175")
    SUPPLIED_USD = SUPPLIED_HUMAN * Decimal("3500")  # = $61.25

    def test_supply_collateral_amount_yields_amount_token(self) -> None:
        """RED until A6. Morpho SUPPLY intent with only supply_collateral_amount
        in extracted_data must still produce a non-None amount_token on the
        typed lending event written by the live drain path."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_collateral_amount": self.SUPPLIED_RAW})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_wsteth_price_json(),
        )
        basis = FIFOBasisStore()

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver(18),
        ):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None, "Writer must produce a typed event for SUPPLY"
        assert event.event_type == LendingEventType.SUPPLY
        # handle_lending upper-cases the asset symbol via the ledger token_in
        # fallback path (line ~139). The casing contract is independent of
        # this F2 fix; assert the canonical upper form.
        assert event.asset == "WSTETH"
        assert event.amount_token == self.SUPPLIED_HUMAN, (
            "F2 live-writer regression: handle_lending's _extract_amount_human "
            "must fall back to 'supply_collateral_amount' for Morpho SUPPLY "
            "intents (which emit SupplyCollateral on-chain). "
            f"Got amount_token={event.amount_token!r}, "
            f"expected {self.SUPPLIED_HUMAN!r}."
        )

    def test_supply_collateral_amount_yields_principal_delta_usd(self) -> None:
        """Downstream principal USD must flow once amount_token resolves."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_collateral_amount": self.SUPPLIED_RAW})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
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
        assert event.principal_delta_usd == self.SUPPLIED_USD, (
            "principal_delta_usd must equal amount_token × price. "
            f"Got {event.principal_delta_usd!r}, expected {self.SUPPLIED_USD!r}."
        )

    # ─── Regression guards ──────────────────────────────────────────────────

    def test_supply_amount_still_drives_writer_for_loan_side_path(self) -> None:
        """Loan-side ``Supply`` events must continue to work after A6 — the
        fallback adds, it does not replace."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 50_000_000_000_000_000})  # 0.05
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
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

    def test_supply_amount_wins_over_supply_collateral_when_both_present(self) -> None:
        """Precedence guard: existing supply_amount must take priority when
        both fields are present. The new fallback only applies when the
        primary key is None."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps(
            {
                "supply_amount": 25_000_000_000_000_000,  # 0.025 — wins
                "supply_collateral_amount": self.SUPPLIED_RAW,  # 0.0175 — ignored
            }
        )
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
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
            "supply_amount must win over supply_collateral_amount when both "
            f"are present. Got {event.amount_token!r}."
        )
