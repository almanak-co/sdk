"""Morpho Blue F2 part B (MorphoMay15 §6.2) — writer must consume
``supply_collateral_amount`` from enriched ``extracted_data``.

Part A (``test_result_enricher_morpho_supply_collateral.py``) lands the
overlay so the enricher surfaces ``extracted_data['supply_collateral_amount']``.
This file scopes the symmetric writer fix:
``build_lending_accounting_event`` currently reads ``raw_amount`` from

    extracted.get("supply_amount") | "borrow_amount" | "repay_amount" | "withdraw_amount"

— ``supply_collateral_amount`` is absent from that fallback. For a Morpho
``SupplyIntent`` against an isolated market (where the on-chain event is
``SupplyCollateral``), ``supply_amount`` is ``None`` and the entire SUPPLY
accounting branch is bypassed silently — ``amount_human`` stays ``None``
and the typed event is written with neither ``amount_token`` nor
``principal_delta_usd`` populated.

RED until A4 extends the fallback chain to include
``supply_collateral_amount``. The mirrored ``withdraw_collateral_amount``
case is deferred until the parser exposes that extractor.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.lending_accounting import build_lending_accounting_event


_WALLET = "0xa11ce0000000000000000000000000000000a11ce"
_CHAIN = "ethereum"
_MARKET_ID = "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41"  # wstETH/WETH

# wstETH ≈ $3500 (test-only oracle), 18 decimals
_PRICE_ORACLE = {"wstETH": Decimal("3500"), "WSTETH": Decimal("3500")}


def _make_morpho_supply_intent() -> MagicMock:
    """Morpho SUPPLY intent depositing wstETH collateral against a wstETH/WETH market."""
    intent = MagicMock()
    intent.intent_type.value = "SUPPLY"
    intent.protocol = "morpho_blue"
    intent.token = "wstETH"
    intent.borrow_token = None
    intent.collateral_token = None
    intent.market_id = _MARKET_ID
    return intent


def _make_result(extracted: dict | None = None) -> MagicMock:
    result = MagicMock()
    result.tx_hash = "0xdeadbeef"
    result.extracted_data = extracted or {}
    result.total_gas_cost_wei = None
    return result


def _mock_resolver_for_18_decimals() -> MagicMock:
    token_info = MagicMock()
    token_info.decimals = 18
    resolver = MagicMock()
    resolver.resolve.return_value = token_info
    return resolver


class TestMorphoSupplyCollateralAmountReachesWriter:
    """F2 writer leg — supply_collateral_amount must flow into the typed event."""

    SUPPLIED_RAW = 17_500_000_000_000_000  # 0.0175 wstETH (18 decimals)
    SUPPLIED_HUMAN = Decimal("0.0175")
    SUPPLIED_USD = SUPPLIED_HUMAN * Decimal("3500")  # = $61.25

    def test_supply_collateral_amount_populates_event_amount_token(self) -> None:
        """RED until A4. The Morpho SUPPLY writer must populate
        ``event.amount_token`` from ``extracted_data['supply_collateral_amount']``
        when ``supply_amount`` is absent (i.e., on-chain event was
        ``SupplyCollateral``, not ``Supply``)."""

        intent = _make_morpho_supply_intent()
        result = _make_result({"supply_collateral_amount": self.SUPPLIED_RAW})

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver_for_18_decimals(),
        ):
            event = build_lending_accounting_event(
                intent=intent,
                result=result,
                deployment_id="dep-1",
                strategy_id="strat-1",
                cycle_id="cycle-001",
                execution_mode="paper",
                chain=_CHAIN,
                wallet_address=_WALLET,
                gateway_client=None,
                basis_store=FIFOBasisStore(),
                price_oracle=_PRICE_ORACLE,
                ledger_entry_id=None,
                pre_execution_state=None,
            )

        assert event is not None, "Writer must produce a typed event for SUPPLY intents"
        assert event.amount_token == self.SUPPLIED_HUMAN, (
            "F2 writer-leg regression: Morpho SUPPLY intents emit "
            "SupplyCollateral on-chain; the writer's raw_amount fallback must "
            "include 'supply_collateral_amount'. "
            f"Got amount_token={event.amount_token!r}, "
            f"expected {self.SUPPLIED_HUMAN!r}. "
            "Fix: extend the raw_amount chain in "
            "build_lending_accounting_event to read supply_collateral_amount."
        )

    def test_supply_collateral_amount_populates_principal_delta_usd(self) -> None:
        """Downstream USD math must flow once amount_token is populated."""
        intent = _make_morpho_supply_intent()
        result = _make_result({"supply_collateral_amount": self.SUPPLIED_RAW})

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver_for_18_decimals(),
        ):
            event = build_lending_accounting_event(
                intent=intent,
                result=result,
                deployment_id="dep-1",
                strategy_id="strat-1",
                cycle_id="cycle-001",
                execution_mode="paper",
                chain=_CHAIN,
                wallet_address=_WALLET,
                gateway_client=None,
                basis_store=FIFOBasisStore(),
                price_oracle=_PRICE_ORACLE,
                ledger_entry_id=None,
                pre_execution_state=None,
            )

        assert event is not None
        assert event.principal_delta_usd == self.SUPPLIED_USD, (
            "principal_delta_usd must be derived from amount_token × price once "
            "the raw_amount fallback picks up supply_collateral_amount. "
            f"Got {event.principal_delta_usd!r}, expected {self.SUPPLIED_USD!r}."
        )

    # ─── Regression guards: existing SUPPLY paths must not regress ──────────

    def test_supply_amount_still_drives_writer_for_loan_side_supply(self) -> None:
        """Loan-side ``Supply`` events (lender depositing borrowable asset)
        must continue to drive the writer. A4 must add to the fallback,
        not replace ``supply_amount`` with ``supply_collateral_amount``.
        """
        intent = _make_morpho_supply_intent()
        intent.token = "WETH"  # treat as loan-side supply for this test
        result = _make_result({"supply_amount": 50_000_000_000_000_000})  # 0.05 WETH

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver_for_18_decimals(),
        ):
            event = build_lending_accounting_event(
                intent=intent,
                result=result,
                deployment_id="dep-1",
                strategy_id="strat-1",
                cycle_id="cycle-001",
                execution_mode="paper",
                chain=_CHAIN,
                wallet_address=_WALLET,
                gateway_client=None,
                basis_store=FIFOBasisStore(),
                price_oracle={"WETH": Decimal("3000"), **_PRICE_ORACLE},
                ledger_entry_id=None,
                pre_execution_state=None,
            )

        assert event is not None
        assert event.amount_token == Decimal("0.05"), (
            "Loan-side supply_amount must still flow into the writer after the "
            f"A4 fallback extension. Got {event.amount_token!r}."
        )

    def test_supply_amount_wins_over_supply_collateral_when_both_present(self) -> None:
        """Defensive: when both fields are present (shouldn't happen in
        practice — a single tx emits one or the other, not both), the
        existing ``supply_amount`` takes precedence to preserve current
        behaviour. ``supply_collateral_amount`` is the new fallback, NOT
        a replacement."""
        intent = _make_morpho_supply_intent()
        result = _make_result(
            {
                "supply_amount": 25_000_000_000_000_000,  # 0.025 wstETH — would-be loan-side
                "supply_collateral_amount": self.SUPPLIED_RAW,  # 0.0175 wstETH — collateral
            }
        )

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_mock_resolver_for_18_decimals(),
        ):
            event = build_lending_accounting_event(
                intent=intent,
                result=result,
                deployment_id="dep-1",
                strategy_id="strat-1",
                cycle_id="cycle-001",
                execution_mode="paper",
                chain=_CHAIN,
                wallet_address=_WALLET,
                gateway_client=None,
                basis_store=FIFOBasisStore(),
                price_oracle=_PRICE_ORACLE,
                ledger_entry_id=None,
                pre_execution_state=None,
            )

        assert event is not None
        assert event.amount_token == Decimal("0.025"), (
            "When both fields are present the existing supply_amount must win — "
            "A4 adds a fallback for the collateral case only, it does not change "
            "precedence on the loan-side path. "
            f"Got {event.amount_token!r}."
        )
