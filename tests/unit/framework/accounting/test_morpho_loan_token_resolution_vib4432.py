"""VIB-4432 / GH #2148 — Morpho Blue valuation tokens come from the market
registry, NOT from ``intent.token``.

Before the original fix, the framework seeded ``loan_token_sym`` from
``intent.borrow_token or intent.token`` for SUPPLY / WITHDRAW; ``intent.token``
is the collateral for those types, so the wrong symbol (and the wrong decimals)
was passed to the Morpho reader, producing wrong ``debt_usd`` and
``health_factor``.

VIB-4929 PR-3a makes this invariant **structural**: the generic
``read_lending_account_state`` no longer derives the valuation tokens from the
intent at all. It reads BOTH legs from the connector's market table via
``LendingReadRegistry.valuation_roles`` (the market id keccak-binds the
collateral/loan pair), then prices + resolves decimals for exactly those symbols.
A SUPPLY intent whose ``token`` is the collateral can no longer poison the loan
leg — there is no intent-derived loan symbol to poison.

Coverage:

1. ``valuation_roles`` — names both legs from ``MORPHO_MARKETS`` for the
   WBTC/USDC market regardless of any intent.
2. ``read_lending_account_state`` — the captured eth_call valuation prices the
   loan as USDC (decimals 6), never WBTC (decimals 8), for every intent type.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.framework.accounting.lending_accounting import read_lending_account_state

# Real WBTC/USDC market on Ethereum, 86 % LLTV. ``loan_token=USDC`` (decimals=6),
# ``collateral_token=WBTC`` (decimals=8).
_WBTC_USDC_MARKET_ID = "0x3a85e619751152991742810df6ec69ce473daef99e28a64ab2340d7b7ccfee49"
_WALLET = "0x0000000000000000000000000000000000000001"
_CHAIN = "ethereum"
# wstETH/USDC market id — a different pair, to prove the table (not the intent)
# picks the legs.
_WSTETH_USDC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


# ─── 1. valuation_roles names both legs from the market table ────────────────


class TestMorphoValuationRolesFromRegistry:
    """The valued (collateral, loan) tokens are resolved from MORPHO_MARKETS via
    the registry — the new, structural home of the VIB-4432 invariant."""

    def test_wbtc_usdc_market_names_usdc_as_loan(self) -> None:
        roles = LendingReadRegistry.valuation_roles("morpho_blue", _CHAIN, _WBTC_USDC_MARKET_ID)
        assert roles == (("collateral_token", "WBTC"), ("loan_token", "USDC"))

    def test_wsteth_usdc_market_names_distinct_pair(self) -> None:
        roles = LendingReadRegistry.valuation_roles("morpho_blue", _CHAIN, _WSTETH_USDC_MARKET_ID)
        assert roles == (("collateral_token", "wstETH"), ("loan_token", "USDC"))


# ─── 2. The generic reader prices the loan as USDC, never the collateral ─────


def _word(value: int) -> str:
    return format(value, "064x")


def _position_hex(supply_shares: int, borrow_shares: int, collateral: int) -> str:
    return "0x" + _word(supply_shares) + _word(borrow_shares) + _word(collateral)


def _market_hex(total_borrow_assets: int, total_borrow_shares: int) -> str:
    return "0x" + "".join(
        _word(w)
        for w in (
            20_000 * 10**6,  # total_supply_assets
            20_000 * 10**6,  # total_supply_shares
            total_borrow_assets,
            total_borrow_shares,
            0,  # last_update
            0,  # fee
        )
    )


def _selector_routing_gateway(position_hex: str, market_hex: str) -> Any:
    """Gateway that records the (to, data) of each call and routes by selector."""
    from almanak.connectors._strategy_base.lending_read_base import (
        _MORPHO_MARKET_SELECTOR,
        _MORPHO_POSITION_SELECTOR,
    )

    class _G:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
            self.calls.append(data)
            if data.startswith(_MORPHO_POSITION_SELECTOR):
                return position_hex
            if data.startswith(_MORPHO_MARKET_SELECTOR):
                return market_hex
            raise AssertionError(f"unexpected selector in calldata: {data[:10]}")

    return _G()


class TestGenericReaderPricesLoanAsUsdc:
    """The bug repro, re-aimed at the generic reader. 1 WBTC collateral
    ($60,000) and a 100-unit debt against the WBTC/USDC market.

    If the loan leg were (wrongly) priced as WBTC, the 100-unit debt would value
    at 100 * $60,000 = $6,000,000. Priced correctly as USDC it is 100 * $1 = $100.
    The decimals difference (6 vs 8) also changes the human amount. We assert the
    correct USDC valuation, proving the loan leg came from the market table.
    """

    @pytest.mark.parametrize("market_id", [_WBTC_USDC_MARKET_ID])
    def test_loan_valued_as_usdc_not_wbtc(self, market_id: str) -> None:
        # 100 USDC debt (1:1 shares), 1 WBTC collateral.
        position = _position_hex(0, 100 * 10**6, 1 * 10**8)
        market = _market_hex(total_borrow_assets=10_000 * 10**6, total_borrow_shares=10_000 * 10**6)
        gateway = _selector_routing_gateway(position, market)
        prices = {"WBTC": Decimal("60000"), "USDC": Decimal("1")}

        state = read_lending_account_state(
            protocol="morpho_blue",
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=market_id,
            gateway_client=gateway,
            price_oracle=prices,
        )
        assert state is not None
        # Collateral: 1 WBTC * $60,000.
        assert state.collateral_usd == Decimal("60000")
        # Debt: 100 USDC * $1 — NOT 100 * $60,000 (which a WBTC mis-pricing gives),
        # and the USDC 6-decimal scale (not WBTC 8) was used for the human amount.
        assert state.debt_usd == Decimal("100")
        # Both calls fired against the Morpho singleton (position then market).
        assert len(gateway.calls) == 2

    def test_loan_priced_missing_usdc_fails_closed(self) -> None:
        # Empty != Zero: if the loan token (USDC) has no price, the reader fails
        # closed rather than fabricate a zero debt.
        position = _position_hex(0, 100 * 10**6, 1 * 10**8)
        market = _market_hex(total_borrow_assets=10_000 * 10**6, total_borrow_shares=10_000 * 10**6)
        gateway = MagicMock()
        gateway.eth_call.side_effect = lambda *a, **k: _selector_routing_gateway(position, market).eth_call(*a, **k)
        state = read_lending_account_state(
            protocol="morpho_blue",
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_WBTC_USDC_MARKET_ID,
            gateway_client=gateway,
            price_oracle={"WBTC": Decimal("60000")},  # USDC price MISSING
        )
        assert state is None
