"""Balancer V2 ``FlashLoanProvider`` implementation.

Vault address is sourced from the connector's existing
``BALANCER_VAULT_ADDRESSES`` so there is one source of truth for the
chains Balancer V2 is deployed on.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._strategy_base.flash_loan_base import (
    FlashLoanProvider,
    FlashLoanProviderInfo,
)
from almanak.connectors.balancer_v2.adapter import BALANCER_VAULT_ADDRESSES

# Balancer V2 charges zero protocol fee for flash loans.
BALANCER_FLASH_LOAN_FEE_BPS = 0
BALANCER_FLASH_LOAN_GAS_ESTIMATE = 250_000
BALANCER_RELIABILITY_SCORE = 0.95

# Tokens known to be flash-loanable on Balancer V2, per chain.
BALANCER_SUPPORTED_TOKENS: dict[str, set[str]] = {
    "ethereum": {
        "WETH",
        "USDC",
        "USDT",
        "DAI",
        "WBTC",
        "BAL",
        "wstETH",
        "rETH",
        "cbETH",
        "GHO",
        "AAVE",
        "LINK",
        "LDO",
        "RPL",
    },
    "arbitrum": {
        "WETH",
        "USDC",
        "USDC.e",
        "USDT",
        "DAI",
        "WBTC",
        "BAL",
        "ARB",
        "wstETH",
        "rETH",
        "LINK",
        "GMX",
        "MAGIC",
    },
    "optimism": {
        "WETH",
        "USDC",
        "USDC.e",
        "USDT",
        "DAI",
        "wstETH",
        "OP",
        "rETH",
        "BAL",
    },
    "polygon": {
        "WMATIC",
        "WETH",
        "USDC",
        "USDC.e",
        "USDT",
        "DAI",
        "WBTC",
        "BAL",
        "wstETH",
        "AAVE",
    },
    "base": {
        "WETH",
        "USDC",
        "cbETH",
        "USDbC",
        "BAL",
    },
}

# Static estimated-liquidity snapshot, USD. Replaced by live reads in
# the gateway follow-up (see blueprint 05 §Flash Loan Selector).
BALANCER_ESTIMATED_LIQUIDITY_USD: dict[str, dict[str, int]] = {
    "ethereum": {
        "WETH": 100_000_000,
        "USDC": 50_000_000,
        "DAI": 30_000_000,
        "wstETH": 200_000_000,
        "rETH": 50_000_000,
    },
    "arbitrum": {
        "WETH": 50_000_000,
        "USDC": 30_000_000,
        "wstETH": 30_000_000,
    },
    "optimism": {
        "WETH": 20_000_000,
        "USDC": 10_000_000,
    },
    "polygon": {
        "WMATIC": 30_000_000,
        "WETH": 20_000_000,
        "USDC": 20_000_000,
    },
    "base": {
        "WETH": 10_000_000,
        "USDC": 10_000_000,
    },
}

_DEFAULT_LIQUIDITY_USD = 5_000_000


class BalancerFlashLoanProvider(FlashLoanProvider):
    """Balancer V2 flash-loan provider (zero-fee)."""

    @property
    def name(self) -> str:
        return "balancer"

    def supports(self, chain: str, token: str) -> bool:
        if chain not in BALANCER_VAULT_ADDRESSES:
            return False
        return token in BALANCER_SUPPORTED_TOKENS.get(chain, set())

    def quote(self, chain: str, token: str, amount: Decimal) -> FlashLoanProviderInfo:
        if chain not in BALANCER_VAULT_ADDRESSES:
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Balancer not available on {chain}",
            )
        if token not in BALANCER_SUPPORTED_TOKENS.get(chain, set()):
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Token {token} not supported on Balancer {chain}",
            )

        estimated_liquidity = BALANCER_ESTIMATED_LIQUIDITY_USD.get(chain, {}).get(token, _DEFAULT_LIQUIDITY_USD)
        return FlashLoanProviderInfo(
            provider=self.name,
            is_available=True,
            fee_bps=BALANCER_FLASH_LOAN_FEE_BPS,
            fee_amount=Decimal("0"),
            estimated_liquidity_usd=estimated_liquidity,
            gas_estimate=BALANCER_FLASH_LOAN_GAS_ESTIMATE,
            pool_address=BALANCER_VAULT_ADDRESSES[chain],
            reliability_score=BALANCER_RELIABILITY_SCORE,
        )


__all__ = [
    "BALANCER_ESTIMATED_LIQUIDITY_USD",
    "BALANCER_FLASH_LOAN_FEE_BPS",
    "BALANCER_FLASH_LOAN_GAS_ESTIMATE",
    "BALANCER_RELIABILITY_SCORE",
    "BALANCER_SUPPORTED_TOKENS",
    "BalancerFlashLoanProvider",
]
