"""Aave V3 ``FlashLoanProvider`` implementation.

Self-contained provider that participates in the cross-protocol
selector at ``almanak.framework.intents.flash_loan_selector``.

Pool addresses are sourced from the connector's existing
``AAVE_V3_POOL_ADDRESSES`` (itself derived from
``almanak.core.contracts.AAVE_V3``) so there is exactly one source of
truth for which chains Aave V3 is deployed on.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._strategy_base.flash_loan_base import (
    FlashLoanProvider,
    FlashLoanProviderInfo,
)
from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES

# Aave V3 charges a fixed 9 bps (0.09%) flash-loan premium on the borrow.
AAVE_V3_FLASH_LOAN_FEE_BPS = 9
AAVE_V3_FLASH_LOAN_GAS_ESTIMATE = 300_000
AAVE_V3_RELIABILITY_SCORE = 0.98

# Tokens known to be available on Aave V3 for flash loans, per chain.
# Static snapshot — drift from on-chain reality is tracked under the
# liquidity-from-chain follow-up (see blueprint 05 §Flash Loan Selector).
AAVE_V3_SUPPORTED_TOKENS: dict[str, set[str]] = {
    "ethereum": {
        "WETH",
        "USDC",
        "USDT",
        "DAI",
        "WBTC",
        "LINK",
        "AAVE",
        "wstETH",
        "cbETH",
        "rETH",
        "GHO",
        "LUSD",
        "crvUSD",
        "FRAX",
    },
    "arbitrum": {
        "WETH",
        "USDC",
        "USDC.e",
        "USDT",
        "DAI",
        "WBTC",
        "LINK",
        "ARB",
        "wstETH",
        "rETH",
        "FRAX",
        "GMX",
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
        "LUSD",
        "sUSD",
    },
    "polygon": {
        "WMATIC",
        "WETH",
        "USDC",
        "USDC.e",
        "USDT",
        "DAI",
        "WBTC",
        "LINK",
        "wstETH",
        "AAVE",
        "BAL",
        "CRV",
        "GHST",
        "SUSHI",
    },
    "base": {
        "WETH",
        "USDC",
        "cbETH",
        "wstETH",
        "USDbC",
    },
    "avalanche": {
        "WAVAX",
        "WETH.e",
        "USDC",
        "USDT",
        "DAI.e",
        "WBTC.e",
        "LINK.e",
        "sAVAX",
        "BTC.b",
    },
}

# Estimated available flash-loan liquidity per chain/token, in USD.
# Static snapshot — replaced by live on-chain reads in the gateway
# follow-up tracked alongside the registry-decorator refactor.
AAVE_V3_ESTIMATED_LIQUIDITY_USD: dict[str, dict[str, int]] = {
    "ethereum": {
        "WETH": 500_000_000,
        "USDC": 2_000_000_000,
        "USDT": 500_000_000,
        "DAI": 300_000_000,
        "WBTC": 200_000_000,
        "wstETH": 400_000_000,
    },
    "arbitrum": {
        "WETH": 200_000_000,
        "USDC": 300_000_000,
        "USDC.e": 100_000_000,
        "USDT": 100_000_000,
        "WBTC": 50_000_000,
    },
    "optimism": {
        "WETH": 100_000_000,
        "USDC": 150_000_000,
        "USDT": 50_000_000,
    },
    "polygon": {
        "WMATIC": 200_000_000,
        "WETH": 100_000_000,
        "USDC": 200_000_000,
        "USDT": 100_000_000,
    },
    "base": {
        "WETH": 50_000_000,
        "USDC": 100_000_000,
        "cbETH": 30_000_000,
    },
}

_DEFAULT_LIQUIDITY_USD = 10_000_000


class AaveFlashLoanProvider(FlashLoanProvider):
    """Aave V3 flash-loan provider."""

    @property
    def name(self) -> str:
        return "aave"

    def supports(self, chain: str, token: str) -> bool:
        if chain not in AAVE_V3_POOL_ADDRESSES:
            return False
        return token in AAVE_V3_SUPPORTED_TOKENS.get(chain, set())

    def quote(self, chain: str, token: str, amount: Decimal) -> FlashLoanProviderInfo:
        if chain not in AAVE_V3_POOL_ADDRESSES:
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Aave V3 not deployed on chain: {chain}",
            )
        if token not in AAVE_V3_SUPPORTED_TOKENS.get(chain, set()):
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Token {token} not supported on Aave V3 {chain}",
            )

        fee_amount = (amount * Decimal(AAVE_V3_FLASH_LOAN_FEE_BPS)) / Decimal("10000")
        estimated_liquidity = AAVE_V3_ESTIMATED_LIQUIDITY_USD.get(chain, {}).get(token, _DEFAULT_LIQUIDITY_USD)
        return FlashLoanProviderInfo(
            provider=self.name,
            is_available=True,
            fee_bps=AAVE_V3_FLASH_LOAN_FEE_BPS,
            fee_amount=fee_amount,
            estimated_liquidity_usd=estimated_liquidity,
            gas_estimate=AAVE_V3_FLASH_LOAN_GAS_ESTIMATE,
            pool_address=AAVE_V3_POOL_ADDRESSES[chain],
            reliability_score=AAVE_V3_RELIABILITY_SCORE,
        )


__all__ = [
    "AAVE_V3_ESTIMATED_LIQUIDITY_USD",
    "AAVE_V3_FLASH_LOAN_FEE_BPS",
    "AAVE_V3_FLASH_LOAN_GAS_ESTIMATE",
    "AAVE_V3_RELIABILITY_SCORE",
    "AAVE_V3_SUPPORTED_TOKENS",
    "AaveFlashLoanProvider",
]
