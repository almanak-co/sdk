"""Morpho Blue ``FlashLoanProvider`` implementation.

Morpho Blue flash loans are zero-fee. ``MORPHO_BLUE_ADDRESSES`` is
derived from ``almanak.core.contracts.MORPHO_BLUE`` so there is one
source of truth for Morpho deployments.

``MORPHO_SUPPORTED_CHAINS`` is *narrower* than the registry: it lists
chains where flash-loan fee behaviour and callback semantics have been
validated. Expand deliberately after testing per chain.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._strategy_base.flash_loan_base import (
    FlashLoanProvider,
    FlashLoanProviderInfo,
)
from almanak.core.contracts import MORPHO_BLUE as _MORPHO_BLUE_REGISTRY

# Morpho Blue charges zero protocol fee for flash loans.
MORPHO_FLASH_LOAN_FEE_BPS = 0
MORPHO_FLASH_LOAN_GAS_ESTIMATE = 200_000
MORPHO_RELIABILITY_SCORE = 0.97

# Morpho Blue deployment addresses, derived from the central registry.
MORPHO_BLUE_ADDRESSES: dict[str, str] = {chain: addrs["morpho"] for chain, addrs in _MORPHO_BLUE_REGISTRY.items()}

# Subset of ``MORPHO_BLUE_ADDRESSES`` where flash-loan support has been
# validated end-to-end. Expansion is gated on per-chain testing.
MORPHO_SUPPORTED_CHAINS: set[str] = {"ethereum", "base"}

# Tokens that can be flash-loaned via Morpho Blue, per chain.
MORPHO_SUPPORTED_TOKENS: dict[str, set[str]] = {
    "ethereum": {
        "WETH",
        "USDC",
        "USDT",
        "DAI",
        "WBTC",
        "wstETH",
        "USDe",
        "sUSDe",
        "eUSDe",
        "cbETH",
        "rETH",
        "PYUSD",
    },
    "base": {
        "WETH",
        "USDC",
        "cbETH",
        "USDbC",
    },
}

# Static estimated-liquidity snapshot, USD. Replaced by live reads in
# the gateway follow-up (see blueprint 05 §Flash Loan Selector).
MORPHO_ESTIMATED_LIQUIDITY_USD: dict[str, dict[str, int]] = {}

_DEFAULT_LIQUIDITY_USD = 50_000_000


class MorphoFlashLoanProvider(FlashLoanProvider):
    """Morpho Blue flash-loan provider (zero-fee)."""

    @property
    def name(self) -> str:
        return "morpho"

    def supports(self, chain: str, token: str) -> bool:
        if chain not in MORPHO_SUPPORTED_CHAINS:
            return False
        if not MORPHO_BLUE_ADDRESSES.get(chain):
            return False
        return token in MORPHO_SUPPORTED_TOKENS.get(chain, set())

    def quote(self, chain: str, token: str, amount: Decimal) -> FlashLoanProviderInfo:
        if chain not in MORPHO_SUPPORTED_CHAINS:
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Morpho Blue flash loans not enabled on chain: {chain}",
            )
        pool_address = MORPHO_BLUE_ADDRESSES.get(chain, "")
        if not pool_address:
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Morpho Blue not available on chain: {chain}",
            )
        if token not in MORPHO_SUPPORTED_TOKENS.get(chain, set()):
            return FlashLoanProviderInfo(
                provider=self.name,
                is_available=False,
                unavailable_reason=f"Token {token} not supported on Morpho Blue {chain}",
            )

        estimated_liquidity = MORPHO_ESTIMATED_LIQUIDITY_USD.get(chain, {}).get(token, _DEFAULT_LIQUIDITY_USD)
        return FlashLoanProviderInfo(
            provider=self.name,
            is_available=True,
            fee_bps=MORPHO_FLASH_LOAN_FEE_BPS,
            fee_amount=Decimal("0"),
            estimated_liquidity_usd=estimated_liquidity,
            gas_estimate=MORPHO_FLASH_LOAN_GAS_ESTIMATE,
            pool_address=pool_address,
            reliability_score=MORPHO_RELIABILITY_SCORE,
        )


__all__ = [
    "MORPHO_BLUE_ADDRESSES",
    "MORPHO_ESTIMATED_LIQUIDITY_USD",
    "MORPHO_FLASH_LOAN_FEE_BPS",
    "MORPHO_FLASH_LOAN_GAS_ESTIMATE",
    "MORPHO_RELIABILITY_SCORE",
    "MORPHO_SUPPORTED_CHAINS",
    "MORPHO_SUPPORTED_TOKENS",
    "MorphoFlashLoanProvider",
]
