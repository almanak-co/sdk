"""Flash Loan Provider Selector.

This module provides the FlashLoanSelector class that evaluates available flash loan
providers and selects the optimal one based on configurable criteria.

Selection Criteria:
- fee: Minimize flash loan premium (Balancer=0%, Aave=0.09%)
- liquidity: Prefer providers with deeper liquidity for the token
- availability: Only consider providers that support the token

Example:
    from almanak.framework.connectors.flash_loan import FlashLoanSelector

    selector = FlashLoanSelector(chain="arbitrum")

    # Select best provider based on fee (prefers Balancer for zero fee)
    result = selector.select_provider(
        token="USDC",
        amount=Decimal("1000000"),
        priority="fee",
    )

    if result.is_success:
        print(f"Selected {result.provider} with fee {result.fee_amount}")
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Aave V3 Pool addresses per chain
AAVE_V3_POOL_ADDRESSES: dict[str, str] = {
    "ethereum": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
    "arbitrum": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    "optimism": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    "polygon": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    "base": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
    "avalanche": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
}

# Balancer Vault address (same on all chains)
BALANCER_VAULT_ADDRESS = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

# Balancer supported chains
BALANCER_SUPPORTED_CHAINS = {"ethereum", "arbitrum", "optimism", "polygon", "base"}

# Morpho Blue addresses per chain (singleton contract)
MORPHO_BLUE_ADDRESSES: dict[str, str] = {
    "ethereum": "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb",
    "base": "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb",
}

# Morpho supported chains for flash loans
MORPHO_SUPPORTED_CHAINS = {"ethereum", "base"}

# Flash loan fees in basis points
PROVIDER_FEES_BPS: dict[str, int] = {
    "aave": 9,  # 0.09%
    "balancer": 0,  # Zero fees!
    "morpho": 0,  # Zero fees!
}

# Gas estimates for flash loans (base, not including callbacks)
PROVIDER_GAS_ESTIMATES: dict[str, int] = {
    "aave": 300000,  # Aave flashLoanSimple
    "balancer": 250000,  # Balancer slightly cheaper
    "morpho": 200000,  # Morpho is very gas-efficient
}

# Default provider reliability scores (0-1, higher is better)
DEFAULT_PROVIDER_RELIABILITY: dict[str, float] = {
    "aave": 0.98,  # Battle-tested, most widely used
    "balancer": 0.95,  # Reliable, but less battle-tested for flash loans
    "morpho": 0.97,  # Highly reliable, immutable contracts
}

# Tokens known to be available on Aave V3 by chain
# Note: In production, this would be fetched from the protocol
AAVE_SUPPORTED_TOKENS: dict[str, set[str]] = {
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

# Tokens known to be available on Balancer by chain
# Balancer has liquidity for most major tokens in its pools
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

# Tokens known to be available on Morpho Blue by chain
# Morpho Blue flash loans can flash any token held in the protocol
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

# Estimated available liquidity per token per provider (in USD)
# In production, this would be fetched from on-chain data
ESTIMATED_LIQUIDITY_USD: dict[str, dict[str, dict[str, int]]] = {
    "ethereum": {
        "aave": {
            "WETH": 500_000_000,
            "USDC": 2_000_000_000,
            "USDT": 500_000_000,
            "DAI": 300_000_000,
            "WBTC": 200_000_000,
            "wstETH": 400_000_000,
        },
        "balancer": {
            "WETH": 100_000_000,
            "USDC": 50_000_000,
            "DAI": 30_000_000,
            "wstETH": 200_000_000,
            "rETH": 50_000_000,
        },
    },
    "arbitrum": {
        "aave": {
            "WETH": 200_000_000,
            "USDC": 300_000_000,
            "USDC.e": 100_000_000,
            "USDT": 100_000_000,
            "WBTC": 50_000_000,
        },
        "balancer": {
            "WETH": 50_000_000,
            "USDC": 30_000_000,
            "wstETH": 30_000_000,
        },
    },
    "optimism": {
        "aave": {
            "WETH": 100_000_000,
            "USDC": 150_000_000,
            "USDT": 50_000_000,
        },
        "balancer": {
            "WETH": 20_000_000,
            "USDC": 10_000_000,
        },
    },
    "polygon": {
        "aave": {
            "WMATIC": 200_000_000,
            "WETH": 100_000_000,
            "USDC": 200_000_000,
            "USDT": 100_000_000,
        },
        "balancer": {
            "WMATIC": 30_000_000,
            "WETH": 20_000_000,
            "USDC": 20_000_000,
        },
    },
    "base": {
        "aave": {
            "WETH": 50_000_000,
            "USDC": 100_000_000,
            "cbETH": 30_000_000,
        },
        "balancer": {
            "WETH": 10_000_000,
            "USDC": 10_000_000,
        },
    },
}


# =============================================================================
# Enums
# =============================================================================


class SelectionPriority(Enum):
    """Priority for flash loan provider selection.

    Attributes:
        FEE: Minimize flash loan fees (Balancer preferred - zero fee)
        LIQUIDITY: Prefer providers with deeper liquidity
        RELIABILITY: Prefer more battle-tested providers (Aave preferred)
        GAS: Minimize gas costs
    """

    FEE = "fee"
    LIQUIDITY = "liquidity"
    RELIABILITY = "reliability"
    GAS = "gas"


# =============================================================================
# Exceptions
# =============================================================================


class FlashLoanSelectorError(Exception):
    """Base exception for flash loan selector errors."""

    pass


class NoProviderAvailableError(FlashLoanSelectorError):
    """Raised when no provider can fulfill the flash loan request."""

    pass


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FlashLoanProviderInfo:
    """Information about a flash loan provider for a specific token.

    Attributes:
        provider: Provider name ("aave" or "balancer")
        is_available: Whether provider supports this token on this chain
        fee_bps: Flash loan fee in basis points
        fee_amount: Calculated fee amount for the requested loan
        estimated_liquidity_usd: Estimated available liquidity in USD
        gas_estimate: Estimated gas for flash loan (base, without callbacks)
        pool_address: Contract address for flash loan
        reliability_score: Historical reliability (0-1)
        score: Calculated overall score (lower is better)
        unavailable_reason: Reason if provider is unavailable
    """

    provider: str
    is_available: bool = False
    fee_bps: int = 0
    fee_amount: Decimal = Decimal("0")
    estimated_liquidity_usd: int = 0
    gas_estimate: int = 0
    pool_address: str = ""
    reliability_score: float = 0.5
    score: float = 1.0
    unavailable_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "is_available": self.is_available,
            "fee_bps": self.fee_bps,
            "fee_amount": str(self.fee_amount),
            "estimated_liquidity_usd": self.estimated_liquidity_usd,
            "gas_estimate": self.gas_estimate,
            "pool_address": self.pool_address,
            "reliability_score": self.reliability_score,
            "score": self.score,
            "unavailable_reason": self.unavailable_reason,
        }


@dataclass
class FlashLoanSelectionResult:
    """Result of flash loan provider selection.

    Attributes:
        provider: Selected provider name (or None if no provider available)
        pool_address: Contract address to call for flash loan
        fee_bps: Fee in basis points for the selected provider
        fee_amount: Calculated fee amount for the requested loan
        total_repay: Total amount to repay (loan + fee)
        gas_estimate: Estimated gas for the flash loan
        providers_evaluated: Information about all evaluated providers
        selection_reasoning: Human-readable explanation of selection
    """

    provider: str | None = None
    pool_address: str = ""
    fee_bps: int = 0
    fee_amount: Decimal = Decimal("0")
    total_repay: Decimal = Decimal("0")
    gas_estimate: int = 0
    providers_evaluated: list[FlashLoanProviderInfo] = field(default_factory=list)
    selection_reasoning: str = ""

    @property
    def is_success(self) -> bool:
        """Check if a provider was successfully selected."""
        return self.provider is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "provider": self.provider,
            "pool_address": self.pool_address,
            "fee_bps": self.fee_bps,
            "fee_amount": str(self.fee_amount),
            "total_repay": str(self.total_repay),
            "gas_estimate": self.gas_estimate,
            "providers_evaluated": [p.to_dict() for p in self.providers_evaluated],
            "selection_reasoning": self.selection_reasoning,
            "is_success": self.is_success,
        }


# =============================================================================
# Flash Loan Selector
# =============================================================================


class FlashLoanSelector:
    """Selects optimal flash loan provider based on configurable criteria.

    The selector evaluates Aave V3 and Balancer for the requested token and
    amount, selecting the best provider based on:
    - Fee: Balancer has zero fees, Aave charges 0.09%
    - Liquidity: Check if provider has sufficient liquidity
    - Reliability: Aave is more battle-tested
    - Gas: Balancer is slightly cheaper on gas

    Example:
        selector = FlashLoanSelector(chain="arbitrum")

        # Select provider for 1M USDC flash loan
        result = selector.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="fee",  # Will prefer Balancer (zero fee)
        )

        if result.is_success:
            print(f"Use {result.provider} at {result.pool_address}")
            print(f"Fee: {result.fee_amount} ({result.fee_bps} bps)")
    """

    def __init__(
        self,
        chain: str,
        reliability_scores: dict[str, float] | None = None,
        default_priority: SelectionPriority = SelectionPriority.FEE,
    ):
        """Initialize the flash loan selector.

        Args:
            chain: Target blockchain (ethereum, arbitrum, optimism, polygon, base)
            reliability_scores: Optional custom reliability scores per provider
            default_priority: Default selection priority
        """
        self.chain = chain
        self.reliability_scores = reliability_scores or DEFAULT_PROVIDER_RELIABILITY.copy()
        self.default_priority = default_priority

        # Validate chain support
        if chain not in AAVE_V3_POOL_ADDRESSES:
            raise ValueError(f"Unsupported chain: {chain}. Supported chains: {list(AAVE_V3_POOL_ADDRESSES.keys())}")

        logger.info(f"FlashLoanSelector initialized for chain={chain}, default_priority={default_priority.value}")

    def select_provider(
        self,
        token: str,
        amount: Decimal,
        priority: str | None = None,
        min_liquidity_usd: int = 0,
    ) -> FlashLoanSelectionResult:
        """Select the optimal flash loan provider.

        Evaluates Aave and Balancer for the requested token and amount,
        returning the best provider based on the priority criteria.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            amount: Flash loan amount in token units
            priority: Selection priority ("fee", "liquidity", "reliability", "gas")
            min_liquidity_usd: Minimum required liquidity in USD

        Returns:
            FlashLoanSelectionResult with selected provider details

        Raises:
            NoProviderAvailableError: If no provider supports the token/amount
        """
        # Parse priority
        selection_priority = self.default_priority
        if priority:
            try:
                selection_priority = SelectionPriority(priority.lower())
            except ValueError:
                logger.warning(f"Unknown priority '{priority}', using default '{self.default_priority.value}'")

        logger.info(
            f"Selecting flash loan provider for {amount} {token} on {self.chain} "
            f"with priority={selection_priority.value}"
        )

        # Evaluate all providers
        providers = self._evaluate_providers(
            token=token,
            amount=amount,
            min_liquidity_usd=min_liquidity_usd,
        )

        # Filter to available providers
        available = [p for p in providers if p.is_available]

        if not available:
            reasons = [f"{p.provider}: {p.unavailable_reason}" for p in providers if p.unavailable_reason]
            error_msg = f"No flash loan provider available for {token} on {self.chain}. Reasons: {'; '.join(reasons)}"
            logger.error(error_msg)
            raise NoProviderAvailableError(error_msg)

        # Calculate scores based on priority
        self._calculate_scores(available, selection_priority)

        # Sort by score (lower is better)
        available.sort(key=lambda p: p.score)

        # Select the best provider
        best = available[0]
        fallback = available[1] if len(available) > 1 else None

        # Build reasoning
        reasoning = self._build_reasoning(
            best=best,
            fallback=fallback,
            priority=selection_priority,
            all_providers=available,
        )

        logger.info(f"Selected flash loan provider: {best.provider}")

        return FlashLoanSelectionResult(
            provider=best.provider,
            pool_address=best.pool_address,
            fee_bps=best.fee_bps,
            fee_amount=best.fee_amount,
            total_repay=amount + best.fee_amount,
            gas_estimate=best.gas_estimate,
            providers_evaluated=providers,
            selection_reasoning=reasoning,
        )

    def get_provider_info(
        self,
        provider: str,
        token: str,
        amount: Decimal,
    ) -> FlashLoanProviderInfo:
        """Get information about a specific provider for a token.

        Args:
            provider: Provider name ("aave" or "balancer")
            token: Token symbol
            amount: Flash loan amount

        Returns:
            FlashLoanProviderInfo for the provider
        """
        provider = provider.lower()
        if provider == "aave":
            return self._evaluate_aave(token, amount)
        elif provider == "balancer":
            return self._evaluate_balancer(token, amount)
        elif provider == "morpho":
            return self._evaluate_morpho(token, amount)
        else:
            return FlashLoanProviderInfo(
                provider=provider,
                is_available=False,
                unavailable_reason=f"Unknown provider: {provider}",
            )

    def is_token_supported(self, token: str, provider: str | None = None) -> bool:
        """Check if a token is supported for flash loans.

        Args:
            token: Token symbol
            provider: Optional specific provider to check

        Returns:
            True if token is supported
        """
        if provider:
            provider = provider.lower()
            if provider == "aave":
                return token in AAVE_SUPPORTED_TOKENS.get(self.chain, set())
            elif provider == "balancer":
                return self.chain in BALANCER_SUPPORTED_CHAINS and token in BALANCER_SUPPORTED_TOKENS.get(
                    self.chain, set()
                )
            elif provider == "morpho":
                return self.chain in MORPHO_SUPPORTED_CHAINS and token in MORPHO_SUPPORTED_TOKENS.get(self.chain, set())
            return False

        # Check any provider
        aave_supported = token in AAVE_SUPPORTED_TOKENS.get(self.chain, set())
        balancer_supported = self.chain in BALANCER_SUPPORTED_CHAINS and token in BALANCER_SUPPORTED_TOKENS.get(
            self.chain, set()
        )
        morpho_supported = self.chain in MORPHO_SUPPORTED_CHAINS and token in MORPHO_SUPPORTED_TOKENS.get(
            self.chain, set()
        )
        return aave_supported or balancer_supported or morpho_supported

    def get_supported_tokens(self, provider: str | None = None) -> set[str]:
        """Get set of tokens supported for flash loans.

        Args:
            provider: Optional specific provider to check

        Returns:
            Set of supported token symbols
        """
        if provider:
            provider = provider.lower()
            if provider == "aave":
                return AAVE_SUPPORTED_TOKENS.get(self.chain, set()).copy()
            elif provider == "balancer":
                if self.chain in BALANCER_SUPPORTED_CHAINS:
                    return BALANCER_SUPPORTED_TOKENS.get(self.chain, set()).copy()
                return set()
            elif provider == "morpho":
                if self.chain in MORPHO_SUPPORTED_CHAINS:
                    return MORPHO_SUPPORTED_TOKENS.get(self.chain, set()).copy()
                return set()
            return set()

        # Union of all providers
        tokens = AAVE_SUPPORTED_TOKENS.get(self.chain, set()).copy()
        if self.chain in BALANCER_SUPPORTED_CHAINS:
            tokens |= BALANCER_SUPPORTED_TOKENS.get(self.chain, set())
        if self.chain in MORPHO_SUPPORTED_CHAINS:
            tokens |= MORPHO_SUPPORTED_TOKENS.get(self.chain, set())
        return tokens

    def estimate_liquidity(
        self,
        token: str,
        provider: str | None = None,
    ) -> dict[str, int]:
        """Estimate available liquidity for a token.

        Args:
            token: Token symbol
            provider: Optional specific provider

        Returns:
            Dict mapping provider name to estimated liquidity in USD
        """
        chain_liquidity = ESTIMATED_LIQUIDITY_USD.get(self.chain, {})
        result: dict[str, int] = {}

        if provider:
            provider = provider.lower()
            provider_liquidity = chain_liquidity.get(provider, {})
            result[provider] = provider_liquidity.get(token, 0)
        else:
            for prov in ["aave", "balancer"]:
                provider_liquidity = chain_liquidity.get(prov, {})
                result[prov] = provider_liquidity.get(token, 0)

        return result

    def _evaluate_providers(
        self,
        token: str,
        amount: Decimal,
        min_liquidity_usd: int,
    ) -> list[FlashLoanProviderInfo]:
        """Evaluate all providers for a flash loan.

        Args:
            token: Token symbol
            amount: Flash loan amount
            min_liquidity_usd: Minimum required liquidity

        Returns:
            List of FlashLoanProviderInfo for each provider
        """
        providers = []

        # Evaluate Aave
        aave_info = self._evaluate_aave(token, amount)
        if min_liquidity_usd > 0 and aave_info.estimated_liquidity_usd < min_liquidity_usd:
            aave_info.is_available = False
            aave_info.unavailable_reason = (
                f"Insufficient liquidity: {aave_info.estimated_liquidity_usd:,} USD "
                f"< required {min_liquidity_usd:,} USD"
            )
        providers.append(aave_info)

        # Evaluate Balancer
        balancer_info = self._evaluate_balancer(token, amount)
        if min_liquidity_usd > 0 and balancer_info.estimated_liquidity_usd < min_liquidity_usd:
            balancer_info.is_available = False
            balancer_info.unavailable_reason = (
                f"Insufficient liquidity: {balancer_info.estimated_liquidity_usd:,} USD "
                f"< required {min_liquidity_usd:,} USD"
            )
        providers.append(balancer_info)

        # Evaluate Morpho
        morpho_info = self._evaluate_morpho(token, amount)
        if min_liquidity_usd > 0 and morpho_info.estimated_liquidity_usd < min_liquidity_usd:
            morpho_info.is_available = False
            morpho_info.unavailable_reason = (
                f"Insufficient liquidity: {morpho_info.estimated_liquidity_usd:,} USD "
                f"< required {min_liquidity_usd:,} USD"
            )
        providers.append(morpho_info)

        return providers

    def _evaluate_aave(
        self,
        token: str,
        amount: Decimal,
    ) -> FlashLoanProviderInfo:
        """Evaluate Aave V3 for flash loan.

        Args:
            token: Token symbol
            amount: Flash loan amount

        Returns:
            FlashLoanProviderInfo for Aave
        """
        # Check if token is supported on Aave for this chain
        supported_tokens = AAVE_SUPPORTED_TOKENS.get(self.chain, set())
        if token not in supported_tokens:
            return FlashLoanProviderInfo(
                provider="aave",
                is_available=False,
                unavailable_reason=f"Token {token} not supported on Aave V3 {self.chain}",
            )

        # Get fee info
        fee_bps = PROVIDER_FEES_BPS["aave"]
        fee_amount = (amount * Decimal(fee_bps)) / Decimal("10000")

        # Get liquidity estimate
        chain_liquidity = ESTIMATED_LIQUIDITY_USD.get(self.chain, {})
        aave_liquidity = chain_liquidity.get("aave", {})
        estimated_liquidity = aave_liquidity.get(token, 10_000_000)  # Default 10M USD

        return FlashLoanProviderInfo(
            provider="aave",
            is_available=True,
            fee_bps=fee_bps,
            fee_amount=fee_amount,
            estimated_liquidity_usd=estimated_liquidity,
            gas_estimate=PROVIDER_GAS_ESTIMATES["aave"],
            pool_address=AAVE_V3_POOL_ADDRESSES[self.chain],
            reliability_score=self.reliability_scores.get("aave", 0.98),
        )

    def _evaluate_balancer(
        self,
        token: str,
        amount: Decimal,
    ) -> FlashLoanProviderInfo:
        """Evaluate Balancer for flash loan.

        Args:
            token: Token symbol
            amount: Flash loan amount

        Returns:
            FlashLoanProviderInfo for Balancer
        """
        # Check if Balancer is supported on this chain
        if self.chain not in BALANCER_SUPPORTED_CHAINS:
            return FlashLoanProviderInfo(
                provider="balancer",
                is_available=False,
                unavailable_reason=f"Balancer not available on {self.chain}",
            )

        # Check if token is supported
        supported_tokens = BALANCER_SUPPORTED_TOKENS.get(self.chain, set())
        if token not in supported_tokens:
            return FlashLoanProviderInfo(
                provider="balancer",
                is_available=False,
                unavailable_reason=f"Token {token} not supported on Balancer {self.chain}",
            )

        # Balancer has zero fees
        fee_bps = PROVIDER_FEES_BPS["balancer"]
        fee_amount = Decimal("0")

        # Get liquidity estimate
        chain_liquidity = ESTIMATED_LIQUIDITY_USD.get(self.chain, {})
        balancer_liquidity = chain_liquidity.get("balancer", {})
        estimated_liquidity = balancer_liquidity.get(token, 5_000_000)  # Default 5M USD

        return FlashLoanProviderInfo(
            provider="balancer",
            is_available=True,
            fee_bps=fee_bps,
            fee_amount=fee_amount,
            estimated_liquidity_usd=estimated_liquidity,
            gas_estimate=PROVIDER_GAS_ESTIMATES["balancer"],
            pool_address=BALANCER_VAULT_ADDRESS,
            reliability_score=self.reliability_scores.get("balancer", 0.95),
        )

    def _evaluate_morpho(
        self,
        token: str,
        amount: Decimal,
    ) -> FlashLoanProviderInfo:
        """Evaluate Morpho Blue for flash loan.

        Morpho Blue offers zero-fee flash loans, making it ideal for
        leverage looping on Morpho Blue markets.

        Args:
            token: Token symbol
            amount: Flash loan amount

        Returns:
            FlashLoanProviderInfo for Morpho
        """
        # Check if Morpho is supported on this chain
        if self.chain not in MORPHO_SUPPORTED_CHAINS:
            return FlashLoanProviderInfo(
                provider="morpho",
                is_available=False,
                unavailable_reason=f"Morpho Blue not available on {self.chain}",
            )

        # Check if token is supported
        supported_tokens = MORPHO_SUPPORTED_TOKENS.get(self.chain, set())
        if token not in supported_tokens:
            return FlashLoanProviderInfo(
                provider="morpho",
                is_available=False,
                unavailable_reason=f"Token {token} not supported on Morpho Blue {self.chain}",
            )

        # Morpho has zero fees
        fee_bps = PROVIDER_FEES_BPS["morpho"]
        fee_amount = Decimal("0")

        # Get liquidity estimate
        chain_liquidity = ESTIMATED_LIQUIDITY_USD.get(self.chain, {})
        morpho_liquidity = chain_liquidity.get("morpho", {})
        estimated_liquidity = morpho_liquidity.get(token, 50_000_000)  # Default 50M USD

        return FlashLoanProviderInfo(
            provider="morpho",
            is_available=True,
            fee_bps=fee_bps,
            fee_amount=fee_amount,
            estimated_liquidity_usd=estimated_liquidity,
            gas_estimate=PROVIDER_GAS_ESTIMATES["morpho"],
            pool_address=MORPHO_BLUE_ADDRESSES.get(self.chain, ""),
            reliability_score=self.reliability_scores.get("morpho", 0.97),
        )

    def _calculate_scores(
        self,
        providers: list[FlashLoanProviderInfo],
        priority: SelectionPriority,
    ) -> None:
        """Calculate overall scores based on priority.

        Modifies providers in place.

        Args:
            providers: List of available providers to score
            priority: Selection priority
        """
        if not providers:
            return

        # Normalize values for scoring
        max_fee = max(p.fee_bps for p in providers) or 1
        max_liquidity = max(p.estimated_liquidity_usd for p in providers) or 1
        max_gas = max(p.gas_estimate for p in providers) or 1

        # Weight configurations for each priority
        # (fee, liquidity, reliability, gas)
        weights = {
            SelectionPriority.FEE: (0.6, 0.2, 0.1, 0.1),
            SelectionPriority.LIQUIDITY: (0.2, 0.6, 0.1, 0.1),
            SelectionPriority.RELIABILITY: (0.1, 0.1, 0.6, 0.2),
            SelectionPriority.GAS: (0.2, 0.1, 0.1, 0.6),
        }

        w = weights.get(priority, weights[SelectionPriority.FEE])

        for p in providers:
            # Fee score: lower fee = lower score (better)
            fee_score = p.fee_bps / max_fee if max_fee > 0 else 0

            # Liquidity score: higher liquidity = lower score (better)
            liquidity_score = 1 - (p.estimated_liquidity_usd / max_liquidity)

            # Reliability score: higher reliability = lower score (better)
            reliability_score = 1 - p.reliability_score

            # Gas score: lower gas = lower score (better)
            gas_score = p.gas_estimate / max_gas if max_gas > 0 else 0

            # Calculate weighted overall score
            p.score = w[0] * fee_score + w[1] * liquidity_score + w[2] * reliability_score + w[3] * gas_score

    def _build_reasoning(
        self,
        best: FlashLoanProviderInfo,
        fallback: FlashLoanProviderInfo | None,
        priority: SelectionPriority,
        all_providers: list[FlashLoanProviderInfo],
    ) -> str:
        """Build human-readable selection reasoning.

        Args:
            best: Best scoring provider
            fallback: Second best provider (if any)
            priority: Selection priority used
            all_providers: All evaluated providers

        Returns:
            Human-readable reasoning string
        """
        parts = []

        # Primary selection
        parts.append(f"Selected {best.provider} based on {priority.value} priority")

        # Key metrics
        fee_desc = "zero" if best.fee_bps == 0 else f"{best.fee_bps} bps"
        parts.append(f"(fee: {fee_desc}, liquidity: ${best.estimated_liquidity_usd:,}, gas: {best.gas_estimate:,})")

        # Fallback info
        if fallback:
            fallback_fee = "zero" if fallback.fee_bps == 0 else f"{fallback.fee_bps} bps"
            parts.append(f"Fallback: {fallback.provider} (fee: {fallback_fee}, score: {fallback.score:.3f})")

        # Score comparison
        if len(all_providers) > 1:
            score_summary = ", ".join(
                f"{p.provider}={p.score:.3f}" for p in sorted(all_providers, key=lambda x: x.score)
            )
            parts.append(f"Scores: {score_summary}")

        return ". ".join(parts)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Main class
    "FlashLoanSelector",
    # Data classes
    "FlashLoanProviderInfo",
    "FlashLoanSelectionResult",
    # Enums
    "SelectionPriority",
    # Exceptions
    "FlashLoanSelectorError",
    "NoProviderAvailableError",
    # Constants
    "DEFAULT_PROVIDER_RELIABILITY",
    "AAVE_V3_POOL_ADDRESSES",
    "BALANCER_VAULT_ADDRESS",
    "MORPHO_BLUE_ADDRESSES",
    "MORPHO_SUPPORTED_TOKENS",
    "PROVIDER_FEES_BPS",
    "PROVIDER_GAS_ESTIMATES",
    "AAVE_SUPPORTED_TOKENS",
    "BALANCER_SUPPORTED_TOKENS",
    "ESTIMATED_LIQUIDITY_USD",
]
