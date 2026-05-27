"""Flash Loan Provider Abstract Base Class.

Defines the interface every flash-loan provider implements so the
selector at ``almanak.framework.intents.flash_loan_selector`` can score
candidates without knowing anything about specific protocols.

Mirrors the bridge precedent (``bridge_base.py`` + ``bridge_selector.py``):
each protocol connector contributes a concrete ``FlashLoanProvider`` from
inside its own directory, and the orchestration layer composes them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

# =============================================================================
# Enums
# =============================================================================


class SelectionPriority(Enum):
    """Priority axis used by the selector to rank candidate providers.

    Attributes:
        FEE: Minimize flash-loan fee (favours zero-fee providers).
        LIQUIDITY: Prefer providers with deeper liquidity for the token.
        RELIABILITY: Prefer more battle-tested providers.
        GAS: Minimize gas overhead of the flash-loan call itself.
    """

    FEE = "fee"
    LIQUIDITY = "liquidity"
    RELIABILITY = "reliability"
    GAS = "gas"


# =============================================================================
# Exceptions
# =============================================================================


class FlashLoanSelectorError(Exception):
    """Base exception for flash-loan selection errors."""


class NoProviderAvailableError(FlashLoanSelectorError):
    """Raised when no provider can fulfill the flash-loan request."""


# =============================================================================
# Data classes
# =============================================================================


@dataclass
class FlashLoanProviderInfo:
    """A provider's quote for a specific (chain, token, amount) request.

    Producers MUST set ``is_available=False`` and populate
    ``unavailable_reason`` when the provider cannot serve the request
    (unsupported chain / token, insufficient liquidity, etc.). All other
    fields may be left at their defaults in that case.

    Attributes:
        provider: Provider name (e.g. ``"aave"``, ``"balancer"``, ``"morpho"``).
        is_available: Whether this provider can serve the request.
        fee_bps: Flash-loan fee in basis points.
        fee_amount: Fee amount in token units for the requested loan.
        estimated_liquidity_usd: Estimated available liquidity in USD.
        gas_estimate: Estimated gas for the flash-loan call (excluding callbacks).
        pool_address: Contract address the orchestrator calls to initiate the loan.
        reliability_score: Historical reliability, 0-1 (higher is better).
        score: Calculated overall score (lower is better). Selector-owned.
        unavailable_reason: Human-readable reason when ``is_available`` is ``False``.
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
    """Result of flash-loan provider selection.

    Attributes:
        provider: Selected provider name (``None`` if no provider available).
        pool_address: Contract address to call for the flash loan.
        fee_bps: Fee in basis points for the selected provider.
        fee_amount: Fee amount in token units for the requested loan.
        total_repay: Total amount to repay (loan + fee).
        gas_estimate: Estimated gas for the flash loan.
        providers_evaluated: Per-provider quotes considered.
        selection_reasoning: Human-readable explanation of the choice.
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
        return self.provider is not None

    def to_dict(self) -> dict[str, Any]:
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
# Provider interface
# =============================================================================


class FlashLoanProvider(ABC):
    """Abstract interface implemented by per-protocol flash-loan providers.

    A provider is a thin object that knows, for a given chain, whether it
    can serve a flash loan of a given token/amount and at what cost.
    Concrete implementations live alongside their protocol connector
    (e.g. ``almanak.connectors.aave_v3.flash_loan_provider``).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for the provider (e.g. ``"aave"``)."""

    @abstractmethod
    def supports(self, chain: str, token: str) -> bool:
        """Whether the provider can serve ``token`` on ``chain``."""

    @abstractmethod
    def quote(self, chain: str, token: str, amount: Decimal) -> FlashLoanProviderInfo:
        """Return availability, fee, liquidity, gas, and pool address for the request.

        Implementations must set ``is_available=False`` and populate
        ``unavailable_reason`` when the provider cannot serve the request.
        """


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "FlashLoanProvider",
    "FlashLoanProviderInfo",
    "FlashLoanSelectionResult",
    "FlashLoanSelectorError",
    "NoProviderAvailableError",
    "SelectionPriority",
]
