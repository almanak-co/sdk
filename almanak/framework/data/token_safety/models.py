"""Data models for token safety analysis.

Dataclasses representing the results of token safety checks from
RugCheck and GoPlus APIs. Used to assess whether a Solana token
is safe to trade (not a scam, honeypot, or rug-pull).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RiskLevel(Enum):
    """Overall risk assessment level."""

    SAFE = "safe"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RiskFlag:
    """A specific risk indicator detected for a token.

    Attributes:
        name: Machine-readable risk identifier (e.g., "mint_authority_enabled").
        description: Human-readable description.
        level: Severity level.
        source: Which API flagged this ("rugcheck", "goplus").
    """

    name: str
    description: str
    level: RiskLevel = RiskLevel.MEDIUM
    source: str = ""


@dataclass(frozen=True)
class RugCheckResult:
    """Result from the RugCheck API.

    Attributes:
        score: Risk score (0 = safest, higher = riskier). RugCheck uses
            a 0-based scoring where low is good.
        risk_level: Categorical risk level from RugCheck.
        risks: List of specific risks identified.
        rugged: Whether the token has already been rugged.
        token_name: Token name from metadata.
        token_symbol: Token symbol from metadata.
        total_market_liquidity: Total liquidity across DEX markets (USD).
        raw_response: Full API response for debugging.
    """

    score: int = 0
    risk_level: str = ""
    risks: list[RiskFlag] = field(default_factory=list)
    rugged: bool = False
    token_name: str = ""
    token_symbol: str = ""
    total_market_liquidity: float = 0.0
    raw_response: dict = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class GoPlusResult:
    """Result from the GoPlus Security API.

    Each boolean field represents a dangerous capability. True = risky.

    Attributes:
        mintable: Token supply can be increased.
        freezable: Token accounts can be frozen by authority.
        closable: Token program can be closed.
        balance_mutable: Authority can modify balances directly.
        has_transfer_fee: Non-zero transfer fee exists.
        transfer_fee_upgradable: Transfer fee can be changed.
        transfer_hook: External transfer hook is attached.
        transfer_hook_upgradable: Transfer hook can be changed.
        metadata_mutable: Token metadata can be changed.
        non_transferable: Token cannot be transferred (soulbound).
        default_account_state_frozen: New accounts start frozen.
        trusted_token: GoPlus marks this as a known/trusted token.
        holder_count: Number of token holders.
        top_holder_pct: Percentage held by the largest single holder.
        raw_response: Full API response for debugging.
    """

    mintable: bool = False
    freezable: bool = False
    closable: bool = False
    balance_mutable: bool = False
    has_transfer_fee: bool = False
    transfer_fee_upgradable: bool = False
    transfer_hook: bool = False
    transfer_hook_upgradable: bool = False
    metadata_mutable: bool = False
    non_transferable: bool = False
    default_account_state_frozen: bool = False
    trusted_token: bool = False
    holder_count: int = 0
    top_holder_pct: float = 0.0
    raw_response: dict = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class TokenSafetyResult:
    """Combined token safety assessment from multiple sources.

    This is the primary result type returned by TokenSafetyClient.check_token().
    Aggregates data from RugCheck and GoPlus into a unified risk assessment.

    Attributes:
        token_address: Solana mint address that was checked.
        risk_level: Overall risk level (worst of all sources).
        risk_score: Normalized risk score (0.0 = safest, 1.0 = most dangerous).
        flags: All risk flags from all sources.
        is_safe: Convenience: True if risk_level is SAFE or LOW.
        rugcheck: Full RugCheck result (None if unavailable).
        goplus: Full GoPlus result (None if unavailable).
        sources: Which APIs contributed to this result.
    """

    token_address: str = ""
    risk_level: RiskLevel = RiskLevel.UNKNOWN
    risk_score: float = 0.5
    flags: list[RiskFlag] = field(default_factory=list)
    rugcheck: RugCheckResult | None = None
    goplus: GoPlusResult | None = None
    sources: list[str] = field(default_factory=list)

    @property
    def is_safe(self) -> bool:
        """Token is considered safe to trade."""
        return self.risk_level in (RiskLevel.SAFE, RiskLevel.LOW)

    @property
    def is_dangerous(self) -> bool:
        """Token has critical/high risk flags — do NOT trade."""
        return self.risk_level in (RiskLevel.HIGH, RiskLevel.CRITICAL)

    @property
    def flag_names(self) -> list[str]:
        """List of all risk flag names for quick filtering."""
        return [f.name for f in self.flags]
