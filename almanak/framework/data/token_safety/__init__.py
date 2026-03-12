"""Token safety analysis for Solana scam/honeypot detection.

Combines RugCheck and GoPlus APIs to assess whether a Solana token
is safe to trade before committing capital.
"""

from .client import TokenSafetyClient, TokenSafetyError
from .models import (
    GoPlusResult,
    RiskFlag,
    RiskLevel,
    RugCheckResult,
    TokenSafetyResult,
)

__all__ = [
    "GoPlusResult",
    "RiskFlag",
    "RiskLevel",
    "RugCheckResult",
    "TokenSafetyClient",
    "TokenSafetyError",
    "TokenSafetyResult",
]
