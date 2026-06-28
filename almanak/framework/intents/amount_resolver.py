"""Resolve amount='all' for intents before compilation.

This module provides the single point of resolution for `amount="all"`
across all intent types. It is called unconditionally as the first step
of `IntentCompiler.compile()`, ensuring that no unresolved `amount="all"`
reaches protocol-specific compilers.

Resolution semantics by intent type:
- WITHDRAW: Query protocol supply balance (aToken, cToken, shares)
- REPAY: Query protocol debt balance (variable debt, borrow balance)
- SWAP / SUPPLY / BRIDGE / STAKE: Query wallet ERC-20 balance
- CHAINED (amount="all" in IntentSequence): Left for sequence resolution

Design doc: docs/internal/discussions/amount-all-resolution-20260407.md
Ticket: VIB-2537
"""

from __future__ import annotations

import logging
from decimal import Decimal
from enum import Enum
from typing import Any

# The per-protocol balance readers live in the gateway-clean ``balance_readers``
# module so that gateway-boundary entry points (``MarketSnapshot``, the
# pool-history data layer) can consume the reader registry without dragging this
# module's compiler/execution import closure (via ``from . import Intent``) into
# theirs (VIB-5468 / VIB-5484). Re-exported here for backward compatibility with
# existing importers.
from .balance_readers import (
    AaveV3BalanceReader,
    CompoundV3BalanceReader,
    MorphoBlueBalanceReader,
    ProtocolBalanceReader,
    get_reader_for_protocol,
)

logger = logging.getLogger("almanak.framework.intents.amount_resolver")

__all__ = [
    "AaveV3BalanceReader",
    "AmountResolutionCategory",
    "CompoundV3BalanceReader",
    "MorphoBlueBalanceReader",
    "ProtocolBalanceReader",
    "get_reader_for_protocol",
    "resolve_amount_all",
]


class AmountResolutionCategory(Enum):
    """How to resolve amount='all' for a given intent type."""

    WALLET_BALANCE = "wallet_balance"  # ERC-20 wallet balance (swap, supply, bridge)
    PROTOCOL_SUPPLY = "protocol_supply"  # Protocol supply position (withdraw)
    PROTOCOL_DEBT = "protocol_debt"  # Protocol debt position (repay)
    NOT_APPLICABLE = "not_applicable"  # Intent type doesn't use amount="all"


# Map intent types to resolution categories
_INTENT_TYPE_TO_CATEGORY: dict[str, AmountResolutionCategory] = {
    "SWAP": AmountResolutionCategory.WALLET_BALANCE,
    "SUPPLY": AmountResolutionCategory.WALLET_BALANCE,
    "BRIDGE": AmountResolutionCategory.WALLET_BALANCE,
    "STAKE": AmountResolutionCategory.WALLET_BALANCE,
    "VAULT_DEPOSIT": AmountResolutionCategory.WALLET_BALANCE,
    "WRAP_NATIVE": AmountResolutionCategory.WALLET_BALANCE,
    "UNWRAP_NATIVE": AmountResolutionCategory.WALLET_BALANCE,
    "WITHDRAW": AmountResolutionCategory.PROTOCOL_SUPPLY,
    "REPAY": AmountResolutionCategory.PROTOCOL_DEBT,
}


# ---------------------------------------------------------------------------
# Main resolution function
# ---------------------------------------------------------------------------


def resolve_amount_all(  # noqa: C901
    intent: Any,
    *,
    chain: str,
    wallet_address: str,
    gateway_client: Any = None,
) -> Any:
    """Resolve amount='all' on an intent, returning a new intent with concrete amount.

    This function is the single point of resolution for amount='all'. It is called
    unconditionally at the top of IntentCompiler.compile().

    If the intent does not have amount='all', it is returned unchanged.

    Args:
        intent: Any intent object with an 'amount' field
        chain: Target blockchain
        wallet_address: Wallet address for balance queries
        gateway_client: Gateway client for RPC queries

    Returns:
        The intent with resolved amount, or the original intent if no resolution needed.
    """
    # Check if this intent has amount="all"
    amount = getattr(intent, "amount", None)
    if amount != "all":
        return intent

    # Check for withdraw_all flag — if set, adapter handles it; no resolution needed
    if getattr(intent, "withdraw_all", False):
        return intent
    if getattr(intent, "repay_full", False):
        return intent

    intent_type = getattr(intent, "intent_type", None)
    if intent_type is None:
        return intent

    intent_type_str = str(intent_type.value if hasattr(intent_type, "value") else intent_type).upper()
    category = _INTENT_TYPE_TO_CATEGORY.get(intent_type_str, AmountResolutionCategory.NOT_APPLICABLE)

    if category == AmountResolutionCategory.NOT_APPLICABLE:
        return intent  # Let the compiler handle it (or reject it)

    protocol = getattr(intent, "protocol", None)
    protocol_lower = protocol.lower() if protocol else ""
    token = getattr(intent, "token", None) or getattr(intent, "from_token", None) or ""
    market_id = getattr(intent, "market_id", None)

    # -----------------------------------------------------------------------
    # WALLET BALANCE resolution (swap, supply, bridge, stake, etc.)
    # -----------------------------------------------------------------------
    if category == AmountResolutionCategory.WALLET_BALANCE:
        # This path is handled by the existing compiler methods (bridge, wrap, unwrap)
        # and by the caller sites (strategy_runner, teardown). We don't resolve here
        # to avoid duplicating the gas-reservation logic for native tokens.
        # The compiler's per-intent-type methods already handle this correctly.
        return intent

    # -----------------------------------------------------------------------
    # PROTOCOL SUPPLY BALANCE resolution (withdraw)
    # -----------------------------------------------------------------------
    if category == AmountResolutionCategory.PROTOCOL_SUPPLY:
        reader = get_reader_for_protocol(protocol_lower)

        if reader is None:
            logger.warning(
                "No balance reader for protocol '%s' — amount='all' will be converted to withdraw_all=True",
                protocol_lower,
            )
            return _set_withdraw_all(intent)

        # Resolve token address
        token_address = _resolve_token_address(token, chain)
        if not token_address:
            logger.warning(
                "Cannot resolve token '%s' on %s for protocol balance query — falling back to withdraw_all=True",
                token,
                chain,
            )
            return _set_withdraw_all(intent)

        balance_wei = reader.get_supply_balance(
            chain=chain,
            token_address=token_address,
            wallet=wallet_address,
            protocol=protocol_lower,
            market_id=market_id,
            gateway_client=gateway_client,
        )

        if balance_wei is None:
            # Reader couldn't query — fall back to withdraw_all=True
            logger.info(
                "Protocol balance query returned None for %s/%s — using withdraw_all=True",
                protocol_lower,
                token,
            )
            return _set_withdraw_all(intent)

        if balance_wei <= 0:
            logger.info("Protocol supply balance is 0 for %s/%s — nothing to withdraw", protocol_lower, token)
            # Use withdraw_all=True with zero balance; the adapter will handle gracefully.
            # Cannot use amount=0 because WithdrawIntent validates amount > 0 when not withdraw_all.
            return _set_withdraw_all(intent)

        # Convert wei to token units
        try:
            decimals = _get_token_decimals(token, chain)
        except Exception:
            logger.warning(
                "Cannot determine decimals for %s on %s — falling back to withdraw_all=True",
                token,
                chain,
            )
            return _set_withdraw_all(intent)
        amount_decimal = Decimal(balance_wei) / Decimal(10**decimals)
        logger.info(
            "Resolved withdraw amount='all' for %s/%s: %s (from %d wei)",
            protocol_lower,
            token,
            amount_decimal,
            balance_wei,
        )
        return _set_resolved_amount(intent, amount_decimal)

    # -----------------------------------------------------------------------
    # PROTOCOL DEBT BALANCE resolution (repay)
    # -----------------------------------------------------------------------
    if category == AmountResolutionCategory.PROTOCOL_DEBT:
        reader = get_reader_for_protocol(protocol_lower)

        if reader is None:
            logger.warning(
                "No balance reader for protocol '%s' — amount='all' for repay will be converted to repay_full=True",
                protocol_lower,
            )
            return _set_repay_full(intent)

        token_address = _resolve_token_address(token, chain)
        if not token_address:
            logger.warning(
                "Cannot resolve token '%s' on %s for debt balance query — falling back to repay_full=True",
                token,
                chain,
            )
            return _set_repay_full(intent)

        balance_wei = reader.get_debt_balance(
            chain=chain,
            token_address=token_address,
            wallet=wallet_address,
            protocol=protocol_lower,
            market_id=market_id,
            gateway_client=gateway_client,
        )

        if balance_wei is None:
            logger.info(
                "Debt balance query returned None for %s/%s — using repay_full=True",
                protocol_lower,
                token,
            )
            return _set_repay_full(intent)

        if balance_wei <= 0:
            logger.info("Protocol debt balance is 0 for %s/%s — nothing to repay", protocol_lower, token)
            return _set_repay_full(intent)

        try:
            decimals = _get_token_decimals(token, chain)
        except Exception:
            logger.warning(
                "Cannot determine decimals for %s on %s — falling back to repay_full=True",
                token,
                chain,
            )
            return _set_repay_full(intent)
        amount_decimal = Decimal(balance_wei) / Decimal(10**decimals)
        logger.info(
            "Resolved repay amount='all' for %s/%s: %s (from %d wei)",
            protocol_lower,
            token,
            amount_decimal,
            balance_wei,
        )
        return _set_resolved_amount(intent, amount_decimal)

    return intent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_token_address(token: str, chain: str) -> str | None:
    """Resolve a token symbol or address to an address."""
    try:
        from ..data.tokens import get_token_resolver
        from ..data.tokens.resolver import TokenNotFoundError

        resolver = get_token_resolver()
        resolved = resolver.resolve(token, chain)
        return resolved.address
    except (ImportError, TokenNotFoundError):
        # If the token is already an address, use it directly
        if token.startswith("0x") and len(token) == 42:
            return token
        return None
    except Exception:
        logger.debug("Unexpected error resolving token '%s' on %s", token, chain, exc_info=True)
        if token.startswith("0x") and len(token) == 42:
            return token
        return None


def _get_token_decimals(token: str, chain: str) -> int:
    """Get token decimals. Raises if decimals are unknown.

    Per codebase guidelines: never default to 18 decimals.
    The caller should handle the exception (fall back to withdraw_all).
    """
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()
    return resolver.get_decimals(chain, token)


def _set_resolved_amount(intent: Any, amount: Decimal) -> Any:
    """Set a resolved concrete amount on an intent."""
    try:
        from . import Intent

        return Intent.set_resolved_amount(intent, amount)
    except (ImportError, AttributeError, TypeError) as e:
        logger.debug("Intent.set_resolved_amount failed: %s, trying model_copy", e)
        if hasattr(intent, "model_copy"):
            return intent.model_copy(update={"amount": amount})
        return intent


def _set_withdraw_all(intent: Any) -> Any:
    """Convert amount='all' to withdraw_all=True."""
    try:
        if hasattr(intent, "model_copy"):
            return intent.model_copy(update={"withdraw_all": True, "amount": Decimal("0")})
    except Exception as e:
        logger.warning("Failed to set withdraw_all on intent: %s", e)
    return intent


def _set_repay_full(intent: Any) -> Any:
    """Convert amount='all' to repay_full=True."""
    try:
        if hasattr(intent, "model_copy"):
            return intent.model_copy(update={"repay_full": True, "amount": Decimal("0")})
    except Exception as e:
        logger.warning("Failed to set repay_full on intent: %s", e)
    return intent
