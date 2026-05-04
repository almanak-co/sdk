"""Intent extraction utilities for PnL backtesting.

Provides standalone functions for extracting information from intent objects,
including intent type detection, protocol extraction, token identification,
amount calculation, gas estimation, and price execution.

These functions are used by PnLBacktester to introspect intent objects
returned by strategy.decide() calls.

Extracted from pnl/engine.py for module size management.
"""

import logging
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState

logger = logging.getLogger(__name__)


def extract_intent(decide_result: Any) -> Any:
    """Extract the intent from a decide() result.

    The decide() method can return various types:
    - An Intent object directly
    - None (equivalent to HOLD)
    - A DecideResult with .intent attribute
    - A HoldIntent

    Args:
        decide_result: Raw result from strategy.decide()

    Returns:
        The intent object, or None if no action
    """
    if decide_result is None:
        return None

    # Check if it's a DecideResult with an intent attribute
    if hasattr(decide_result, "intent"):
        return decide_result.intent

    # Check if it's a DecideResult tuple-like (intent, context)
    if isinstance(decide_result, tuple) and len(decide_result) >= 1:
        return decide_result[0]

    # Otherwise, assume it's an intent directly
    return decide_result


def is_hold_intent(intent: Any) -> bool:
    """Check if an intent is a HOLD intent.

    Args:
        intent: Intent to check

    Returns:
        True if this is a hold/no-action intent
    """
    if intent is None:
        return True

    # Check intent_type attribute
    if hasattr(intent, "intent_type"):
        intent_type = intent.intent_type
        if hasattr(intent_type, "value"):
            is_hold: bool = intent_type.value == "HOLD"
            return is_hold
        is_hold_str: bool = str(intent_type) == "HOLD"
        return is_hold_str

    # Check if it's a HoldIntent class
    if hasattr(intent, "__class__"):
        class_name: str = intent.__class__.__name__
        if class_name == "HoldIntent":
            return True

    return False


def get_intent_type(intent: Any) -> IntentType:  # noqa: C901
    """Extract the IntentType from an intent object.

    Args:
        intent: Intent object

    Returns:
        IntentType enum value
    """
    # Check for intent_type attribute
    if hasattr(intent, "intent_type"):
        intent_type_value = intent.intent_type
        # If it's already an IntentType, return it
        if isinstance(intent_type_value, IntentType):
            return intent_type_value
        # If it has a value attribute (enum from another module)
        if hasattr(intent_type_value, "value"):
            try:
                return IntentType(intent_type_value.value)
            except ValueError:
                pass
        # Try direct conversion
        try:
            return IntentType(str(intent_type_value))
        except ValueError:
            pass

    # Check class name for common intent types
    class_name = intent.__class__.__name__.upper()
    if "SWAP" in class_name:
        return IntentType.SWAP
    if "LP_OPEN" in class_name or "LPOPEN" in class_name:
        return IntentType.LP_OPEN
    if "LP_CLOSE" in class_name or "LPCLOSE" in class_name:
        return IntentType.LP_CLOSE
    if "PERP_OPEN" in class_name or "PERPOPEN" in class_name:
        return IntentType.PERP_OPEN
    if "PERP_CLOSE" in class_name or "PERPCLOSE" in class_name:
        return IntentType.PERP_CLOSE
    if "SUPPLY" in class_name:
        return IntentType.SUPPLY
    if "WITHDRAW" in class_name:
        return IntentType.WITHDRAW
    if "BORROW" in class_name:
        return IntentType.BORROW
    if "REPAY" in class_name:
        return IntentType.REPAY
    if "BRIDGE" in class_name:
        return IntentType.BRIDGE
    if "VAULTDEPOSIT" in class_name or "VAULT_DEPOSIT" in class_name:
        return IntentType.VAULT_DEPOSIT
    if "VAULTREDEEM" in class_name or "VAULT_REDEEM" in class_name:
        return IntentType.VAULT_REDEEM
    if "HOLD" in class_name:
        return IntentType.HOLD

    return IntentType.UNKNOWN


def get_intent_protocol(intent: Any) -> str:
    """Extract the protocol from an intent object.

    Args:
        intent: Intent object

    Returns:
        Protocol name string
    """
    # Common attribute names for protocol
    for attr in ["protocol", "protocol_name", "connector", "adapter"]:
        if hasattr(intent, attr):
            value = getattr(intent, attr)
            if value and isinstance(value, str):
                protocol_str: str = value.lower()
                return protocol_str

    # Infer from class name
    class_name = intent.__class__.__name__.lower()
    if "uniswap" in class_name:
        return "uniswap_v3"
    if "gmx" in class_name:
        return "gmx"
    if "aave" in class_name:
        return "aave_v3"
    if "hyperliquid" in class_name:
        return "hyperliquid"
    if "across" in class_name or "stargate" in class_name:
        return "bridge"

    return "default"


def get_intent_tokens(intent: Any) -> list[str]:
    """Extract the tokens involved in an intent.

    Args:
        intent: Intent object

    Returns:
        List of token symbols
    """
    tokens: list[str] = []

    # Common attribute names for tokens
    for attr in [
        "token",
        "from_token",
        "to_token",
        "token0",
        "token1",
        "asset",
        "collateral",
        "borrow_token",
        "supply_token",
        "deposit_token",
    ]:
        if hasattr(intent, attr):
            value = getattr(intent, attr)
            if value and isinstance(value, str) and value not in tokens:
                tokens.append(value.upper())

    # Check for tokens list attribute
    if hasattr(intent, "tokens"):
        intent_tokens = intent.tokens
        if isinstance(intent_tokens, list):
            for t in intent_tokens:
                if isinstance(t, str) and t.upper() not in tokens:
                    tokens.append(t.upper())

    return tokens if tokens else ["UNKNOWN"]


def get_intent_amount_usd(  # noqa: C901
    intent: Any,
    market_state: MarketState,
    strict_reproducibility: bool = False,
    track_fallback: Callable[[str], None] | None = None,
) -> Decimal:
    """Extract or calculate the USD amount for an intent.

    Args:
        intent: Intent object
        market_state: Market state for price lookups
        strict_reproducibility: If True, raise ValueError when USD amount cannot
            be determined. If False, log warning and return raw amount or zero.
        track_fallback: Optional callback to track fallback usage

    Returns:
        Amount in USD

    Raises:
        ValueError: If strict_reproducibility is True and USD amount cannot be
            determined (no USD field, no price available, or no amount field).
    """
    # Check for direct USD amount
    for attr in ["amount_usd", "notional_usd", "value_usd", "collateral_usd"]:
        if hasattr(intent, attr):
            value = getattr(intent, attr)
            if value is not None:
                return Decimal(str(value))

    # Check for amount + token (need to convert to USD)
    amount: Decimal | None = None
    token: str | None = None

    for amount_attr in ["amount", "amount_in", "amount_out", "collateral", "size", "shares"]:
        if hasattr(intent, amount_attr):
            value = getattr(intent, amount_attr)
            if value is not None:
                str_value = str(value)
                if str_value.lower() == "all":
                    continue
                try:
                    amount = Decimal(str_value)
                except Exception:
                    continue
                break

    for token_attr in ["token", "from_token", "asset", "collateral_token", "deposit_token"]:
        if hasattr(intent, token_attr):
            value = getattr(intent, token_attr)
            if value and isinstance(value, str):
                token = value.upper()
                break

    if amount is not None and token:
        try:
            price = market_state.get_price(token)
            return amount * price
        except KeyError as err:
            # Can't convert to USD without price - handle based on strict mode
            if strict_reproducibility:
                msg = (
                    f"Cannot determine USD amount for intent: found amount={amount} for token '{token}' "
                    "but no price available. Set strict_reproducibility=False to use zero as fallback."
                )
                raise ValueError(msg) from err
            logger.warning(
                f"No price available for token '{token}' to convert amount {amount} to USD. "
                "Using zero as fallback to avoid misinterpreting token amount as USD."
            )
            if track_fallback:
                track_fallback("default_usd_amount")
            return Decimal("0")

    # Could not determine USD amount - handle based on strict mode
    if amount is not None:
        # Have raw amount but no token for price lookup
        if strict_reproducibility:
            msg = (
                f"Cannot determine USD amount for intent: found amount={amount} but no token "
                "for price lookup. Set strict_reproducibility=False to use zero as fallback."
            )
            raise ValueError(msg)
        logger.warning(
            f"Intent has amount={amount} but no token for USD conversion. "
            "Using zero as fallback to avoid misinterpreting token amount as USD."
        )
        if track_fallback:
            track_fallback("default_usd_amount")
        return Decimal("0")

    # No amount found at all
    if strict_reproducibility:
        msg = (
            "Cannot determine USD amount for intent: no USD amount field and no "
            "token amount found. Set strict_reproducibility=False to use zero as fallback."
        )
        raise ValueError(msg)
    logger.warning("Intent has no USD amount or token amount field. Using zero as fallback to avoid arbitrary values.")
    if track_fallback:
        track_fallback("default_usd_amount")
    return Decimal("0")


def estimate_gas_for_intent(intent_type: IntentType) -> int:
    """Estimate gas usage for an intent type.

    Args:
        intent_type: Type of intent

    Returns:
        Estimated gas units
    """
    # Gas estimates based on typical transaction costs across protocols.
    # These are conservative estimates for gas cost calculations in backtests.
    # Actual gas usage varies by protocol, chain, and execution conditions.
    #
    # Uniswap V3 swaps: ~130k-180k (depends on pools in route)
    # Aave V3 supply/withdraw: ~200k-250k
    # GMX V2 market orders: ~300k-500k
    # LP operations: ~250k-400k
    gas_estimates: dict[IntentType, int] = {
        IntentType.SWAP: 180000,  # Conservative for multi-hop swaps
        IntentType.LP_OPEN: 400000,  # NFT mint + liquidity add
        IntentType.LP_CLOSE: 300000,  # NFT burn + liquidity remove
        IntentType.SUPPLY: 220000,  # Aave/Compound supply
        IntentType.WITHDRAW: 220000,  # Aave/Compound withdraw
        IntentType.BORROW: 280000,  # Includes collateral checks
        IntentType.REPAY: 220000,  # Aave/Compound repay
        IntentType.PERP_OPEN: 450000,  # GMX V2 market increase
        IntentType.PERP_CLOSE: 350000,  # GMX V2 market decrease
        IntentType.BRIDGE: 200000,  # Cross-chain bridge
        IntentType.VAULT_DEPOSIT: 250000,  # ERC-4626 deposit (approve + deposit)
        IntentType.VAULT_REDEEM: 200000,  # ERC-4626 redeem
        IntentType.HOLD: 0,  # No execution
        IntentType.UNKNOWN: 200000,  # Conservative default
    }
    return gas_estimates.get(intent_type, 200000)


def get_executed_price(
    intent: Any,
    market_state: MarketState,
    slippage_pct: Decimal,
    intent_type: IntentType,
) -> Decimal:
    """Get the executed price for an intent after applying slippage.

    For swaps and perps, the executed price is the market price adjusted
    for slippage. For other intent types, we use the market price directly.

    Args:
        intent: Intent object
        market_state: Market state for price lookups
        slippage_pct: Slippage percentage as decimal
        intent_type: Type of intent

    Returns:
        Executed price after slippage
    """
    # Get the primary token for price lookup
    tokens = get_intent_tokens(intent)
    primary_token = tokens[0] if tokens else "WETH"

    # Get market price
    try:
        market_price = market_state.get_price(primary_token)
    except KeyError:
        market_price = Decimal("1")

    # Apply slippage for market orders
    if intent_type in (IntentType.SWAP, IntentType.PERP_OPEN, IntentType.PERP_CLOSE):
        # Slippage is adverse: buying gets a higher price, selling gets a lower price.
        # primary_token = tokens[0], which for swaps is from_token (the token being sold).
        # Determine direction by checking to_token: if the intent has a to_token that
        # matches primary_token, we're BUYING it (pay more). Otherwise we're selling it
        # (receive less).
        to_token = getattr(intent, "to_token", None)
        if to_token and to_token.upper() == primary_token.upper():
            # Buying primary_token: adverse slippage means higher price
            return market_price * (Decimal("1") + slippage_pct)
        # Selling primary_token (or no to_token): adverse slippage means lower price
        return market_price * (Decimal("1") - slippage_pct)

    return market_price
