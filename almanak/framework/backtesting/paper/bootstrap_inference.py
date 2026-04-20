"""Bootstrap inference: infer token requirements from strategy.decide().

When a strategy has no explicit bootstrap or anvil_funding config, this module
attempts to discover what tokens the strategy needs by calling decide() with a
synthetic MarketSnapshot containing placeholder prices.

Known limitation: conditional-entry strategies that return HoldIntent on the
first tick will yield empty requirements. This is advisory, not a failure.
"""

import logging
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Default amounts when the intent uses amount_usd (convert from USD placeholder)
_DEFAULT_TOKEN_AMOUNTS: dict[str, Decimal] = {
    "ETH": Decimal("5"),
    "WETH": Decimal("5"),
    "WBTC": Decimal("0.5"),
    "USDC": Decimal("10000"),
    "USDT": Decimal("10000"),
    "DAI": Decimal("10000"),
}
_DEFAULT_AMOUNT = Decimal("10000")  # USD-denominated fallback

# Placeholder prices for synthetic MarketSnapshot
_PLACEHOLDER_PRICES: dict[str, Decimal] = {
    "ETH": Decimal("2000"),
    "WETH": Decimal("2000"),
    "BTC": Decimal("50000"),
    "WBTC": Decimal("50000"),
    "USDC": Decimal("1"),
    "USDT": Decimal("1"),
    "DAI": Decimal("1"),
    "AVAX": Decimal("30"),
    "WAVAX": Decimal("30"),
    "MATIC": Decimal("0.50"),
    "WMATIC": Decimal("0.50"),
    "OP": Decimal("2"),
    "ARB": Decimal("1"),
    "SOL": Decimal("150"),
}

# Safety buffer multiplier for inferred amounts
SAFETY_BUFFER = Decimal("1.5")


def infer_token_requirements(
    strategy: Any,
    chain: str,
) -> dict[str, Decimal]:
    """Infer token requirements by calling strategy.decide() with synthetic data.

    Creates a minimal MarketSnapshot with placeholder prices and empty balances,
    calls decide(), and extracts tokens from the returned intent(s).

    Args:
        strategy: Strategy instance to query.
        chain: Chain name for the MarketSnapshot.

    Returns:
        Dict of token_symbol -> amount (scaled by 1.5x safety buffer).
        Empty dict if decide() returns HoldIntent/None or raises.
    """
    try:
        market = _create_synthetic_snapshot(chain)
        result = strategy.decide(market)
        tokens = _extract_tokens_from_result(result)

        if not tokens:
            logger.info(
                "[paper-trading] Bootstrap inference yielded no token requirements. "
                "Add explicit bootstrap: config if your strategy has conditional entry logic."
            )
            return {}

        # Scale by safety buffer
        scaled = {token: (amount * SAFETY_BUFFER).quantize(Decimal("0.000001")) for token, amount in tokens.items()}
        logger.info("[paper-trading] Inferred token requirements from decide(): %s", scaled)
        return scaled

    except Exception as e:
        logger.warning("[paper-trading] Bootstrap inference failed (decide() raised): %s", e)
        return {}


def check_divergence(
    explicit: dict[str, Decimal],
    inferred: dict[str, Decimal],
    threshold: Decimal = Decimal("0.20"),
) -> None:
    """Warn if explicit and inferred token requirements diverge significantly.

    Compares overlapping tokens only. Logs a warning for each token where the
    difference exceeds the threshold (default 20%).

    Args:
        explicit: Explicit bootstrap/anvil_funding amounts.
        inferred: Amounts inferred from decide() dry-run.
        threshold: Fractional divergence threshold (0.20 = 20%).
    """
    for token in set(explicit) & set(inferred):
        exp_val = explicit[token]
        inf_val = inferred[token]
        if exp_val <= 0 or inf_val <= 0:
            continue
        ratio = abs(exp_val - inf_val) / max(exp_val, inf_val)
        if ratio > threshold:
            logger.warning(
                "[paper-trading] Bootstrap divergence for %s: explicit=%s, inferred=%s (%.0f%% difference). "
                "Consider updating your bootstrap config.",
                token,
                exp_val,
                inf_val,
                ratio * 100,
            )


def _create_synthetic_snapshot(chain: str) -> Any:
    """Create a minimal MarketSnapshot with placeholder prices."""
    from datetime import UTC, datetime

    from almanak.framework.strategies.intent_strategy import MarketSnapshot

    # Explicit wrapped-token alias map (no heuristic stripping)
    _WRAPPED_ALIASES: dict[str, str] = {
        "WAVAX": "AVAX",
        "WETH": "ETH",
        "WMATIC": "MATIC",
        "WBTC": "BTC",
    }

    def _placeholder_price(token: str, quote: str = "USD") -> Decimal:
        upper = token.upper()
        price = _PLACEHOLDER_PRICES.get(upper)
        if price is not None:
            return price
        # Try unwrapped alias
        alias = _WRAPPED_ALIASES.get(upper)
        if alias is not None:
            price = _PLACEHOLDER_PRICES.get(alias)
            if price is not None:
                return price
        # Unknown token — return 0 so _resolve_amount falls through to defaults
        return Decimal("0")

    def _empty_balance(token: str) -> Any:
        from almanak.framework.strategies.strategy_models import TokenBalance

        return TokenBalance(
            symbol=token,
            balance=Decimal("0"),
            balance_usd=Decimal("0"),
        )

    return MarketSnapshot(
        chain=chain,
        wallet_address="0x0000000000000000000000000000000000000000",
        price_oracle=_placeholder_price,
        balance_provider=_empty_balance,
        timestamp=datetime.now(UTC),
    )


def _extract_tokens_from_result(result: Any) -> dict[str, Decimal]:
    """Extract token requirements from a decide() result.

    Handles single intents, IntentSequences, and lists of intents.
    Only extracts tokens that the strategy needs funded (source tokens).
    """
    if result is None:
        return {}

    tokens: dict[str, Decimal] = {}

    # Normalize to a flat list of intents
    intents = _flatten_intents(result)

    for intent in intents:
        intent_tokens = _extract_tokens_from_intent(intent)
        for token, amount in intent_tokens.items():
            if token in tokens:
                # Sum across intents: multiple swaps from same token need cumulative funding
                tokens[token] += amount
            else:
                tokens[token] = amount

    return tokens


def _flatten_intents(result: Any) -> list[Any]:
    """Flatten a decide() result into a list of individual intents."""
    from almanak.framework.intents.vocabulary import IntentSequence

    # Unwrap DecideResult or (intent, context) tuples
    if hasattr(result, "intent"):
        result = result.intent
    elif isinstance(result, tuple) and len(result) >= 1:
        result = result[0]

    if isinstance(result, list):
        flat = []
        for item in result:
            flat.extend(_flatten_intents(item))
        return flat
    elif isinstance(result, IntentSequence):
        return list(result.intents)
    else:
        return [result]


def _extract_tokens_from_intent(intent: Any) -> dict[str, Decimal]:
    """Extract source tokens from a single intent.

    Returns:
        Dict of token -> amount needed for this intent.
    """
    from almanak.framework.intents.vocabulary import HoldIntent, IntentType, SwapIntent

    intent_type = getattr(intent, "intent_type", None)
    if intent_type is None:
        return {}

    if isinstance(intent, HoldIntent) or intent_type == IntentType.HOLD:
        return {}

    if isinstance(intent, SwapIntent) or intent_type == IntentType.SWAP:
        token = getattr(intent, "from_token", None)
        if token:
            amount = _resolve_amount(intent, token)
            return {token: amount}

    if intent_type == IntentType.SUPPLY:
        token = getattr(intent, "token", None)
        if token:
            amount = _resolve_amount(intent, token)
            return {token: amount}

    if intent_type == IntentType.REPAY:
        token = getattr(intent, "token", None)
        if token:
            amount = _resolve_amount(intent, token)
            return {token: amount}

    if intent_type == IntentType.PERP_OPEN:
        token = getattr(intent, "collateral_token", None)
        if token:
            amount = _resolve_amount(intent, token)
            return {token: amount}

    if intent_type == IntentType.VAULT_DEPOSIT:
        token = getattr(intent, "deposit_token", None) or getattr(intent, "token", None)
        if token:
            amount = _resolve_amount(intent, token)
            return {token: amount}

    if intent_type == IntentType.BRIDGE:
        token = getattr(intent, "from_token", getattr(intent, "token", None))
        if token:
            amount = _resolve_amount(intent, token)
            return {token: amount}

    return {}


def _resolve_amount(intent: Any, token: str) -> Decimal:
    """Resolve token amount from an intent, using defaults for USD amounts."""
    # Try direct token amount first
    amount = getattr(intent, "amount", None)
    if isinstance(amount, Decimal) and amount > 0:
        return amount

    # Try collateral_amount (used by PERP_OPEN)
    collateral_amount = getattr(intent, "collateral_amount", None)
    if isinstance(collateral_amount, Decimal) and collateral_amount > 0:
        return collateral_amount

    # Try USD amount and convert to token amount
    amount_usd = getattr(intent, "amount_usd", None)
    if isinstance(amount_usd, Decimal) and amount_usd > 0:
        price = _PLACEHOLDER_PRICES.get(token.upper())
        if price is not None and price > 0:
            return (amount_usd / price).quantize(Decimal("0.000001"))

    # Use default amount for token
    return _DEFAULT_TOKEN_AMOUNTS.get(token.upper(), _DEFAULT_AMOUNT)
