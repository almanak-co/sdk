"""Intent extraction utilities for PnL backtesting.

Provides standalone functions for extracting information from intent objects,
including intent type detection, protocol extraction, token identification,
amount calculation, gas estimation, and price execution.

These functions are used by PnLBacktester to introspect intent objects
returned by strategy.decide() calls.

Extracted from pnl/engine.py for module size management.
"""

import logging
from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState

logger = logging.getLogger(__name__)

# Separators seen in perp market identifiers: "ETH/USD" (GMX), "ETH-USD",
# "SOL-PERP" (Drift); bare symbols ("ETH", Hyperliquid) have no separator.
_PERP_MARKET_SEPARATORS = ("/", "-", ":", "_")


def _perp_market_base_token(market: str) -> str | None:
    """Parse the base token symbol from a perp market identifier.

    Returns None for address-style identifiers (0x...), which cannot be
    mapped to a priceable symbol without chain data.
    """
    candidate = market.strip()
    if not candidate or candidate.lower().startswith("0x"):
        return None
    for separator in _PERP_MARKET_SEPARATORS:
        if separator in candidate:
            candidate = candidate.split(separator)[0].strip()
            break
    if not candidate:
        return None
    return candidate.upper()


def lp_pool_tokens(pool: Any) -> tuple[str, str] | None:
    """Parse ``(token0, token1)`` from a symbolic LP pool identifier.

    LP vocabulary intents (``LPOpenIntent``) declare the pair as a single
    ``pool`` string ("WETH/USDC", optionally with a fee-tier or bin-step
    suffix: "WETH/USDC/500") and carry no token0/token1 attributes; this
    mirrors the parsing in ``adapters/lp_adapter.py:_execute_lp_open``.
    Address-style pools (0x...) cannot be mapped to priceable symbols
    without chain data and return None.
    """
    if not isinstance(pool, str):
        return None
    candidate = pool.strip()
    if not candidate or candidate.lower().startswith("0x") or "/" not in candidate:
        return None
    segments = [segment.strip() for segment in candidate.split("/")]
    if not segments[0] or not segments[1]:
        return None
    return segments[0].upper(), segments[1].upper()


def lp_explicit_pair(intent: Any) -> tuple[Any, Any]:
    """Resolve an LP intent's explicit ``(token0, token1)`` attributes.

    Accepts ``token_a``/``token_b`` as aliases (some duck-typed intents use
    them); attributes set to None count as absent. Shared by
    ``get_intent_tokens`` and ``_engine_helpers._resolve_lp_tokens`` so the
    simulated position and its token flows always resolve the same pair.
    """
    token0 = getattr(intent, "token0", None)
    if token0 is None:
        token0 = getattr(intent, "token_a", None)
    token1 = getattr(intent, "token1", None)
    if token1 is None:
        token1 = getattr(intent, "token_b", None)
    return token0, token1


def _decimal_or_none(value: Any) -> Decimal | None:
    """Convert ``value`` to Decimal, returning None when not numeric.

    ``Decimal(str(value))`` round-trips Decimal inputs exactly, so no
    type-dispatch is needed (VIB-4062: no caller-bifurcation on Decimal).
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def intent_is_long(intent: Any) -> bool:
    """Resolve the directional side of a perp intent.

    An explicit ``side`` string ("short") overrides the boolean ``is_long``
    attribute; defaults to long, matching the engine's historical behaviour.
    """
    side = getattr(intent, "side", None)
    if isinstance(side, str) and side.lower() == "short":
        return False
    return bool(getattr(intent, "is_long", True))


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

    Perp intents carry a market identifier ("ETH/USD") instead of token
    attributes; the base symbol goes first so price lookups and the simulated
    position track the traded asset, with the collateral token after it.
    Address-style markets return the UNKNOWN sentinel (the position falls
    back to its entry price) rather than letting the collateral token become
    the priced token, which would hide all price PnL.

    LP vocabulary intents (``LPOpenIntent``) similarly declare the pair as a
    single ``pool`` string ("WETH/USDC") with no token0/token1 attributes;
    the parsed pair goes first so the simulated LP position is price-tracked.
    A fully explicit pair (token0/token1, or the token_a/token_b aliases)
    takes precedence over the pool string; a partially specified pair falls
    back to the pool, mirroring ``_engine_helpers._resolve_lp_tokens`` so
    position tokens and token flows never diverge. Address-style pools
    (0x...) keep the UNKNOWN sentinel.

    Args:
        intent: Intent object

    Returns:
        List of token symbols
    """
    tokens: list[str] = []

    market = getattr(intent, "market", None)
    if isinstance(market, str) and market:
        base_token = _perp_market_base_token(market)
        if base_token is None:
            logger.warning(
                "Cannot resolve a token symbol from perp market %r; the simulated position will not be price-tracked",
                market,
            )
            return ["UNKNOWN"]
        tokens.append(base_token)
        collateral_token = getattr(intent, "collateral_token", None)
        if isinstance(collateral_token, str) and collateral_token and collateral_token.upper() not in tokens:
            tokens.append(collateral_token.upper())

    explicit_token0, explicit_token1 = lp_explicit_pair(intent)
    if explicit_token0 is None or explicit_token1 is None:
        pool_pair = lp_pool_tokens(getattr(intent, "pool", None))
        if pool_pair is not None:
            tokens.extend(pool_token for pool_token in pool_pair if pool_token not in tokens)

    # Common attribute names for tokens (token_a/token_b are LP aliases for
    # token0/token1 -- see lp_explicit_pair)
    for attr in [
        "token",
        "from_token",
        "to_token",
        "token0",
        "token1",
        "token_a",
        "token_b",
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


# Full V3 tick range (MIN_TICK / MAX_TICK) -- the legacy default for LP
# intents that declare no range, making the position behave like V2.
_FULL_RANGE_TICKS = (-887272, 887272)


def get_lp_tick_range(intent: Any, price_to_tick: Callable[[Decimal], int]) -> tuple[int, int]:
    """Resolve the ``(tick_lower, tick_upper)`` range for an LP intent.

    Explicit ``tick_lower``/``tick_upper`` attributes win. LP vocabulary
    intents (``LPOpenIntent``) declare the range as price bounds
    (``range_lower``/``range_upper``) instead, converted via
    ``price_to_tick``; protocols listed in the intent's
    ``_TICK_BASED_LP_PROTOCOLS`` carry raw ticks in those same fields and
    are used directly. Falls back to the full V3 range when no usable
    bounds are present.

    Args:
        intent: Intent object
        price_to_tick: Converter from a positive price to the nearest tick
            (``ImpermanentLossCalculator.price_to_tick``)

    Returns:
        Tuple of (tick_lower, tick_upper)
    """
    tick_lower = getattr(intent, "tick_lower", None)
    tick_upper = getattr(intent, "tick_upper", None)
    if tick_lower is not None and tick_upper is not None:
        return int(tick_lower), int(tick_upper)

    range_lower = _decimal_or_none(getattr(intent, "range_lower", None))
    range_upper = _decimal_or_none(getattr(intent, "range_upper", None))
    if range_lower is None or range_upper is None or range_lower >= range_upper:
        return _FULL_RANGE_TICKS

    tick_based_protocols: frozenset[str] = getattr(intent, "_TICK_BASED_LP_PROTOCOLS", frozenset())
    if getattr(intent, "protocol", None) in tick_based_protocols:
        return int(range_lower), int(range_upper)
    if range_lower <= 0:
        return _FULL_RANGE_TICKS
    return price_to_tick(range_lower), price_to_tick(range_upper)


def _lp_pair_amount_usd(
    intent: Any,
    market_state: MarketState,
    strict_reproducibility: bool,
    track_fallback: Callable[[str], None] | None,
) -> Decimal | None:
    """Price an LP intent's ``amount0``/``amount1`` token legs in USD.

    Returns None when the intent does not carry the LP pair-leg shape
    (both ``amount0`` and ``amount1``), letting the caller's generic
    resolution run. Zero-amount legs need no price (single-sided opens);
    a nonzero leg whose token cannot be resolved or priced raises in
    strict mode and falls back to zero otherwise -- never a $1 guess,
    which would misprice the position (blueprint 31 section 4).
    """
    amount0 = _decimal_or_none(getattr(intent, "amount0", None))
    amount1 = _decimal_or_none(getattr(intent, "amount1", None))
    if amount0 is None or amount1 is None:
        return None

    tokens = get_intent_tokens(intent)
    token0 = tokens[0] if len(tokens) > 0 else "UNKNOWN"
    token1 = tokens[1] if len(tokens) > 1 else "UNKNOWN"

    total = Decimal("0")
    for token, amount in ((token0, amount0), (token1, amount1)):
        if amount == 0:
            continue
        try:
            price: Decimal | None = market_state.get_price(token)
        except KeyError:
            price = None
        if price is None or price <= 0:
            # Zero/negative quotes are bad data, not a $0 valuation -- treat
            # them exactly like a missing price.
            if strict_reproducibility:
                msg = (
                    f"Cannot determine USD amount for LP intent: no positive price available "
                    f"for leg token '{token}'. Set strict_reproducibility=False to use zero as fallback."
                )
                raise ValueError(msg)
            logger.warning(
                "No positive price available for LP leg token '%s' to convert amount %s to USD. "
                "Using zero as fallback to avoid misinterpreting token amounts as USD.",
                token,
                amount,
            )
            if track_fallback:
                track_fallback("default_usd_amount")
            return Decimal("0")
        total += amount * price
    return total


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
    # Check for direct USD amount. size_usd is the perp notional
    # (PerpOpenIntent / PerpCloseIntent) — the fee and slippage base — and
    # must rank above collateral_usd so collateral never shadows notional.
    for attr in ["amount_usd", "notional_usd", "size_usd", "value_usd", "collateral_usd"]:
        if hasattr(intent, attr):
            value = getattr(intent, attr)
            if value is not None:
                return Decimal(str(value))

    # LP vocabulary intents (LPOpenIntent) size the position via per-leg
    # token amounts instead of a USD field; price both legs.
    lp_amount_usd = _lp_pair_amount_usd(intent, market_state, strict_reproducibility, track_fallback)
    if lp_amount_usd is not None:
        return lp_amount_usd

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


def _collateral_usd_from_intent(
    intent: Any,
    market_state: MarketState,
    strict_reproducibility: bool,
    track_fallback: Callable[[str], None] | None,
) -> Decimal | None:
    """Price a perp intent's declared collateral, or None when unresolvable.

    A chained ``"all"`` amount has no value in the generic lane (there is no
    previous-step output to consume), so it resolves to None and the caller
    falls back to ``size_usd / leverage``.
    """
    collateral_amount = getattr(intent, "collateral_amount", None)
    if collateral_amount is None or str(collateral_amount).lower() == "all":
        return None
    amount = _decimal_or_none(collateral_amount)
    if amount is None:
        return None
    collateral_token = getattr(intent, "collateral_token", None)
    if not isinstance(collateral_token, str) or not collateral_token:
        return None
    try:
        return amount * market_state.get_price(collateral_token)
    except KeyError as err:
        if strict_reproducibility:
            msg = (
                f"Cannot price perp collateral: no price available for {collateral_token!r}. "
                "Set strict_reproducibility=False to fall back to size_usd / leverage."
            )
            raise ValueError(msg) from err
        logger.warning(
            "No price for perp collateral token %r; deriving collateral from size_usd / leverage",
            collateral_token,
        )
        if track_fallback:
            track_fallback("default_usd_amount")
        return None


def get_perp_open_params(
    intent: Any,
    market_state: MarketState,
    fallback_amount_usd: Decimal,
    strict_reproducibility: bool = False,
    track_fallback: Callable[[str], None] | None = None,
) -> tuple[Decimal, Decimal]:
    """Resolve ``(collateral_usd, leverage)`` for a PERP_OPEN intent.

    Collateral comes from ``collateral_amount * price(collateral_token)``;
    a chained ``"all"`` amount or an unpriceable collateral token falls back
    to ``size_usd / leverage``. Intents without perp fields (duck-typed test
    intents) keep the legacy semantics: ``fallback_amount_usd`` is the
    collateral and the declared leverage is used as-is.

    When ``size_usd`` is present, the returned leverage is derived as
    ``size_usd / collateral_usd`` so the simulated position's notional
    (``collateral_usd * leverage``) equals the intent's size exactly.
    """
    size_usd = _decimal_or_none(getattr(intent, "size_usd", None))
    leverage = _decimal_or_none(getattr(intent, "leverage", None)) or Decimal("1")
    if leverage <= 0:
        leverage = Decimal("1")

    collateral_usd = _collateral_usd_from_intent(intent, market_state, strict_reproducibility, track_fallback)
    if collateral_usd is None and size_usd is not None:
        collateral_usd = size_usd / leverage
    if collateral_usd is None:
        collateral_usd = fallback_amount_usd
    if size_usd is not None and collateral_usd > 0:
        leverage = size_usd / collateral_usd
    return collateral_usd, leverage


def find_perp_close_position_id(intent: Any, positions: Sequence[Any]) -> str | None:
    """Resolve the simulated position a PERP_CLOSE intent targets.

    Venue position ids (e.g. PancakeSwap Perps' 0x tradeHash) never equal
    simulated ids ("PERP_LONG_gmx_v2_ETH_<ts>"), so after an exact-id check
    the match falls back to (base token from market, side, protocol) — the
    way real venues key perp positions. The oldest matching position wins
    (FIFO) when several are open.

    Args:
        intent: PERP_CLOSE intent object
        positions: Open positions to match against

    Returns:
        The matched simulated position id, or None when nothing matches
    """
    from almanak.framework.backtesting.pnl.position_models import PositionType

    explicit_id = getattr(intent, "position_id", None)
    if isinstance(explicit_id, str) and explicit_id:
        for position in positions:
            if position.position_id == explicit_id:
                return explicit_id

    market = getattr(intent, "market", None)
    base_token = None
    if isinstance(market, str) and market:
        base_token = _perp_market_base_token(market)
        if base_token is None:
            # Fail closed: an unparseable (address-style) market cannot
            # discriminate between open positions, and closing the wrong
            # position silently corrupts the books.
            logger.warning(
                "PERP_CLOSE market %r cannot be resolved to a base token; refusing ambiguous close matching",
                market,
            )
            return None
    is_long = intent_is_long(intent)
    # Resolve protocol with the same resolver the open path used to stamp the
    # position, so protocol_name / connector / adapter spellings match too.
    protocol: str | None = get_intent_protocol(intent)
    if protocol == "default":
        protocol = None

    candidates = []
    for position in positions:
        if not getattr(position, "is_perp", False):
            continue
        if (position.position_type == PositionType.PERP_LONG) != is_long:
            continue
        if base_token is not None:
            position_token = position.tokens[0].upper() if position.tokens else ""
            if position_token != base_token:
                continue
        if protocol and position.protocol and position.protocol.lower() != protocol:
            continue
        candidates.append(position)

    if not candidates:
        logger.warning(
            "PERP_CLOSE matched no open simulated perp position (market=%s, is_long=%s, protocol=%s)",
            market,
            is_long,
            protocol,
        )
        return None
    candidates.sort(key=lambda position: position.entry_time)
    if len(candidates) > 1:
        logger.warning(
            "PERP_CLOSE matched %d open perp positions for market=%s is_long=%s; closing the oldest (%s)",
            len(candidates),
            market,
            is_long,
            candidates[0].position_id,
        )
    return candidates[0].position_id


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
    if intent_type in (IntentType.PERP_OPEN, IntentType.PERP_CLOSE):
        # Adverse slippage per side: open long / close short are buys
        # (higher price); open short / close long are sells (lower price).
        # Perp intents carry no to_token, so direction comes from the side.
        is_long = intent_is_long(intent)
        is_buy = is_long if intent_type == IntentType.PERP_OPEN else not is_long
        if is_buy:
            return market_price * (Decimal("1") + slippage_pct)
        return market_price * (Decimal("1") - slippage_pct)

    if intent_type == IntentType.SWAP:
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
