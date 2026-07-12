"""CLI command for scaffolding new strategies.

Usage:
    almanak new-strategy --template <template> --name <name> --chain <chain>

Example:
    almanak new-strategy --template dynamic_lp --name my_strategy --chain arbitrum
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import click

from almanak.core.chains import DEFAULT_CHAIN, ChainRegistry
from almanak.framework.anvil.accounts import anvil_default_address
from almanak.framework.cli.chain_params import ChainChoice


class StrategyTemplate(StrEnum):
    """Available strategy templates."""

    BLANK = "blank"
    TA_SWAP = "ta_swap"
    DYNAMIC_LP = "dynamic_lp"
    LENDING_LOOP = "lending_loop"
    BASIS_TRADE = "basis_trade"
    VAULT_YIELD = "vault_yield"
    COPY_TRADER = "copy_trader"
    PERPS = "perps"
    MULTI_STEP = "multi_step"
    STAKING = "staking"


# Aliases accepted in addition to canonical StrategyTemplate values.
# Edge sends semantic names that don't exactly match the SDK enum;
# rather than push translation into every Edge consumer (AlmanakCode,
# the future Portfolio Manager, etc.), absorb the mapping here. VIB-3703.
TEMPLATE_ALIASES: dict[str, "StrategyTemplate"] = {
    "swap": StrategyTemplate.TA_SWAP,
    "bridge": StrategyTemplate.MULTI_STEP,
}


class UnknownTemplateError(ValueError):
    """Raised when a template string matches neither a StrategyTemplate value nor an alias."""


def parse_template(value: str) -> StrategyTemplate:
    """Resolve a user/Edge-supplied template string to a StrategyTemplate.

    Accepts canonical enum values (`ta_swap`, `dynamic_lp`, ...) and the
    aliases in TEMPLATE_ALIASES. Comparison is case-insensitive and tolerant
    of leading/trailing whitespace.

    Raises UnknownTemplateError with a message that lists every valid value
    and alias when the input matches nothing.
    """
    if not isinstance(value, str):
        raise UnknownTemplateError(
            f"Template must be a string, got {type(value).__name__}. "
            f"Valid: {', '.join(t.value for t in StrategyTemplate)}. "
            f"Aliases: {', '.join(f'{a} -> {t.value}' for a, t in TEMPLATE_ALIASES.items())}."
        )
    normalized = value.strip().lower()
    try:
        return StrategyTemplate(normalized)
    except ValueError:
        pass
    if normalized in TEMPLATE_ALIASES:
        return TEMPLATE_ALIASES[normalized]
    raise UnknownTemplateError(
        f"{value!r} is not a known SDK template. "
        f"Valid: {', '.join(t.value for t in StrategyTemplate)}. "
        f"Aliases: {', '.join(f'{a} -> {t.value}' for a, t in TEMPLATE_ALIASES.items())}."
    )


# Structured warning codes emitted by validate_lending_loop_template.
# AlmanakCode's scaffold planner greps for these prefixes to surface the
# message in its own telemetry, so the strings are part of the public
# contract — do not rename without notifying the AlmanakCode owners.
LENDING_LOOP_INCOMPLETE = "LENDING_LOOP_INCOMPLETE"
LENDING_LOOP_CROSS_PROTOCOL = "LENDING_LOOP_CROSS_PROTOCOL"


def validate_lending_loop_template(supply_protocol: str, borrow_protocol: str | None = None) -> list[str]:
    """Validate a `lending_loop` scaffold input and return human-readable warnings.

    The lending_loop SDK template loops supply + borrow on a *single* protocol.
    Edge signals frequently describe a cross-protocol arb (supply on aave_v3,
    borrow on morpho-blue) and AlmanakCode silently drops the borrow leg when it
    forces the signal into the lending_loop mold. The result is a supply-only
    strategy whose declared "arb" is unrealisable — the QA tester sees no error,
    but the alpha is gone.

    Returns:
        A list of warning strings (empty when configuration is consistent).
        Each string starts with a structured prefix from
        ``LENDING_LOOP_INCOMPLETE`` / ``LENDING_LOOP_CROSS_PROTOCOL`` so callers
        (CLI banner, AlmanakCode planner) can route them.

    Args:
        supply_protocol: protocol resolved from sdkSpec.protocol (always set).
        borrow_protocol: protocol intended for the borrow leg, or None when the
            Edge signal omits it / buries it in `metadata` only.
    """
    warnings: list[str] = []
    normalized_supply = (supply_protocol or "").strip()
    if not normalized_supply:
        warnings.append(
            f"{LENDING_LOOP_INCOMPLETE}: lending_loop scaffold received empty supply_protocol; cannot validate."
        )
        return warnings

    normalized_borrow = (borrow_protocol or "").strip() or None
    if normalized_borrow is None:
        warnings.append(
            f"{LENDING_LOOP_INCOMPLETE}: Strategy declares lending_loop template but "
            "no borrow leg is configured. Resulting strategy is supply-only and "
            "will not realize the arb spread. "
            f"supply_protocol={normalized_supply}, borrow_protocol=<unset>."
        )
    elif normalized_borrow.lower() != normalized_supply.lower():
        # Case-insensitive equality so "AAVE_V3" and "aave_v3" don't trip the
        # cross-protocol warning.
        warnings.append(
            f"{LENDING_LOOP_CROSS_PROTOCOL}: lending_loop template loops supply "
            "and borrow on a single protocol, but the scaffold input asks for a "
            f"cross-protocol pair (supply_protocol={normalized_supply}, "
            f"borrow_protocol={normalized_borrow}). Use the multi_step template "
            "to express cross-protocol lending arbitrage."
        )
    return warnings


def _normalize_and_validate_scaffold_protocol(template_enum: StrategyTemplate, protocol: str | None) -> str | None:
    """Normalize the ``--protocol`` choice and reject incompatible combinations.

    Returns the normalized (``strip().lower()``) protocol slug, or ``None`` when
    the caller passed nothing. Raises ``click.Abort`` (after echoing to stderr)
    for a malformed slug, or for a ``multi_step`` scaffold paired with a
    tick-spacing protocol.

    ``multi_step`` still opens LPs with price-denominated ``range_lower`` /
    ``range_upper``; only ``dynamic_lp`` emits spacing-aligned integer ticks
    today (VIB-5557 follow-up). A tick-spacing protocol (Aerodrome Slipstream)
    would feed a price band into the Aerodrome compiler's
    ``_validate_slipstream_tick_bounds`` and fail the first ``LP_OPEN`` at
    compile time — so reject it at scaffold time and point at ``dynamic_lp``
    rather than generating code that cannot compile.
    """
    if protocol is not None:
        protocol = protocol.strip().lower() or None
    if protocol is None:
        return None

    if not re.fullmatch(r"[a-z0-9][a-z0-9_.\-]*", protocol):
        click.echo(
            f"Error: invalid --protocol {protocol!r}: expected a protocol slug like "
            "aerodrome_slipstream, morpho_blue, or hyperliquid.",
            err=True,
        )
        raise click.Abort()

    from almanak.framework.agent_tools.schemas import _normalize_protocol_key

    # Canonicalize the RETURNED slug, not just the gate check below: the value
    # is emitted verbatim into the generated strategy (config.json + decorator
    # + ``self.protocol``), and the generated tick-logic compares against
    # canonical underscore literals (e.g. ``self.protocol ==
    # "aerodrome_slipstream"``). Returning a hyphenated alias would pass the
    # slug regex here yet silently route the scaffold's first LP_OPEN down the
    # price-band path and fail Slipstream tick validation at compile time —
    # the exact failure this scaffold batch exists to prevent.
    protocol = _normalize_protocol_key(protocol)

    if template_enum == StrategyTemplate.MULTI_STEP:
        from almanak.connectors._strategy_protocol_family_registry import (
            PROTOCOL_FAMILY_REGISTRY,
            ProtocolFamily,
        )

        tick_spacing_protocols = PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.TICK_SPACING_FEE_DISPLAY)
        if _normalize_protocol_key(protocol) in tick_spacing_protocols:
            click.echo(
                f"Error: the multi_step template cannot scaffold a tick-spacing protocol "
                f"({protocol}). multi_step opens LPs with price-denominated ranges, but "
                f"Slipstream-style pools require spacing-aligned integer ticks -- the first "
                f"LP_OPEN would fail to compile. Use --template dynamic_lp for {protocol}.",
                err=True,
            )
            raise click.Abort()

    return protocol


@dataclass
class TemplateConfig:
    """Configuration for a strategy template."""

    name: str
    description: str
    default_protocol: str
    config_params: dict[str, str]


# Chain-specific anvil_funding defaults (native + wrapped + WETH + USDC).
# Chains not listed here get the ETH-native default in generate_config_json().
_CHAIN_NATIVE_FUNDING: dict[str, dict[str, object]] = {
    "mantle": {"MNT": 1000, "WMNT": 10, "WETH": 5, "USDC": 10000},
    "avalanche": {"AVAX": 100, "WAVAX": 10, "WETH": 5, "USDC": 10000},
    "bsc": {"BNB": 10, "WBNB": 5, "WETH": 5, "USDC": 10000},
    "polygon": {"MATIC": 1000, "WMATIC": 100, "WETH": 5, "USDC": 10000},
    "sonic": {"S": 100, "WETH": 5, "USDC": 10000},
    "monad": {"MON": 100, "WETH": 5, "USDC": 10000},
    "zerog": {"A0GI": 50, "W0G": 20, "USDC.E": 100},
}

_DEFAULT_ANVIL_FUNDING: dict[str, object] = {"ETH": 10, "WETH": 5, "USDC": 10000}

# Default token_funding entries as (symbol, amount, amount_type). Addresses are
# resolved per-chain at scaffold time from the static token registry — the
# generator knows the chain, so it must never emit zero-address placeholders
# that users have to hand-replace.
_DEFAULT_TOKEN_FUNDING_SPECS: tuple[tuple[str, str, str], ...] = (
    ("WETH", "1", "token"),
    ("USDC", "5000", "usd"),
)


def _default_token_funding(chain: str) -> list[dict[str, str]]:
    """Build the default ``token_funding`` list with real per-chain addresses.

    Resolves each default symbol through the token registry's static layers
    (``get_token_resolver`` with ``skip_gateway=True`` — no gateway runs at
    scaffold time). Symbols the registry does not know on *chain* are omitted
    entirely: an unmeasured address must never be fabricated as ``0x000…0``
    (Empty ≠ Zero).
    """
    from almanak.framework.data.tokens import get_token_resolver

    resolver = get_token_resolver()
    entries: list[dict[str, str]] = []
    for symbol, amount, amount_type in _DEFAULT_TOKEN_FUNDING_SPECS:
        # Best-effort cosmetic default: any resolver failure (not just the
        # documented TokenResolutionError — an unsupported chain surfaces as
        # ValueError/KeyError from the normalizer, and helper bugs can raise
        # anything) must degrade to "omit this symbol", never abort scaffolding.
        try:
            resolved = resolver.resolve(symbol, chain, log_errors=False, skip_gateway=True)
        except Exception:  # noqa: BLE001 — scaffold-time best-effort default
            continue
        # Empty ≠ Zero: a resolver that returns nothing (or an addressless
        # record) leaves the symbol unmeasured — never fabricate a 0x000…0
        # placeholder the user would have to hand-replace.
        if resolved is None or not getattr(resolved, "address", None):
            continue
        entries.append(
            {
                "symbol": symbol,
                "address": resolved.address,
                "amount": amount,
                "amount_type": amount_type,
            }
        )
    return entries


# Template configurations with sensible defaults
TEMPLATE_CONFIGS: dict[StrategyTemplate, TemplateConfig] = {
    StrategyTemplate.BLANK: TemplateConfig(
        name="Blank",
        description="Minimal strategy template for custom implementations",
        default_protocol="custom",
        config_params={},
    ),
    StrategyTemplate.TA_SWAP: TemplateConfig(
        name="TA Swap",
        description="Technical analysis swap strategy with configurable RSI, Bollinger Bands, or combined signals",
        default_protocol="uniswap_v3",
        config_params={
            "indicator": "rsi",
            "base_token": "WETH",
            "quote_token": "USDC",
        },
    ),
    StrategyTemplate.DYNAMIC_LP: TemplateConfig(
        name="Dynamic LP",
        description="Price-based LP range management with position tracking and rebalancing",
        default_protocol="uniswap_v3",
        config_params={
            "range_width_pct": "5",
            "rebalance_threshold_pct": "80",
        },
    ),
    StrategyTemplate.LENDING_LOOP: TemplateConfig(
        name="Lending Loop",
        description="Supply/borrow leverage loop with state machine and health monitoring",
        default_protocol="aave_v3",
        config_params={
            "collateral_token": "WETH",
            "borrow_token": "USDC",
        },
    ),
    StrategyTemplate.BASIS_TRADE: TemplateConfig(
        name="Basis Trade",
        description="Spot+perp delta-neutral strategy capturing funding rate arbitrage",
        default_protocol="gmx_v2",
        config_params={
            "base_token": "WETH",
            "perp_market": "ETH/USD",
        },
    ),
    StrategyTemplate.VAULT_YIELD: TemplateConfig(
        name="Vault Yield",
        description="ERC-4626 vault deposit/redeem strategy for optimized DeFi lending yield",
        default_protocol="metamorpho",
        config_params={
            "vault_address": "0x0000000000000000000000000000000000000000",
            "deposit_token": "USDC",
        },
    ),
    StrategyTemplate.COPY_TRADER: TemplateConfig(
        name="Copy Trader",
        description="Copy trading strategy that monitors leader wallets and replicates trades",
        default_protocol="uniswap_v3",
        config_params={
            "fixed_usd": "100",
            "max_trade_usd": "1000",
            "max_slippage": "0.01",
        },
    ),
    StrategyTemplate.PERPS: TemplateConfig(
        name="Perps",
        description="Perpetual futures trading with take-profit and stop-loss levels",
        default_protocol="gmx_v2",
        config_params={
            "market": "ETH/USD",
            "collateral_token": "USDC",
            "direction": "LONG",
        },
    ),
    StrategyTemplate.MULTI_STEP: TemplateConfig(
        name="Multi Step",
        description="Atomic multi-step operations using IntentSequence for LP rebalancing",
        default_protocol="uniswap_v3",
        config_params={
            "pool": "WETH/USDC/3000",
            "base_token": "WETH",
            "quote_token": "USDC",
        },
    ),
    StrategyTemplate.STAKING: TemplateConfig(
        name="Staking",
        description="Liquid staking strategy with optional token swap before staking",
        default_protocol="lido",
        config_params={
            "stake_token": "ETH",
            "stake_amount": "1",
        },
    ),
}


# -----------------------------------------------------------------------------
# Template state machine definitions
# -----------------------------------------------------------------------------
# Each stateful template gets its own typed ``StrEnum`` in the scaffolded
# strategy. Using StrEnum instead of raw string literals (``"idle"``,
# ``"open"``) gives authors:
#   - Editor/LSP completion and rename support
#   - ``mypy`` / static type safety (typos are compile-time errors)
#   - Grep-ability (``grep LendingLoopState.BORROWED`` is far more precise
#     than grepping ``"borrowed"``)
#
# Backwards compatibility: ``StrEnum`` members ARE strings, so
#   - ``json.dumps(state)`` serializes to the bare string value
#     (old persisted state files keep working unchanged)
#   - ``state == "idle"`` still evaluates to ``True`` for
#     ``LendingLoopState.IDLE`` (existing tests keep working unchanged)
#   - ``<EnumClass>(raw_string)`` coerces a plain string back to the
#     enum member (used in ``load_persistent_state`` hooks)
#
# Format: (state_attribute_name, enum_class_name, [(MEMBER_NAME, value), ...])
# -----------------------------------------------------------------------------
_TEMPLATE_STATE_ENUMS: dict[StrategyTemplate, tuple[str, str, tuple[tuple[str, str], ...]]] = {
    StrategyTemplate.LENDING_LOOP: (
        "_loop_state",
        "LendingLoopState",
        (
            ("IDLE", "idle"),
            ("SUPPLIED", "supplied"),
            ("BORROWED", "borrowed"),
            ("MONITORING", "monitoring"),
        ),
    ),
    StrategyTemplate.BASIS_TRADE: (
        "_trade_state",
        "BasisTradeState",
        (
            ("IDLE", "idle"),
            ("SPOT_BOUGHT", "spot_bought"),
            ("HEDGED", "hedged"),
            ("UNWINDING", "unwinding"),
        ),
    ),
    StrategyTemplate.VAULT_YIELD: (
        "_state",
        "VaultYieldState",
        (
            ("IDLE", "idle"),
            ("DEPOSITED", "deposited"),
        ),
    ),
    StrategyTemplate.PERPS: (
        "_position_state",
        "PerpsState",
        (
            ("IDLE", "idle"),
            ("OPEN", "open"),
        ),
    ),
    StrategyTemplate.STAKING: (
        "_stake_state",
        "StakingState",
        (
            ("IDLE", "idle"),
            ("STAKED", "staked"),
        ),
    ),
}


def _generate_state_enum_definition(template: StrategyTemplate) -> str:
    """Return the ``class <Template>State(StrEnum): ...`` source block.

    Returns an empty string for templates without a state machine. The emitted
    code lives at module level above the strategy class so external code
    (e.g. tests or AlmanakCode generation) can import and reference it.
    """
    if template not in _TEMPLATE_STATE_ENUMS:
        return ""
    _attr, cls, members = _TEMPLATE_STATE_ENUMS[template]
    lines = [
        f"class {cls}(StrEnum):",
        f'    """Typed state machine values for the {template.value} strategy template.',
        "",
        "    Inherits from ``StrEnum`` so persisted state files (JSON) round-trip as",
        f"    plain strings. Use ``{cls}(raw_value)`` to coerce a loaded string back",
        "    to the enum member (see ``load_persistent_state``).",
        '    """',
        "",
    ]
    for member_name, member_value in members:
        lines.append(f'    {member_name} = "{member_value}"')
    return "\n".join(lines) + "\n"


def _quote_asset_decorator_line(template: StrategyTemplate, chain: str) -> str:
    """Render the ``quote_asset=...,`` decorator line for a template.

    Emitted explicitly for every template (USD is the framework default, but
    an omitted field is invisible — an explicit one documents the decision).
    Staking is the one template whose goal is to grow the staked asset rather
    than USD value, so it quotes in the chain's wrapped native, resolved from
    :class:`ChainRegistry` — the scaffold never invents an address. Chains
    missing from the registry fall back to USD with a TODO.
    """
    if template is StrategyTemplate.STAKING:
        descriptor = ChainRegistry.try_resolve(chain)
        if descriptor is not None and descriptor.native.wrapped_address:
            native = descriptor.native
            return (
                f"# PnL measured in {native.wrapped_symbol} (the staked asset), not USD\n"
                f'    quote_asset={{"type": "token", "chain_id": {descriptor.chain_id}, '
                f'"address": "{native.wrapped_address}"}},'
            )
        return 'quote_asset="USD",  # TODO: quote in the staked token (chain not in SDK registry)'
    return 'quote_asset="USD",  # performance denomination; token form only for accumulators'


def to_snake_case(name: str) -> str:
    """Convert a string to snake_case."""
    # Replace spaces and hyphens with underscores
    name = re.sub(r"[\s\-]+", "_", name)
    # Insert underscore before uppercase letters and convert to lowercase
    name = re.sub(r"([A-Z])", r"_\1", name).lower()
    # Remove leading underscores and collapse multiple underscores
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def to_pascal_case(name: str) -> str:
    """Convert a string to PascalCase."""
    snake = to_snake_case(name)
    return "".join(word.capitalize() for word in snake.split("_"))


def _get_template_decide_logic(template: StrategyTemplate, config: TemplateConfig) -> str:
    """Generate template-specific decide() logic."""
    if template == StrategyTemplate.TA_SWAP:
        return """
            indicator = getattr(self, '_indicator', 'rsi')

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Reconcile the cached position-side flag against live balance each
            # cycle: the persisted `_holding_base` flag is only a HINT; the live
            # wallet balance is TRUTH. Without this, a stale/false flag (e.g.
            # after a restart whose runtime state desynced) could HOLD-lock a
            # valid risk-off exit even though the wallet actually holds base.
            # See VIB-5155 / ALM-2719.
            self._reconcile_holding_base(market, base_balance=base_balance)

            buy_signal = False
            sell_signal = False
            reason = ""

            # RSI analysis
            if indicator in ("rsi", "rsi_bb"):
                try:
                    rsi = market.rsi(self.base_token, period=self.rsi_period)
                    if rsi.value <= self.rsi_oversold:
                        buy_signal = True
                        reason = f"RSI oversold ({rsi.value:.1f})"
                    elif rsi.value >= self.rsi_overbought:
                        sell_signal = True
                        reason = f"RSI overbought ({rsi.value:.1f})"
                    else:
                        reason = f"RSI neutral ({rsi.value:.1f})"
                except ValueError as e:
                    logger.warning(f"RSI unavailable: {e}")
                    return Intent.hold(reason="RSI data unavailable")

            # Bollinger Bands analysis
            if indicator in ("bollinger", "rsi_bb"):
                try:
                    bb = market.bollinger_bands(self.base_token, period=self.bb_period, std_dev=self.bb_std_dev)
                    if bb.bandwidth < self.squeeze_threshold:
                        return Intent.hold(reason=f"BB squeeze (bandwidth={bb.bandwidth:.4f})")
                    bb_buy = bb.percent_b <= self.buy_percent_b
                    bb_sell = bb.percent_b >= self.sell_percent_b
                    if indicator == "bollinger":
                        buy_signal = bb_buy
                        sell_signal = bb_sell
                        reason = f"%B={bb.percent_b:.4f}"
                    elif indicator == "rsi_bb":
                        buy_signal = buy_signal and bb_buy
                        sell_signal = sell_signal and bb_sell
                        reason += f", %B={bb.percent_b:.4f}"
                except ValueError as e:
                    logger.warning(f"BB unavailable: {e}")
                    if indicator == "bollinger":
                        return Intent.hold(reason="BB data unavailable")
                    # rsi_bb mode: falling back to RSI-only signals
                    logger.warning("Bollinger Bands unavailable in rsi_bb mode -- falling back to RSI-only signals")

            # Neutral re-arm: act only when a signal first appears, not every tick
            # the indicator stays in the extreme zone. Reset to neutral here when
            # there's no signal; the buy/sell latch is set in on_intent_executed on
            # a SUCCESSFUL swap, so a held-back (gas/balance) or failed swap never
            # locks out the next attempt.
            current_signal = "buy" if buy_signal else "sell" if sell_signal else "neutral"
            if current_signal == "neutral":
                self._last_signal = "neutral"
                return Intent.hold(reason=reason or "No signal")
            if current_signal == self._last_signal:
                return Intent.hold(
                    reason=f"{reason} -- already acted on this {current_signal} signal; awaiting neutral reset"
                )

            if buy_signal and quote_balance.balance_usd >= self.trade_size_usd:
                # Gas-worthiness gate: don't pay $5 gas to move $1. Authors can
                # tune via `min_trade_value_usd` (absolute floor) and
                # `max_gas_ratio` (dynamic ratio) in config.json.
                if self.trade_size_usd < self.min_trade_value_usd:
                    return Intent.hold(
                        reason=f"trade size ${self.trade_size_usd} below min_trade_value_usd "
                        f"${self.min_trade_value_usd}"
                    )
                if not market.is_trade_worthwhile(
                    amount_usd=self.trade_size_usd,
                    chain=market.chain,
                    max_gas_ratio=self.max_gas_ratio,
                ):
                    gas_cost = market.estimate_swap_gas_cost_usd(market.chain)
                    return Intent.hold(
                        reason=f"gas cost ${gas_cost} exceeds {self.max_gas_ratio:.2%} of trade value "
                        f"${self.trade_size_usd}"
                    )
                logger.info(f"BUY: {reason}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                )
            elif sell_signal:
                base_price = market.price(self.base_token)
                min_sell = self.trade_size_usd / base_price if base_price > 0 else Decimal("0")
                if base_balance.balance >= min_sell:
                    # Gas-worthiness gate (same as buy branch).
                    if self.trade_size_usd < self.min_trade_value_usd:
                        return Intent.hold(
                            reason=f"trade size ${self.trade_size_usd} below min_trade_value_usd "
                            f"${self.min_trade_value_usd}"
                        )
                    if not market.is_trade_worthwhile(
                        amount_usd=self.trade_size_usd,
                        chain=market.chain,
                        max_gas_ratio=self.max_gas_ratio,
                    ):
                        gas_cost = market.estimate_swap_gas_cost_usd(market.chain)
                        return Intent.hold(
                            reason=f"gas cost ${gas_cost} exceeds {self.max_gas_ratio:.2%} of trade value "
                            f"${self.trade_size_usd}"
                        )
                    logger.info(f"SELL: {reason}")
                    return Intent.swap(
                        from_token=self.base_token,
                        to_token=self.quote_token,
                        amount_usd=self.trade_size_usd,
                        max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    )

            return Intent.hold(reason=reason or "No signal")"""

    elif template == StrategyTemplate.DYNAMIC_LP:
        return """
            base_price = market.price(self.base_token)
            range_pct = Decimal(str(self.range_width_pct)) / Decimal("100")
            lower_price = base_price * (Decimal("1") - range_pct)
            upper_price = base_price * (Decimal("1") + range_pct)

            # If we have an open position, check if rebalance needed
            if self._position_id is not None:
                rebalance_pct = Decimal(str(self.rebalance_threshold_pct)) / Decimal("100")
                if self._range_lower is not None and self._range_upper is not None:
                    # Tick-ranged protocols (Aerodrome Slipstream) store the band
                    # in raw ticks -- measure the current position in tick space
                    # so the in-range math compares like units.
                    if self._uses_tick_ranges():
                        current = Decimal(self._pool_tick_for_price(market, base_price))
                    else:
                        current = base_price
                    range_size = self._range_upper - self._range_lower
                    dist_from_lower = current - self._range_lower
                    position_in_range = dist_from_lower / range_size if range_size > 0 else Decimal("0.5")
                    lower_bound = (Decimal("1") - rebalance_pct) / Decimal("2")
                    upper_bound = (Decimal("1") + rebalance_pct) / Decimal("2")
                    if position_in_range < lower_bound or position_in_range > upper_bound:
                        logger.info(f"Rebalance needed: price {base_price} at {position_in_range:.1%} of range")
                        return Intent.lp_close(
                            position_id=self._position_id,
                            pool=self.pool,
                            collect_fees=True,
                            protocol=self.protocol,
                        )
                return Intent.hold(reason=f"LP position {self._position_id} in range")

            # No position -- rebalance inventory toward ~50/50, then open. A range
            # that drifted out before closing leaves a heavily skewed inventory
            # (mostly one token), so swap the heavy side's excess over half to the
            # light side BEFORE reopening. Without this the new range opens lopsided
            # -- and the old "both sides funded" check could never reopen at all
            # once the inventory went one-sided.
            try:
                base_balance = market.balance(self.base_token)
                quote_balance = market.balance(self.quote_token)
            except ValueError:
                return Intent.hold(reason="Cannot check balances")

            base_usd = base_balance.balance_usd
            quote_usd = quote_balance.balance_usd
            total_usd = base_usd + quote_usd
            if total_usd < self.min_position_usd:
                return Intent.hold(reason="Insufficient balance for LP -- total below min_position_usd")

            # Swap the heavy side down to half once it exceeds a 10% tolerance band;
            # the next iteration (now balanced) opens the range.
            half_usd = total_usd / Decimal("2")
            tolerance_usd = total_usd * Decimal("0.10")
            if base_usd - half_usd > tolerance_usd:
                logger.info(
                    f"Rebalance swap before reopen: {self.base_token} -> {self.quote_token} "
                    f"(${base_usd - half_usd:.2f} to reach ~50/50)"
                )
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=base_usd - half_usd,
                    max_slippage=Decimal("0.01"),
                    protocol=self.protocol,
                )
            if quote_usd - half_usd > tolerance_usd:
                logger.info(
                    f"Rebalance swap before reopen: {self.quote_token} -> {self.base_token} "
                    f"(${quote_usd - half_usd:.2f} to reach ~50/50)"
                )
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=quote_usd - half_usd,
                    max_slippage=Decimal("0.01"),
                    protocol=self.protocol,
                )

            # Inventory balanced -- open the new range. Symbolic pool format
            # (e.g. "WETH/USDC/3000"); amounts in that order (amount0=base,
            # amount1=quote). The compiler reorders to on-chain token0/token1.
            #
            # Deploy ~95% of each side (the multiplier is the fraction of the
            # wallet balance committed to the pool). The swap above already
            # rebalanced inventory to ~50/50, so deploy nearly everything; the
            # 5% buffer covers gas and the small token-ratio rounding the pool
            # needs. NOTE: a 50/50 split is only capital-efficient for a NARROW
            # range centered on price -- widen `range_width_pct` materially and
            # the efficient split drifts off 50/50, leaving idle inventory. There
            # is also no rebalance cooldown here: each drift costs close + swap +
            # open (gas + swap fee + slippage), so add hysteresis before running
            # this on a choppy pair with real funds.
            logger.info(f"Opening LP: {lower_price:.2f} - {upper_price:.2f}")
            amount_base = base_balance.balance * Decimal("0.95")
            amount_quote = quote_balance.balance * Decimal("0.95")
            if self._uses_tick_ranges():
                # Slipstream's compiler consumes RAW INTEGER TICKS aligned to
                # the pool's tick spacing (pool format
                # "TOKEN0/TOKEN1/<tick_spacing>"), not price bounds (VIB-5557).
                # It also does NOT reorder amounts: amount0 always funds the
                # pool string's first token.
                from almanak.framework.intents import TickBand

                tick_lower, tick_upper = self._tick_band_for_prices(market, lower_price, upper_price)
                logger.info(f"Tick band for {self.protocol}: [{tick_lower}, {tick_upper}]")
                if self.pool.split("/")[0].upper() == self.quote_token.upper():
                    amount0, amount1 = amount_quote, amount_base
                else:
                    amount0, amount1 = amount_base, amount_quote
                return Intent.lp_open(
                    pool=self.pool,
                    amount0=amount0,
                    amount1=amount1,
                    range_spec=TickBand(lower=tick_lower, upper=tick_upper),
                    protocol=self.protocol,
                )
            return Intent.lp_open(
                pool=self.pool,
                amount0=amount_base,
                amount1=amount_quote,
                range_lower=lower_price,
                range_upper=upper_price,
                protocol=self.protocol,
            )"""

    elif template == StrategyTemplate.LENDING_LOOP:
        return """
            # Leverage loop state machine:
            #   IDLE -> SUPPLIED -> BORROWED -> (check leverage) -> IDLE (loop) or MONITORING
            # Each loop iteration: supply collateral -> borrow -> swap back to collateral
            # Loops until target_leverage is reached, then monitors health via the unified
            # health-factor provider (Aave V3 / Morpho Blue / Compound V3).

            # ----- Health-factor guard runs EVERY iteration once borrowed -----
            # HF < emergency_threshold -> full deleverage.
            # HF < min_health_factor   -> partial repay (scale = partial_repay_pct of debt).
            #
            # Sizing rule: partial repay = partial_repay_pct * outstanding debt
            # (NOT wallet balance -- after swapping borrowed tokens into collateral
            # each loop, the wallet usually holds 0 borrow_token). If the wallet
            # doesn't hold enough borrow_token to cover the repay, we first swap
            # collateral_token -> borrow_token so the repay can actually execute.
            if self._loop_state in (LendingLoopState.BORROWED, LendingLoopState.MONITORING) or self._loop_count > 0:
                try:
                    hf_health = market.position_health(
                        protocol=self.lending_protocol,
                        market_id=self.lending_market,
                    )
                    hf = hf_health.health_factor
                    debt_usd = getattr(hf_health, "debt_value_usd", Decimal("0")) or Decimal("0")
                    logger.info(
                        f"Health factor check: {hf} (debt=${debt_usd}) "
                        f"(min={self.min_health_factor}, emergency={self.emergency_threshold})"
                    )

                    # Check wallet balance of the debt token (used for both thresholds).
                    try:
                        borrow_wallet = market.balance(self.borrow_token).balance
                    except Exception:
                        borrow_wallet = Decimal("0")

                    # USD-pegged stablecoins where 1 token ~ $1 is a safe fallback.
                    STABLE_DEBT_TOKENS = {
                        "USDC", "USDT", "DAI", "USDC.E", "USDBC", "USDS", "FRAX", "LUSD"
                    }

                    def _debt_tokens() -> Decimal | None:
                        # Convert debt USD -> debt token amount.
                        # 1) price oracle: preferred (works for any token)
                        # 2) stablecoin 1:1 fallback (only for the allow-list above)
                        # 3) None: refuse to guess; caller must not emit a sized repay
                        try:
                            price = market.price(self.borrow_token)
                            if price and price > 0:
                                return debt_usd / Decimal(str(price))
                        except Exception:
                            pass
                        if self.borrow_token.upper() in STABLE_DEBT_TOKENS:
                            return debt_usd
                        return None

                    if hf < self.emergency_threshold:
                        logger.warning(
                            f"Health factor {hf} < emergency_threshold "
                            f"{self.emergency_threshold}: full deleverage."
                        )
                        required_tokens = _debt_tokens()
                        # If wallet can't cover the debt (common case: loop just
                        # swapped all borrow_token -> collateral_token), first
                        # unwind collateral so the repay has funds to transfer.
                        # If we cannot size required_tokens (no oracle, non-stable
                        # debt), fall back to "wallet empty" heuristic.
                        wallet_short = (
                            required_tokens is not None and borrow_wallet < required_tokens
                        ) or (required_tokens is None and borrow_wallet <= Decimal("0"))
                        if wallet_short and debt_usd > Decimal("0"):
                            logger.warning(
                                f"Emergency deleverage needs {self.borrow_token} "
                                f"(required~{required_tokens}, wallet={borrow_wallet}) "
                                f"-- swapping {self.collateral_token} -> "
                                f"{self.borrow_token} first."
                            )
                            self._loop_state = LendingLoopState.MONITORING
                            return Intent.swap(
                                from_token=self.collateral_token,
                                to_token=self.borrow_token,
                                amount="all",
                                max_slippage=Decimal("0.02"),  # wider in emergency
                            )
                        self._loop_state = LendingLoopState.MONITORING
                        repay_kwargs = {
                            "protocol": self.lending_protocol,
                            "token": self.borrow_token,
                            "repay_full": True,
                        }
                        if self.lending_market:
                            repay_kwargs["market_id"] = self.lending_market
                        return Intent.repay(**repay_kwargs)

                    if hf < self.min_health_factor:
                        # Partial repay sized from DEBT (not wallet balance).
                        debt_tokens = _debt_tokens()
                        if debt_tokens is None:
                            # No oracle and non-stable debt -- fall through to emergency
                            # only if HF continues to drop; for now we HOLD and
                            # explicitly log the reason rather than sizing a repay
                            # with a guessed value that could over-repay drastically.
                            return Intent.hold(
                                reason=f"HF {hf} < min {self.min_health_factor} but "
                                f"no oracle/stablecoin pricing for {self.borrow_token}"
                            )
                        target_amt = (debt_tokens * self.partial_repay_pct).quantize(
                            Decimal("0.0001"), rounding=ROUND_DOWN
                        )
                        if target_amt <= Decimal("0"):
                            return Intent.hold(
                                reason=f"HF {hf} < min {self.min_health_factor} "
                                f"but computed zero debt to repay"
                            )
                        # If wallet can't cover the target, first free up funds by
                        # swapping collateral -> debt token. Once we start
                        # deleveraging we transition to MONITORING so on_intent_executed
                        # does not mis-count this swap as a normal loop iteration.
                        if borrow_wallet < target_amt:
                            logger.warning(
                                f"Partial repay needs {target_amt} {self.borrow_token} "
                                f"but wallet holds {borrow_wallet} -- swapping collateral first."
                            )
                            self._loop_state = LendingLoopState.MONITORING
                            return Intent.swap(
                                from_token=self.collateral_token,
                                to_token=self.borrow_token,
                                amount="all",
                                max_slippage=Decimal("0.01"),
                            )
                        repay_amt = target_amt.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                        logger.warning(
                            f"Health factor {hf} < min_health_factor {self.min_health_factor}: "
                            f"partial repay {repay_amt} {self.borrow_token}."
                        )
                        # Transition to MONITORING so subsequent iterations evaluate HF
                        # rather than continuing to loop.
                        self._loop_state = LendingLoopState.MONITORING
                        partial_kwargs = {
                            "protocol": self.lending_protocol,
                            "token": self.borrow_token,
                            "amount": repay_amt,
                        }
                        if self.lending_market:
                            partial_kwargs["market_id"] = self.lending_market
                        return Intent.repay(**partial_kwargs)
                except Exception as e:
                    logger.warning(f"Health factor unavailable, continuing loop: {e}")

            if self._loop_state == LendingLoopState.IDLE:
                # Supply collateral (first loop: configured amount, subsequent: all available)
                try:
                    collateral_bal = market.balance(self.collateral_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check collateral balance")

                if self._loop_count == 0 and collateral_bal.balance_usd < self.min_collateral_usd:
                    return Intent.hold(reason=f"Insufficient {self.collateral_token}")
                if self._loop_count > 0 and collateral_bal.balance_usd < Decimal("10"):
                    # Dust remaining after swap -- stop looping
                    self._loop_state = LendingLoopState.MONITORING
                    return Intent.hold(reason="Insufficient collateral for next loop, entering monitoring")

                # Re-supply the full wallet balance, resolved to a concrete Decimal so
                # on_intent_executed can track it into _total_collateral (it skips "all").
                amount = self.supply_amount if self._loop_count == 0 else collateral_bal.balance
                logger.info(
                    f"Loop {self._loop_count + 1}: supplying {amount} {self.collateral_token} "
                    f"on {self.lending_protocol}"
                )
                supply_kwargs = {
                    "protocol": self.lending_protocol,
                    "token": self.collateral_token,
                    "amount": amount,
                    "use_as_collateral": True,
                }
                if self.lending_market:
                    supply_kwargs["market_id"] = self.lending_market
                return Intent.supply(**supply_kwargs)

            elif self._loop_state == LendingLoopState.SUPPLIED:
                # Borrow against collateral -- amount decays each loop
                # First loop: full borrow_amount. Each subsequent: scaled by borrow_ratio.
                if self.borrow_ratio <= Decimal("0"):
                    self._loop_state = LendingLoopState.MONITORING
                    return Intent.hold(reason="borrow_ratio must be > 0; entering monitoring")
                scale = self.borrow_ratio ** self._loop_count
                borrow_amount = (self.borrow_amount * scale).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if borrow_amount < Decimal("1"):
                    self._loop_state = LendingLoopState.MONITORING
                    return Intent.hold(reason="Borrow amount too small, entering monitoring")

                # Refuse the borrow if it would push the projected health factor below
                # min_health_factor. Skipped (with a warning) if HF data is unavailable.
                try:
                    pre = market.position_health(
                        protocol=self.lending_protocol,
                        market_id=self.lending_market,
                    )
                    borrow_price = market.price(self.borrow_token)
                    if borrow_price and borrow_price > 0 and pre.collateral_value_usd > 0:
                        new_debt_usd = borrow_amount * Decimal(str(borrow_price))
                        projected_debt_usd = pre.debt_value_usd + new_debt_usd
                        if projected_debt_usd > 0:
                            projected_hf = (pre.collateral_value_usd * pre.lltv) / projected_debt_usd
                            if projected_hf < self.min_health_factor:
                                self._loop_state = LendingLoopState.MONITORING
                                return Intent.hold(
                                    reason=(
                                        f"Refusing borrow: projected HF {projected_hf:.3f} "
                                        f"< min_health_factor {self.min_health_factor} "
                                        f"(collateral=${pre.collateral_value_usd:.2f}, "
                                        f"projected_debt=${projected_debt_usd:.2f}, lltv={pre.lltv}) "
                                        f"-- entering monitoring"
                                    )
                                )
                except Exception as e:
                    logger.warning(f"Projected-HF guard unavailable, proceeding with borrow: {e}")

                logger.info(
                    f"Loop {self._loop_count + 1}: borrowing {borrow_amount} {self.borrow_token} "
                    f"on {self.lending_protocol}"
                )
                borrow_kwargs = {
                    "protocol": self.lending_protocol,
                    "collateral_token": self.collateral_token,
                    "collateral_amount": Decimal("0"),
                    "borrow_token": self.borrow_token,
                    "borrow_amount": borrow_amount,
                }
                if self.lending_market:
                    borrow_kwargs["market_id"] = self.lending_market
                return Intent.borrow(**borrow_kwargs)

            elif self._loop_state == LendingLoopState.BORROWED:
                # Swap borrowed tokens back to collateral for next loop iteration
                logger.info(f"Loop {self._loop_count + 1}: swapping {self.borrow_token} -> {self.collateral_token}")
                return Intent.swap(
                    from_token=self.borrow_token,
                    to_token=self.collateral_token,
                    amount="all",
                    max_slippage=Decimal("0.005"),
                )

            elif self._loop_state == LendingLoopState.MONITORING:
                # Leverage target reached -- HF is already checked at the top of decide().
                # If we reached here, the position is healthy.
                logger.info(
                    f"Monitoring: leverage ~{self._current_leverage:.2f}x "
                    f"(target {self.target_leverage}x, {self._loop_count} loops, "
                    f"min_health_factor={self.min_health_factor})"
                )
                return Intent.hold(
                    reason=f"Monitoring leveraged position (~{self._current_leverage:.2f}x, "
                    f"{self._loop_count} loops)"
                )

            return Intent.hold(reason=f"Unknown state: {self._loop_state}")"""

    elif template == StrategyTemplate.BASIS_TRADE:
        return """
            spot_price = market.price(self.base_token)

            if self._trade_state == BasisTradeState.IDLE:
                # Check funding rate before entering -- only trade when funding is attractive
                try:
                    funding = market.funding_rate(self.protocol, self.perp_market)
                    hourly_rate = funding.rate_hourly
                    logger.info(f"Funding rate for {self.perp_market}: {hourly_rate:.6f}/hr")
                except Exception as e:
                    logger.warning(f"Cannot fetch funding rate: {e}")
                    return Intent.hold(reason="Cannot check funding rate")

                if hourly_rate < self.funding_entry_threshold:
                    return Intent.hold(
                        reason=f"Funding rate {hourly_rate:.6f}/hr < entry threshold "
                        f"{self.funding_entry_threshold}/hr"
                    )

                try:
                    quote_balance = market.balance(self.quote_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check balance")

                if quote_balance.balance_usd < self.spot_size_usd:
                    return Intent.hold(reason=f"Insufficient {self.quote_token}")

                # Funding rate is attractive -- buy spot (first leg of basis trade)
                logger.info(
                    f"Opening basis: buying {self.base_token} spot at {spot_price} "
                    f"(funding={hourly_rate:.6f}/hr)"
                )
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.spot_size_usd,
                    max_slippage=Decimal("0.005"),
                )

            elif self._trade_state == BasisTradeState.SPOT_BOUGHT:
                # Hedge with short perp (second leg)
                logger.info(f"Hedging: opening short perp on {self.perp_market}")
                return Intent.perp_open(
                    market=self.perp_market,
                    collateral_token=self.quote_token,
                    collateral_amount=self.spot_size_usd * Decimal("0.1"),
                    size_usd=self.spot_size_usd * self.hedge_ratio,
                    is_long=False,
                    leverage=Decimal("10"),
                    protocol=self.protocol,
                )

            elif self._trade_state == BasisTradeState.HEDGED:
                # Monitor funding rate -- exit if it drops below threshold
                try:
                    funding = market.funding_rate(self.protocol, self.perp_market)
                    hourly_rate = funding.rate_hourly
                except Exception as e:
                    logger.warning(f"Cannot fetch funding rate: {e}")
                    return Intent.hold(reason=f"Cannot check funding rate: {e}")

                if hourly_rate < self.funding_exit_threshold:
                    # Funding has turned unfavorable -- close perp first (higher priority).
                    # State advances to "unwinding" in on_intent_executed() after success.
                    logger.info(
                        f"Exiting basis: funding {hourly_rate:.6f}/hr < exit threshold "
                        f"{self.funding_exit_threshold}/hr -- closing perp"
                    )
                    return Intent.perp_close(
                        market=self.perp_market,
                        collateral_token=self.quote_token,
                        is_long=False,
                        size_usd=self.spot_size_usd * self.hedge_ratio,
                        max_slippage=Decimal("0.005"),
                        protocol=self.protocol,
                    )

                return Intent.hold(
                    reason=f"Basis trade active (funding={hourly_rate:.6f}/hr, "
                    f"exit_threshold={self.funding_exit_threshold})"
                )

            elif self._trade_state == BasisTradeState.UNWINDING:
                # Perp closed, now sell spot to complete unwind
                logger.info(f"Unwinding: selling {self.base_token} spot")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=Decimal("0.005"),
                )

            return Intent.hold(reason=f"Unknown state: {self._trade_state}")"""

    elif template == StrategyTemplate.COPY_TRADER:
        return """
            # Read leader signals from wallet activity provider
            signals = market.wallet_activity(action_types=self.action_types)

            if not signals:
                return Intent.hold(reason="No new leader activity")

            provider = getattr(self, "_wallet_activity_provider", None)

            for signal in signals:
                decision = self.policy_engine.evaluate(signal)
                if decision.action != "execute":
                    logger.info(f"Policy blocked signal {signal.signal_id}: {decision.skip_reason_code}")
                    if provider:
                        provider.consume_signals([signal.event_id])
                    continue

                result = self.intent_builder.build(signal)
                if result.intent is None:
                    logger.info(f"Could not map signal {signal.signal_id}: {result.reason_code}")
                    if provider:
                        provider.consume_signals([signal.event_id])
                    continue

                logger.info(f"Copy intent mapped: {signal.action_type} via {signal.protocol}")
                return result.intent

            return Intent.hold(reason="No actionable signals")"""

    elif template == StrategyTemplate.VAULT_YIELD:
        return """
            # Guard: ensure vault_address has been configured
            if self.vault_address == "0x0000000000000000000000000000000000000000":
                return Intent.hold(reason="vault_address not configured: update config.json with a valid vault address")

            # Check available balance for deposit
            try:
                balance_info = market.balance(self.deposit_token)
                available = balance_info.balance
                available_usd = balance_info.balance_usd
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not check {self.deposit_token} balance: {e}")
                return Intent.hold(reason=f"Balance unavailable: {e}")

            if self._state == VaultYieldState.IDLE:
                if available_usd < self.min_deposit_usd:
                    return Intent.hold(
                        reason=f"Insufficient {self.deposit_token}: ${available_usd:.2f} < ${self.min_deposit_usd}"
                    )
                # Deposit into vault
                pct = max(0, min(self.max_vault_allocation_pct, 100))
                max_deposit = available * Decimal(str(pct)) / Decimal("100")
                deposit_amount = min(self.deposit_amount, max_deposit)
                logger.info(f"DEPOSIT: {deposit_amount} {self.deposit_token} into vault")
                return Intent.vault_deposit(
                    protocol=self.protocol,
                    vault_address=self.vault_address,
                    amount=deposit_amount,
                    chain=self.chain,
                )

            elif self._state == VaultYieldState.DEPOSITED:
                # Hold position -- yield accrues passively in the vault
                return Intent.hold(reason="Vault position active, earning yield")

            else:
                return Intent.hold(reason=f"Unknown state: {self._state}")"""

    elif template == StrategyTemplate.PERPS:
        return """
            entry_price = market.price(self.base_token)

            if self._position_state == PerpsState.IDLE:
                try:
                    collateral_bal = market.balance(self.collateral_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check balance")

                if collateral_bal.balance < self.collateral_amount:
                    return Intent.hold(reason=f"Insufficient {self.collateral_token}")

                # Direction is config-driven (self._is_long). Update the
                # signal logic below to match your strategy's thesis.
                logger.info(f"Opening {self.direction} {self.perp_market} at {entry_price}")
                # Capture price at decide time for entry_price fallback
                # (GMX V2 two-step flow means ResultEnricher may not have entry_price)
                self._pending_entry_price = entry_price
                return Intent.perp_open(
                    market=self.perp_market,
                    collateral_token=self.collateral_token,
                    collateral_amount=self.collateral_amount,
                    size_usd=self.position_size_usd,
                    is_long=self._is_long,
                    leverage=self.leverage,
                    protocol=self.protocol,
                )

            elif self._position_state == PerpsState.OPEN:
                # Check TP/SL. For SHORT positions, profit = price DOWN, so
                # the raw pnl_pct is flipped to match directional exposure.
                if self._entry_price:
                    raw_pnl_pct = (entry_price - self._entry_price) / self._entry_price
                    pnl_pct = raw_pnl_pct if self._is_long else -raw_pnl_pct
                    if pnl_pct >= self.take_profit_pct:
                        logger.info(f"Take profit hit ({self.direction}): {pnl_pct:.2%}")
                        return Intent.perp_close(
                            market=self.perp_market,
                            collateral_token=self.collateral_token,
                            is_long=self._is_long,
                            size_usd=self.position_size_usd,
                            protocol=self.protocol,
                        )
                    elif pnl_pct <= -self.stop_loss_pct:
                        logger.info(f"Stop loss hit ({self.direction}): {pnl_pct:.2%}")
                        return Intent.perp_close(
                            market=self.perp_market,
                            collateral_token=self.collateral_token,
                            is_long=self._is_long,
                            size_usd=self.position_size_usd,
                            protocol=self.protocol,
                        )
                msg = f"Position open, PnL: {pnl_pct:.2%}" if self._entry_price else "Position open"
                return Intent.hold(reason=msg)

            return Intent.hold(reason=f"Unknown state: {self._position_state}")"""

    elif template == StrategyTemplate.MULTI_STEP:
        return """
            base_price = market.price(self.base_token)
            range_pct = Decimal(str(self.range_width_pct)) / Decimal("100")

            # If we have a position, check for rebalance
            if self._position_id is not None:
                # Check if price moved enough to rebalance
                if self._range_lower and self._range_upper:
                    mid = (self._range_lower + self._range_upper) / Decimal("2")
                    drift = abs(base_price - mid) / mid
                    if drift < self.rebalance_drift_pct:
                        return Intent.hold(reason=f"Position in range, drift={drift:.2%}")

                # Rebalance: use IntentSequence to atomically close LP + consolidate
                # into quote token. The next iteration will open a fresh LP.
                # Intent.sequence() ensures close happens before swap, and
                # amount="all" chains the swap to use whatever the close released.
                logger.info(f"Rebalancing LP around {base_price} via IntentSequence")
                return Intent.sequence(
                    [
                        Intent.lp_close(
                            position_id=self._position_id,
                            pool=self.pool,
                            collect_fees=True,
                            protocol=self.protocol,
                        ),
                        Intent.swap(
                            from_token=self.base_token,
                            to_token=self.quote_token,
                            amount="all",
                            max_slippage=Decimal("0.005"),
                        ),
                    ],
                    description=f"Close LP #{self._position_id} and consolidate to {self.quote_token}",
                )

            # No position -- open one with fresh balances
            try:
                quote_balance = market.balance(self.quote_token)
            except ValueError:
                return Intent.hold(reason="Cannot check balances")

            if quote_balance.balance_usd < self.min_position_usd:
                return Intent.hold(reason=f"Insufficient {self.quote_token} for LP")

            # Swap half of quote to base, then open LP with both tokens.
            # LPOpenIntent requires concrete Decimal amounts (not "all"), so we
            # estimate the base amount after the swap using current price with a 5%
            # buffer for slippage. IntentSequence ensures swap executes first.
            # IMPORTANT: half_base_est is an ESTIMATE. Actual swap output may differ.
            # The compiler handles partial fills gracefully.
            half_quote = quote_balance.balance * Decimal("0.5")
            # Estimate how much base token we'll receive after swapping half_quote.
            # Fetch quote price so this works for non-stablecoin pairs (e.g. WETH/WBTC).
            quote_price = market.price(self.quote_token)
            half_base_est = (
                (half_quote * quote_price / base_price * Decimal("0.95"))
                if base_price > 0 and quote_price > 0
                else Decimal("0")
            )
            lower_price = base_price * (Decimal("1") - range_pct)
            upper_price = base_price * (Decimal("1") + range_pct)
            logger.info(f"Opening LP via IntentSequence: {lower_price:.2f} - {upper_price:.2f}")
            return Intent.sequence(
                [
                    Intent.swap(
                        from_token=self.quote_token,
                        to_token=self.base_token,
                        amount=half_quote,
                        max_slippage=Decimal("0.005"),
                    ),
                    Intent.lp_open(
                        pool=self.pool,
                        amount0=half_base_est,
                        amount1=half_quote * Decimal("0.95"),
                        range_lower=lower_price,
                        range_upper=upper_price,
                        protocol=self.protocol,
                    ),
                ],
                description=f"Swap {self.quote_token} -> {self.base_token} and open LP",
            )"""

    elif template == StrategyTemplate.STAKING:
        return """
            if self._stake_state == StakingState.IDLE:
                try:
                    token_balance = market.balance(self.stake_token)
                except ValueError:
                    return Intent.hold(reason=f"Cannot check {self.stake_token} balance")

                if token_balance.balance < self.stake_amount:
                    # Not enough stake token -- swap quote to get it
                    if self.swap_before_stake:
                        try:
                            quote_bal = market.balance(self.quote_token)
                        except ValueError:
                            return Intent.hold(reason=f"Cannot check {self.quote_token} balance")
                        stake_price = market.price(self.stake_token)
                        if stake_price <= 0:
                            return Intent.hold(reason=f"Invalid {self.stake_token} price: {stake_price}")
                        needed_usd = self.stake_amount * stake_price
                        if needed_usd > 0 and quote_bal.balance_usd >= needed_usd:
                            logger.info(f"Swapping {self.quote_token} -> {self.stake_token}")
                            return Intent.swap(
                                from_token=self.quote_token,
                                to_token=self.stake_token,
                                amount_usd=needed_usd,
                                max_slippage=Decimal("0.005"),
                            )
                    return Intent.hold(reason=f"Insufficient {self.stake_token}")

                logger.info(f"Staking {self.stake_amount} {self.stake_token}")
                return Intent.stake(
                    protocol=self.staking_protocol,
                    token_in=self.stake_token,
                    amount=self.stake_amount,
                )

            elif self._stake_state == StakingState.STAKED:
                return Intent.hold(reason="Staked, earning yield")

            return Intent.hold(reason=f"Unknown state: {self._stake_state}")"""

    else:  # BLANK template
        return """
            # Get market price
            # price = market.price("ETH")

            # Get wallet balance
            # balance = market.balance("USDC")

            # Implement your trading logic here
            # Example:
            # if some_condition:
            #     return Intent.swap(
            #         from_token="USDC",
            #         to_token="ETH",
            #         amount_usd=Decimal("100"),
            #     )

            return Intent.hold(reason="Strategy logic not implemented")"""


def _get_teardown_comment(template: StrategyTemplate) -> str:
    """Return a template-specific TODO hint for generate_teardown_intents()."""
    hints = {
        StrategyTemplate.BLANK: "Swap all holdings back to quote token",
        StrategyTemplate.TA_SWAP: "Swap all holdings back to quote token",
        StrategyTemplate.DYNAMIC_LP: "Close LP position, then swap tokens to quote",
        StrategyTemplate.LENDING_LOOP: "Repay borrows, withdraw collateral, swap to quote",
        StrategyTemplate.BASIS_TRADE: "Close perp position, then swap to quote",
        StrategyTemplate.VAULT_YIELD: "Redeem all vault shares back to underlying token",
        StrategyTemplate.COPY_TRADER: "Close all copied positions in reverse order",
        StrategyTemplate.PERPS: "Close all perp positions",
        StrategyTemplate.MULTI_STEP: "Close LP position, swap back to quote",
        StrategyTemplate.STAKING: "Unstake and optionally swap back to quote",
    }
    return hints.get(template, "Close all positions and convert to stable")


def _get_template_teardown(
    template: StrategyTemplate,
    config: TemplateConfig,
    strategy_name: str,
    protocol: str | None = None,
) -> str:
    """Generate template-specific get_open_positions() and generate_teardown_intents() implementations.

    ``protocol`` is the scaffold-time protocol choice (defaults to the
    template's canonical protocol); it is rendered into position metadata for
    templates without a runtime ``self.protocol``-style attribute.
    """
    teardown_comment = _get_teardown_comment(template)
    protocol = protocol or config.default_protocol

    if template == StrategyTemplate.BLANK:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import TeardownPositionSummary

        # Blank template: no positions tracked by default.
        # Add PositionInfo entries here as you implement your strategy logic.
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=[],
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}
        """
        # Blank template: no teardown intents by default.
        # Add Intent entries here matching your decide() logic.
        return []

'''

    elif template == StrategyTemplate.TA_SWAP:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview.

        Reconciles the cached ``_holding_base`` flag from live balance first so
        a stale/false flag (e.g. after a desynced restart) cannot hide a base
        position the wallet actually holds (VIB-5155 / ALM-2719). Falls back to
        the cached hint only if a live snapshot is unavailable.
        """
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        try:
            self._reconcile_holding_base(self.create_market_snapshot())
        except Exception as e:
            logger.warning(
                f"get_open_positions: live-balance reconcile unavailable, "
                f"using cached holding flag: {{e}}"
            )

        positions = []

        if self._holding_base:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="{strategy_name}_base_token",
                    chain=self.chain,
                    protocol="{protocol}",
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{"asset": self.base_token, "quote": self.quote_token}},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}

        Live balance is truth: reconcile the cached ``_holding_base`` flag from
        the provided ``market`` (or a freshly-built snapshot) BEFORE deciding
        whether to emit the risk-off swap. A stale/false flag must never block
        a valid exit (VIB-5155 / ALM-2719).
        """
        from almanak.framework.teardown import TeardownMode

        try:
            self._reconcile_holding_base(market or self.create_market_snapshot())
        except Exception as e:
            logger.warning(
                f"generate_teardown_intents: live-balance reconcile unavailable, "
                f"using cached holding flag: {{e}}"
            )

        intents: list[AnyIntent] = []

        if self._holding_base:
            max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                )
            )

        return intents

'''

    elif template == StrategyTemplate.DYNAMIC_LP:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._position_id is not None:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._position_id),
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{
                        "pool": self.pool,
                        "range_lower": str(self._range_lower) if self._range_lower is not None else None,
                        "range_upper": str(self._range_upper) if self._range_upper is not None else None,
                    }},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[AnyIntent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        if self._position_id is not None:
            intents.append(
                Intent.lp_close(
                    position_id=self._position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol=self.protocol,
                )
            )
            # Swap remaining base tokens back to quote
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                )
            )

        return intents

'''

    elif template == StrategyTemplate.LENDING_LOOP:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        # After looping, borrows exist even in SUPPLIED state (from prior loops)
        has_borrows = (
            self._loop_state in (LendingLoopState.BORROWED, LendingLoopState.MONITORING)
            or self._loop_count > 0
        )
        if has_borrows:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id="{strategy_name}_borrow",
                    chain=self.chain,
                    protocol=self.lending_protocol,
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{
                        "borrow_token": self.borrow_token,
                        "loop_count": self._loop_count,
                        "market_id": self.lending_market,
                    }},
                )
            )

        # Supply is open in SUPPLIED/BORROWED/MONITORING states OR whenever looping
        # (after a SWAP the state returns to IDLE but collateral remains on Aave)
        has_supply = (
            self._loop_state
            in (LendingLoopState.SUPPLIED, LendingLoopState.BORROWED, LendingLoopState.MONITORING)
            or self._loop_count > 0
        )
        if has_supply:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="{strategy_name}_supply",
                    chain=self.chain,
                    protocol=self.lending_protocol,
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{
                        "collateral_token": self.collateral_token,
                        "supply_amount": str(self.supply_amount),
                        "market_id": self.lending_market,
                    }},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Health-factor-aware unwind: {teardown_comment}

        Delegates to the framework's first-class lending unwind primitive, which
        sizes each leg from the LIVE on-chain position (variableDebt / balanceOf)
        and sequences withdraws to keep the post-withdraw health factor safe —
        avoiding the dust-debt / single-shot withdraw revert. See
        generate_lending_unwind.
        """
        from almanak.framework.teardown import generate_lending_unwind

        snapshot = market if market is not None else self.create_market_snapshot()
        return generate_lending_unwind(
            market=snapshot,
            protocol=self.lending_protocol,
            collateral_token=self.collateral_token,
            borrow_token=self.borrow_token,
            market_id=self.lending_market or None,
            chain=self.chain,
            mode=mode,
        )

'''

    elif template == StrategyTemplate.BASIS_TRADE:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._trade_state == BasisTradeState.HEDGED:
            # Report PERP first (higher priority for closing)
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id="{strategy_name}_short_perp",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=self.spot_size_usd * self.hedge_ratio,
                    details={{
                        "market": self.perp_market,
                        "is_long": False,
                        "collateral_token": self.quote_token,
                    }},
                )
            )
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="{strategy_name}_spot",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=self.spot_size_usd,
                    details={{"asset": self.base_token}},
                )
            )
        elif self._trade_state in (BasisTradeState.SPOT_BOUGHT, BasisTradeState.UNWINDING):
            # UNWINDING = perp already closed, still holding spot
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="{strategy_name}_spot",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=self.spot_size_usd,
                    details={{"asset": self.base_token}},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}

        Priority: close short perp first (liquidation risk), then sell spot.
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[AnyIntent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        # 1. Close short perp (if hedged)
        if self._trade_state == BasisTradeState.HEDGED:
            intents.append(
                Intent.perp_close(
                    market=self.perp_market,
                    collateral_token=self.quote_token,
                    is_long=False,
                    size_usd=self.spot_size_usd * self.hedge_ratio,
                    max_slippage=max_slippage,
                    protocol=self.protocol,
                )
            )

        # 2. Sell spot position
        if self._trade_state in (
            BasisTradeState.SPOT_BOUGHT,
            BasisTradeState.HEDGED,
            BasisTradeState.UNWINDING,
        ):
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                )
            )

        return intents

'''

    elif template == StrategyTemplate.VAULT_YIELD:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._state == VaultYieldState.DEPOSITED:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="{strategy_name}_vault",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{
                        "vault_address": self.vault_address,
                        "deposit_token": self.deposit_token,
                    }},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}
        """
        intents: list[AnyIntent] = []

        if self._state == VaultYieldState.DEPOSITED:
            intents.append(
                Intent.vault_redeem(
                    protocol=self.protocol,
                    vault_address=self.vault_address,
                    shares="all",
                    chain=self.chain,
                )
            )

        return intents

'''

    elif template == StrategyTemplate.COPY_TRADER:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []
        _type_map = {{
            "SWAP": PositionType.TOKEN,
            "LP_OPEN": PositionType.LP,
            "SUPPLY": PositionType.SUPPLY,
            "BORROW": PositionType.BORROW,
            "PERP_OPEN": PositionType.PERP,
            "STAKE": PositionType.STAKE,
        }}

        for i, trade in enumerate(self._open_trades):
            pos_type = _type_map.get(trade.get("intent_type"), PositionType.TOKEN)
            positions.append(
                PositionInfo(
                    position_type=pos_type,
                    position_id=f"{strategy_name}_copy_{{i}}",
                    chain=self.chain,
                    protocol=trade.get("protocol", "unknown"),
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details=trade,
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}

        Reverses each copied trade.
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[AnyIntent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        # Process in reverse order (last opened = first closed)
        for trade in reversed(self._open_trades):
            intent_type = trade.get("intent_type")
            if intent_type == "SWAP":
                # Reverse swap
                if trade.get("to_token"):
                    intents.append(
                        Intent.swap(
                            from_token=trade["to_token"],
                            to_token=trade.get("from_token", "USDC"),
                            amount="all",
                            max_slippage=max_slippage,
                        )
                    )
            elif intent_type == "LP_OPEN" and trade.get("position_id"):
                intents.append(
                    Intent.lp_close(
                        position_id=trade["position_id"],
                        pool=trade.get("pool", ""),
                        collect_fees=True,
                        protocol=trade.get("protocol", "uniswap_v3"),
                    )
                )
            elif intent_type == "PERP_OPEN":
                intents.append(
                    Intent.perp_close(
                        market=trade.get("market", ""),
                        collateral_token=trade.get("collateral_token", "USDC"),
                        is_long=trade.get("is_long", True),
                        size_usd=Decimal(str(trade.get("size_usd", "0"))),
                        max_slippage=max_slippage,
                        protocol=trade.get("protocol", "gmx_v2"),
                    )
                )
            elif intent_type == "SUPPLY":
                intents.append(
                    Intent.withdraw(
                        protocol=trade.get("protocol", "aave_v3"),
                        token=trade.get("token", ""),
                        amount="all",
                    )
                )
            elif intent_type == "BORROW":
                intents.append(
                    Intent.repay(
                        protocol=trade.get("protocol", "aave_v3"),
                        token=trade.get("borrow_token") or trade.get("token", ""),
                        amount="all",
                    )
                )
            elif intent_type == "STAKE":
                intents.append(
                    Intent.unstake(
                        protocol=trade.get("protocol", "lido"),
                        token_in=trade.get("token", ""),
                        amount="all",
                    )
                )

        return intents

'''

    elif template == StrategyTemplate.PERPS:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._position_state == PerpsState.OPEN:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"{strategy_name}_perp_{{self.direction.lower()}}",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=self.position_size_usd,
                    details={{
                        "market": self.perp_market,
                        "collateral_token": self.collateral_token,
                        "is_long": self._is_long,
                        "direction": self.direction,
                        "entry_price": str(self._entry_price) if self._entry_price else "unknown",
                    }},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[AnyIntent] = []

        if self._position_state == PerpsState.OPEN:
            max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
            intents.append(
                Intent.perp_close(
                    market=self.perp_market,
                    collateral_token=self.collateral_token,
                    is_long=self._is_long,
                    size_usd=self.position_size_usd,
                    max_slippage=max_slippage,
                    protocol=self.protocol,
                )
            )

        return intents

'''

    elif template == StrategyTemplate.MULTI_STEP:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._position_id is not None:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._position_id),
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{
                        "pool": self.pool,
                        "range_lower": str(self._range_lower) if self._range_lower else None,
                        "range_upper": str(self._range_upper) if self._range_upper else None,
                    }},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[AnyIntent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        if self._position_id is not None:
            intents.append(
                Intent.lp_close(
                    position_id=self._position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol=self.protocol,
                )
            )
            # Swap remaining base tokens back to quote
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                )
            )

        return intents

'''

    elif template == StrategyTemplate.STAKING:
        return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # Teardown-state posture (VIB-5464 / TD-06): whatever get_open_positions()
    # reads to know a position is open MUST survive a restart - persist it via
    # get_persistent_state()/load_persistent_state() (both sides), or re-derive
    # it purely from chain and set teardown_state_derived_from_chain = True.
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._stake_state == StakingState.STAKED:
            staked_amt = self._staked_amount or self.stake_amount
            positions.append(
                PositionInfo(
                    position_type=PositionType.STAKE,
                    position_id="{strategy_name}_stake",
                    chain=self.chain,
                    protocol=self.staking_protocol,
                    value_usd=Decimal("0"),  # Will be enriched by framework
                    details={{
                        "stake_token": self.stake_token,
                        "staked_amount": str(staked_amt),
                    }},
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[AnyIntent] = []

        if self._stake_state == StakingState.STAKED:
            intents.append(
                Intent.unstake(
                    protocol=self.staking_protocol,
                    token_in=self.stake_token,
                    amount="all",
                )
            )
            # Optionally swap back to quote token
            if self.swap_before_stake:
                max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
                intents.append(
                    Intent.swap(
                        from_token=self.stake_token,
                        to_token=self.quote_token,
                        amount="all",
                        max_slippage=max_slippage,
                    )
                )

        return intents

'''

    # Fallback (should not be reached)
    return f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # See: docs/internal/blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        from datetime import UTC, datetime
        from almanak.framework.teardown import TeardownPositionSummary
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=[],
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        return []

'''


def _default_lp_pool(protocol: str) -> str:
    """Default symbolic pool for the LP templates, per protocol.

    Tick-spacing protocols (e.g. Aerodrome Slipstream) address pools as
    ``TOKEN0/TOKEN1/<tick_spacing>`` (200 is the canonical WETH/USDC CL pool on
    Base); fee-tier protocols use ``TOKEN0/TOKEN1/<fee_bps>``. Family membership
    is resolved through ``PROTOCOL_FAMILY_REGISTRY`` rather than a hardcoded
    protocol literal, so new tick-spacing connectors are picked up automatically.
    """
    from almanak.connectors._strategy_protocol_family_registry import (
        PROTOCOL_FAMILY_REGISTRY,
        ProtocolFamily,
    )
    from almanak.framework.agent_tools.schemas import _normalize_protocol_key

    tick_spacing_protocols = PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.TICK_SPACING_FEE_DISPLAY)
    if _normalize_protocol_key(protocol) in tick_spacing_protocols:
        return "WETH/USDC/200"
    return "WETH/USDC/3000"


def _get_template_init_params(
    template: StrategyTemplate,
    config: TemplateConfig,
    protocol: str | None = None,
) -> str:
    """Generate template-specific __init__ parameter extraction.

    ``protocol`` is the scaffold-time protocol choice; it becomes the
    ``get_config(...)`` default for the template's protocol attribute so the
    generated code, config.json, and decorator metadata all agree. Falls
    back to the template's canonical protocol.
    """
    protocol = protocol or config.default_protocol
    if template == StrategyTemplate.TA_SWAP:
        return """
        # Indicator mode: "rsi", "bollinger", or "rsi_bb" (combined)
        self._indicator = get_config("indicator", "rsi")

        # RSI parameters
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", "70")))

        # Bollinger Bands parameters
        self.bb_period = int(get_config("bb_period", 20))
        self.bb_std_dev = float(get_config("bb_std_dev", 2.0))
        self.squeeze_threshold = float(get_config("squeeze_threshold", 0.02))
        self.buy_percent_b = float(get_config("buy_percent_b", 0.0))
        self.sell_percent_b = float(get_config("sell_percent_b", 1.0))

        # Trading parameters
        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "1000")))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 50))

        # Gas-worthiness gate:
        #   min_trade_value_usd: absolute floor below which a trade is skipped
        #   max_gas_ratio: reject trades where gas_cost > this fraction of trade value
        self.min_trade_value_usd = Decimal(str(get_config("min_trade_value_usd", "10")))
        self.max_gas_ratio = Decimal(str(get_config("max_gas_ratio", "0.05")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")

        # Dust floor (USD) above which the wallet is considered to be HOLDING
        # base. Used to reconcile the cached `_holding_base` flag against live
        # balance (VIB-5155 / ALM-2719) so rounding dust isn't treated as an
        # open position.
        self.holding_dust_usd = Decimal(str(get_config("holding_dust_usd", "1")))

        # Position tracking. The cached flag is a HINT; live balance is the
        # source of truth and is reconciled each cycle / on resume / before
        # teardown via _reconcile_holding_base() (VIB-5155 / ALM-2719).
        self._holding_base = False
        # Neutral-rearm latch: last signal we acted on (buy/sell/neutral)
        self._last_signal = 'neutral'"""

    elif template == StrategyTemplate.DYNAMIC_LP:
        default_pool = _default_lp_pool(protocol)
        return f"""
        # LP parameters. For aerodrome_slipstream the pool's 3rd component is
        # the TICK SPACING (e.g. WETH/USDC/200), not a fee tier.
        self.pool = get_config("pool", "{default_pool}")
        self.protocol = get_config("protocol", "{protocol}")
        self.range_width_pct = float(get_config("range_width_pct", 5))
        self.rebalance_threshold_pct = float(get_config("rebalance_threshold_pct", 80))
        self.min_position_usd = Decimal(str(get_config("min_position_usd", "500")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")

        # Position tracking (restored via load_persistent_state)
        self._position_id = None
        self._range_lower = None
        self._range_upper = None"""

    elif template == StrategyTemplate.LENDING_LOOP:
        # Plain string (not an f-string): the block embeds runtime f-strings
        # whose braces must reach the scaffold verbatim. The scaffold-time
        # protocol choice is spliced in via the placeholder below.
        lending_init = """
        # Lending parameters
        self.supply_amount = Decimal(str(get_config("supply_amount", "1")))
        self.borrow_amount = Decimal(str(get_config("borrow_amount", "500")))
        self.target_leverage = Decimal(str(get_config("target_leverage", "2.0")))
        self.borrow_ratio = Decimal(str(get_config("borrow_ratio", "0.7")))
        if self.borrow_ratio >= Decimal("1"):
            raise ValueError(
                f"borrow_ratio={self.borrow_ratio} >= 1.0 causes exponential borrow growth. "
                "Set borrow_ratio to a value between 0 and 1 (e.g. 0.7) in config.json."
            )
        # Health-factor thresholds (unified across aave_v3 / morpho_blue / compound_v3).
        # HF < min_health_factor -> partial repay. HF < emergency_threshold -> full deleverage.
        self.min_health_factor = Decimal(str(get_config("min_health_factor", "1.5")))
        self.emergency_threshold = Decimal(str(get_config("emergency_threshold", "1.2")))
        if self.emergency_threshold >= self.min_health_factor:
            raise ValueError(
                f"emergency_threshold ({self.emergency_threshold}) must be strictly less than "
                f"min_health_factor ({self.min_health_factor}). Example: 1.2 vs 1.5."
            )
        self.min_collateral_usd = Decimal(str(get_config("min_collateral_usd", "100")))
        self.partial_repay_pct = Decimal(str(get_config("partial_repay_pct", "0.25")))

        # Protocol / market for health-factor dispatch.
        # For aave_v3 market_id is informational; for morpho_blue set the bytes32 market id;
        # for compound_v3 set the Comet market key (e.g. "usdc", "weth").
        self.lending_protocol = get_config("lending_protocol", "__SCAFFOLD_PROTOCOL__")
        self.lending_market = get_config("lending_market", "")

        # Token configuration
        self.collateral_token = get_config("collateral_token", "WETH")
        self.borrow_token = get_config("borrow_token", "USDC")

        # State machine: IDLE -> SUPPLIED -> BORROWED -> (check leverage) -> IDLE or MONITORING
        self._loop_state = LendingLoopState.IDLE
        self._loop_count = 0
        self._current_leverage = Decimal("1.0")

        # Position totals tracked in on_intent_executed(). The teardown lane
        # uses these to size the unwind slice: without them the safe leveraged-
        # loop unwind (withdraw slice -> swap to debt -> repay_full -> withdraw
        # rest) degenerates back to repay-then-withdraw, which reverts because
        # the loop re-supplied the debt token and the wallet holds collateral.
        self._total_borrowed = Decimal("0")
        self._total_collateral = Decimal("0")"""
        return lending_init.replace("__SCAFFOLD_PROTOCOL__", protocol)

    elif template == StrategyTemplate.BASIS_TRADE:
        return f"""
        # Basis trade parameters
        self.spot_size_usd = Decimal(str(get_config("spot_size_usd", "10000")))
        self.hedge_ratio = Decimal(str(get_config("hedge_ratio", "1.0")))

        # Funding rate thresholds (hourly rate, e.g. 0.0001 = 0.01%/hr)
        self.funding_entry_threshold = Decimal(str(get_config("funding_entry_threshold", "0.0001")))
        self.funding_exit_threshold = Decimal(str(get_config("funding_exit_threshold", "-0.00005")))

        # Perp venue for the hedge leg (funding-rate reads + perp intents)
        self.protocol = get_config("protocol", "{protocol}")

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.perp_market = get_config("perp_market", "ETH/USD")

        # State machine: IDLE -> SPOT_BOUGHT -> HEDGED -> UNWINDING -> IDLE
        self._trade_state = BasisTradeState.IDLE"""

    elif template == StrategyTemplate.COPY_TRADER:
        return """
        from almanak.framework.services.copy_trading import (
            CopyIntentBuilder,
            CopyPolicyEngine,
            CopySizer,
            CopySizingConfig,
            CopyTradingConfigV2,
        )

        # Copy trading config
        ct_config = get_config("copy_trading", {})
        self.copy_config = CopyTradingConfigV2.from_config(ct_config if isinstance(ct_config, dict) else {})
        self.action_types = self.copy_config.global_policy.action_types

        sizing_dict = self.copy_config.sizing.model_dump(mode="python")
        risk_dict = self.copy_config.risk.model_dump(mode="python")
        self.sizer = CopySizer(config=CopySizingConfig.from_config(sizing_dict, risk_dict))

        self.policy_engine = CopyPolicyEngine(config=self.copy_config)
        self.intent_builder = CopyIntentBuilder(config=self.copy_config, sizer=self.sizer)

        # Position tracking (restored via load_persistent_state)
        self._open_trades = []"""

    elif template == StrategyTemplate.VAULT_YIELD:
        return f"""
        # Vault parameters
        self.protocol = get_config("protocol", "{protocol}")
        self.vault_address = get_config("vault_address", "0x0000000000000000000000000000000000000000")
        if self.vault_address == "0x0000000000000000000000000000000000000000":
            logger.warning("vault_address is zero address -- strategy will HOLD every iteration. Update config.json.")
        self.deposit_token = get_config("deposit_token", "USDC")
        self.deposit_amount = Decimal(str(get_config("deposit_amount", "1000")))
        self.min_deposit_usd = Decimal(str(get_config("min_deposit_usd", "100")))
        self.max_vault_allocation_pct = int(get_config("max_vault_allocation_pct", 80))

        # State machine: IDLE -> DEPOSITED
        self._state = VaultYieldState.IDLE"""

    elif template == StrategyTemplate.PERPS:
        # Plain string (not an f-string): the block embeds runtime f-strings
        # whose braces must reach the scaffold verbatim.
        perps_init = """
        # Perps parameters
        self.protocol = get_config("protocol", "__SCAFFOLD_PROTOCOL__")  # perp venue
        self.perp_market = get_config("perp_market", "ETH/USD")
        self.collateral_token = get_config("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(get_config("collateral_amount", "100")))
        self.position_size_usd = Decimal(str(get_config("position_size_usd", "1000")))
        self.leverage = Decimal(str(get_config("leverage", "5")))
        self.take_profit_pct = Decimal(str(get_config("take_profit_pct", "0.05")))
        self.stop_loss_pct = Decimal(str(get_config("stop_loss_pct", "0.03")))

        # Token for price checks
        self.base_token = get_config("base_token", "ETH")

        # Direction: "LONG" or "SHORT". Defaults to "LONG" with a one-time
        # warning if omitted so users notice they should set it explicitly.
        _direction_raw = get_config("direction", None)
        if _direction_raw is None:
            logger.warning(
                "'direction' not set in config -- defaulting to 'LONG'. "
                "Set direction='LONG' or 'SHORT' explicitly in config.json."
            )
            _direction_raw = "LONG"
        self.direction = str(_direction_raw).upper()
        if self.direction not in ("LONG", "SHORT"):
            raise ValueError(
                f"Invalid direction {_direction_raw!r}: must be 'LONG' or 'SHORT'"
            )
        self._is_long = self.direction == "LONG"

        # Position tracking (restored via load_persistent_state)
        # State machine: IDLE -> OPEN
        # position_is_long/position_direction pin the direction of the currently
        # open position; they override the config-derived direction if the user
        # changes config.json mid-position.
        self._position_state = PerpsState.IDLE
        self._entry_price = None
        self._position_is_long = None
        self._position_direction = None"""
        return perps_init.replace("__SCAFFOLD_PROTOCOL__", protocol)

    elif template == StrategyTemplate.MULTI_STEP:
        default_pool = _default_lp_pool(protocol)
        return f"""
        # Multi-step LP parameters. For aerodrome_slipstream the pool's 3rd
        # component is the TICK SPACING (e.g. WETH/USDC/200), not a fee tier.
        self.pool = get_config("pool", "{default_pool}")
        self.protocol = get_config("protocol", "{protocol}")
        self.range_width_pct = float(get_config("range_width_pct", 5))
        # rebalance_drift_pct is configured as a percentage (e.g. 3 = 3% price drift)
        # and divided by 100 here to convert to a decimal fraction for comparison
        self.rebalance_drift_pct = Decimal(str(get_config("rebalance_drift_pct", "3"))) / Decimal("100")
        self.min_position_usd = Decimal(str(get_config("min_position_usd", "500")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")

        # Position tracking (restored via load_persistent_state)
        self._position_id = None
        self._range_lower = None
        self._range_upper = None"""

    elif template == StrategyTemplate.STAKING:
        return f"""
        # Staking parameters (stake_amount is the canonical amount)
        self.stake_token = get_config("stake_token", "ETH")
        self.stake_amount = Decimal(str(get_config("stake_amount", "1")))
        self.staking_protocol = get_config("staking_protocol", "{protocol}")
        self.quote_token = get_config("quote_token", "USDC")
        self.swap_before_stake = get_config("swap_before_stake", True)

        # State tracking (restored via load_persistent_state)
        # State machine: IDLE -> STAKED
        self._stake_state = StakingState.IDLE
        self._staked_amount = None"""

    else:  # BLANK template
        return """
        # Example configuration -- customize for your strategy
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "100")))"""


# ---------------------------------------------------------------------------
# Template-specific get_status() bodies
# ---------------------------------------------------------------------------
# Every template's ``get_status()`` returns a dict that always includes the
# canonical trio ``{strategy, chain, wallet}``. Stateful templates add a
# ``state`` field (the StrEnum ``.value`` for JSON-safety) plus a handful of
# template-specific fields sourced from instance attributes that ``__init__``
# + ``on_intent_executed`` already maintain. No gateway round-trips, no
# computation beyond simple attribute reads.
#
# Serialisation rules (enforced in the emitted bodies):
#   - ``Decimal`` values are cast to ``str`` (JSON-safe, preserves precision).
#   - ``datetime`` values use ``.isoformat()`` (None-safe via guard).
#   - ``StrEnum`` members are exposed via ``getattr(x, 'value', x)`` which
#     degrades gracefully when the attribute is already a plain string
#     (e.g. loaded from a legacy state file).
#
# If a template does not track a particular field (e.g. ``health_factor``
# without a gateway query), the field is set to ``None`` rather than
# fabricated — the operator dashboard renders ``None`` as "n/a".
# ---------------------------------------------------------------------------


def _get_template_get_status(template: StrategyTemplate, strategy_name: str) -> str:
    """Return the full indented source of the template-specific ``get_status``.

    The returned string includes the ``def get_status(self)`` signature and a
    trailing blank line, so it can be substituted verbatim into the class body.
    """
    # Base dict is always the same; templates append fields to it.
    # Templates reference a module-level ``_safe`` helper (emitted by the
    # scaffold in the file header) to normalise Decimal / datetime / Enum
    # values coming out of ``_last_position_snapshot`` — without it, the
    # docstring's "JSON-safe" promise was a lie, and operator dashboards
    # that call json.dumps(strategy.get_status()) crashed the moment a
    # snapshot carried a Decimal health_factor or a datetime last_trade_ts.
    base = (
        "    def get_status(self) -> dict[str, Any]:\n"
        '        """Get current strategy status for monitoring/dashboards.\n'
        "\n"
        "        Pure accessor: reads only instance state (no gateway calls, no I/O).\n"
        "        Returned values are JSON-safe (Decimal->str, datetime->isoformat,\n"
        "        StrEnum->str via getattr(.value, fallback)).\n"
        '        """\n'
        "        status: dict[str, Any] = {\n"
        f'            "strategy": "{strategy_name}",\n'
        '            "chain": self.chain,\n'
        '            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else None,\n'
        "        }\n"
    )

    if template == StrategyTemplate.LENDING_LOOP:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        status.update(\n"
            "            {\n"
            '                "state": getattr(self._loop_state, "value", self._loop_state),\n'
            '                "loop_count": self._loop_count,\n'
            '                "current_leverage": str(self._current_leverage),\n'
            '                "target_leverage": str(self.target_leverage),\n'
            '                "health_factor": _safe(snapshot.get("health_factor")),\n'
            '                "supply_usd": _safe(snapshot.get("supply_usd")),\n'
            '                "debt_usd": _safe(snapshot.get("debt_usd")),\n'
            '                "ltv": _safe(snapshot.get("ltv")),\n'
            '                "collateral_token": self.collateral_token,\n'
            '                "borrow_token": self.borrow_token,\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.BASIS_TRADE:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        status.update(\n"
            "            {\n"
            '                "state": getattr(self._trade_state, "value", self._trade_state),\n'
            '                "base_token": self.base_token,\n'
            '                "quote_token": self.quote_token,\n'
            '                "perp_market": self.perp_market,\n'
            '                "spot_size_usd": str(self.spot_size_usd),\n'
            '                "hedge_ratio": str(self.hedge_ratio),\n'
            '                "spot_leg_value_usd": _safe(snapshot.get("spot_leg_value_usd")),\n'
            '                "perp_leg_value_usd": _safe(snapshot.get("perp_leg_value_usd")),\n'
            '                "funding_pnl_usd": _safe(snapshot.get("funding_pnl_usd")),\n'
            '                "net_delta": _safe(snapshot.get("net_delta")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.VAULT_YIELD:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        status.update(\n"
            "            {\n"
            '                "state": getattr(self._state, "value", self._state),\n'
            '                "vault_address": self.vault_address,\n'
            '                "deposit_token": self.deposit_token,\n'
            '                "deposit_amount": str(self.deposit_amount),\n'
            '                "vault_shares": _safe(snapshot.get("vault_shares")),\n'
            '                "current_yield_apr": _safe(snapshot.get("current_yield_apr")),\n'
            '                "deposited_usd": _safe(snapshot.get("deposited_usd")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.PERPS:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        direction = self._position_direction or self.direction\n"
            "        status.update(\n"
            "            {\n"
            '                "state": getattr(self._position_state, "value", self._position_state),\n'
            '                "direction": direction,\n'
            '                "perp_market": self.perp_market,\n'
            '                "collateral_token": self.collateral_token,\n'
            '                "position_size_usd": str(self.position_size_usd),\n'
            '                "entry_price": str(self._entry_price) if self._entry_price else None,\n'
            '                "leverage": str(self.leverage),\n'
            '                "pnl_usd": _safe(snapshot.get("pnl_usd")),\n'
            '                "liq_price": _safe(snapshot.get("liq_price")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.STAKING:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        status.update(\n"
            "            {\n"
            '                "state": getattr(self._stake_state, "value", self._stake_state),\n'
            '                "stake_token": self.stake_token,\n'
            '                "staking_protocol": self.staking_protocol,\n'
            '                "staked_amount": str(self._staked_amount) if self._staked_amount else None,\n'
            '                "rewards_usd": _safe(snapshot.get("rewards_usd")),\n'
            '                "unbonding_end_ts": _safe(snapshot.get("unbonding_end_ts")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.TA_SWAP:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        status.update(\n"
            "            {\n"
            '                "state": "holding_base" if self._holding_base else "holding_quote",\n'
            '                "holding_base": self._holding_base,\n'
            '                "base_token": self.base_token,\n'
            '                "quote_token": self.quote_token,\n'
            '                "indicator": self._indicator,\n'
            '                "last_signal": _safe(snapshot.get("last_signal")),\n'
            '                "last_trade_ts": _safe(snapshot.get("last_trade_ts")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.DYNAMIC_LP:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        tick_range = None\n"
            "        if self._range_lower is not None and self._range_upper is not None:\n"
            "            tick_range = [str(self._range_lower), str(self._range_upper)]\n"
            "        status.update(\n"
            "            {\n"
            '                "state": "open" if self._position_id is not None else "idle",\n'
            '                "position_id": self._position_id,\n'
            '                "tick_range": tick_range,\n'
            '                "pool": self.pool,\n'
            '                "in_range": _safe(snapshot.get("in_range")),\n'
            '                "fees_earned_usd": _safe(snapshot.get("fees_earned_usd")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.MULTI_STEP:
        extra = (
            '        snapshot = getattr(self, "_last_position_snapshot", None) or {}\n'
            "        tick_range = None\n"
            "        if self._range_lower is not None and self._range_upper is not None:\n"
            "            tick_range = [str(self._range_lower), str(self._range_upper)]\n"
            "        status.update(\n"
            "            {\n"
            '                "state": "open" if self._position_id is not None else "idle",\n'
            '                "position_id": self._position_id,\n'
            '                "tick_range": tick_range,\n'
            '                "pool": self.pool,\n'
            '                "in_range": _safe(snapshot.get("in_range")),\n'
            '                "fees_earned_usd": _safe(snapshot.get("fees_earned_usd")),\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    if template == StrategyTemplate.COPY_TRADER:
        extra = (
            "        status.update(\n"
            "            {\n"
            '                "open_trades_count": len(self._open_trades),\n'
            '                "action_types": [str(a) for a in self.action_types],\n'
            "            }\n"
            "        )\n"
            "        return status\n\n"
        )
        return base + extra

    # BLANK template: return the base dict as-is.
    return base + "        return status\n\n"


def _get_template_callbacks(template: StrategyTemplate) -> str:
    """Generate on_intent_executed and persistence callbacks for stateful templates."""
    if template == StrategyTemplate.DYNAMIC_LP:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track LP position after open/close."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "LP_OPEN" and result:\n'
            "            pid = getattr(result, 'position_id', None)\n"
            "            # LPCloseIntent.position_id requires a string, but the LP_OPEN\n"
            "            # result returns the NFT id as an int -- cast so both the\n"
            "            # rebalance close and the teardown close validate.\n"
            "            self._position_id = str(pid) if pid is not None else None\n"
            "            self._range_lower = getattr(intent, 'range_lower', None)\n"
            "            self._range_upper = getattr(intent, 'range_upper', None)\n"
            '            logger.info(f"LP opened: position_id={self._position_id}")\n'
            '        elif intent_type and intent_type.value == "LP_CLOSE":\n'
            "            self._position_id = None\n"
            "            self._range_lower = None\n"
            "            self._range_upper = None\n"
            '            logger.info("LP closed")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save position state for crash recovery."""\n'
            "        return {\n"
            '            "position_id": self._position_id,\n'
            "            # `is not None` guards: Slipstream tick bounds are raw ticks and a\n"
            "            # tick of 0 is a legitimate bound that a truthiness check would drop.\n"
            '            "range_lower": str(self._range_lower) if self._range_lower is not None else None,\n'
            '            "range_upper": str(self._range_upper) if self._range_upper is not None else None,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore position state after restart."""\n'
            "        if state:\n"
            '            pid = state.get("position_id")\n'
            "            self._position_id = str(pid) if pid is not None else None\n"
            '            rl = state.get("range_lower")\n'
            '            ru = state.get("range_upper")\n'
            '            # `in (None, "")` guards: a Slipstream tick bound of 0 must\n'
            "            # round-trip (truthiness would drop it) while a legacy empty\n"
            "            # string still reads as unmeasured.\n"
            '            self._range_lower = Decimal(rl) if rl not in (None, "") else None\n'
            '            self._range_upper = Decimal(ru) if ru not in (None, "") else None\n'
            "\n"
            "    # ------------------------------------------------------------------\n"
            "    # Aerodrome Slipstream tick helpers (VIB-5557)\n"
            "    # ------------------------------------------------------------------\n"
            "    # Slipstream's compiler consumes raw integer ticks aligned to the\n"
            '    # pool\'s tick spacing (pool format "TOKEN0/TOKEN1/<tick_spacing>").\n'
            "    # The price->tick conversion lives here so a config-only switch to\n"
            "    # protocol=aerodrome_slipstream keeps this scaffold compiling.\n"
            "\n"
            "    def _uses_tick_ranges(self) -> bool:\n"
            '        """True when the configured protocol addresses LP ranges in raw ticks."""\n'
            '        return self.protocol == "aerodrome_slipstream"\n'
            "\n"
            "    def _pool_tick_for_price(self, market, price_usd):\n"
            '        """Convert a USD price of ``base_token`` into the pool\'s native tick.\n'
            "\n"
            "        Ticks are defined on the pool's canonical token0/token1 pair\n"
            "        (ordered by address; the compiler rejects non-canonical pool\n"
            '        strings), so the USD price is rebased to "token1 per token0"\n'
            "        before conversion. Token decimals come from the token registry --\n"
            "        never guessed (a wrong decimals pair shifts the tick by ~276k).\n"
            '        """\n'
            "        from almanak.framework.data.tokens import get_token_resolver\n"
            "        from almanak.framework.intents import price_to_tick\n"
            "\n"
            '        token0, token1 = self.pool.split("/")[0], self.pool.split("/")[1]\n'
            "        resolver = get_token_resolver()\n"
            "        decimals0 = resolver.get_decimals(self.chain, token0)\n"
            "        decimals1 = resolver.get_decimals(self.chain, token1)\n"
            "        quote_price = market.price(self.quote_token)\n"
            "        # Both prices divide below; a None / zero / negative price would\n"
            "        # raise TypeError / ZeroDivisionError or feed price_to_tick a\n"
            "        # non-positive ratio (log domain error -> garbage tick). Fail loud.\n"
            "        if not quote_price or quote_price <= 0 or not price_usd or price_usd <= 0:\n"
            "            raise ValueError(\n"
            "                f'Non-positive price for tick conversion: '\n"
            "                f'base={price_usd}, quote={quote_price}'\n"
            "            )\n"
            "        if token0.upper() == self.base_token.upper():\n"
            "            pool_price = price_usd / quote_price  # token1 per token0\n"
            "        else:\n"
            "            pool_price = quote_price / price_usd  # base is token1 -> invert\n"
            "        return price_to_tick(pool_price, decimals0=decimals0, decimals1=decimals1)\n"
            "\n"
            "    def _tick_band_for_prices(self, market, lower_price, upper_price):\n"
            '        """Convert a USD price band into a spacing-aligned (tick_lower, tick_upper)."""\n'
            "        import math\n"
            "\n"
            '        tick_spacing = int(self.pool.split("/")[2])\n'
            "        tick_a = self._pool_tick_for_price(market, lower_price)\n"
            "        tick_b = self._pool_tick_for_price(market, upper_price)\n"
            "        # Inverted pairs (base token is pool token1) flip the band direction.\n"
            "        tick_lower, tick_upper = min(tick_a, tick_b), max(tick_a, tick_b)\n"
            "        tick_lower = math.floor(tick_lower / tick_spacing) * tick_spacing\n"
            "        tick_upper = math.floor(tick_upper / tick_spacing) * tick_spacing\n"
            "        if tick_upper <= tick_lower:\n"
            "            tick_upper = tick_lower + tick_spacing\n"
            "        return tick_lower, tick_upper\n"
            "\n"
        )

    elif template == StrategyTemplate.LENDING_LOOP:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Advance leverage loop state machine after intent execution."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "SUPPLY":\n'
            "            self._loop_state = LendingLoopState.SUPPLIED\n"
            '            supply_amt = getattr(intent, "amount", None)\n'
            "            if isinstance(supply_amt, Decimal):\n"
            "                self._total_collateral += supply_amt\n"
            '            logger.info(f"Supply confirmed (loop {self._loop_count + 1}) -> supplied")\n'
            '        elif intent_type.value == "BORROW":\n'
            "            self._loop_state = LendingLoopState.BORROWED\n"
            '            borrow_amt = getattr(intent, "borrow_amount", None)\n'
            "            if isinstance(borrow_amt, Decimal):\n"
            "                self._total_borrowed += borrow_amt\n"
            '            logger.info(f"Borrow confirmed (loop {self._loop_count + 1}) -> borrowed")\n'
            '        elif intent_type.value == "SWAP":\n'
            "            # In MONITORING state, a SWAP is the collateral->debt unwind that\n"
            "            # precedes a repay; do NOT advance the loop counter or leverage.\n"
            "            if self._loop_state == LendingLoopState.MONITORING:\n"
            '                logger.info("Unwind swap confirmed -- awaiting repay")\n'
            "                return\n"
            "            self._loop_count += 1\n"
            "            # Estimate leverage: geometric series 1 + r + r^2 + ... + r^n\n"
            "            # where r = borrow_ratio (approximate LTV usage)\n"
            "            leverage = sum(\n"
            "                self.borrow_ratio ** i for i in range(self._loop_count + 1)\n"
            "            )\n"
            "            self._current_leverage = leverage\n"
            "            if leverage >= self.target_leverage:\n"
            "                self._loop_state = LendingLoopState.MONITORING\n"
            "                logger.info(\n"
            '                    f"Loop {self._loop_count} complete: leverage ~{leverage:.2f}x "\n'
            '                    f">= target {self.target_leverage}x -> monitoring"\n'
            "                )\n"
            "            else:\n"
            "                self._loop_state = LendingLoopState.IDLE  # Loop again\n"
            "                logger.info(\n"
            '                    f"Loop {self._loop_count} complete: leverage ~{leverage:.2f}x "\n'
            '                    f"< target {self.target_leverage}x -> continuing"\n'
            "                )\n"
            '        elif intent_type.value == "REPAY":\n'
            "            # Deleverage confirmed -- refresh leverage estimate so subsequent\n"
            "            # log lines are accurate and the monitoring path shows the new state.\n"
            '            repay_full = bool(getattr(intent, "repay_full", False))\n'
            "            if repay_full:\n"
            '                self._total_borrowed = Decimal("0")\n'
            '                self._current_leverage = Decimal("1.0")\n'
            "                self._loop_state = LendingLoopState.MONITORING\n"
            '                logger.info("Full repay confirmed -- leverage reset to 1.0x")\n'
            "            else:\n"
            '                repay_amt = getattr(intent, "amount", None)\n'
            "                if isinstance(repay_amt, Decimal):\n"
            '                    self._total_borrowed = max(Decimal("0"), self._total_borrowed - repay_amt)\n'
            "                # Partial repay: conservatively shave ~25% off the estimate\n"
            "                # (the HF guard sizes partial repays at partial_repay_pct of debt).\n"
            "                self._current_leverage = max(\n"
            '                    Decimal("1.0"),\n'
            '                    self._current_leverage * (Decimal("1") - self.partial_repay_pct),\n'
            "                )\n"
            "                logger.info(\n"
            '                    f"Partial repay confirmed -- leverage ~{self._current_leverage:.2f}x"\n'
            "                )\n"
            '        elif intent_type.value == "WITHDRAW":\n'
            "            # WITHDRAW fires only from teardown. Track the recovered collateral\n"
            "            # so the post-recovery swap-back step can be skipped when nothing\n"
            "            # remains. withdraw_all=True clears the counter; a typed amount\n"
            "            # subtracts and clamps at zero.\n"
            '            withdraw_all = bool(getattr(intent, "withdraw_all", False))\n'
            '            withdraw_amt = getattr(intent, "amount", None)\n'
            '            if withdraw_all or withdraw_amt == "all":\n'
            '                self._total_collateral = Decimal("0")\n'
            "            elif isinstance(withdraw_amt, Decimal):\n"
            '                self._total_collateral = max(Decimal("0"), self._total_collateral - withdraw_amt)\n'
            '            logger.info(f"Withdraw confirmed -- collateral remaining: {self._total_collateral}")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save loop state and leverage tracking."""\n'
            "        return {\n"
            "            # StrEnum members serialize to their string value in JSON,\n"
            "            # so old persisted state files remain compatible.\n"
            '            "loop_state": self._loop_state,\n'
            '            "loop_count": self._loop_count,\n'
            '            "current_leverage": str(self._current_leverage),\n'
            '            "total_borrowed": str(self._total_borrowed),\n'
            '            "total_collateral": str(self._total_collateral),\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore loop state and leverage tracking.\n'
            "\n"
            "        Coerces the persisted string value back to the StrEnum member.\n"
            "        Pre-StrEnum state files (plain strings like 'idle') round-trip\n"
            "        cleanly because ``StrEnum(value)`` accepts the raw string.\n"
            '        """\n'
            "        if state:\n"
            '            raw_state = state.get("loop_state", LendingLoopState.IDLE.value)\n'
            "            self._loop_state = LendingLoopState(raw_state)\n"
            '            self._loop_count = state.get("loop_count", 0)\n'
            '            cl = state.get("current_leverage", "1.0")\n'
            "            self._current_leverage = Decimal(str(cl))\n"
            '            self._total_borrowed = Decimal(str(state.get("total_borrowed", "0")))\n'
            '            self._total_collateral = Decimal(str(state.get("total_collateral", "0")))\n'
            "\n"
        )

    elif template == StrategyTemplate.BASIS_TRADE:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Advance basis trade state machine."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "SWAP" and self._trade_state == BasisTradeState.IDLE:\n'
            "            self._trade_state = BasisTradeState.SPOT_BOUGHT\n"
            '            logger.info("Spot bought -> spot_bought")\n'
            '        elif intent_type.value == "SWAP" and self._trade_state == BasisTradeState.UNWINDING:\n'
            "            self._trade_state = BasisTradeState.IDLE\n"
            '            logger.info("Spot sold -> idle (unwind complete)")\n'
            '        elif intent_type.value == "PERP_OPEN":\n'
            "            self._trade_state = BasisTradeState.HEDGED\n"
            '            logger.info("Perp opened -> hedged")\n'
            '        elif intent_type.value == "PERP_CLOSE":\n'
            "            self._trade_state = BasisTradeState.UNWINDING\n"
            '            logger.info("Perp closed -> unwinding")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save trade state.\n'
            "\n"
            "        StrEnum members serialize to their string value in JSON, so old\n"
            "        persisted state files remain compatible.\n"
            '        """\n'
            '        return {"trade_state": self._trade_state}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore trade state.\n'
            "\n"
            "        Coerces the persisted string back to the StrEnum member. Accepts\n"
            "        both new (enum-backed) and legacy (plain-string) state files.\n"
            '        """\n'
            "        if state:\n"
            '            raw_state = state.get("trade_state", BasisTradeState.IDLE.value)\n'
            "            self._trade_state = BasisTradeState(raw_state)\n"
            "\n"
        )

    elif template == StrategyTemplate.VAULT_YIELD:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Update vault state after deposit/redeem."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "VAULT_DEPOSIT":\n'
            "            self._state = VaultYieldState.DEPOSITED\n"
            '            logger.info("Vault deposit confirmed -> deposited")\n'
            '        elif intent_type and intent_type.value == "VAULT_REDEEM":\n'
            "            self._state = VaultYieldState.IDLE\n"
            '            logger.info("Vault redeem confirmed -> idle")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save vault state.\n'
            "\n"
            "        StrEnum members serialize to their string value in JSON.\n"
            '        """\n'
            '        return {"state": self._state}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore vault state (coerces persisted string back to StrEnum)."""\n'
            "        if state:\n"
            '            raw_state = state.get("state", VaultYieldState.IDLE.value)\n'
            "            self._state = VaultYieldState(raw_state)\n"
            "\n"
        )

    elif template == StrategyTemplate.PERPS:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track perp position state."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "PERP_OPEN":\n'
            "            self._position_state = PerpsState.OPEN\n"
            "            # Pin the direction used for this open position. The config-driven\n"
            "            # direction could change between restarts while a position is still\n"
            "            # on-chain -- we must close the side we actually opened, not the\n"
            "            # newly-configured one. Source of truth for the live position.\n"
            "            self._position_is_long = self._is_long\n"
            "            self._position_direction = self.direction\n"
            "            # Try ResultEnricher extracted_data first, fall back to pending price\n"
            "            extracted = getattr(result, 'extracted_data', {}) or {}\n"
            "            self._entry_price = extracted.get('entry_price')\n"
            "            if self._entry_price is None:\n"
            "                self._entry_price = getattr(self, '_pending_entry_price', None)\n"
            '            logger.info(f"Perp opened {self._position_direction} at {self._entry_price}")\n'
            '        elif intent_type.value == "PERP_CLOSE":\n'
            "            self._position_state = PerpsState.IDLE\n"
            "            self._entry_price = None\n"
            "            self._position_is_long = None\n"
            "            self._position_direction = None\n"
            '            logger.info("Perp closed -> idle")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save perp state (StrEnum serializes to string for JSON compat)."""\n'
            "        return {\n"
            '            "position_state": self._position_state,\n'
            '            "entry_price": str(self._entry_price) if self._entry_price else None,\n'
            '            "position_is_long": self._position_is_long,\n'
            '            "position_direction": self._position_direction,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore perp state (coerces persisted string back to StrEnum).\n'
            "\n"
            "        When a position is open, restore the direction it was opened with\n"
            "        (ignoring any config change) so PnL math and teardown target the\n"
            "        correct side. When idle, use the config-driven direction.\n"
            '        """\n'
            "        if state:\n"
            '            raw_state = state.get("position_state", PerpsState.IDLE.value)\n'
            "            self._position_state = PerpsState(raw_state)\n"
            '            ep = state.get("entry_price")\n'
            "            self._entry_price = Decimal(ep) if ep else None\n"
            '            persisted_is_long = state.get("position_is_long")\n'
            '            persisted_direction = state.get("position_direction")\n'
            "            if self._position_state == PerpsState.OPEN and persisted_is_long is not None:\n"
            "                # Persisted direction wins over config for the live position\n"
            "                if persisted_is_long != self._is_long:\n"
            "                    logger.warning(\n"
            '                        f"Config direction={self.direction} differs from "\n'
            '                        f"persisted position_direction={persisted_direction}. "\n'
            '                        f"Using persisted direction for open position."\n'
            "                    )\n"
            "                self._position_is_long = persisted_is_long\n"
            "                self._position_direction = persisted_direction\n"
            "                self._is_long = persisted_is_long\n"
            "                self.direction = persisted_direction or self.direction\n"
            "            else:\n"
            "                self._position_is_long = None\n"
            "                self._position_direction = None\n"
            "\n"
        )

    elif template == StrategyTemplate.STAKING:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track staking state and amount."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "STAKE":\n'
            "            self._stake_state = StakingState.STAKED\n"
            "            self._staked_amount = getattr(intent, 'amount', self.stake_amount)\n"
            '            logger.info(f"Staked {self._staked_amount} {self.stake_token}")\n'
            '        elif intent_type.value == "UNSTAKE":\n'
            "            self._stake_state = StakingState.IDLE\n"
            "            self._staked_amount = None\n"
            '            logger.info("Unstaked -> idle")\n'
            '        elif intent_type.value == "SWAP" and self._stake_state == StakingState.IDLE:\n'
            "            # Track swap-before-stake output\n"
            '            logger.info("Pre-stake swap completed")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save stake state (StrEnum serializes to string for JSON compat)."""\n'
            "        return {\n"
            '            "stake_state": self._stake_state,\n'
            '            "staked_amount": str(self._staked_amount) if self._staked_amount else None,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore stake state (coerces persisted string back to StrEnum)."""\n'
            "        if state:\n"
            '            raw_state = state.get("stake_state", StakingState.IDLE.value)\n'
            "            self._stake_state = StakingState(raw_state)\n"
            '            sa = state.get("staked_amount")\n'
            "            self._staked_amount = Decimal(sa) if sa else None\n"
            "\n"
        )

    elif template == StrategyTemplate.MULTI_STEP:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track LP position after open/close in multi-step sequence."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "LP_OPEN" and result:\n'
            "            pid = getattr(result, 'position_id', None)\n"
            "            # LPCloseIntent.position_id requires a string, but the LP_OPEN\n"
            "            # result returns the NFT id as an int -- cast so the close validates.\n"
            "            self._position_id = str(pid) if pid is not None else None\n"
            "            self._range_lower = getattr(intent, 'range_lower', None)\n"
            "            self._range_upper = getattr(intent, 'range_upper', None)\n"
            '            logger.info(f"LP opened: position_id={self._position_id}")\n'
            '        elif intent_type and intent_type.value == "LP_CLOSE":\n'
            "            self._position_id = None\n"
            "            self._range_lower = None\n"
            "            self._range_upper = None\n"
            '            logger.info("LP closed")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save position state for crash recovery."""\n'
            "        return {\n"
            '            "position_id": self._position_id,\n'
            '            "range_lower": str(self._range_lower) if self._range_lower else None,\n'
            '            "range_upper": str(self._range_upper) if self._range_upper else None,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore position state after restart."""\n'
            "        if state:\n"
            '            pid = state.get("position_id")\n'
            "            self._position_id = str(pid) if pid is not None else None\n"
            '            rl = state.get("range_lower")\n'
            '            ru = state.get("range_upper")\n'
            "            self._range_lower = Decimal(rl) if rl else None\n"
            "            self._range_upper = Decimal(ru) if ru else None\n"
            "\n"
        )

    elif template == StrategyTemplate.TA_SWAP:
        return (
            "    def _reconcile_holding_base(self, market, base_balance=None):\n"
            '        """Re-derive the cached `_holding_base` flag from live balance.\n'
            "\n"
            "        The persisted `_holding_base` flag is only a HINT. The wallet's\n"
            "        live base-token balance is the source of truth. A stale/false\n"
            "        flag (e.g. after a restart whose runtime state desynced) must\n"
            "        never HOLD-lock a valid risk-off exit, so every cycle / resume /\n"
            "        teardown reconciles the flag from the live snapshot before any\n"
            "        decision is made (VIB-5155 / ALM-2719).\n"
            "\n"
            "        Returns True if the flag disagreed with live balance and was\n"
            "        corrected, False if it already agreed, and None if the live\n"
            "        balance could not be read (flag left untouched).\n"
            '        """\n'
            "        try:\n"
            "            if base_balance is None:\n"
            "                base_balance = market.balance(self.base_token)\n"
            "            native = base_balance.balance\n"
            "            usd = base_balance.balance_usd\n"
            "        except (ValueError, AttributeError) as e:\n"
            "            # Live balance unavailable: keep the cached hint, do not flip.\n"
            '            logger.debug(f"Could not reconcile holding flag from live balance: {e}")\n'
            "            return None\n"
            "        # Empty != Zero (VIB-5155): a non-zero native balance whose USD\n"
            "        # coerced to 0 means the price was UNMEASURED, not flat. Treating\n"
            "        # that as 'not holding' would withhold the teardown risk-off swap\n"
            "        # and strand the position. Native balance is truth; USD is only the\n"
            "        # dust threshold. Mirrors snapshot.py _coerce_balance_result.\n"
            "        if native > Decimal('0') and usd == Decimal('0'):\n"
            "            # Funded but unpriceable: hold (cannot dust-floor without a price).\n"
            "            live_holding = True\n"
            "        else:\n"
            "            live_holding = native > Decimal('0') and usd > self.holding_dust_usd\n"
            "        if live_holding != self._holding_base:\n"
            "            logger.warning(\n"
            '                f"Reconciling _holding_base {self._holding_base} -> {live_holding} '
            'from live balance "\n'
            '                f"({self.base_token} ${base_balance.balance_usd})"\n'
            "            )\n"
            "            self._holding_base = live_holding\n"
            "            return True\n"
            "        return False\n"
            "\n"
            "    def reconcile_resumed_state(self, market):\n"
            '        """Post-resume guardrail hook (VIB-5155 / ALM-2719).\n'
            "\n"
            "        Called once by the runner after state is restored and before\n"
            "        the first decide(). Re-derives `_holding_base` from live\n"
            "        balance so a desynced restart cannot strand a position.\n"
            '        """\n'
            "        return self._reconcile_holding_base(market)\n"
            "\n"
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track swap executions for position tracking."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "SWAP":\n'
            "            from_token = getattr(intent, 'from_token', None)\n"
            "            to_token = getattr(intent, 'to_token', None)\n"
            "            if to_token == self.base_token:\n"
            "                self._holding_base = True\n"
            '                self._last_signal = "buy"\n'
            '                logger.info(f"Bought {self.base_token}")\n'
            "            elif from_token == self.base_token:\n"
            "                self._holding_base = False\n"
            '                self._last_signal = "sell"\n'
            '                logger.info(f"Sold {self.base_token}")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save position state for crash recovery."""\n'
            '        return {"holding_base": self._holding_base, "last_signal": self._last_signal}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore position state after restart.\n'
            "\n"
            "        The restored `holding_base` value is only a HINT — it is\n"
            "        reconciled against live on-chain balance before the first\n"
            "        decision via reconcile_resumed_state() / _reconcile_holding_base()\n"
            "        (VIB-5155 / ALM-2719). Never trust the cached flag alone.\n"
            '        """\n'
            "        if state:\n"
            '            self._holding_base = state.get("holding_base", False)\n'
            '            self._last_signal = state.get("last_signal", "neutral")\n'
            "\n"
        )

    elif template == StrategyTemplate.COPY_TRADER:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track copied trades for position tracking and teardown."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            "        trade_record = {\n"
            '            "intent_type": intent_type.value,\n'
            "            \"from_token\": getattr(intent, 'from_token', None),\n"
            "            \"to_token\": getattr(intent, 'to_token', None),\n"
            "            \"token\": getattr(intent, 'token', None),\n"
            "            \"protocol\": getattr(intent, 'protocol', None),\n"
            '            "position_id": (\n'
            "                str(getattr(result, 'position_id', None))\n"
            "                if result and getattr(result, 'position_id', None) is not None\n"
            "                else None\n"
            "            ),\n"
            "            # Fields needed for LP/perp/borrow teardown\n"
            "            \"pool\": getattr(intent, 'pool', None),\n"
            "            \"market\": getattr(intent, 'market', None),\n"
            "            \"collateral_token\": getattr(intent, 'collateral_token', None),\n"
            "            \"is_long\": getattr(intent, 'is_long', None),\n"
            "            \"size_usd\": str(getattr(intent, 'size_usd', None)) if getattr(intent, 'size_usd', None) else None,\n"
            "            \"borrow_token\": getattr(intent, 'borrow_token', None),\n"
            "        }\n"
            "        self._open_trades.append(trade_record)\n"
            '        logger.info(f"Tracked copy trade: {intent_type.value}")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save copied trades for crash recovery."""\n'
            '        return {"open_trades": self._open_trades}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore copied trades after restart."""\n'
            "        if state:\n"
            '            self._open_trades = state.get("open_trades", [])\n'
            "\n"
        )

    # BLANK template: scaffold the teardown-state persistence hooks (VIB-5464 /
    # TD-06) so a blank strategy declares a posture the moment it opens a tracked
    # position. A strategy that opens a tracked position MUST guarantee it survives
    # a restart, or teardown goes blind. Fill these in as you add positions —
    # persist the state get_open_positions() reads.
    return (
        "    def get_persistent_state(self):\n"
        '        """Persist the position-tracking state teardown depends on.\n'
        "\n"
        "        VIB-5464 / TD-06: a restarted runner re-derives its open set from\n"
        "        what you persist here. If your strategy opens a tracked position,\n"
        "        return the fields get_open_positions() reads (e.g. position id,\n"
        "        amounts, state-machine phase) so teardown is never blind to it.\n"
        "        ALTERNATIVE: if get_open_positions() re-derives the open set purely\n"
        "        from on-chain reads, set the class attribute\n"
        "        ``teardown_state_derived_from_chain = True`` instead and return {}.\n"
        '        """\n'
        '        # TODO: return {"position_id": self._position_id, ...}\n'
        "        return {}\n\n"
        "    def load_persistent_state(self, state):\n"
        '        """Restore the state persisted by get_persistent_state() on restart."""\n'
        '        # TODO: self._position_id = state.get("position_id")\n'
        "        return None\n\n"
    )


def _build_strategy_content(
    name: str,
    template: StrategyTemplate,
    chain: str,
    output_dir: Path,
    protocol: str | None = None,
) -> str:
    """Build the strategy.py file content for v2 IntentStrategy.

    ``protocol`` is the scaffold-time protocol choice; it is rendered into the
    decorator metadata, the class docstring, and every template protocol
    default so nothing in the scaffold hardcodes a protocol the user did not
    choose. Defaults to the template's canonical protocol.
    """
    class_name = to_pascal_case(name) + "Strategy"
    strategy_name = to_snake_case(name)
    config = TEMPLATE_CONFIGS[template]
    protocol = protocol or config.default_protocol

    # Get template-specific code
    init_params = _get_template_init_params(template, config, protocol)
    decide_logic = _get_template_decide_logic(template, config)
    callbacks_str = _get_template_callbacks(template)
    get_status_block = _get_template_get_status(template, strategy_name)

    # State machine enum (typed StrEnum; empty for stateless templates).
    # Injected above the strategy class so authors (and tests) can import it.
    state_enum_block = _generate_state_enum_definition(template)
    # Only pull StrEnum into the generated file when a state machine is emitted;
    # otherwise the import would be unused (F401 lint error).
    # ``_safe`` (emitted below) always needs ``from enum import Enum``. When
    # the template also defines a StrEnum state machine, merge the two into a
    # single ``from enum import Enum, StrEnum`` to keep ruff/isort happy (two
    # separate ``from enum import ...`` lines trigger I001 in the scaffolded
    # file). The non-StrEnum path keeps ``from enum import Enum`` on its own.
    enum_import = "from enum import Enum, StrEnum\n" if state_enum_block else "from enum import Enum\n"
    # Blank line + block when we have an enum; empty string otherwise so the
    # resulting file has no awkward trailing blank lines.
    state_enum_section = f"\n\n{state_enum_block}" if state_enum_block else ""

    # Determine intent types based on template
    intent_types = {
        StrategyTemplate.BLANK: '["SWAP", "HOLD"]',
        StrategyTemplate.TA_SWAP: '["SWAP", "HOLD"]',
        StrategyTemplate.DYNAMIC_LP: '["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"]',
        StrategyTemplate.LENDING_LOOP: '["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"]',
        StrategyTemplate.BASIS_TRADE: '["SWAP", "PERP_OPEN", "PERP_CLOSE", "HOLD"]',
        StrategyTemplate.VAULT_YIELD: '["VAULT_DEPOSIT", "VAULT_REDEEM", "HOLD"]',
        StrategyTemplate.COPY_TRADER: (
            '[\n        "SWAP", "LP_OPEN", "LP_CLOSE", "SUPPLY", "WITHDRAW",\n'
            '        "BORROW", "REPAY", "PERP_OPEN", "PERP_CLOSE", "HOLD",\n    ]'
        ),
        StrategyTemplate.PERPS: '["PERP_OPEN", "PERP_CLOSE", "HOLD"]',
        StrategyTemplate.MULTI_STEP: '["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"]',
        StrategyTemplate.STAKING: '["STAKE", "UNSTAKE", "SWAP", "HOLD"]',
    }

    teardown_code = _get_template_teardown(template, config, strategy_name, protocol)

    quote_asset_line = _quote_asset_decorator_line(template, chain)

    part1 = f'''"""
{config.name} Strategy: {name}

{config.description}

Generated by: almanak strat new
Template: {template.value}
Chain: {chain}
Created: {datetime.now().isoformat()}

Strategy Pattern:
-----------------
1. Inherit from IntentStrategy
2. Use @almanak_strategy decorator for metadata
3. Implement decide(market) method that returns an Intent
4. The framework handles compilation and execution
"""

import logging
from datetime import date, datetime
from decimal import ROUND_DOWN, Decimal  # noqa: F401 - ROUND_DOWN used by lending template only
{enum_import}from typing import Any

# Core strategy framework imports
from almanak.framework.intents import AnyIntent, Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import (
    DecideResult,
    IntentStrategy,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


def _safe(v: Any) -> Any:
    """Normalise a ``get_status()`` field into a JSON-serialisable primitive.

    Called by the generated ``get_status()`` on every value that comes out of
    ``_last_position_snapshot`` (which strategies populate with whatever types
    they like — raw Decimal prices, datetime timestamps, Enum signals, ...).
    Without this, ``json.dumps(strategy.get_status())`` on an operator
    dashboard would crash the first time a snapshot carried a Decimal or a
    datetime.
    """
    if v is None:
        return None
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime | date):
        return v.isoformat()
    if isinstance(v, Enum):
        return getattr(v, "value", str(v))
    return v

{state_enum_section}

@almanak_strategy(
    name="{strategy_name}",
    description="{config.description}",
    version="1.0.0",
    author="Generated",
    tags=["generated", "{template.value}"],
    supported_chains=["{chain}"],
    supported_protocols=["{protocol}"],
    intent_types={intent_types[template]},
    default_chain="{chain}",
    {quote_asset_line}
)
class {class_name}(IntentStrategy):
    """
    {config.description}

    Chain: {chain}
    Protocol: {protocol}

    Configuration Parameters:
    -------------------------
    See config.json for configurable parameters.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the strategy with configuration.

        The base class (IntentStrategy) handles:
        - self.config: Strategy configuration (dict or dataclass)
        - self.chain: The blockchain to operate on
        - self.wallet_address: The wallet executing trades
        """
        super().__init__(*args, **kwargs)

        # Helper to get config value from dict or object attributes
        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)
{init_params}

        logger.info(f"{class_name} initialized on {{self.chain}}")

    def decide(self, market: MarketSnapshot) -> DecideResult:
        """
        Make a trading decision based on current market conditions.

        This is the core method of the strategy. It's called by the framework
        on each iteration with fresh market data.

        Parameters:
            market: MarketSnapshot containing:
                - market.price(token): Get current price in USD
                - market.rsi(token, period): Get RSI indicator
                - market.balance(token): Get wallet balance
                - market.chain: Current chain
                - market.wallet_address: Current wallet

        Returns:
            DecideResult: What action to take
                - Intent.swap(...): Execute a swap
                - Intent.hold(...): Do nothing
                - Intent.sequence([...]): Execute dependent intents in order
                - None: Also means hold
        """
        try:{decide_logic}

        except Exception as e:
            logger.exception(f"Error in decide(): {{e}}")
            return Intent.hold(reason=f"Error: {{str(e)}}")

{get_status_block}'''

    part2 = f'''
if __name__ == "__main__":
    print("=" * 60)
    print("{class_name}")
    print("=" * 60)
    print(f"Strategy Name: {{{class_name}.STRATEGY_NAME}}")
    print(f"Supported Chains: {{{class_name}.SUPPORTED_CHAINS}}")
    print(f"Supported Protocols: {{{class_name}.SUPPORTED_PROTOCOLS}}")
    print(f"Intent Types: {{{class_name}.INTENT_TYPES}}")
    print("\\nTo run this strategy:")
    print("  uv run almanak strat run --once")
'''

    return part1 + callbacks_str + teardown_code + part2


def generate_strategy_file(
    name: str,
    template: StrategyTemplate,
    chain: str,
    output_dir: Path,
    protocol: str | None = None,
) -> str:
    """Generate the main strategy.py file content for v2 IntentStrategy.

    ``protocol`` optionally overrides the template's canonical protocol; it is
    rendered into decorator metadata and template protocol defaults.
    """
    return _build_strategy_content(name, template, chain, output_dir, protocol)


def generate_config_json(
    name: str,
    template: StrategyTemplate,
    chain: str,
    protocol: str | None = None,
) -> str:
    """Generate config.json content for the strategy.

    This produces the runtime config file that load_strategy_config() reads.
    The top-level ``chain`` field is emitted so tools reading config.json (AI
    planners, operators, deployment UIs) can see the target chain without
    importing the strategy module. At runtime it acts as an explicit override
    of the @almanak_strategy decorator's default_chain (priority order set in
    ``almanak/framework/cli/run.py``).

    ``protocol`` optionally overrides the template's canonical protocol; the
    emitted protocol keys always match the generated strategy.py defaults.
    """
    import json

    protocol = protocol or TEMPLATE_CONFIGS[template].default_protocol

    # Chain first, then tunable template parameters.
    data: dict[str, object] = {"chain": chain}

    # Template-specific parameters (matching what __init__ reads via get_config)
    if template == StrategyTemplate.TA_SWAP:
        data.update(
            {
                "indicator": "rsi",
                "base_token": "WETH",
                "quote_token": "USDC",
                "rsi_period": 14,
                "rsi_oversold": 30,
                "rsi_overbought": 70,
                "bb_period": 20,
                "bb_std_dev": 2.0,
                "bb_timeframe": "1h",
                "squeeze_threshold": 0.02,
                "buy_percent_b": 0.0,
                "sell_percent_b": 1.0,
                "trade_size_usd": 1000,
                "max_slippage_bps": 50,
                # Gas-worthiness gate (see strategy decide()):
                # - min_trade_value_usd: absolute floor; trade is held if trade_size < floor
                # - max_gas_ratio: reject when estimated gas cost > ratio * trade_size
                "min_trade_value_usd": "10",
                "max_gas_ratio": "0.05",
                # Live-balance reconciliation dust floor (VIB-5155): the cached
                # ``_holding_base`` flag is re-derived from live balance each
                # cycle; a base position worth <= this USD value is treated as
                # dust (not "holding base"), so it cannot HOLD-lock an exit.
                "holding_dust_usd": "1",
            }
        )
    elif template == StrategyTemplate.DYNAMIC_LP:
        data.update(
            {
                "pool": _default_lp_pool(protocol),
                "protocol": protocol,
                "base_token": "WETH",
                "quote_token": "USDC",
                "range_width_pct": 5,
                "rebalance_threshold_pct": 80,
                "min_position_usd": 500,
            }
        )
    elif template == StrategyTemplate.LENDING_LOOP:
        data.update(
            {
                "collateral_token": "WETH",
                "borrow_token": "USDC",
                "supply_amount": "1",
                "borrow_amount": "500",
                "target_leverage": "2.0",
                "borrow_ratio": "0.7",
                "min_health_factor": "1.5",
                "emergency_threshold": "1.2",
                "partial_repay_pct": "0.25",
                "lending_protocol": protocol,
                # Morpho Blue: set the bytes32 market id here; Compound V3: set
                # the Comet market key (e.g. "usdc", "weth"); Aave V3: leave blank.
                "lending_market": "",
                "min_collateral_usd": "100",
            }
        )
    elif template == StrategyTemplate.BASIS_TRADE:
        data.update(
            {
                "protocol": protocol,
                "base_token": "WETH",
                "quote_token": "USDC",
                "perp_market": "ETH/USD",
                "spot_size_usd": "10000",
                "hedge_ratio": "1.0",
                "funding_entry_threshold": "0.0001",
                "funding_exit_threshold": "-0.00005",
            }
        )
    elif template == StrategyTemplate.VAULT_YIELD:
        data.update(
            {
                "protocol": protocol,
                "vault_address": "0x0000000000000000000000000000000000000000",
                "deposit_token": "USDC",
                "deposit_amount": 1000,
                "min_deposit_usd": 100,
                "max_vault_allocation_pct": 80,
            }
        )
    elif template == StrategyTemplate.COPY_TRADER:
        data.update(
            {
                "copy_trading": {
                    "leaders": [{"address": anvil_default_address(1), "chain": chain}],
                    "sizing": {"mode": "fixed_usd", "fixed_usd": 100},
                    "risk": {"max_trade_usd": 1000, "max_slippage": "0.01"},
                },
            }
        )
    elif template == StrategyTemplate.PERPS:
        data.update(
            {
                "protocol": protocol,
                "perp_market": "ETH/USD",
                "collateral_token": "USDC",
                "collateral_amount": 100,
                "position_size_usd": 1000,
                "leverage": 5,
                "take_profit_pct": 0.05,
                "stop_loss_pct": 0.03,
                "base_token": "ETH",
                "direction": "LONG",
            }
        )
    elif template == StrategyTemplate.MULTI_STEP:
        data.update(
            {
                "pool": _default_lp_pool(protocol),
                "protocol": protocol,
                "base_token": "WETH",
                "quote_token": "USDC",
                "range_width_pct": 5,
                "rebalance_drift_pct": 3,
                "min_position_usd": 500,
            }
        )
    elif template == StrategyTemplate.STAKING:
        data.update(
            {
                "stake_token": "ETH",
                "stake_amount": 1,
                "staking_protocol": protocol,
                "quote_token": "USDC",
                "swap_before_stake": True,
            }
        )
    else:  # BLANK: seed with example config
        data.update(
            {
                "base_token": "WETH",
                "quote_token": "USDC",
                "trade_size_usd": "100",
            }
        )

    # Add token_funding for all templates (except COPY_TRADER which discovers
    # tokens dynamically). Addresses are resolved from the static token registry
    # for the scaffold's chain; unresolvable symbols are omitted rather than
    # emitted as placeholders.
    if template != StrategyTemplate.COPY_TRADER and "token_funding" not in data:
        token_funding = _default_token_funding(chain)
        if token_funding:
            data["token_funding"] = token_funding

    # Add anvil_funding for all templates (unless already set).
    # This ensures `almanak strat run --network anvil` funds the wallet automatically.
    if "anvil_funding" not in data:
        data["anvil_funding"] = _CHAIN_NATIVE_FUNDING.get(chain, _DEFAULT_ANVIL_FUNDING)

    return json.dumps(data, indent=4) + "\n"


# ---------------------------------------------------------------------------
# Test scaffolding helpers (for generate_test_file below)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StateTransition:
    """A single on_intent_executed state transition to test.

    Describes how to invoke on_intent_executed() and which state field on
    the strategy should have changed after the call.

    Fields:
        name: pytest-parameterize id for the transition
        intent_type: intent_type.value passed in the mock intent (e.g. "SWAP")
        intent_attrs: extra attributes to set on the mock intent (for templates
            that read intent.from_token / intent.to_token / intent.amount)
        result_attrs: extra attributes to set on the mock result (e.g. position_id)
        pre_state: dict of attribute_name -> value to set BEFORE the callback
        expected: dict of attribute_name -> expected value after the callback
    """

    name: str
    intent_type: str
    intent_attrs: dict[str, object]
    result_attrs: dict[str, object]
    pre_state: dict[str, object]
    expected: dict[str, object]


@dataclass(frozen=True)
class _TemplateTestSpec:
    """Test metadata for a strategy template.

    Drives what the emitted test file exercises per-template:
    * state_fields: names of ``self._xxx`` attributes that hold runtime state
        (used to skip persistence tests cleanly on stateless templates)
    * has_callbacks: whether the template emits on_intent_executed / persistence
    * has_teardown_intents: whether generate_teardown_intents() returns
        non-empty results once a position is held (affects SOFT vs HARD tests)
    * position_setup: Python source to set on the strategy to give it an
        "open position" (so get_open_positions() returns something and
        generate_teardown_intents() produces intents)
    * transitions: list of on_intent_executed calls to test
    * persistent_state_sample: a representative state dict that round-trips
    * reconciles_side_state: whether the template caches a position-side flag
        that is reconciled from live balance on resume / before teardown
        (VIB-5155 / ALM-2719). When True the emitted suite includes a
        desync regression test proving a stale/false flag can still exit.
    """

    state_fields: tuple[str, ...] = ()
    has_callbacks: bool = False
    has_teardown_intents: bool = False
    position_setup: str = ""
    transitions: tuple[_StateTransition, ...] = ()
    persistent_state_sample: dict[str, object] | None = None
    reconciles_side_state: bool = False


_BLANK_TEST_SPEC = _TemplateTestSpec()


# Position setup snippets are embedded into emitted test code and run against
# the strategy instance (as ``strategy.<field> = ...``). They must match the
# __init__ fields produced by ``_get_template_init_params``.
_TEMPLATE_TEST_SPECS: dict[StrategyTemplate, _TemplateTestSpec] = {
    StrategyTemplate.TA_SWAP: _TemplateTestSpec(
        state_fields=("_holding_base", "_last_signal"),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup="strategy._holding_base = True",
        transitions=(
            _StateTransition(
                name="swap_to_base_sets_holding_and_buy_latch",
                intent_type="SWAP",
                intent_attrs={"from_token": "USDC", "to_token": "WETH"},
                result_attrs={},
                pre_state={"_holding_base": False, "_last_signal": "neutral"},
                expected={"_holding_base": True, "_last_signal": "buy"},
            ),
            _StateTransition(
                name="swap_from_base_clears_holding_and_sell_latch",
                intent_type="SWAP",
                intent_attrs={"from_token": "WETH", "to_token": "USDC"},
                result_attrs={},
                pre_state={"_holding_base": True, "_last_signal": "buy"},
                expected={"_holding_base": False, "_last_signal": "sell"},
            ),
        ),
        persistent_state_sample={"holding_base": True, "last_signal": "buy"},
        reconciles_side_state=True,
    ),
    StrategyTemplate.DYNAMIC_LP: _TemplateTestSpec(
        state_fields=("_position_id", "_range_lower", "_range_upper"),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup=(
            # position_id is a string because Intent.lp_close() expects str
            'strategy._position_id = "12345"\n'
            '        strategy._range_lower = Decimal("1900")\n'
            '        strategy._range_upper = Decimal("2100")'
        ),
        transitions=(
            _StateTransition(
                name="lp_open_coerces_int_position_id_to_str",
                intent_type="LP_OPEN",
                intent_attrs={"range_lower": "1800", "range_upper": "2200"},
                # Int NFT id from the LP_OPEN receipt must be coerced to str so
                # LPCloseIntent (which requires str) validates -- seed an int to
                # pin the coercion; dropping str(...) makes this assert fail.
                result_attrs={"position_id": 99999},
                pre_state={"_position_id": None},
                expected={"_position_id": "99999"},
            ),
            _StateTransition(
                name="lp_close_clears_position_id",
                intent_type="LP_CLOSE",
                intent_attrs={},
                result_attrs={},
                pre_state={"_position_id": "12345"},
                expected={"_position_id": None},
            ),
        ),
        persistent_state_sample={
            "position_id": "12345",
            "range_lower": "1900",
            "range_upper": "2100",
        },
    ),
    StrategyTemplate.MULTI_STEP: _TemplateTestSpec(
        state_fields=("_position_id", "_range_lower", "_range_upper"),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup=(
            'strategy._position_id = "12345"\n'
            '        strategy._range_lower = Decimal("1900")\n'
            '        strategy._range_upper = Decimal("2100")'
        ),
        transitions=(
            _StateTransition(
                name="lp_open_coerces_int_position_id_to_str",
                intent_type="LP_OPEN",
                intent_attrs={},
                # Int NFT id from the LP_OPEN receipt must be coerced to str so
                # LPCloseIntent (which requires str) validates -- seed an int to
                # pin the coercion; dropping str(...) makes this assert fail.
                result_attrs={"position_id": 99999},
                pre_state={"_position_id": None},
                expected={"_position_id": "99999"},
            ),
            _StateTransition(
                name="lp_close_clears_position_id",
                intent_type="LP_CLOSE",
                intent_attrs={},
                result_attrs={},
                pre_state={"_position_id": "12345"},
                expected={"_position_id": None},
            ),
        ),
        persistent_state_sample={
            "position_id": "12345",
            "range_lower": "1900",
            "range_upper": "2100",
        },
    ),
    StrategyTemplate.LENDING_LOOP: _TemplateTestSpec(
        state_fields=(
            "_loop_state",
            "_loop_count",
            "_current_leverage",
            "_total_borrowed",
            "_total_collateral",
        ),
        has_callbacks=True,
        has_teardown_intents=True,
        # Inject totals plus a stubbed ``create_market_snapshot`` so the
        # no-``market``-arg teardown tests resolve without a network call. The
        # teardown now delegates to the health-factor-aware staircase helper,
        # which reads ``position_health`` (collateral/debt/lltv) to size the
        # unwind -- so the stub must expose a numeric health snapshot, prices,
        # and balances. The real production caller (TeardownManager) always
        # passes ``market``; this is a test-only safety net.
        position_setup=(
            'strategy._loop_state = "borrowed"\n'
            "        strategy._loop_count = 1\n"
            '        strategy._total_borrowed = Decimal("100")\n'
            '        strategy._total_collateral = Decimal("0.5")\n'
            "        _lending_loop_snapshot = MagicMock()\n"
            '        _lending_loop_snapshot.price.return_value = Decimal("1")\n'
            "        _ll_balance = MagicMock()\n"
            '        _ll_balance.balance = Decimal("0")\n'
            "        _lending_loop_snapshot.balance.return_value = _ll_balance\n"
            "        _ll_health = MagicMock()\n"
            '        _ll_health.collateral_value_usd = Decimal("1000")\n'
            '        _ll_health.debt_value_usd = Decimal("500")\n'
            '        _ll_health.lltv = Decimal("0.83")\n'
            '        _ll_health.health_factor = Decimal("1.66")\n'
            "        _lending_loop_snapshot.position_health.return_value = _ll_health\n"
            "        strategy.create_market_snapshot = lambda: _lending_loop_snapshot"
        ),
        transitions=(
            _StateTransition(
                name="supply_idle_to_supplied",
                intent_type="SUPPLY",
                intent_attrs={},
                result_attrs={},
                pre_state={"_loop_state": "idle"},
                expected={"_loop_state": "supplied"},
            ),
            _StateTransition(
                name="borrow_supplied_to_borrowed",
                intent_type="BORROW",
                intent_attrs={},
                result_attrs={},
                pre_state={"_loop_state": "supplied"},
                expected={"_loop_state": "borrowed"},
            ),
        ),
        persistent_state_sample={
            "loop_state": "monitoring",
            "loop_count": 2,
            "current_leverage": "2.19",
            "total_borrowed": "120.50",
            "total_collateral": "0.85",
        },
    ),
    StrategyTemplate.BASIS_TRADE: _TemplateTestSpec(
        state_fields=("_trade_state",),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup='strategy._trade_state = "hedged"',
        transitions=(
            _StateTransition(
                name="swap_idle_to_spot_bought",
                intent_type="SWAP",
                intent_attrs={},
                result_attrs={},
                pre_state={"_trade_state": "idle"},
                expected={"_trade_state": "spot_bought"},
            ),
            _StateTransition(
                name="perp_open_to_hedged",
                intent_type="PERP_OPEN",
                intent_attrs={},
                result_attrs={},
                pre_state={"_trade_state": "spot_bought"},
                expected={"_trade_state": "hedged"},
            ),
            _StateTransition(
                name="perp_close_to_unwinding",
                intent_type="PERP_CLOSE",
                intent_attrs={},
                result_attrs={},
                pre_state={"_trade_state": "hedged"},
                expected={"_trade_state": "unwinding"},
            ),
        ),
        persistent_state_sample={"trade_state": "hedged"},
    ),
    StrategyTemplate.VAULT_YIELD: _TemplateTestSpec(
        state_fields=("_state",),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup='strategy._state = "deposited"',
        transitions=(
            _StateTransition(
                name="vault_deposit_to_deposited",
                intent_type="VAULT_DEPOSIT",
                intent_attrs={},
                result_attrs={},
                pre_state={"_state": "idle"},
                expected={"_state": "deposited"},
            ),
            _StateTransition(
                name="vault_redeem_to_idle",
                intent_type="VAULT_REDEEM",
                intent_attrs={},
                result_attrs={},
                pre_state={"_state": "deposited"},
                expected={"_state": "idle"},
            ),
        ),
        persistent_state_sample={"state": "deposited"},
    ),
    StrategyTemplate.PERPS: _TemplateTestSpec(
        state_fields=("_position_state", "_entry_price"),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup=('strategy._position_state = "open"\n        strategy._entry_price = Decimal("2000")'),
        transitions=(
            _StateTransition(
                name="perp_open_sets_state",
                intent_type="PERP_OPEN",
                intent_attrs={},
                result_attrs={"extracted_data": {"entry_price": "2000"}},
                pre_state={"_position_state": "idle"},
                expected={"_position_state": "open"},
            ),
            _StateTransition(
                name="perp_close_clears_state",
                intent_type="PERP_CLOSE",
                intent_attrs={},
                result_attrs={},
                pre_state={"_position_state": "open"},
                expected={"_position_state": "idle"},
            ),
        ),
        persistent_state_sample={"position_state": "open", "entry_price": "2000"},
    ),
    StrategyTemplate.STAKING: _TemplateTestSpec(
        state_fields=("_stake_state", "_staked_amount"),
        has_callbacks=True,
        has_teardown_intents=True,
        position_setup=('strategy._stake_state = "staked"\n        strategy._staked_amount = Decimal("1")'),
        transitions=(
            _StateTransition(
                name="stake_sets_staked",
                intent_type="STAKE",
                intent_attrs={"amount": "1"},
                result_attrs={},
                pre_state={"_stake_state": "idle"},
                expected={"_stake_state": "staked"},
            ),
            _StateTransition(
                name="unstake_returns_to_idle",
                intent_type="UNSTAKE",
                intent_attrs={},
                result_attrs={},
                pre_state={"_stake_state": "staked"},
                expected={"_stake_state": "idle"},
            ),
        ),
        persistent_state_sample={"stake_state": "staked", "staked_amount": "1"},
    ),
    StrategyTemplate.COPY_TRADER: _TemplateTestSpec(
        state_fields=("_open_trades",),
        has_callbacks=True,
        has_teardown_intents=False,  # teardown depends on leader trades; skip with-position tests
        position_setup="",
        transitions=(
            _StateTransition(
                name="swap_appends_trade",
                intent_type="SWAP",
                intent_attrs={"from_token": "USDC", "to_token": "WETH"},
                result_attrs={},
                pre_state={"_open_trades": []},
                expected={"_open_trades_len": 1},  # special: checks len()
            ),
        ),
        persistent_state_sample={"open_trades": []},
    ),
}


def _render_test_file_header(
    name: str,
    class_name: str,
    chain: str,
    template: StrategyTemplate,
) -> str:
    """Module docstring, imports, and shared fixtures."""
    return f'''"""
Tests for {name} strategy.

Generated by: almanak strat new
Template: {template.value}

These tests are a starting point -- extend them as your strategy evolves.
They cover: init, decide(), error handling, state transitions, persistence
round-trip, teardown intents, and common edge cases (zero balance, zero price).
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from strategy import {class_name}

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json, falling back to a minimal stub."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {{
        "deployment_id": "test-strategy-001",
        "chain": "{chain}",
    }}


@pytest.fixture
def strategy(config: dict) -> {class_name}:
    """Fresh strategy instance for each test (no shared mutable state)."""
    return {class_name}(
        config=config,
        chain=config.get("chain", "{chain}"),
        wallet_address="0x" + "1" * 40,
    )


def _make_mock_market(
    *,
    price: Decimal = Decimal("2000"),
    balance: Decimal = Decimal("100"),
    balance_usd: Decimal = Decimal("100000"),
    rsi: Decimal = Decimal("50"),
    timestamp: datetime | None = None,
) -> MagicMock:
    """Build a configurable MarketSnapshot mock (generic smoke-test scaffolding).

    For tests of your strategy logic, prefer the real seeding API
    (``almanak.framework.market.testing.seeded``) and drive time-based tests
    through the snapshot ``timestamp`` rather than a patched clock helper.
    """
    market = MagicMock()
    market.price.return_value = price
    market.chain = "{chain}"
    market.wallet_address = "0x" + "1" * 40
    market.timestamp = timestamp or datetime(2026, 1, 1, tzinfo=UTC)

    balance_mock = MagicMock()
    balance_mock.balance = balance
    balance_mock.balance_usd = balance_usd
    market.balance.return_value = balance_mock

    rsi_mock = MagicMock()
    rsi_mock.value = rsi
    rsi_mock.is_oversold = rsi <= Decimal("30")
    rsi_mock.is_overbought = rsi >= Decimal("70")
    market.rsi.return_value = rsi_mock

    # Bollinger bands (used by ta_swap when indicator='bollinger' or 'rsi_bb')
    bb_mock = MagicMock()
    bb_mock.bandwidth = 0.05
    bb_mock.percent_b = 0.5
    market.bollinger_bands.return_value = bb_mock

    # Funding rate (used by basis_trade)
    funding_mock = MagicMock()
    funding_mock.rate_hourly = Decimal("0.0002")
    market.funding_rate.return_value = funding_mock

    return market


@pytest.fixture
def mock_market() -> MagicMock:
    """MarketSnapshot mock with healthy defaults (ETH=$2000, balances funded)."""
    return _make_mock_market()


def _make_mock_intent(intent_type_value: str, **attrs: object) -> MagicMock:
    """Build a minimal intent mock with intent_type.value == intent_type_value.

    Extra kwargs become attributes on the returned mock (e.g. from_token).
    """
    intent = MagicMock()
    intent.intent_type.value = intent_type_value
    for key, value in attrs.items():
        setattr(intent, key, value)
    return intent


def _make_mock_result(**attrs: object) -> MagicMock:
    """Build a minimal execution-result mock (used by on_intent_executed tests)."""
    result = MagicMock()
    result.extracted_data = attrs.pop("extracted_data", {{}})
    for key, value in attrs.items():
        setattr(result, key, value)
    return result
'''


def _render_base_tests(
    class_name: str,
    chain: str,
) -> str:
    """init, decide(), error handling, get_status -- the 'always-emitted' tests."""
    return f'''

# ---------------------------------------------------------------------------
# Base tests: init, decide(), error handling, get_status
# ---------------------------------------------------------------------------


class Test{class_name}Basics:
    """Core contract: strategy constructs, decides, and reports status cleanly."""

    def test_initialization(self, strategy: {class_name}) -> None:
        """Strategy reports the chain and wallet it was constructed with."""
        assert strategy.chain == "{chain}"
        assert strategy.wallet_address == "0x" + "1" * 40

    def test_decide_returns_intent_or_none(
        self, strategy: {class_name}, mock_market: MagicMock
    ) -> None:
        """decide() must return None, an Intent (with intent_type), or an IntentSequence.

        Any other return type breaks the framework's intent compiler.
        """
        result = strategy.decide(mock_market)
        # Accept three valid return types: None, Intent (has .intent_type),
        # or IntentSequence (has .intents).
        assert (
            result is None
            or hasattr(result, "intent_type")
            or hasattr(result, "intents")
        ), (
            f"decide() returned {{type(result).__name__}} which is not a "
            "valid Intent / IntentSequence / None"
        )

    def test_decide_handles_market_errors_gracefully(
        self, strategy: {class_name}, mock_market: MagicMock
    ) -> None:
        """When market providers raise, decide() must NOT propagate -- return hold/None."""
        # Blow up every market access to simulate a gateway outage.
        mock_market.balance.side_effect = ValueError("Balance unavailable")
        mock_market.price.side_effect = ValueError("Price unavailable")
        mock_market.rsi.side_effect = ValueError("RSI unavailable")
        mock_market.bollinger_bands.side_effect = ValueError("BB unavailable")
        mock_market.funding_rate.side_effect = ValueError("Funding unavailable")
        mock_market.wallet_activity.side_effect = ValueError("Wallet activity unavailable")

        result = strategy.decide(mock_market)

        # Must not raise. Must return a hold intent or None.
        assert (
            result is None
            or hasattr(result, "intent_type")
            or hasattr(result, "intents")
        ), "decide() returned non-Intent on error"
        if result is not None and hasattr(result, "intent_type"):
            intent_type = getattr(result.intent_type, "value", str(result.intent_type))
            assert intent_type == "HOLD", (
                f"Expected HOLD on error, got {{intent_type}}. "
                f"Reason: {{getattr(result, 'reason', '<no reason>')}}"
            )

    def test_get_status_contract(self, strategy: {class_name}) -> None:
        """get_status() returns a dict with at minimum 'strategy' and 'chain'."""
        status = strategy.get_status()

        assert isinstance(status, dict), "get_status() must return a dict"
        assert "strategy" in status, "status must include 'strategy' key"
        assert "chain" in status, "status must include 'chain' key"
        assert status["chain"] == "{chain}"

'''


def _render_edge_case_tests(class_name: str) -> str:
    """Zero-balance and zero-price edge cases.

    These catch a common beginner bug: dividing by price without guarding
    against price=0, or sizing trades without checking balance>0.
    """
    return f'''
# ---------------------------------------------------------------------------
# Edge cases: degenerate market inputs
# ---------------------------------------------------------------------------


class Test{class_name}EdgeCases:
    """Degenerate market inputs must not crash the strategy."""

    def test_decide_with_zero_balance_does_not_raise(
        self, strategy: {class_name}
    ) -> None:
        """With zero balance, decide() should return cleanly (typically a hold)."""
        market = _make_mock_market(balance=Decimal("0"), balance_usd=Decimal("0"))
        result = strategy.decide(market)
        assert (
            result is None
            or hasattr(result, "intent_type")
            or hasattr(result, "intents")
        )

    def test_decide_with_zero_price_does_not_raise(
        self, strategy: {class_name}
    ) -> None:
        """A price=0 (bad oracle) must not trigger a ZeroDivisionError.

        Strategies that size by ``amount_usd / price`` are especially vulnerable.
        If decide() raises anything except a hold, that is a bug to fix.
        """
        market = _make_mock_market(price=Decimal("0"))
        try:
            result = strategy.decide(market)
        except ZeroDivisionError as exc:
            pytest.fail(
                "decide() raised ZeroDivisionError on zero price; "
                "guard with ``if price > 0`` before sizing trades: "
                f"{{exc}}"
            )
        assert (
            result is None
            or hasattr(result, "intent_type")
            or hasattr(result, "intents")
        )

'''


def _render_teardown_tests(
    class_name: str,
    spec: "_TemplateTestSpec",
) -> str:
    """Teardown contract tests: returns summary, soft/hard modes, slippage."""
    # The "returns a list" contract tests call generate_teardown_intents() with
    # no market arg, so the strategy falls back to create_market_snapshot(). For
    # templates whose teardown reads on-chain state (e.g. lending_loop, via the
    # health-factor-aware staircase helper), reuse the same position_setup that
    # stubs create_market_snapshot -- otherwise the fallback hits the network.
    if spec.has_teardown_intents and spec.position_setup:
        returns_list_setup = spec.position_setup
    else:
        returns_list_setup = "# no held position required; teardown returns an empty list"

    position_tests = ""
    if spec.has_teardown_intents and spec.position_setup:
        position_tests = f'''
    def test_generate_teardown_intents_soft_mode_with_position(
        self, strategy: {class_name}
    ) -> None:
        """With a position held, SOFT teardown generates at least one intent."""
        from almanak.framework.teardown import TeardownMode

        # Simulate a held position
        {spec.position_setup}

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert isinstance(intents, list), "generate_teardown_intents() must return a list"
        assert len(intents) > 0, "Expected non-empty teardown intents when position is held"
        for intent in intents:
            assert hasattr(intent, "intent_type"), (
                f"Teardown returned non-Intent: {{type(intent).__name__}}"
            )

    def test_generate_teardown_intents_hard_mode_with_position(
        self, strategy: {class_name}
    ) -> None:
        """With a position held, HARD teardown also generates intents."""
        from almanak.framework.teardown import TeardownMode

        {spec.position_setup}

        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert isinstance(intents, list)
        assert len(intents) > 0, "Expected non-empty teardown intents in HARD mode"

    def test_generate_teardown_intents_hard_mode_higher_slippage(
        self, strategy: {class_name}
    ) -> None:
        """HARD mode should tolerate at least as much slippage as SOFT.

        Teardown goal in HARD mode is speed over cost; slippage must not
        be tighter than SOFT.
        """
        from almanak.framework.teardown import TeardownMode

        {spec.position_setup}
        soft_intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        # Reset state for a fresh HARD run (position_setup applied twice)
        {spec.position_setup}
        hard_intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)

        def _swap_slippages(intents):
            return [
                getattr(i, "max_slippage", None)
                for i in intents
                if getattr(i.intent_type, "value", str(i.intent_type)) == "SWAP"
                and getattr(i, "max_slippage", None) is not None
            ]

        soft_slippages = _swap_slippages(soft_intents)
        hard_slippages = _swap_slippages(hard_intents)
        if soft_slippages and hard_slippages:
            assert max(hard_slippages) >= max(soft_slippages), (
                f"HARD slippage {{max(hard_slippages)}} must be >= "
                f"SOFT slippage {{max(soft_slippages)}}"
            )

    def test_get_open_positions_reports_held_position(
        self, strategy: {class_name}
    ) -> None:
        """After faking a position, get_open_positions() should list it."""
        {spec.position_setup}

        summary = strategy.get_open_positions()
        assert summary is not None, "get_open_positions() must not return None"
        positions = getattr(summary, "positions", None)
        assert positions is not None, "summary must have a .positions attribute"
        assert len(positions) >= 1, "Expected at least one position to be reported"

    def test_generate_teardown_intents_no_position_returns_empty_list(
        self, strategy: {class_name}
    ) -> None:
        """A fresh strategy with no open position returns an empty list (no raise).

        The held-position path is covered above; this pins the []/no-raise
        contract on a FRESH strategy. ``create_market_snapshot`` is stubbed to a
        no-position snapshot so the no-``market``-arg fallback stays network-free
        (the teardown helper reads price/balance/position_health off it).
        """
        from almanak.framework.teardown import TeardownMode

        _empty = MagicMock()
        _empty.price.return_value = Decimal("1")
        _empty_bal = MagicMock()
        _empty_bal.balance = Decimal("0")
        _empty.balance.return_value = _empty_bal
        _empty_health = MagicMock()
        _empty_health.collateral_value_usd = Decimal("0")
        _empty_health.debt_value_usd = Decimal("0")
        _empty_health.lltv = Decimal("0.83")
        _empty_health.health_factor = Decimal("0")
        _empty.position_health.return_value = _empty_health
        strategy.create_market_snapshot = lambda: _empty

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert isinstance(intents, list), (
            f"Must return a list, got {{type(intents).__name__}}"
        )
        assert intents == [], "fresh strategy (no position) must yield no teardown intents"
'''

    return f'''
# ---------------------------------------------------------------------------
# Teardown: close-position safety (operators rely on this)
# ---------------------------------------------------------------------------


class Test{class_name}Teardown:
    """Teardown methods must honour the operator safety contract.

    See: docs/internal/blueprints/14-teardown-system.md
    """

    def test_teardown_methods_exist(self, strategy: {class_name}) -> None:
        """Both teardown methods are implemented (not inherited as no-ops)."""
        assert hasattr(strategy, "get_open_positions")
        assert hasattr(strategy, "generate_teardown_intents")
        assert callable(strategy.get_open_positions)
        assert callable(strategy.generate_teardown_intents)

    def test_get_open_positions_returns_summary(
        self, strategy: {class_name}
    ) -> None:
        """get_open_positions() returns a TeardownPositionSummary (or None)."""
        summary = strategy.get_open_positions()
        if summary is not None:
            assert hasattr(summary, "positions"), (
                "summary must be a TeardownPositionSummary with .positions"
            )
            assert isinstance(summary.positions, list)

    def test_generate_teardown_intents_soft_returns_list(
        self, strategy: {class_name}
    ) -> None:
        """Must return a list (possibly empty) of Intent-like objects."""
        from almanak.framework.teardown import TeardownMode

        {returns_list_setup}
        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert isinstance(intents, list), (
            f"Must return a list, got {{type(intents).__name__}}"
        )

    def test_generate_teardown_intents_hard_returns_list(
        self, strategy: {class_name}
    ) -> None:
        """HARD mode must also return a list (possibly empty)."""
        from almanak.framework.teardown import TeardownMode

        {returns_list_setup}
        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert isinstance(intents, list)
{position_tests}

'''


def _fmt_state_fields_tuple(fields: tuple[str, ...]) -> str:
    """Render ``fields`` as a Python tuple literal that fits the 120-col line budget.

    For short tuples emits a single line (matches the pre-existing format); for
    longer ones breaks across lines so generated tests stay lint-clean even as
    new state fields get added to a template.
    """
    single = repr(fields)
    if len(single) <= 60:
        return single
    inner = "\n".join(f"                {f!r}," for f in fields)
    return f"(\n{inner}\n            )"


def _fmt_persistent_sample(sample: dict[str, object]) -> str:
    """Same idea as ``_fmt_state_fields_tuple`` but for the persistence sample."""
    single = repr(sample)
    if len(single) <= 60:
        return single
    items = "\n".join(f"            {k!r}: {v!r}," for k, v in sample.items())
    return f"{{\n{items}\n        }}"


def _render_callback_tests(
    class_name: str,
    spec: "_TemplateTestSpec",
) -> str:
    """State machine transition tests driven by the template's transition spec."""
    state_fields_block = _fmt_state_fields_tuple(spec.state_fields)
    param_entries: list[str] = []
    for t in spec.transitions:
        param_entries.append(
            "        pytest.param(\n"
            f"            {t.intent_type!r},\n"
            f"            {t.intent_attrs!r},\n"
            f"            {t.result_attrs!r},\n"
            f"            {t.pre_state!r},\n"
            f"            {t.expected!r},\n"
            f"            id={t.name!r},\n"
            "        ),"
        )
    params_block = "\n".join(param_entries)

    return f'''
# ---------------------------------------------------------------------------
# State machine: on_intent_executed transitions
# ---------------------------------------------------------------------------

import copy  # noqa: E402 -- used by failure-path deepcopy assertions below


class Test{class_name}StateMachine:
    """on_intent_executed() must advance / clear state correctly."""

    @pytest.mark.parametrize(
        "intent_type_value,intent_attrs,result_attrs,pre_state,expected",
        [
{params_block}
        ],
    )
    def test_on_intent_executed_advances_state(
        self,
        strategy: {class_name},
        intent_type_value: str,
        intent_attrs: dict,
        result_attrs: dict,
        pre_state: dict,
        expected: dict,
    ) -> None:
        """Each template-specific transition should update the right state field."""
        # Seed the pre-state onto the strategy instance.
        for field, value in pre_state.items():
            setattr(strategy, field, value)

        intent = _make_mock_intent(intent_type_value, **intent_attrs)
        result = _make_mock_result(**result_attrs)

        strategy.on_intent_executed(intent, success=True, result=result)

        # Special-case: "<field>_len" asserts against len(strategy.<field>)
        for field, expected_value in expected.items():
            if field.endswith("_len"):
                real_field = field[: -len("_len")]
                actual_len = len(getattr(strategy, real_field))
                assert actual_len == expected_value, (
                    f"Expected len({{real_field}}) == {{expected_value}}, got {{actual_len}}"
                )
            else:
                actual = getattr(strategy, field)
                assert actual == expected_value, (
                    f"Expected {{field}} == {{expected_value!r}}, got {{actual!r}}"
                )

    def test_on_intent_executed_ignores_failures(
        self, strategy: {class_name}
    ) -> None:
        """success=False must NOT mutate state -- framework retries on failure.

        Uses deepcopy so in-place mutations of mutable fields (lists, dicts)
        are detected, not silently passed.
        """
        tracked_fields = [
            f for f in {state_fields_block} if hasattr(strategy, f)
        ]
        before = {{f: copy.deepcopy(getattr(strategy, f)) for f in tracked_fields}}

        intent = _make_mock_intent("SWAP")
        result = _make_mock_result()
        strategy.on_intent_executed(intent, success=False, result=result)

        after = {{f: copy.deepcopy(getattr(strategy, f)) for f in tracked_fields}}
        assert before == after, (
            f"Failed intents must not mutate state. Diff: "
            f"{{[(f, before[f], after[f]) for f in tracked_fields if before[f] != after[f]]}}"
        )

'''


def _render_persistence_tests(
    class_name: str,
    spec: "_TemplateTestSpec",
    chain: str,
) -> str:
    """get_persistent_state() / load_persistent_state() round-trip tests."""
    sample = spec.persistent_state_sample or {}
    sample_block = _fmt_persistent_sample(sample)
    return f'''
# ---------------------------------------------------------------------------
# Persistence: get_persistent_state / load_persistent_state round-trip
# ---------------------------------------------------------------------------


class Test{class_name}Persistence:
    """State must survive a save / load cycle so restarts don't lose context."""

    def test_get_persistent_state_returns_dict(
        self, strategy: {class_name}
    ) -> None:
        """get_persistent_state() returns a JSON-serializable dict."""
        state = strategy.get_persistent_state()
        assert isinstance(state, dict), "persistent state must be a dict"
        try:
            json.dumps(state, default=str)
        except (TypeError, ValueError) as exc:
            pytest.fail(f"persistent state is not JSON-serializable: {{exc}}")

    def test_load_persistent_state_round_trip(
        self, strategy: {class_name}, config: dict
    ) -> None:
        """Save state -> fresh instance -> load -> state preserved.

        This is the lifecycle the runner performs on restart. A break here
        means the strategy will 'forget' open positions after a crash.
        """
        sample = {sample_block}
        # Seed the current instance with the sample state
        strategy.load_persistent_state(sample)

        saved = strategy.get_persistent_state()
        assert isinstance(saved, dict)

        # Load into a brand-new instance
        fresh = {class_name}(
            config=config,
            chain=config.get("chain", "{chain}"),
            wallet_address="0x" + "1" * 40,
        )
        fresh.load_persistent_state(saved)

        # Both instances should now agree on every field they persist.
        fresh_saved = fresh.get_persistent_state()
        for key in saved:
            assert fresh_saved.get(key) == saved.get(key), (
                f"Round-trip lost key={{key}}: {{saved.get(key)!r}} -> {{fresh_saved.get(key)!r}}"
            )

    def test_load_persistent_state_with_empty_dict_does_not_raise(
        self, strategy: {class_name}
    ) -> None:
        """Empty / missing state on first run must be handled (no crash)."""
        strategy.load_persistent_state({{}})
        assert isinstance(strategy.get_persistent_state(), dict)

'''


def _render_resume_reconcile_tests(
    class_name: str,
    chain: str,
) -> str:
    """Resume-state reconciliation regression tests (VIB-5155 / ALM-2719).

    The bug: a strategy that caches a position-side flag can resume that flag
    desynced from live balance. If the cached flag is stale/false while the
    wallet actually holds base AND a sell/exit fires, the strategy HOLD-locks
    and cannot exit. These tests pin that the cached flag is treated as a HINT
    and the live balance is truth, so the desync->exit path works again.
    """
    return f'''
# ---------------------------------------------------------------------------
# Resume-state reconciliation (VIB-5155 / ALM-2719)
#
# A stale/false cached side-state flag must never HOLD-lock a valid exit:
# live wallet balance is truth, the persisted flag is only a hint.
# ---------------------------------------------------------------------------


class Test{class_name}ResumeReconcile:
    """Cached side-state flag is reconciled from live balance, not trusted blindly."""

    def test_reconcile_resumed_state_flips_stale_false_flag(
        self, strategy: {class_name}, mock_market: MagicMock
    ) -> None:
        """Resume hook flips a stale FALSE flag to True when the wallet holds base.

        Simulates a desynced restart: persisted state said 'not holding base'
        but the wallet actually holds base. The post-resume guardrail must
        correct the flag from live balance and report the correction.
        """
        # Persisted (desynced) state: flag false despite holding base on-chain.
        strategy.load_persistent_state({{"holding_base": False, "last_signal": "buy"}})
        assert strategy._holding_base is False

        corrected = strategy.reconcile_resumed_state(mock_market)

        assert corrected is True, "desync should be detected and corrected"
        assert strategy._holding_base is True, "flag must follow live balance (truth)"

    def test_reconcile_resumed_state_noop_when_already_agrees(
        self, strategy: {class_name}, mock_market: MagicMock
    ) -> None:
        """No desync -> hook returns False and leaves the flag unchanged."""
        strategy.load_persistent_state({{"holding_base": True, "last_signal": "buy"}})
        corrected = strategy.reconcile_resumed_state(mock_market)
        assert corrected is False
        assert strategy._holding_base is True

    def test_teardown_exits_despite_stale_false_flag(
        self, strategy: {class_name}, mock_market: MagicMock
    ) -> None:
        """THE FUND-SAFETY CASE: balance-true-but-flag-false must still exit.

        Wallet holds base, but the resumed flag is False. A teardown request
        (operator risk-off) must still generate the exit swap by reconciling
        from live balance -- the cached flag must not block the exit.
        """
        from almanak.framework.teardown import TeardownMode

        # Desynced resume: flag false, but the wallet holds base (mock_market
        # reports a funded base balance).
        strategy.load_persistent_state({{"holding_base": False, "last_signal": "buy"}})
        assert strategy._holding_base is False

        intents = strategy.generate_teardown_intents(
            mode=TeardownMode.SOFT, market=mock_market
        )

        assert len(intents) > 0, (
            "teardown must reconcile from live balance and exit even when the "
            "cached holding flag is stale/false (VIB-5155)"
        )
        swap = intents[0]
        assert getattr(swap, "from_token", None) == strategy.base_token
        assert getattr(swap, "to_token", None) == strategy.quote_token

    def test_teardown_no_exit_when_truly_flat(
        self, strategy: {class_name}
    ) -> None:
        """Symmetric guard: flag true but wallet flat -> no phantom exit swap.

        Live balance is truth in BOTH directions. A stale TRUE flag on an empty
        wallet must not generate a swap of funds the wallet does not hold.
        """
        from almanak.framework.teardown import TeardownMode

        flat = _make_mock_market(balance=Decimal("0"), balance_usd=Decimal("0"))
        strategy.load_persistent_state({{"holding_base": True, "last_signal": "buy"}})

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT, market=flat)

        assert intents == [], "no exit swap when the wallet is genuinely flat"
        assert strategy._holding_base is False, "stale TRUE flag corrected to flat"

    def test_teardown_exits_when_base_held_but_unpriceable(
        self, strategy: {class_name}
    ) -> None:
        """Empty != Zero: a funded base token whose USD is UNMEASURED still exits.

        When the price oracle cannot price the base token, MarketSnapshot
        coerces ``balance_usd`` to ``Decimal('0')`` while the native balance
        stays non-zero (VIB-4843 sentinel). Reconciling on USD alone would read
        that as 'flat', clear the holding flag, and emit no teardown swap --
        stranding a real position. Native balance is truth: the exit must still
        fire (VIB-5155 / ALM-2719).
        """
        from almanak.framework.teardown import TeardownMode

        # Funded but unpriceable: non-zero native balance, USD coerced to 0.
        unpriceable = _make_mock_market(balance=Decimal("5"), balance_usd=Decimal("0"))
        strategy.load_persistent_state({{"holding_base": True, "last_signal": "buy"}})

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT, market=unpriceable)

        assert len(intents) > 0, (
            "teardown must exit on native balance even when USD price is "
            "unmeasured -- a couldn't-price $0 must not strand a held position"
        )
        assert strategy._holding_base is True, "non-zero native holding stays 'holding'"

'''


def generate_test_file(
    name: str,
    template: StrategyTemplate,
    chain: str,
) -> str:
    """Generate the test_strategy.py file content.

    The emitted test suite exercises the scaffolded strategy at multiple
    levels so a beginner who runs ``pytest`` after ``almanak strat new``
    gets meaningful coverage out of the box, not just smoke tests:

    - Always emitted (all templates):
      * init sanity + decide()-returns-intent-or-hold
      * decide() error handling (balance/price/RSI providers raising)
      * get_status() contract
      * Zero-balance edge case -> should hold or decide cleanly
      * Zero-price edge case -> must not raise
      * Teardown methods return valid types
      * generate_teardown_intents(SOFT) and generate_teardown_intents(HARD)
      * HARD mode uses higher-or-equal slippage than SOFT (where swaps exist)

    - Stateful templates (those with on_intent_executed):
      * State machine transitions (parameterized per template)
      * on_intent_executed(success=False) does not mutate state
      * get_persistent_state() / load_persistent_state() round-trip
      * on_intent_executed stores position_id / state updates correctly

    See docs/internal/blueprints/10-testing-quality.md for testing patterns.
    """
    class_name = to_pascal_case(name) + "Strategy"

    spec = _TEMPLATE_TEST_SPECS.get(template, _BLANK_TEST_SPEC)

    header = _render_test_file_header(name, class_name, chain, template)
    base_tests = _render_base_tests(class_name, chain)
    edge_tests = _render_edge_case_tests(class_name)
    teardown_tests = _render_teardown_tests(class_name, spec)

    callback_tests = ""
    persistence_tests = ""
    if spec.has_callbacks:
        callback_tests = _render_callback_tests(class_name, spec)
        persistence_tests = _render_persistence_tests(class_name, spec, chain)

    # Resume-state reconciliation regression (VIB-5155 / ALM-2719) — only for
    # templates that cache a position-side flag reconciled from live balance.
    reconcile_tests = ""
    if spec.reconciles_side_state:
        reconcile_tests = _render_resume_reconcile_tests(class_name, chain)

    return header + base_tests + edge_tests + teardown_tests + callback_tests + persistence_tests + reconcile_tests


def generate_init_file(name: str) -> str:
    """Generate the __init__.py file content."""
    class_name = to_pascal_case(name) + "Strategy"

    content = f'''"""
{to_pascal_case(name)} Strategy Package.

Generated by: almanak strat new
"""

from .strategy import {class_name}

__all__ = [
    "{class_name}",
]
'''

    return content


def generate_pyproject_toml(
    name: str,
) -> str:
    """Generate pyproject.toml for a self-contained strategy Python project.

    The generated file is a lean manifest for the hosted platform.
    The platform handles lockfile generation during cloud Docker builds.
    """
    from almanak._version import __version__

    snake_name = to_snake_case(name)

    return f"""[project]
name = "{snake_name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "almanak>={__version__}",
]

[tool.almanak.run]
interval = 60
"""


def generate_gitignore() -> str:
    """Generate .gitignore for a strategy directory."""
    return """.venv/
__pycache__/
*.pyc
.env
*.db
*.db-journal
.pytest_cache/
.coverage
dist/
build/
*.egg-info/
.DS_Store
"""


def generate_python_version() -> str:
    """Generate .python-version file matching the Dockerfile base image."""
    return "3.12\n"


def generate_env_file() -> str:
    """Generate the .env file with required environment variables."""
    return """# Required
ALMANAK_PRIVATE_KEY=

# RPC access (set one of these, or leave empty for free public RPCs)
# RPC_URL=https://your-rpc-provider.com/v1/your-key
# ALCHEMY_API_KEY=

# Optional
# ALMANAK_GATEWAY_PRIVATE_KEY=  # falls back to ALMANAK_PRIVATE_KEY if unset
# ENSO_API_KEY=
# COINGECKO_API_KEY=
# ALMANAK_API_KEY=
"""


def _docstring_safe(s: str) -> str:
    """Neutralise characters that break a triple-quoted docstring block.

    ``json.dumps`` can't escape inside a docstring (the surrounding
    triple quotes are part of the file), so backslashes (escape-sequence
    warnings) and embedded ``"`` are rewritten.
    """
    return s.replace("\\", "/").replace('"', "'")


def generate_dashboard_ui(
    name: str,
    template: StrategyTemplate = StrategyTemplate.BLANK,
) -> str:
    """Generate a starter ``dashboard/ui.py``.

    For templates that have a matching framework dashboard renderer
    (``DYNAMIC_LP``, ``LENDING_LOOP``, ``PERPS``, ``TA_SWAP``), emit a
    scaffold wired to the renderer — the renderer owns the title, the
    strategy header, and the three audit sections (PnL / Cost Stack /
    Trade Tape), so the scaffold is short by design.

    For every other template (``BLANK``, ``MULTI_STEP``, ``STAKING``,
    ``VAULT_YIELD``, ``COPY_TRADER``, ``BASIS_TRADE``), fall back to the
    generic direct-sections starter — title + ``render_pnl_section`` →
    author's primitive-specific UI → ``render_cost_stack_section`` +
    ``render_trade_tape_section``.
    """
    if template == StrategyTemplate.DYNAMIC_LP:
        return _generate_dashboard_ui_lp(name)
    if template == StrategyTemplate.LENDING_LOOP:
        return _generate_dashboard_ui_lending(name)
    if template == StrategyTemplate.PERPS:
        return _generate_dashboard_ui_perp(name)
    if template == StrategyTemplate.TA_SWAP:
        return _generate_dashboard_ui_ta(name)
    return _generate_dashboard_ui_generic(name)


def _generate_dashboard_ui_generic(name: str) -> str:
    """Direct-sections starter used for blank / multi-step / etc. templates."""
    snake = to_snake_case(name)
    display_name = snake.replace("_", " ").title()
    # Embed strings via ``json.dumps`` so quotes / backslashes / unicode
    # in the strategy name can never break the generated Python file.
    title_literal = json.dumps(display_name)

    display_doc = _docstring_safe(display_name)
    snake_doc = _docstring_safe(snake)
    return f'''"""{display_doc} Dashboard.

Custom Streamlit dashboard for the {snake_doc} strategy. Loaded by the
hosted platform's dashboard image and by ``almanak dashboard``
locally — both call ``render_custom_dashboard()`` with the same
arguments.
"""

from typing import Any

import streamlit as st

from almanak.framework.dashboard import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the {display_doc} custom dashboard.

    Args:
        deployment_id: Stable identifier for this deployment.
        strategy_config: Snapshot of the strategy's runtime config.
        api_client: Gateway-backed API client (read-only).
        session_state: Shared Streamlit session state.
    """
    st.title({title_literal})
    st.markdown(f"**Deployment ID:** `{{deployment_id}}`")

    # 1. PnL eyeball — am I making or losing money? (top of dashboard)
    render_pnl_section(deployment_id)

    # 2. TODO(strategy author): replace this placeholder with your own
    # metrics, charts, and tables — LP range plots, health-factor
    # gauges, indicator charts, etc. See almanak/demo_strategies/*/
    # dashboard/ui.py for end-to-end examples and
    # almanak/framework/dashboard/templates/ for prebuilt LP / lending
    # / perp / TA / prediction sections.
    st.divider()
    st.markdown("### Position")
    st.info("This is a starter dashboard. Add your strategy-specific UI here.")

    # 3. Audit — life-to-date costs + transaction-level detail (bottom)
    st.divider()
    st.markdown("## Audit")
    render_cost_stack_section(deployment_id, heading="")
    render_trade_tape_section(deployment_id)
'''


def _generate_dashboard_ui_lp(name: str) -> str:
    """LP scaffold — wired to ``render_lp_dashboard`` via
    ``LPDashboardConfig`` + ``prepare_lp_session_state``.

    The renderer owns the title, the strategy header, and the three
    audit sections. Strategy-specific content (custom panels, etc.) goes
    BELOW the renderer call — wrapping it with ``st.title(...)`` or any
    of the section helpers double-renders.
    """
    snake = to_snake_case(name)
    display_doc = _docstring_safe(snake.replace("_", " ").title())
    snake_doc = _docstring_safe(snake)
    return f'''"""{display_doc} Dashboard.

Custom Streamlit dashboard for the {snake_doc} strategy. Loaded by the
hosted platform's dashboard image and by ``almanak dashboard``
locally — both call ``render_custom_dashboard()`` with the same
arguments.

Wired to the framework LP template renderer
(``render_lp_dashboard``), which owns the title, the strategy header,
and the three audit sections (PnL / Cost Stack / Trade Tape). Do NOT
wrap it with ``st.title(...)`` or extra ``render_pnl_section`` /
``render_cost_stack_section`` / ``render_trade_tape_section`` calls —
that double-renders.
"""

from typing import Any

from almanak.framework.dashboard.templates import (
    LPDashboardConfig,
    prepare_lp_session_state,
    render_lp_dashboard,
)

_FEE_BPS_TO_PCT = {{
    "100": "0.01%",
    "500": "0.05%",
    "3000": "0.30%",
    "10000": "1.00%",
}}


def _parse_pool(pool: str, default_fee_tier: str) -> tuple[str, str, str]:
    """Parse ``TOKEN0/TOKEN1[/FEE_BPS]`` from ``config.json``.

    ``fee_tier`` can be either embedded in the pool string
    (``WETH/USDC/3000``) or a separate config field. Both layouts are
    seen across strategies.
    """
    parts = [p.strip() for p in pool.split("/") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], _format_fee_tier(parts[2])
    if len(parts) == 2:
        return parts[0], parts[1], default_fee_tier
    return "WETH", "USDC", default_fee_tier


def _format_fee_tier(value: Any) -> str:
    """Normalise a ``fee_tier`` config value to a display string."""
    if isinstance(value, str) and value.endswith("%"):
        return value
    try:
        return _FEE_BPS_TO_PCT.get(str(int(value)), f"{{int(value) / 10000:.2f}}%")
    except (TypeError, ValueError):
        return "0.30%"


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    default_fee_tier = _format_fee_tier(strategy_config.get("fee_tier", 3000))
    token0, token1, fee_tier = _parse_pool(
        str(strategy_config.get("pool", "WETH/USDC")),
        default_fee_tier=default_fee_tier,
    )

    config = LPDashboardConfig(
        protocol=str(strategy_config.get("protocol", "uniswap_v3")),
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=str(strategy_config.get("chain", "arbitrum")),
    )

    session_state = prepare_lp_session_state(
        api_client,
        session_state=session_state,
        config=config,
        deployment_id=deployment_id,
    )

    # Pass api_client through so the LP template renders the gateway-backed
    # Positions registry + Position Lifecycle sections.
    render_lp_dashboard(deployment_id, strategy_config, session_state, config, api_client=api_client)
'''


def _generate_dashboard_ui_lending(name: str) -> str:
    """Lending scaffold — wired to ``render_lending_dashboard``.

    Defaults to Aave V3; swap ``get_aave_v3_config`` for
    ``get_morpho_blue_config`` / ``get_compound_v3_config`` /
    ``get_spark_config`` to point the dashboard at a different protocol
    (or build a ``LendingDashboardConfig`` directly).
    """
    snake = to_snake_case(name)
    display_doc = _docstring_safe(snake.replace("_", " ").title())
    snake_doc = _docstring_safe(snake)
    return f'''"""{display_doc} Dashboard.

Custom Streamlit dashboard for the {snake_doc} strategy. Loaded by the
hosted platform's dashboard image and by ``almanak dashboard``
locally — both call ``render_custom_dashboard()`` with the same
arguments.

Wired to the framework lending template renderer
(``render_lending_dashboard``), which owns the title, the strategy
header, and the three audit sections (PnL / Cost Stack / Trade Tape).
Do NOT wrap it with ``st.title(...)`` or extra section helpers — that
double-renders.
"""

from typing import Any

from almanak.framework.dashboard.templates import (
    get_aave_v3_config,
    render_lending_dashboard,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    config = get_aave_v3_config(
        collateral_token=str(strategy_config.get("collateral_token", "WETH")),
        borrow_token=str(strategy_config.get("borrow_token", "USDC")),
        chain=str(strategy_config.get("chain", "arbitrum")),
    )

    render_lending_dashboard(deployment_id, strategy_config, session_state, config)
'''


def _generate_dashboard_ui_perp(name: str) -> str:
    """Perp scaffold — wired to ``render_perp_dashboard``.

    Defaults to GMX V2; swap ``get_gmx_v2_config`` for
    ``get_hyperliquid_config`` (or build a ``PerpDashboardConfig``
    directly) to point the dashboard at a different venue.
    """
    snake = to_snake_case(name)
    display_doc = _docstring_safe(snake.replace("_", " ").title())
    snake_doc = _docstring_safe(snake)
    return f'''"""{display_doc} Dashboard.

Custom Streamlit dashboard for the {snake_doc} strategy. Loaded by the
hosted platform's dashboard image and by ``almanak dashboard``
locally — both call ``render_custom_dashboard()`` with the same
arguments.

Wired to the framework perp template renderer
(``render_perp_dashboard``), which owns the title, the strategy
header, and the three audit sections (PnL / Cost Stack / Trade Tape).
Do NOT wrap it with ``st.title(...)`` or extra section helpers — that
double-renders.
"""

from typing import Any

from almanak.framework.dashboard.templates import (
    get_gmx_v2_config,
    render_perp_dashboard,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    config = get_gmx_v2_config(
        market=str(strategy_config.get("perp_market", strategy_config.get("market", "ETH/USD"))),
        collateral_token=str(strategy_config.get("collateral_token", "USDC")),
        chain=str(strategy_config.get("chain", "arbitrum")),
    )

    render_perp_dashboard(deployment_id, strategy_config, session_state, config)
'''


def _generate_dashboard_ui_ta(name: str) -> str:
    """TA scaffold — wired to ``render_ta_dashboard``.

    Defaults to RSI; swap ``get_rsi_config`` for ``get_macd_config`` /
    ``get_bollinger_config`` / ``get_cci_config`` / ``get_stochastic_config``
    / ``get_atr_config`` / ``get_adx_config`` (or build a
    ``TADashboardConfig`` directly) to point the dashboard at a different
    indicator.
    """
    snake = to_snake_case(name)
    display_doc = _docstring_safe(snake.replace("_", " ").title())
    snake_doc = _docstring_safe(snake)
    return f'''"""{display_doc} Dashboard.

Custom Streamlit dashboard for the {snake_doc} strategy. Loaded by the
hosted platform's dashboard image and by ``almanak dashboard``
locally — both call ``render_custom_dashboard()`` with the same
arguments.

Wired to the framework TA template renderer (``render_ta_dashboard``),
which owns the title, the strategy header, and the three audit
sections (PnL / Cost Stack / Trade Tape). Do NOT wrap it with
``st.title(...)`` or extra section helpers — that double-renders.
"""

from typing import Any

from almanak.framework.dashboard.templates import (
    get_rsi_config,
    prepare_ta_session_state,
    render_ta_dashboard,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    config = get_rsi_config(
        period=int(strategy_config.get("rsi_period", 14)),
        overbought=float(strategy_config.get("rsi_overbought", 70)),
        oversold=float(strategy_config.get("rsi_oversold", 30)),
    )
    config.base_token = str(strategy_config.get("base_token", config.base_token))
    config.quote_token = str(strategy_config.get("quote_token", config.quote_token))
    config.chain = str(strategy_config.get("chain", config.chain))
    config.protocol = str(strategy_config.get("protocol", config.protocol))

    session_state = prepare_ta_session_state(
        api_client,
        session_state=session_state,
        config=config,
        deployment_id=deployment_id,
    )

    render_ta_dashboard(deployment_id, strategy_config, session_state, config)
'''


def generate_dashboard_metadata(name: str) -> str:
    """Generate ``dashboard/metadata.json`` (display name, icon, blurb).

    The icon field is intentionally left empty — strategy authors set
    their own (e.g. ``"icon": "📊"``) when they want one. The CLAUDE.md
    "no emojis unless asked" rule keeps the framework default neutral.
    """
    snake = to_snake_case(name)
    display_name = snake.replace("_", " ").title()
    payload = {
        "display_name": display_name,
        "description": f"Custom dashboard for the {snake} strategy.",
        "icon": "",
    }
    return json.dumps(payload, indent=4) + "\n"


def register_strategy_in_factory(
    name: str,
    strategies_dir: Path,
) -> None:
    """Register the new strategy in the strategy factory."""
    factory_file = strategies_dir / "__init__.py"
    class_name = f"Strategy{to_pascal_case(name)}"
    module_name = to_snake_case(name)

    # Read existing factory file or create new one
    if factory_file.exists():
        with open(factory_file) as f:
            content = f.read()
    else:
        content = '''"""
Strategy Factory - Auto-registers all available strategies.

Generated by: almanak new-strategy
"""

from typing import Type, Dict, Any

# Strategy registry - maps strategy names to their classes
STRATEGY_REGISTRY: Dict[str, Type[Any]] = {}


def register_strategy(name: str, strategy_class: Type[Any]) -> None:
    """Register a strategy class in the factory."""
    STRATEGY_REGISTRY[name] = strategy_class


def get_strategy(name: str) -> Type[Any]:
    """Get a strategy class by name."""
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Unknown strategy: {name}. Available: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[name]


def list_strategies() -> list[str]:
    """List all registered strategy names."""
    return list(STRATEGY_REGISTRY.keys())

'''

    # Add import and registration if not already present
    import_line = f"from .{module_name} import {class_name}"
    register_line = f'register_strategy("{module_name}", {class_name})'

    if import_line not in content:
        lines = content.split("\n")

        # Find position to insert import - after docstring and existing imports
        import_insert_pos = 0
        in_docstring = False

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Track docstring boundaries
            if stripped.startswith('"""') or stripped.startswith("'''"):
                if in_docstring:
                    in_docstring = False
                    import_insert_pos = i + 1
                elif stripped.count('"""') == 2 or stripped.count("'''") == 2:
                    # Single line docstring
                    import_insert_pos = i + 1
                else:
                    in_docstring = True
                continue

            if in_docstring:
                continue

            # After docstring, look for import section
            if stripped.startswith("from ") or stripped.startswith("import "):
                import_insert_pos = i + 1
            elif stripped and not stripped.startswith("#") and import_insert_pos > 0:
                # First non-import, non-comment line after imports
                break

        # Insert the import line
        lines.insert(import_insert_pos, import_line)

        # Add registration at the end of the file
        if register_line not in content:
            # Add a blank line if file doesn't end with one
            if lines and lines[-1].strip():
                lines.append("")
            lines.append(register_line)

        content = "\n".join(lines)

        with open(factory_file, "w") as fh:
            fh.write(content)


@click.command("new-strategy")
@click.option(
    "--template",
    "-t",
    # Accept both canonical enum values and aliases. parse_template() does the
    # final resolution and raises UnknownTemplateError when neither matches —
    # we don't use click.Choice because it would reject aliases up-front before
    # the helper can translate them.
    default=StrategyTemplate.BLANK.value,
    help=(
        "Strategy template: "
        + ", ".join(t.value for t in StrategyTemplate)
        + " (aliases: "
        + ", ".join(f"{a}->{t.value}" for a, t in TEMPLATE_ALIASES.items())
        + ")"
    ),
)
@click.option(
    "--name",
    "-n",
    required=True,
    help="Name for the new strategy (e.g., 'my_awesome_strategy')",
)
@click.option(
    "--chain",
    "-c",
    # Registry-derived canonical choices; registered aliases (e.g. "bnb")
    # are accepted and converted to the canonical name so the scaffolded
    # config.json always carries the vocabulary every runtime seam expects.
    type=ChainChoice(),
    default=DEFAULT_CHAIN,
    help="Target blockchain network (canonical name or registered alias)",
)
@click.option(
    "--protocol",
    "-p",
    default=None,
    help=(
        "Protocol slug rendered into the scaffold (decorator metadata and the "
        "template's config protocol defaults), e.g. aerodrome_slipstream, "
        "morpho_blue, hyperliquid. Defaults to the template's canonical protocol."
    ),
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help=(
        "Output directory for the new strategy. "
        "Defaults to strategies/incubating/<name> when run from the SDK root "
        "(detected by presence of strategies/incubating/), "
        "otherwise ./<name> in the current working directory."
    ),
)
@click.option(
    "--supply-protocol",
    default=None,
    help=(
        "Lending supply leg protocol (only used with --template lending_loop). "
        "Triggers a scaffold-input check that warns when the borrow leg is "
        "missing or when supply/borrow are different protocols (lending_loop "
        "is single-protocol; cross-protocol pairs need --template multi_step)."
    ),
)
@click.option(
    "--borrow-protocol",
    default=None,
    help=(
        "Lending borrow leg protocol (only used with --template lending_loop). "
        "See --supply-protocol; passing both makes the scaffold input "
        "explicit and surfaces an early warning if the pair is cross-protocol."
    ),
)
def new_strategy(
    template: str,
    name: str,
    chain: str,
    protocol: str | None,
    output_dir: str | None,
    supply_protocol: str | None,
    borrow_protocol: str | None,
) -> None:
    """
    Scaffold a new Almanak strategy from a template.

    This command generates a complete strategy directory structure with:
    - strategy.py: Main strategy implementation
    - config.json: Runtime configuration file
    - tests/test_strategy.py: Example test cases
    - __init__.py: Package initialization with exports

    Examples:

        almanak new-strategy --template dynamic_lp --name my_lp_strategy --chain arbitrum

        almanak new-strategy -t ta_swap -n rsi_trader -c ethereum

        almanak new-strategy -t ta_swap -n my_strat --output-dir /path/to/output
    """
    try:
        template_enum = parse_template(template)
    except UnknownTemplateError as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort() from exc
    snake_name = to_snake_case(name)

    # Normalize + validate the scaffold-time protocol choice (PR #3216
    # multi_step tick-spacing gate).
    protocol = _normalize_and_validate_scaffold_protocol(template_enum, protocol)

    # Validate template-chain compatibility
    if template_enum == StrategyTemplate.STAKING and chain != "ethereum":
        click.echo(
            f"Error: The staking template (Lido) only supports Ethereum, got: {chain}. "
            "Use --chain ethereum or choose a different template.",
            err=True,
        )
        raise click.Abort()

    # Surface lending_loop scaffold warnings (single-leg / cross-protocol) when
    # the user passes --supply-protocol or --borrow-protocol. VIB-3702 — keeps
    # the SDK CLI in sync with the structured warnings AlmanakCode emits.
    if template_enum == StrategyTemplate.LENDING_LOOP and (supply_protocol or borrow_protocol):
        for warning in validate_lending_loop_template(
            supply_protocol=supply_protocol or "",
            borrow_protocol=borrow_protocol,
        ):
            click.echo(f"warning: {warning}", err=True)

    # Determine output directory
    if output_dir:
        strategy_dir = Path(output_dir).resolve()
    else:
        # Auto-detect SDK root: if strategies/incubating/ exists relative to cwd
        # and we're not in a CI environment, default output there so users don't
        # need to manually mv after scaffolding.
        # (VIB-2328: every portfolio experiment required a manual mv after strat new)
        from almanak.config import cli_runtime_config_from_env

        incubating_dir = Path.cwd() / "strategies" / "incubating"
        # Reading ``is_ci`` is purely an output-directory hint — a malformed
        # unrelated env var (e.g. ``ANVIL_*_PORT=abc``) must not abort
        # scaffolding before any file is written. When the typed config
        # refuses to load we force the cwd default: the safer surprise is
        # "scaffold landed in cwd, mv it" rather than "scaffold landed in
        # ``strategies/incubating/`` while the user's env was broken"
        # (PR #2152 review).
        try:
            use_incubating = incubating_dir.is_dir() and not cli_runtime_config_from_env().is_ci
        except Exception:  # noqa: BLE001 — degrade gracefully for any config error
            use_incubating = False
        if use_incubating:
            strategy_dir = incubating_dir / snake_name
        else:
            # Fall back to current working directory / strategy name
            strategy_dir = Path.cwd() / snake_name

    # Check if directory already has strategy files (allow scaffolding into empty or dotfile-only dirs)
    if strategy_dir.exists():
        if not strategy_dir.is_dir():
            click.echo(f"Error: Path exists and is not a directory: {strategy_dir}", err=True)
            raise click.Abort()
        if any(f for f in strategy_dir.iterdir() if not f.name.startswith(".")):
            click.echo(f"Error: Directory already contains files: {strategy_dir}", err=True)
            raise click.Abort()

    # Create directory structure
    click.echo(f"Creating strategy: {snake_name}")
    click.echo(f"Template: {template_enum.value}")
    click.echo(f"Chain: {chain}")
    click.echo(f"Protocol: {protocol or TEMPLATE_CONFIGS[template_enum].default_protocol}")
    click.echo(f"Output: {strategy_dir}")
    click.echo()

    created_dir = not strategy_dir.exists()
    try:
        # Create directories
        strategy_dir.mkdir(parents=True, exist_ok=True)
        tests_dir = strategy_dir / "tests"
        tests_dir.mkdir(exist_ok=True)

        # Generate files
        files_created: list[str] = []

        # strategy.py
        strategy_file = strategy_dir / "strategy.py"
        strategy_content = generate_strategy_file(name, template_enum, chain, strategy_dir, protocol)
        with open(strategy_file, "w") as fh:
            fh.write(strategy_content)
        files_created.append("strategy.py")

        # config.json (runtime config read by load_strategy_config)
        config_json_file = strategy_dir / "config.json"
        config_json_content = generate_config_json(name, template_enum, chain, protocol)
        with open(config_json_file, "w") as fh:
            fh.write(config_json_content)
        files_created.append("config.json")

        # pyproject.toml
        pyproject_file = strategy_dir / "pyproject.toml"
        pyproject_content = generate_pyproject_toml(name)
        with open(pyproject_file, "w") as fh:
            fh.write(pyproject_content)
        files_created.append("pyproject.toml")

        # .python-version
        python_version_file = strategy_dir / ".python-version"
        with open(python_version_file, "w") as fh:
            fh.write(generate_python_version())
        files_created.append(".python-version")

        # __init__.py
        init_file = strategy_dir / "__init__.py"
        init_content = generate_init_file(name)
        with open(init_file, "w") as fh:
            fh.write(init_content)
        files_created.append("__init__.py")

        # tests/__init__.py
        tests_init = tests_dir / "__init__.py"
        with open(tests_init, "w") as fh:
            fh.write('"""Tests for the strategy."""\n')
        files_created.append("tests/__init__.py")

        # tests/test_strategy.py
        test_file = tests_dir / "test_strategy.py"
        test_content = generate_test_file(name, template_enum, chain)
        with open(test_file, "w") as fh:
            fh.write(test_content)
        files_created.append("tests/test_strategy.py")

        # .env
        env_file = strategy_dir / ".env"
        env_content = generate_env_file()
        with open(env_file, "w") as fh:
            fh.write(env_content)
        files_created.append(".env")

        # .gitignore
        gitignore_file = strategy_dir / ".gitignore"
        with open(gitignore_file, "w") as fh:
            fh.write(generate_gitignore())
        files_created.append(".gitignore")

        # AGENTS.md (per-strategy agent guide)
        from almanak.framework.cli.strategy_agent_guide import (
            StrategyGuideConfig,
            generate_strategy_agents_md,
        )

        guide_config = StrategyGuideConfig(
            strategy_name=snake_name,
            template_name=template_enum.value,
            chain=chain,
            class_name=to_pascal_case(name) + "Strategy",
        )
        agents_md_file = strategy_dir / "AGENTS.md"
        agents_md_content = generate_strategy_agents_md(guide_config)
        with open(agents_md_file, "w") as fh:
            fh.write(agents_md_content)
        files_created.append("AGENTS.md")

        # dashboard/ui.py + dashboard/metadata.json — every strategy
        # ships with a starter custom dashboard. The stub already
        # includes the trade-tape section so accounting is visually
        # QA'able from day one, both locally and on the hosted platform.
        dashboard_dir = strategy_dir / "dashboard"
        dashboard_dir.mkdir(exist_ok=True)
        dashboard_ui_file = dashboard_dir / "ui.py"
        dashboard_ui_file.write_text(generate_dashboard_ui(name, template_enum), encoding="utf-8")
        files_created.append("dashboard/ui.py")

        dashboard_metadata_file = dashboard_dir / "metadata.json"
        dashboard_metadata_file.write_text(generate_dashboard_metadata(name), encoding="utf-8")
        files_created.append("dashboard/metadata.json")

        # Print success message
        click.echo()
        click.echo(f"Created strategy '{snake_name}' in {strategy_dir}")
        click.echo()
        click.echo("Files:")
        click.echo("  strategy.py          - Strategy implementation")
        click.echo("  config.json          - Runtime configuration")
        click.echo("  pyproject.toml       - Dependencies and metadata")
        click.echo("  .env                 - Environment variables (edit this)")
        click.echo("  .gitignore           - Git ignore rules")
        click.echo("  AGENTS.md            - AI agent guide")
        click.echo("  dashboard/           - Streamlit dashboard (with trade tape)")
        click.echo("  tests/               - Test scaffold")
        click.echo()
        click.echo("Next steps:")
        click.echo(f"  cd {strategy_dir}")
        click.echo("  almanak strat run --once --dry-run")

    except Exception as e:
        click.echo(f"Error creating strategy: {e}", err=True)
        # Clean up on failure — only rmtree if we created the directory.
        # If the directory existed before scaffolding (e.g. -o . or dotfile-only dir),
        # don't delete it — just leave the partially-written files for the user to clean up.
        if strategy_dir.exists() and created_dir:
            import shutil

            shutil.rmtree(strategy_dir)
        raise click.Abort() from e


if __name__ == "__main__":
    new_strategy()
