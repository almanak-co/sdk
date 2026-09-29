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
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL


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


def _scaffold_v3_lp_protocols() -> frozenset[str]:
    """V3-shaped protocol keys that have an LP_OPEN-capable compiler manifest."""
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.connectors._strategy_protocol_family_registry import (
        PROTOCOL_FAMILY_REGISTRY,
        ProtocolFamily,
    )
    from almanak.core.intent_types import IntentType

    grouping_protocols = PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.UNIV3_LP_GROUPING)
    executable_protocols = {
        protocol_key
        for connector in CONNECTOR_REGISTRY.all()
        if connector.compiler is not None and IntentType.LP_OPEN in (connector.strategy_intents or ())
        for protocol_key in connector.compiler_keys
    }
    return grouping_protocols & executable_protocols


def _normalize_and_validate_scaffold_protocol(template_enum: StrategyTemplate, protocol: str | None) -> str | None:
    """Normalize the ``--protocol`` choice and reject incompatible combinations.

    Returns the normalized (``strip().lower()``) protocol slug, or ``None`` when
    the caller passed nothing. Raises ``click.Abort`` (after echoing to stderr)
    for a malformed slug, for an LP template outside the V3-shaped protocol
    family, or for a ``multi_step`` scaffold paired with a tick-spacing protocol.

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

    if template_enum in (StrategyTemplate.DYNAMIC_LP, StrategyTemplate.MULTI_STEP):
        v3_lp_protocols = _scaffold_v3_lp_protocols()
        if protocol not in v3_lp_protocols:
            click.echo(
                f"Error: the {template_enum.value} template requires a V3-family LP protocol "
                f"({', '.join(sorted(v3_lp_protocols))}); {protocol} has different range or "
                "minimum-amount semantics, so this scaffold cannot promise its two-sided "
                "LP protection contract.",
                err=True,
            )
            raise click.Abort()

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


@dataclass(frozen=True)
class _AnvilFundingSpec:
    """Address-authoring labels and amounts for one scaffold chain."""

    native_amount: object
    erc20_amounts: dict[str, object]


# Native gas uses the SDK's address-shaped sentinel identity; every ERC-20
# label is joined to its exact chain address by ``_default_anvil_funding``.
# One chain table avoids parallel dispatch maps.
_CHAIN_ANVIL_FUNDING_SPECS: dict[str, _AnvilFundingSpec] = {
    "mantle": _AnvilFundingSpec(1000, {"WMNT": 10, "WETH": 5, "USDC": 10000}),
    "avalanche": _AnvilFundingSpec(100, {"WAVAX": 10, "WETH": 5, "USDC": 10000}),
    "bsc": _AnvilFundingSpec(10, {"WBNB": 5, "WETH": 5, "USDC": 10000}),
    "polygon": _AnvilFundingSpec(1000, {"WMATIC": 100, "WETH": 5, "USDC": 10000}),
    "sonic": _AnvilFundingSpec(100, {"WETH": 5, "USDC": 10000}),
    "monad": _AnvilFundingSpec(100, {"WETH": 5, "USDC": 10000}),
    "zerog": _AnvilFundingSpec(50, {"W0G": 20, "USDC.E": 100}),
}
_DEFAULT_ANVIL_FUNDING_SPEC = _AnvilFundingSpec(10, {"WETH": 5, "USDC": 10000})

# Default token_funding entries as (symbol, amount, amount_type). Addresses are
# resolved per-chain at scaffold time from the static token registry — the
# generator knows the chain, so it must never emit zero-address placeholders
# that users have to hand-replace.
_DEFAULT_TOKEN_FUNDING_SPECS: tuple[tuple[str, str, str], ...] = (
    ("WETH", "1", "token"),
    ("USDC", "5000", "usd"),
)


def _static_token_address(chain: str, label: str) -> str | None:
    """Join a scaffold label to descriptor-owned address metadata.

    This is an authoring-time join over static chain data, not user token
    resolution. Runtime funding receives only the returned address.
    """
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return None
    if descriptor.native.wrapped_symbol and label.upper() == descriptor.native.wrapped_symbol.upper():
        return descriptor.native.wrapped_address
    matches = [
        address
        for symbol, address in (descriptor.anvil.funding_tokens or {}).items()
        if symbol.upper() == label.upper()
    ]
    if len(matches) == 1:
        return matches[0]

    chain_tokens = {symbol.upper(): address for symbol, address in (descriptor.tokens or {}).items()}
    direct = chain_tokens.get(label.upper())
    if direct is not None:
        return direct

    # A descriptor may author a canonical bridged token with an ``.e`` suffix
    # while scaffold metadata uses the unsuffixed label. Only accept a unique
    # normalized match; ambiguous identities remain unmeasured.
    normalized_label = label.upper().removesuffix(".E")
    bridged_matches = {
        address
        for symbol, address in (descriptor.anvil.funding_tokens or {}).items()
        if symbol.upper().removesuffix(".E") == normalized_label
    }
    return next(iter(bridged_matches)) if len(bridged_matches) == 1 else None


def _default_anvil_funding(chain: str) -> dict[str, object]:
    """Build address-keyed ERC-20 funding defaults for one chain."""
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return {}

    spec = _CHAIN_ANVIL_FUNDING_SPECS.get(descriptor.name, _DEFAULT_ANVIL_FUNDING_SPEC)
    funding: dict[str, object] = {NATIVE_SENTINEL: spec.native_amount}
    for label, amount in spec.erc20_amounts.items():
        address = _static_token_address(chain, label)
        if address is not None:
            funding[address] = amount
    return funding


def _default_token_funding(chain: str) -> list[dict[str, str]]:
    """Build the default ``token_funding`` list with real per-chain addresses.

    Joins the human-readable scaffold labels to descriptor-owned static
    addresses. Labels the chain does not know are omitted entirely: an
    unmeasured address must never be fabricated as ``0x000…0`` (Empty ≠ Zero).
    """
    entries: list[dict[str, str]] = []
    for symbol, amount, amount_type in _DEFAULT_TOKEN_FUNDING_SPECS:
        address = _static_token_address(chain, symbol)
        if address is None:
            continue
        entries.append(
            {
                "symbol": symbol,
                "address": address,
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
                    max_slippage=self.max_slippage_bps / Decimal("10000"),
                    protocol=self.protocol,
                    swap_params=self.swap_params,
                )
            elif sell_signal:
                base_price = market.price(self.base_token)
                # An unpriced market cannot SIZE a sell. Treating price <= 0 as
                # "min_sell = 0" is not a safe fallback: it makes the balance
                # check below pass on ANY dust, so the strategy emits a SWAP at
                # the full configured notional off a $0 oracle. Empty != Zero --
                # refuse rather than substitute a zero for a number you do not
                # have. (The buy branch is gated by `quote_balance.balance_usd`,
                # which a degraded oracle already drags to 0.)
                if not base_price or base_price <= 0:
                    return Intent.hold(
                        reason=f"No valid {self.base_token} price ({base_price}); refusing to size a sell"
                    )
                min_sell = self.trade_size_usd / base_price
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
                        max_slippage=self.max_slippage_bps / Decimal("10000"),
                        protocol=self.protocol,
                        swap_params=self.swap_params,
                    )

            return Intent.hold(reason=reason or "No signal")"""

    elif template == StrategyTemplate.DYNAMIC_LP:
        return """
            # The range and its drift test are execution-facing, so they read the
            # pool's own price. market.price() is a USD valuation oracle: it is
            # hardcoded to 1.0 for stablecoins and can drift from the pool for any
            # pair, so a range centred on it can mint out of range without error.
            try:
                spot, pool_tick = self._pool_spot(market)
            except (PoolPriceUnavailableError, ValueError) as exc:
                return Intent.hold(reason=f"Pool price unavailable: {exc}")
            range_pct = Decimal(str(self.range_width_pct)) / Decimal("100")
            lower_price = spot * (Decimal("1") - range_pct)
            upper_price = spot * (Decimal("1") + range_pct)

            # If we have an open position, check if rebalance needed
            if self._position_id is not None:
                rebalance_pct = Decimal(str(self.rebalance_threshold_pct)) / Decimal("100")
                if self._range_lower is not None and self._range_upper is not None:
                    # Tick-ranged protocols (Aerodrome Slipstream) store the band
                    # in raw ticks -- measure the current position in tick space
                    # so the in-range math compares like units.
                    current = Decimal(pool_tick) if self._uses_tick_ranges() else spot
                    range_size = self._range_upper - self._range_lower
                    dist_from_lower = current - self._range_lower
                    position_in_range = dist_from_lower / range_size if range_size > 0 else Decimal("0.5")
                    lower_bound = (Decimal("1") - rebalance_pct) / Decimal("2")
                    upper_bound = (Decimal("1") + rebalance_pct) / Decimal("2")
                    if position_in_range < lower_bound or position_in_range > upper_bound:
                        logger.info(f"Rebalance needed: price {spot} at {position_in_range:.1%} of range")
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
            # NOTE ON SLIPPAGE -- the lp_open calls below DECLARE `max_slippage`.
            # Since VIB-6269 a CL mint's tolerance is a PRICE band: the compiler
            # emits that band's image in the amount0Min/amount1Min the ABI actually
            # has, so 0.005 means "revert if price moved more than 0.5%" on every
            # range width.
            #
            # Since ALM-3186 / VIB-6225 omitting it inherits the SAME instrument at
            # `default_lp_slippage` (1%) rather than the old permissive flat
            # haircut, so an undeclared mint is no longer unfloored. It is still
            # declared here because the right tolerance is pair-specific and must be
            # chosen against THIS position's range half-width, not inherited.
            #
            # Pick it SMALLER than the range half-width above: a band that reaches a
            # live range bound would leave that leg unfloored, so this scaffold opts
            # into a compiler safety refusal. Swap floors are a different quantity --
            # they bound a quoted VALUE against a counterparty-chosen price, so they
            # stay tight. Full
            # rationale: "LP slippage doctrine" in
            # docs/internal/blueprints/03-intent-system.md.
            if self._uses_tick_ranges():
                # Slipstream's compiler consumes RAW INTEGER TICKS aligned to
                # the pool's tick spacing (pool format
                # "TOKEN0/TOKEN1/<tick_spacing>"), not price bounds (VIB-5557).
                # It also does NOT reorder amounts: amount0 always funds the
                # pool string's first token.
                from almanak.framework.intents import TickBand

                tick_lower, tick_upper = self._tick_band(pool_tick)
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
                    max_slippage=self.max_slippage,
                    require_two_sided_minimums=True,
                )
            return Intent.lp_open(
                pool=self.pool,
                amount0=amount_base,
                amount1=amount_quote,
                range_lower=lower_price,
                range_upper=upper_price,
                protocol=self.protocol,
                max_slippage=self.max_slippage,
                require_two_sided_minimums=True,
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
                        market_id=self.lending_market_id,
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
                        if self.lending_market_id:
                            repay_kwargs["market_id"] = self.lending_market_id
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
                        if self.lending_market_id:
                            partial_kwargs["market_id"] = self.lending_market_id
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
                if self.lending_market_id:
                    supply_kwargs["market_id"] = self.lending_market_id
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
                        market_id=self.lending_market_id,
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
                if self.lending_market_id:
                    borrow_kwargs["market_id"] = self.lending_market_id
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
            hedge_size_usd = self.spot_size_usd * self.hedge_ratio
            collateral_usd = hedge_size_usd / self.perp_leverage
            if self._trade_state in (BasisTradeState.IDLE, BasisTradeState.SPOT_BOUGHT):
                try:
                    quote_price = market.price(self.quote_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check collateral token price")
                if not quote_price.is_finite() or quote_price <= 0:
                    return Intent.hold(reason="Collateral token price is unavailable or invalid")
                collateral_amount = collateral_usd / quote_price

            if self._trade_state == BasisTradeState.IDLE:
                try:
                    spot_price = market.price(self.base_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check base token price")
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

                required_quote = (self.spot_size_usd + collateral_usd) / quote_price
                if quote_balance.balance < required_quote:
                    return Intent.hold(reason=f"Insufficient {self.quote_token} for spot plus perp collateral")

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
                try:
                    quote_balance = market.balance(self.quote_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check balance")
                if quote_balance.balance < collateral_amount:
                    return Intent.hold(reason=f"Insufficient {self.quote_token} for perp collateral")
                logger.info(f"Hedging: opening short perp on {self.perp_market}")
                return Intent.perp_open(
                    market=self.perp_market,
                    collateral_token=self.quote_token,
                    collateral_amount=collateral_amount,
                    size_usd=hedge_size_usd,
                    is_long=False,
                    leverage=self.perp_leverage,
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

            # An unpriced market cannot open leverage OR judge an open one.
            # entry_price is the basis for the take-profit / stop-loss
            # comparisons below, so acting at 0 would arm both exits against a
            # meaningless reference (and the PnL ratio divides by it). Refuse
            # rather than guess -- Empty != Zero. The guard deliberately covers
            # BOTH states, so the reason names the state it actually blocked:
            # an operator reading "refusing to open leverage" while a position
            # is already open would go looking for the wrong problem.
            if not entry_price or entry_price <= 0:
                _blocked = (
                    "refusing to open leverage"
                    if self._position_state == PerpsState.IDLE
                    else "refusing to evaluate exits on an open position"
                )
                return Intent.hold(
                    reason=f"No valid {self.base_token} price ({entry_price}); {_blocked}"
                )

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
            # The range and its drift test are execution-facing, so they read the
            # pool's own price. market.price() is a USD valuation oracle: it is
            # hardcoded to 1.0 for stablecoins and can drift from the pool for any
            # pair, so a range centred on it can mint out of range without error.
            try:
                spot, _pool_tick = self._pool_spot(market)
            except (PoolPriceUnavailableError, ValueError) as exc:
                return Intent.hold(reason=f"Pool price unavailable: {exc}")
            range_pct = Decimal(str(self.range_width_pct)) / Decimal("100")

            # If we have a position, check for rebalance
            if self._position_id is not None:
                # Check if price moved enough to rebalance
                if self._range_lower and self._range_upper:
                    mid = (self._range_lower + self._range_upper) / Decimal("2")
                    drift = abs(spot - mid) / mid
                    if drift < self.rebalance_drift_pct:
                        return Intent.hold(reason=f"Position in range, drift={drift:.2%}")

                # Rebalance: use IntentSequence to atomically close LP + consolidate
                # into quote token. The next iteration will open a fresh LP.
                # Intent.sequence() ensures close happens before swap, and
                # amount="all" chains the swap to use whatever the close released.
                logger.info(f"Rebalancing LP around {spot} via IntentSequence")
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
            # `spot` is quote-per-base in the pool string's order, so this holds for
            # non-stablecoin pairs (e.g. WETH/WBTC) without a second price lookup.
            half_base_est = half_quote / spot * Decimal("0.95")
            lower_price = spot * (Decimal("1") - range_pct)
            upper_price = spot * (Decimal("1") + range_pct)
            logger.info(f"Opening LP via IntentSequence: {lower_price:.2f} - {upper_price:.2f}")
            return Intent.sequence(
                [
                    # BOTH legs declare `max_slippage`. Since VIB-6269 a CL mint's
                    # tolerance is a PRICE band; since ALM-3186 / VIB-6225 an
                    # undeclared tolerance reaches the same band at
                    # `default_lp_slippage` (1%) instead of the old permissive flat
                    # haircut. Declaring it is still right here: the right tolerance
                    # is pair- and range-specific, and it matters most in this
                    # shape -- this sequence swaps first, so the
                    # deposit embeds an implicit swap and execution price directly
                    # determines value received. Keep the LP tolerance below the
                    # range half-width; this scaffold asks the live compiler to refuse
                    # rather than emit an unfloored leg. See "LP slippage
                    # doctrine" in docs/internal/blueprints/03-intent-system.md.
                    Intent.swap(
                        from_token=self.quote_token,
                        to_token=self.base_token,
                        amount=half_quote,
                        # Deliberately its OWN literal, not self.max_slippage: a
                        # swap floor bounds a quoted VALUE, the LP field bounds a
                        # PRICE band. One knob for both would silently retune the
                        # swap whenever the LP range is retuned.
                        max_slippage=Decimal("0.005"),
                    ),
                    Intent.lp_open(
                        pool=self.pool,
                        amount0=half_base_est,
                        amount1=half_quote * Decimal("0.95"),
                        range_lower=lower_price,
                        range_upper=upper_price,
                        protocol=self.protocol,
                        max_slippage=self.max_slippage,
                        require_two_sided_minimums=True,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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

        snapshot = None
        try:
            snapshot = self.create_market_snapshot()
            self._reconcile_holding_base(snapshot)
        except Exception as e:
            logger.warning(
                f"get_open_positions: live-balance reconcile unavailable, "
                f"using cached holding flag: {{e}}"
            )

        positions = []

        if self._holding_base:
            # VALUE THE POSITION LIVE -- nothing in the framework back-fills
            # PositionInfo.value_usd. It is consumed verbatim: summed into the
            # teardown total and read by the max-acceptable-loss guard, where a
            # fabricated 0 silently buys the LOOSEST slippage tier at the exact
            # moment you are unwinding. Empty != Zero: when the read is
            # unavailable, emit 0 PAIRED WITH BOTH markers --
            # details["value_usd_unknown"] = True and
            # details["valuation_status"] = "no_path". Different consumers;
            # setting only one leaves the other treating this as a measured zero.
            #
            # BE PRECISE ABOUT WHAT THE MARKERS BUY YOU: they make the gap
            # VISIBLE, they do NOT restore the loss cap. "value_usd_unknown" is
            # read by the teardown CLI preview
            # (framework/cli/teardown_helpers.py), which prints "Value: unknown"
            # and drives the warning banner; "valuation_status" is read by the
            # portfolio valuer. The max-acceptable-loss guard itself
            # (``calculate_max_acceptable_loss``, framework/teardown/models.py)
            # still reads ONLY the summed numeric total and consults NEITHER
            # marker -- an unmeasured row therefore still contributes 0 and still
            # yields the most permissive tier. Closing that is VIB-5604. This is
            # exactly why valuing live above matters more than marking.
            #
            # DO NOT rely on the read RAISING. ``balance_usd()`` returns a plain
            # Decimal and does not raise for an unpriceable holding -- an illiquid
            # token or a degraded oracle yields a bare 0 while the balance is
            # non-zero, and a try/except alone would emit that as a MEASURED $0.
            # Inspect the pair semantically instead: a positive token balance
            # with a non-positive USD value is UNMEASURED. A genuinely zero
            # holding may stay a measured $0 -- that is a real answer.
            details = {{"asset": self.base_token, "quote": self.quote_token}}
            value_usd = Decimal("0")
            try:
                if snapshot is None:
                    raise RuntimeError("no market snapshot available")
                token_balance = snapshot.balance(self.base_token)
                value_usd = Decimal(str(token_balance.balance_usd))
                if Decimal(str(token_balance.balance)) > 0 and value_usd <= 0:
                    raise RuntimeError(
                        f"holding {{token_balance.balance}} {{self.base_token}} but USD "
                        f"value read back as {{value_usd}} -- unpriceable, not worthless"
                    )
            except Exception as e:
                logger.warning(
                    f"get_open_positions: {{self.base_token}} valuation unavailable: {{e}}"
                )
                value_usd = Decimal("0")
                details["valuation_status"] = "no_path"
                details["value_usd_unknown"] = True

            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="{strategy_name}_base_token",
                    chain=self.chain,
                    protocol="{protocol}",
                    value_usd=value_usd,
                    details=details,
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
            slippage_bps = self.hard_teardown_max_slippage_bps if mode == TeardownMode.HARD else self.max_slippage_bps
            max_slippage = slippage_bps / Decimal("10000")
            intents.append(
                Intent.swap(
                    chain=self.chain,
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol=self.protocol,
                    swap_params=self.swap_params,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
            # VALUE THE POSITION LIVE -- nothing in the framework back-fills
            # PositionInfo.value_usd. It is consumed verbatim: summed into the
            # teardown total and read by the max-acceptable-loss guard, where a
            # fabricated 0 silently buys the LOOSEST slippage tier at the exact
            # moment you are unwinding.
            #
            # market.lp_position_value() reuses the SAME repricing engine as the
            # portfolio valuer, and returns None (never a fabricated $0) when the
            # position cannot be measured. Empty != Zero: on an unmeasured read
            # emit 0 PAIRED WITH BOTH markers. They are read by DIFFERENT
            # consumers and setting only one leaves the other treating this as a
            # measured zero:
            #   * details["value_usd_unknown"] = True -- read by the teardown CLI
            #     preview (framework/cli/teardown_helpers.py), which prints
            #     "Value: unknown" and drives the warning banner.
            #   * details["valuation_status"] = "no_path" -- what the portfolio
            #     valuer reads (framework/valuation/portfolio_valuer.py).
            # Both make the gap VISIBLE; NEITHER restores the loss cap. The
            # max-acceptable-loss guard (``calculate_max_acceptable_loss``,
            # framework/teardown/models.py) reads only the summed numeric total,
            # so an unmeasured row still contributes 0 and still yields the most
            # permissive tier -- closing that is VIB-5604. Value live where you
            # can; marking is the fallback, not a substitute.
            # A genuinely empty position returns an all-zero MEASURED result,
            # which is a different thing and must stay different.
            details = {{
                "pool": self.pool,
                "range_lower": str(self._range_lower) if self._range_lower is not None else None,
                "range_upper": str(self._range_upper) if self._range_upper is not None else None,
            }}
            value_usd = Decimal("0")
            try:
                lp_value = self.create_market_snapshot().lp_position_value(
                    str(self._position_id), self.protocol
                )
                if lp_value is None:
                    raise RuntimeError("position could not be measured")
                # .total_usd includes uncollected fees; .value_usd excludes them.
                value_usd = Decimal(str(lp_value.total_usd))
            except Exception as e:
                logger.warning(
                    f"get_open_positions: LP #{{self._position_id}} valuation unavailable: {{e}}"
                )
                details["valuation_status"] = "no_path"
                details["value_usd_unknown"] = True

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._position_id),
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=value_usd,
                    details=details,
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
                    chain=self.chain,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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

        # VALUE BOTH LEGS LIVE -- nothing in the framework back-fills
        # PositionInfo.value_usd. It is consumed verbatim: summed into the
        # teardown total and read by the max-acceptable-loss guard, where a
        # fabricated 0 silently buys the LOOSEST slippage tier at the exact
        # moment you are unwinding.
        #
        # DENOMINATION GUARD -- read this before copying the pattern. On a
        # degraded price read the framework stamps
        # ``price_source == PRICE_SOURCE_SAME_ASSET_UNIT``, and
        # ``collateral_value_usd`` is then TOKEN-denominated, not USD. Trusting
        # it blindly books tokens as dollars (a ~2000x understatement for WETH
        # collateral). Refuse the read instead of converting a unit you cannot
        # name: Empty != Zero, and a wrong unit is worse than a missing number.
        from almanak.framework.data.position_health import PRICE_SOURCE_SAME_ASSET_UNIT

        health = None
        try:
            health = self.create_market_snapshot().position_health(
                self.lending_protocol, self.lending_market_id
            )
            if getattr(health, "price_source", "") == PRICE_SOURCE_SAME_ASSET_UNIT:
                raise RuntimeError(
                    "degraded price read -- values are token-denominated, not USD"
                )
        except Exception as e:
            logger.warning(f"get_open_positions: lending valuation unavailable: {{e}}")
            health = None

        # After looping, borrows exist even in SUPPLIED state (from prior loops)
        has_borrows = (
            self._loop_state in (LendingLoopState.BORROWED, LendingLoopState.MONITORING)
            or self._loop_count > 0
        )
        if has_borrows:
            borrow_details = {{
                "borrow_token": self.borrow_token,
                "loop_count": self._loop_count,
                "market_id": self.lending_market_id,
            }}
            if health is None:
                borrow_details["valuation_status"] = "no_path"
                borrow_details["value_usd_unknown"] = True
            # SIGN MATTERS -- a BORROW is a LIABILITY, so report it NEGATIVE.
            #
            # Two different consumers read this row and only one of them
            # normalises the sign for you:
            #   * TeardownPositionSummary sums `p.value_usd` DIRECTLY, with no
            #     sign handling at all (framework/teardown/models.py). A positive
            #     debt here is ADDED to collateral, so a $10k supply against $6k
            #     of debt totals $16k instead of the true $4k net -- and that
            #     inflated total is what `max_loss_usd` and
            #     `protected_minimum_usd` are computed from. Reporting gross debt
            #     positive would swap a fabricated-zero bug for an inflated-total
            #     one on the same teardown safety calculation.
            #   * PortfolioValuer accepts EITHER convention: it negates a
            #     positive debt and passes an already-negative one through
            #     unchanged (framework/valuation/portfolio_valuer.py). So a
            #     negative value is correct for the valuer too.
            # Negative therefore satisfies both; positive silently breaks one.
            # `value_usd == 0` still means UNMEASURED to the valuer, which is
            # exactly what the `health is None` branch above wants.
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id="{strategy_name}_borrow",
                    chain=self.chain,
                    protocol=self.lending_protocol,
                    value_usd=(
                        -abs(Decimal(str(health.debt_value_usd)))
                        if health is not None
                        else Decimal("0")
                    ),
                    details=borrow_details,
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
            supply_details = {{
                "collateral_token": self.collateral_token,
                "supply_amount": str(self.supply_amount),
                "market_id": self.lending_market_id,
            }}
            if health is None:
                supply_details["valuation_status"] = "no_path"
                supply_details["value_usd_unknown"] = True
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="{strategy_name}_supply",
                    chain=self.chain,
                    protocol=self.lending_protocol,
                    value_usd=(
                        Decimal(str(health.collateral_value_usd))
                        if health is not None
                        else Decimal("0")
                    ),
                    details=supply_details,
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
            market_id=self.lending_market_id or None,
            chain=self.chain,
            mode=mode,
        )

    def get_teardown_profile(self):
        """Teardown metadata + exit asset policy."""
        from almanak.framework.teardown import TeardownProfile

        return TeardownProfile(
            natural_exit_assets=[self.collateral_token, self.borrow_token],
            has_lending_positions=True,
            chains_involved=[self.chain],
            # TODO: exit asset policy. Default (None) consolidation-swaps
            # recovered tokens to a target token after positions close. If the
            # approved spec forbids swaps, set
            # preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
            # (import from almanak.framework.teardown) — this skips only the
            # consolidation swap; generate_lending_unwind() above may still
            # emit staircase swaps when wallet balance cannot cover the debt.
            preferred_asset_policy=None,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
                    chain=self.chain,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
            # TODO(you): value this position before running with real funds.
            #
            # Nothing in the framework back-fills PositionInfo.value_usd -- it is
            # consumed verbatim, summed into the teardown total, and read by the
            # max-acceptable-loss guard, where a fabricated 0 silently buys the
            # LOOSEST slippage tier at the moment of unwind. There is no generic
            # ERC-4626 share-valuation helper on MarketSnapshot, so the scaffold
            # cannot measure this one for you.
            #
            # Until you implement it, this row is declared UNMEASURED via
            # BOTH markers: details["value_usd_unknown"] = True (read by the
            # teardown CLI preview, framework/cli/teardown_helpers.py) and
            # details["valuation_status"] = "no_path" (read by the portfolio
            # valuer). Different consumers -- setting only one leaves the other
            # treating this as a measured zero. The markers make the gap VISIBLE
            # but do NOT restore the loss cap: the max-acceptable-loss guard
            # (framework/teardown/models.py) reads only the summed numeric total
            # and consults neither (VIB-5604). Convert your share balance to
            # assets (``convertToAssets``) and price the underlying; then drop
            # the "no_path" marker. Do NOT simply delete the marker and leave the
            # 0 -- that is the fabrication this branch exists to avoid.
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="{strategy_name}_vault",
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=Decimal("0"),
                    details={{
                        "vault_address": self.vault_address,
                        "deposit_token": self.deposit_token,
                        "valuation_status": "no_path",
                        "value_usd_unknown": True,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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

        # TODO(you): value these positions before running with real funds.
        #
        # Nothing in the framework back-fills PositionInfo.value_usd -- it is
        # consumed verbatim, summed into the teardown total, and read by the
        # max-acceptable-loss guard, where a fabricated 0 silently buys the
        # LOOSEST slippage tier at the moment of unwind. Copied trades span
        # heterogeneous position types, so there is no single call the scaffold
        # can make for you: dispatch on ``pos_type`` and use the matching API
        # (TOKEN -> market.balance_usd, LP -> market.lp_position_value,
        # SUPPLY/BORROW -> market.position_health, PERP -> your perp read).
        #
        # Until then every row is declared UNMEASURED via
        # BOTH markers: details["value_usd_unknown"] = True (read by the teardown
        # CLI preview, framework/cli/teardown_helpers.py) and
        # details["valuation_status"] = "no_path" (read by the portfolio valuer).
        # Different consumers -- setting only one leaves the other treating this
        # as a measured zero. The markers make the gap VISIBLE but do NOT restore
        # the loss cap: the max-acceptable-loss guard (framework/teardown/
        # models.py) reads only the summed numeric total and consults neither
        # (VIB-5604). Remove them only once you emit a real value.
        for i, trade in enumerate(self._open_trades):
            pos_type = _type_map.get(trade.get("intent_type"), PositionType.TOKEN)
            positions.append(
                PositionInfo(
                    position_type=pos_type,
                    position_id=f"{strategy_name}_copy_{{i}}",
                    chain=self.chain,
                    protocol=trade.get("protocol", "unknown"),
                    value_usd=Decimal("0"),
                    details={{**trade, "valuation_status": "no_path", "value_usd_unknown": True}},
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
                            chain=self.chain,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
    # -------------------------------------------------------------------------

    def _venue_probe(self, market=None):
        """Ask the VENUE what it holds — never `self._position_state` (ALM-3109).

        `_position_state` records what this strategy REQUESTED: it is set in
        `on_intent_executed`, which fires when the order is accepted, not when the
        venue fills it. On an async-settled perp venue (GMX V2, Hyperliquid,
        Aster) the fill can revert, be cancelled, or land after a crash that lost
        the state save -- and teardown enumerates from `get_open_positions()`, so
        a position the cache never learned about is NEVER closed. That is a user
        losing access to their own money, not a bookkeeping detail.

        `probe_perp_position` returns THREE values, and the third is the point:

            OPEN        the venue holds it        (measured)
            FLAT        the venue does not       (measured)
            UNMEASURED  the read did not run     (NOT flat)

        Empty != Zero. Treating UNMEASURED as flat certifies a teardown that
        closed nothing while the position is live (VIB-6497).
        """
        from almanak.framework.strategies import probe_perp_position

        snapshot = market
        if snapshot is None:
            try:
                snapshot = self.create_market_snapshot()
            except Exception as e:  # no snapshot => UNMEASURED, never flat
                logger.warning(f"teardown: no market snapshot for the venue probe: {{e}}")
                snapshot = None
        return probe_perp_position(
            snapshot,
            protocol=self.protocol,
            chain=self.chain,
            market_symbol=self.perp_market,
            index_token_address=self.index_token_address,
        )

    def get_open_positions(self):
        """Return all open positions for teardown preview -- from the VENUE.

        VALUE THE POSITION FOR REAL. `value_usd=0` is NOT free: the harness drops
        rows worth <= $0.01 as dust when it measures what teardown left behind, so
        a truthful position valued at zero is invisible to exactly the check that
        should catch it -- and a zero total buys the loosest tier of the
        position-aware loss cap. When the notional cannot be priced, fall back to
        the requested size PAIRED WITH both unmeasured markers
        (`value_usd_unknown` for the teardown CLI preview, `valuation_status` for
        the portfolio valuer) -- never publish an unmeasured number bare.

        NOTE ON `teardown_state_derived_from_chain`: do NOT set it here. It is an
        assertion that the open set is re-derived PURELY from chain, which would
        make persisting `_position_state` optional. This implementation keeps the
        cached side as its UNMEASURED fallback, so the state must still survive a
        restart -- `get_persistent_state()`/`load_persistent_state()` above are
        what make that true.
        """
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        def _row(is_long: bool, value_usd: Decimal, measured: bool) -> PositionInfo:
            direction = "LONG" if is_long else "SHORT"
            details = {{
                "market": self.perp_market,
                # REQUIRED: venue position identity is derived from
                # market + collateral + side. Without collateral_token the row
                # falls back to its raw position_id and the SAME physical
                # position can enumerate twice (VIB-6316).
                "collateral_token": self.collateral_token,
                "is_long": is_long,
                "direction": direction,
                "entry_price": str(self._entry_price) if self._entry_price else "unknown",
                "position_source": "venue" if measured else "strategy_cache_unverified",
            }}
            if not measured:
                details["value_usd_unknown"] = True
                details["valuation_status"] = "no_path"
            return PositionInfo(
                position_type=PositionType.PERP,
                position_id=f"{strategy_name}_perp_{{direction.lower()}}",
                chain=self.chain,
                protocol=self.protocol,
                value_usd=value_usd,
                details=details,
            )

        probe = self._venue_probe()
        positions = []
        if probe.is_open:
            # The venue is authoritative -- including for the SIDE, which is what
            # lets this report a position the cache never recorded.
            positions = [
                _row(
                    found.is_long,
                    found.notional_usd if found.notional_usd is not None else self.position_size_usd,
                    found.notional_usd is not None,
                )
                for found in probe.positions
            ]
        elif not probe.is_measured:
            logger.warning(
                f"teardown: perp position read UNMEASURED ({{probe.reason}}) -- falling back to "
                f"cached state {{self._position_state}}. An unmeasured read is not a flat account."
            )
            if self._position_state == PerpsState.OPEN:
                positions = [_row(self._is_long, self.position_size_usd, measured=False)]
        elif self._position_state == PerpsState.OPEN:
            # Measured flat while the cache says open: the cache is stale (a
            # reverted/cancelled fill). Publishing it would fail the teardown
            # verdict with a phantom residual on an account that is really flat.
            logger.info("teardown: venue measured FLAT while cached state is OPEN -- reporting the venue")

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),  # reporting-only (decisions use market.timestamp)
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}

        Reads the SAME probe as get_open_positions(): an enumerated position with
        no closing intent fails the teardown completeness check and is stranded
        anyway, so the two halves must never disagree about what is open.
        """
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        probe = self._venue_probe(market)
        if probe.is_open:
            sides = [found.is_long for found in probe.positions]
        elif probe.is_flat:
            sides = []
        else:
            sides = [self._is_long] if self._position_state == PerpsState.OPEN else []

        return [
            Intent.perp_close(
                market=self.perp_market,
                collateral_token=self.collateral_token,
                is_long=is_long,
                # None = close the FULL live position. A cached notional drifts
                # from the venue via funding, price impact and partial fills;
                # closing a stale number strands a residual while teardown
                # reports success, and an OVER-sized decrease reverts outright
                # (VIB-5950 / VIB-6160).
                size_usd=None,
                max_slippage=max_slippage,
                protocol=self.protocol,
            )
            for is_long in sides
        ]

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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
            # VALUE THE POSITION LIVE -- nothing in the framework back-fills
            # PositionInfo.value_usd. It is consumed verbatim: summed into the
            # teardown total and read by the max-acceptable-loss guard, where a
            # fabricated 0 silently buys the LOOSEST slippage tier at the exact
            # moment you are unwinding. lp_position_value() returns None (never a
            # fabricated $0) when the position cannot be measured; Empty != Zero,
            # so an unmeasured read is declared via BOTH markers --
            # valuation_status="no_path" (read by the portfolio valuer) and
            # value_usd_unknown=True (read by the teardown CLI preview,
            # framework/cli/teardown_helpers.py). Setting only one leaves the
            # other consumer treating this as a measured zero. Neither restores
            # the loss cap: the max-acceptable-loss guard
            # (framework/teardown/models.py) reads only the summed numeric total
            # and consults neither marker (VIB-5604). A genuinely empty position
            # returns an all-zero MEASURED result -- those two must stay
            # distinguishable.
            details = {{
                "pool": self.pool,
                # `is not None`, NOT truthiness: a range bound of 0 is a legitimate
                # value (ticks are signed and Decimal("0") is a real price bound),
                # and `if self._range_lower` would serialise it as None -- reporting
                # the bound as ABSENT rather than zero. Matches dynamic_lp.
                "range_lower": str(self._range_lower) if self._range_lower is not None else None,
                "range_upper": str(self._range_upper) if self._range_upper is not None else None,
            }}
            value_usd = Decimal("0")
            try:
                lp_value = self.create_market_snapshot().lp_position_value(
                    str(self._position_id), self.protocol
                )
                if lp_value is None:
                    raise RuntimeError("position could not be measured")
                # .total_usd includes uncollected fees; .value_usd excludes them.
                value_usd = Decimal(str(lp_value.total_usd))
            except Exception as e:
                logger.warning(
                    f"get_open_positions: LP #{{self._position_id}} valuation unavailable: {{e}}"
                )
                details["valuation_status"] = "no_path"
                details["value_usd_unknown"] = True

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._position_id),
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=value_usd,
                    details=details,
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
                    chain=self.chain,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
            # TODO(you): value this position before running with real funds.
            #
            # Nothing in the framework back-fills PositionInfo.value_usd. It is
            # consumed verbatim: summed into the teardown total and read by the
            # max-acceptable-loss guard, where a fabricated 0 silently buys the
            # LOOSEST slippage tier at the exact moment you are unwinding.
            #
            # This row is declared UNMEASURED rather than valued, because the
            # scaffold cannot do it correctly for you: ``staked_amt`` is what you
            # DEPOSITED (denominated in ``stake_token``), but most liquid-staking
            # protocols hand back a RECEIPT token at a non-1:1 and drifting ratio
            # (ETH -> wstETH is ~1.18 and rises with accrued rewards). Pricing the
            # deposited amount with the deposited token's price therefore reports
            # the wrong number in the wrong denomination, and reporting it as
            # MEASURED is worse than reporting nothing — a wrong number the guard
            # TRUSTS does more damage than a missing one it can SEE. Empty != Zero.
            #
            # To implement: read your receipt-token balance
            # (``market.balance("<receipt token>")``), price THAT token, and drop
            # the "no_path" marker. Do not simply delete the marker and price
            # ``staked_amt`` — that is the denomination bug this branch avoids.
            details = {{
                "stake_token": self.stake_token,
                "staked_amount": str(staked_amt),
                "valuation_status": "no_path",
                "value_usd_unknown": True,
            }}
            value_usd = Decimal("0")

            positions.append(
                PositionInfo(
                    position_type=PositionType.STAKE,
                    position_id="{strategy_name}_stake",
                    chain=self.chain,
                    protocol=self.staking_protocol,
                    value_usd=value_usd,
                    details=details,
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
                        chain=self.chain,
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
    # Exit assets: after positions close, the framework's consolidation phase
    # swaps recovered tokens to a target token by default. Declaring
    # get_teardown_profile() with preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS
    # skips ONLY that consolidation swap — swap intents emitted by
    # generate_teardown_intents() itself are unaffected, so a no-swap mandate
    # also requires a swap-free unwind above.
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
        self.max_slippage_bps = Decimal(str(get_config("max_slippage_bps", 50)))
        self.hard_teardown_max_slippage_bps = Decimal(str(get_config("hard_teardown_max_slippage_bps", 300)))
        for name in ("max_slippage_bps", "hard_teardown_max_slippage_bps"):
            value = getattr(self, name)
            if not value.is_finite() or not Decimal("0") <= value < Decimal("10000"):
                raise ValueError(f"{name} must be finite and in [0, 10000)")
        self.protocol = get_config("protocol", None)
        self.swap_params = get_config("swap_params", None)
        if self.swap_params is not None and not isinstance(self.swap_params, dict):
            raise ValueError("swap_params must be a mapping or null")

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
        supported_lp_protocols = repr(tuple(sorted(_scaffold_v3_lp_protocols())))
        # dynamic_lp supports BOTH pool encodings, so the emitted guard asks the
        # strategy which one it is on. multi_step below is V3-family only.
        pool_encodes_spacing_expr = "self._uses_tick_ranges()"
        return f"""
        # LP parameters. For aerodrome_slipstream the pool's 3rd component is
        # the TICK SPACING (e.g. WETH/USDC/200), not a fee tier.
        self.pool = get_config("pool", "{default_pool}")
        self.protocol = get_config("protocol", "{protocol}")
        if self.protocol not in {supported_lp_protocols}:
            raise ValueError(
                f"dynamic_lp requires a V3-family LP protocol; {{self.protocol!r}} has "
                "different range or minimum-amount semantics, so this strategy cannot "
                "promise its two-sided LP protection contract."
            )
        try:
            self.range_width_pct = float(get_config("range_width_pct", 5))
        except (TypeError, ValueError) as exc:
            raise ValueError("range_width_pct must be a number between 0 and 100") from exc
        self.rebalance_threshold_pct = float(get_config("rebalance_threshold_pct", 80))
        self.min_position_usd = Decimal(str(get_config("min_position_usd", "500")))
        # LP price-band tolerance, declared on every lp_open (see decide()).
        try:
            self.max_slippage = Decimal(str(get_config("max_slippage", "0.005")))
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise ValueError(
                "max_slippage must be a positive fraction below 1, such as 0.005"
            ) from exc
        # It is a PRICE band (VIB-6269), so it must fit INSIDE the range: where the
        # band reaches a bound, that leg is worth zero at the band edge and ships
        # with NO minimum at all. Tick spacing is the part a naive half-width
        # comparison misses -- the compiler FLOORS both bounds, so the realised
        # bound sits up to one spacing below the requested one. The SDK helper owns
        # that arithmetic (and the fee-tier-vs-spacing distinction) so it is written
        # and tested once rather than copied into every strategy. It REFUSES rather
        # than clamps: VIB-6217's lesson is that silently repairing an out-of-range
        # tolerance hides the misconfiguration that produced it.
        require_lp_tolerance_fits_range(
            max_slippage=self.max_slippage,
            range_half_width_frac=Decimal(str(self.range_width_pct)) / Decimal("100"),
            pool=self.pool,
            pool_encodes_tick_spacing={pool_encodes_spacing_expr},
        )

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
        # For aave_v3 market_id is informational (omitted from config.json by
        # the scaffold); for morpho_blue set the bytes32 market id, verified
        # via `ax lending-market` -- never leave this empty, it can never
        # trade; for compound_v3 set the Comet market key (e.g. "usdc", "weth").
        self.lending_protocol = get_config("lending_protocol", "__SCAFFOLD_PROTOCOL__")
        self.lending_market_id = get_config("lending_market_id", "")

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
        self.perp_leverage = Decimal(str(get_config("perp_leverage", "10")))
        for name in ("spot_size_usd", "hedge_ratio", "perp_leverage"):
            value = getattr(self, name)
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{{name}} must be finite and positive")

        # Funding rate thresholds (hourly rate, e.g. 0.0001 = 0.01%/hr)
        self.funding_entry_threshold = Decimal(str(get_config("funding_entry_threshold", "0.0001")))
        self.funding_exit_threshold = Decimal(str(get_config("funding_exit_threshold", "-0.00005")))

        # Perp venue for the hedge leg (funding-rate reads + perp intents)
        self.protocol = get_config("protocol", "{protocol}")
        from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

        capabilities = get_protocol_capabilities(self.protocol)
        if not capabilities.get("supports_leverage"):
            raise ValueError(f"{{self.protocol}} does not declare leverage support")
        minimum = Decimal(str(capabilities.get("min_leverage", "1")))
        maximum = Decimal(str(capabilities["max_leverage"]))
        if not minimum <= self.perp_leverage <= maximum:
            raise ValueError(f"perp_leverage must be between {{minimum}} and {{maximum}} for {{self.protocol}}")

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
        # EVM venues require an exact chain address for mark valuation.
        # Addressless venues leave this unset and use their connector-declared
        # market mark instead.
        self.index_token_address = get_config("index_token_address", None)
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
        supported_lp_protocols = repr(tuple(sorted(_scaffold_v3_lp_protocols())))
        # multi_step scaffolds V3-family protocols only (tick-spacing protocols are
        # rejected at scaffold time), so the pool's 3rd component is always a fee tier.
        pool_encodes_spacing_expr = "False"
        return f"""
        # Multi-step LP parameters. For aerodrome_slipstream the pool's 3rd
        # component is the TICK SPACING (e.g. WETH/USDC/200), not a fee tier.
        self.pool = get_config("pool", "{default_pool}")
        self.protocol = get_config("protocol", "{protocol}")
        if self.protocol not in {supported_lp_protocols}:
            raise ValueError(
                f"multi_step requires a V3-family LP protocol; {{self.protocol!r}} has "
                "different range or minimum-amount semantics, so this strategy cannot "
                "promise its two-sided LP protection contract."
            )
        try:
            self.range_width_pct = float(get_config("range_width_pct", 5))
        except (TypeError, ValueError) as exc:
            raise ValueError("range_width_pct must be a number between 0 and 100") from exc
        # rebalance_drift_pct is configured as a percentage (e.g. 3 = 3% price drift)
        # and divided by 100 here to convert to a decimal fraction for comparison
        self.rebalance_drift_pct = Decimal(str(get_config("rebalance_drift_pct", "3"))) / Decimal("100")
        self.min_position_usd = Decimal(str(get_config("min_position_usd", "500")))
        # LP price-band tolerance, declared on every lp_open (see decide()).
        try:
            self.max_slippage = Decimal(str(get_config("max_slippage", "0.005")))
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise ValueError(
                "max_slippage must be a positive fraction below 1, such as 0.005"
            ) from exc
        # It is a PRICE band (VIB-6269), so it must fit INSIDE the range: where the
        # band reaches a bound, that leg is worth zero at the band edge and ships
        # with NO minimum at all. Tick spacing is the part a naive half-width
        # comparison misses -- the compiler FLOORS both bounds, so the realised
        # bound sits up to one spacing below the requested one. The SDK helper owns
        # that arithmetic (and the fee-tier-vs-spacing distinction) so it is written
        # and tested once rather than copied into every strategy. It REFUSES rather
        # than clamps: VIB-6217's lesson is that silently repairing an out-of-range
        # tolerance hides the misconfiguration that produced it.
        require_lp_tolerance_fits_range(
            max_slippage=self.max_slippage,
            range_half_width_frac=Decimal(str(self.range_width_pct)) / Decimal("100"),
            pool=self.pool,
            pool_encodes_tick_spacing={pool_encodes_spacing_expr},
        )

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


# Emitted verbatim into every LP scaffold: the range, its drift test and the
# LP_OPEN amounts are execution-facing, so they read the pool's own price rather
# than the market.price() USD oracle.
_POOL_SPOT_HELPER = (
    "    def _pool_spot(self, market):\n"
    '        """Live pool price in this strategy\'s pool-string orientation, plus the raw pool tick.\n'
    "\n"
    "        ``PoolPrice.price`` is token0-in-token1 in the pool's ON-CHAIN order\n"
    "        (lower address first). The price band, the drift test and the LP_OPEN\n"
    "        amounts all use the pool string's order, so the reading is inverted when\n"
    "        the string is non-canonical. Legs written as 0x addresses are compared\n"
    "        directly; only symbol legs go through the token resolver. Ticks are\n"
    "        always on-chain and returned as read (or derived from the pool price when\n"
    "        the reader has none). Raises PoolPriceUnavailableError / ValueError rather\n"
    "        than falling back to the oracle: an unreadable pool must hold, not mint\n"
    "        blind.\n"
    '        """\n'
    "        from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver\n"
    "\n"
    "        def leg_address(leg: str) -> str:\n"
    '            if leg.startswith("0x"):\n'
    "                return leg.lower()\n"
    "            try:\n"
    "                return get_token_resolver().get_address(self.chain, leg).lower()\n"
    "            except TokenResolutionError as exc:\n"
    '                raise ValueError(f"Cannot orient pool {self.pool}: {leg!r} did not resolve on {self.chain}") from exc\n'
    "\n"
    '        first, second, pool_key = self.pool.split("/")\n'
    "        pool = market.pool_price_by_pair(\n"
    "            first, second, chain=self.chain, protocol=self.protocol, fee_tier=int(pool_key)\n"
    "        )\n"
    "        price = Decimal(str(pool.price))\n"
    "        if price <= 0:\n"
    '            raise ValueError(f"Non-positive pool price for {self.pool}: {price}")\n'
    "        tick = pool.tick\n"
    "        if tick is None:\n"
    "            # Backtests serve a pair-ratio proxy with no slot0 tick; derive it\n"
    "            # from the same pool-oriented price so the band stays pool-based.\n"
    "            from almanak.framework.intents import price_to_tick\n"
    "\n"
    "            tick = price_to_tick(price, decimals0=pool.token0_decimals, decimals1=pool.token1_decimals)\n"
    "        canonical = leg_address(first) < leg_address(second)\n"
    '        return (price if canonical else Decimal("1") / price), tick\n'
    "\n"
)


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
            + _POOL_SPOT_HELPER
            + "    def _uses_tick_ranges(self) -> bool:\n"
            + '        """True when the configured protocol addresses LP ranges in raw ticks."""\n'
            + '        return self.protocol == "aerodrome_slipstream"\n'
            + "\n"
            + "    def _tick_band(self, pool_tick):\n"
            + '        """Spacing-aligned (tick_lower, tick_upper) bracketing the live tick by range_width_pct.\n'
            + "\n"
            + "        Slipstream's compiler consumes raw integer ticks aligned to the pool's\n"
            + '        tick spacing (pool format "TOKEN0/TOKEN1/<tick_spacing>"). The band is\n'
            + "        built in tick space from the pool's own tick, so it is exact for either\n"
            + "        token orientation: floor/ceil on the two edges keeps the live tick\n"
            + "        strictly inside it.\n"
            + '        """\n'
            + "        import math\n"
            + "\n"
            + '        tick_spacing = int(self.pool.split("/")[2])\n'
            + "        range_frac = float(self.range_width_pct) / 100.0\n"
            + "        log_per_tick = math.log(1.0001)\n"
            + "        tick_lower = pool_tick + math.log(1.0 - range_frac) / log_per_tick\n"
            + "        tick_upper = pool_tick + math.log(1.0 + range_frac) / log_per_tick\n"
            + "        tick_lower = math.floor(tick_lower / tick_spacing) * tick_spacing\n"
            + "        tick_upper = math.ceil(tick_upper / tick_spacing) * tick_spacing\n"
            + "        if tick_upper <= tick_lower:\n"
            + "            tick_upper = tick_lower + tick_spacing\n"
            + "        return tick_lower, tick_upper\n"
            + "\n"
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
            "            self._range_lower = Decimal(rl) if rl else None\n"
            "            self._range_upper = Decimal(ru) if ru else None\n"
            "\n" + _POOL_SPOT_HELPER
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
    # Only the two LP templates emit the boot-time tolerance guard, so only they
    # import it — an unused import would be an F401 in every other scaffold.
    is_lp_template = template in (StrategyTemplate.DYNAMIC_LP, StrategyTemplate.MULTI_STEP)
    lp_range_guard_import = (
        "from almanak.connectors._strategy_base.cl_range import require_lp_tolerance_fits_range\n"
        if is_lp_template
        else ""
    )
    pool_price_error_import = ", PoolPriceUnavailableError" if is_lp_template else ""
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
{lp_range_guard_import}from almanak.framework.intents import AnyIntent, Intent
from almanak.framework.market import MarketSnapshot{pool_price_error_import}
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


def _protocol_requires_market_id(protocol: str) -> bool:
    """True when ``protocol`` declares ``requires_market_id=True`` (isolated markets).

    Reads the authoritative per-connector ``capabilities.py`` registry (the
    same source ``ax.py`` and the agent-tool schema layer use) so this stays
    in sync with the intent-layer validator instead of hardcoding a protocol
    name here. Lazy import: this module is imported by lightweight CLI entry
    points and must not eagerly pull the connector registry.
    """
    from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

    return bool(get_protocol_capabilities(protocol).get("requires_market_id"))


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
                "hard_teardown_max_slippage_bps": 300,
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
                "max_slippage": "0.005",
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
                "min_collateral_usd": "100",
            }
        )
        # Isolated-market protocols (Morpho Blue and others declaring
        # requires_market_id=True in their connector capabilities.py -- the
        # connector manifest is the single source of truth) can never trade
        # without a verified market id, so the key is always emitted (even
        # empty) for the market-identity scanner (scan_market_identity_findings)
        # to catch at `strat check` / boot. Pooled-reserve protocols like
        # Aave V3 have no such identity -- the scanner's contract is "authors
        # who don't use the field omit it" (config_validation.py), so the
        # key is omitted entirely rather than emitted empty.
        if _protocol_requires_market_id(protocol):
            # Deliberately left for the human/agent to fill in -- resolve a
            # candidate with `ax lending-reserves`, then verify it on-chain
            # with `ax lending-market` before pinning it here. The tool
            # lists candidates; the caller always chooses, never an auto-pick.
            data["lending_market_id"] = ""
    elif template == StrategyTemplate.BASIS_TRADE:
        data.update(
            {
                "protocol": protocol,
                "base_token": "WETH",
                "quote_token": "USDC",
                "perp_market": "ETH/USD",
                "spot_size_usd": "10000",
                "hedge_ratio": "1.0",
                "perp_leverage": "10",
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
        index_token_address = _static_token_address(chain, "WETH")
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
        if index_token_address is not None:
            data["index_token_address"] = index_token_address
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
                "max_slippage": "0.005",
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

    # Add anvil_funding for all templates (unless already set). Native gas uses
    # the EVM native sentinel; ERC-20 keys are exact addresses.
    if "anvil_funding" not in data:
        data["anvil_funding"] = _default_anvil_funding(chain)

    return json.dumps(data, indent=4) + "\n"


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


def _check_market_identity_or_abort(
    config_json_content: str,
    *,
    chain: str,
    protocol: str | None,
    template_enum: "StrategyTemplate",
    strategy_dir: Path,
) -> None:
    """Report + abort (without deleting the scaffold) on an unresolved market id.

    Split out of :func:`new_strategy` purely to keep that function's branch
    count under the CRAP gate. Raises ``click.Abort`` — the caller must let
    that propagate past its generic ``except Exception`` cleanup (which
    deletes a newly created directory) rather than swallowing it, since the
    scaffold is a legitimate starting point that just needs one more
    manual edit, not a failed generation.
    """
    from typing import Any

    from .config_validation import market_identity_findings

    try:
        parsed_config: dict[str, Any] | None = json.loads(config_json_content)
    except (json.JSONDecodeError, TypeError):
        parsed_config = None
    identity_errors = [f for f in market_identity_findings(parsed_config) if f.severity == "error"]
    if not identity_errors:
        return

    resolved_protocol = protocol or TEMPLATE_CONFIGS[template_enum].default_protocol
    click.echo("Market identity unresolved -- this strategy CANNOT trade yet:", err=True)
    for finding in identity_errors:
        click.echo(f"  {finding.field or '<config>'}: {finding.message}", err=True)
    click.echo(err=True)
    click.echo("Resolve and verify before running:", err=True)
    click.echo(
        f"  almanak ax --chain {chain} lending-reserves --protocol {resolved_protocol} "
        "--collateral <token> --loan <token>",
        err=True,
    )
    click.echo(
        f"  almanak ax --chain {chain} lending-market --protocol {resolved_protocol} --market-id <candidate id>",
        err=True,
    )
    click.echo("  Then set the verified id in config.json before running.", err=True)
    click.echo()
    click.echo(f"Scaffold written to {strategy_dir} (edit config.json, then retry).")
    raise click.Abort()


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

        # Structural market-identity check: a scaffold that ships a
        # present-but-empty market_id/*_market_id key can never trade.
        # `strat check` / boot enforce the same scan
        # (config_validation.enforce_market_identity) -- this just surfaces
        # it immediately instead of implying the scaffold is ready to run.
        # Raises click.Abort() (caught below, without deleting the scaffold)
        # when the config is unresolved. Runs BEFORE the success banner so a
        # caller keying off "Created strategy" text rather than the exit
        # code can never read a market-less scaffold as ready.
        _check_market_identity_or_abort(
            config_json_content, chain=chain, protocol=protocol, template_enum=template_enum, strategy_dir=strategy_dir
        )

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
        click.echo()
        click.echo("Next steps:")
        click.echo(f"  cd {strategy_dir}")
        click.echo("  almanak strat run --once --dry-run")

    except click.Abort:
        raise
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
