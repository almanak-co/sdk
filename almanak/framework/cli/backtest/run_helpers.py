"""Shared backtest CLI helpers consumed by `pnl_backtest` and `sweep_backtest`.

Extracted in Phase 5B.1 of the CLI CC reduction plan. Functions here are
deliberately minimal and side-effect-compatible with the original inline
implementations so that extracting them does not change observable behaviour:
- `click.echo` strings stay byte-for-byte identical
- Error surface (`click.Abort`, `click.UsageError`) is preserved
- Exit codes are preserved
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

import click

from ...backtesting import PnLBacktestConfig
from ...strategies import get_strategy
from .helpers import list_strategies_fn

logger = logging.getLogger(__name__)


def _funding_token_address_map(
    strategy_config: dict[str, Any], chain: str, canonical_token_funding_entries: Any
) -> dict[str, tuple[str, str]]:
    funding_map: dict[str, tuple[str, str]] = {}
    funding = canonical_token_funding_entries(strategy_config.get("token_funding"), chain=chain)
    for entry in funding:
        entry_chain = entry.chain or chain
        funding_map[entry.symbol.upper()] = (entry_chain, entry.address)
    return funding_map


def _native_symbol_set(asset_id_map: Any) -> set[str]:
    return {symbol.upper() for symbol in asset_id_map()}


def _resolve_registry_token_address(
    *,
    symbol: str,
    chain: str,
    resolver: Any,
    token_resolution_error: type[Exception],
) -> tuple[str, str] | None:
    try:
        resolved = resolver.resolve(symbol, chain)
    except token_resolution_error:
        logger.debug("No registry address for tracked token %s on %s; leaving as preflight miss", symbol, chain)
        return None
    if getattr(resolved, "is_native", False):
        return None
    return chain, resolved.address


def _registry_token_address_map(
    *,
    tracked_tokens: list[str],
    chain: str,
    existing: dict[str, tuple[str, str]],
    native_symbols: set[str],
    resolver: Any,
    token_resolution_error: type[Exception],
) -> dict[str, tuple[str, str]]:
    registry_map: dict[str, tuple[str, str]] = {}
    for raw_symbol in tracked_tokens:
        symbol = raw_symbol.upper()
        if symbol in existing or symbol in native_symbols:
            continue
        resolved = _resolve_registry_token_address(
            symbol=symbol,
            chain=chain,
            resolver=resolver,
            token_resolution_error=token_resolution_error,
        )
        if resolved is not None:
            registry_map[symbol] = resolved
    return registry_map


def build_token_address_map(
    strategy_config: dict[str, Any],
    tracked_tokens: list[str],
    chain: str,
) -> dict[str, tuple[str, str]]:
    """Build the ``SYMBOL_UPPER -> (chain, address)`` map for CoinGecko resolution.

    Threaded into ``CoinGeckoDataProvider(token_addresses=...)`` so non-native
    ERC20s resolve their coin id dynamically via the contract-address endpoint.

    Sources, in order:

    1. ``token_funding`` entries from ``strategy_config`` (normalized through
       the PnL funding canonicalization boundary) supply
       ``{symbol: (chain, address)}`` for funded tokens. This includes mapping
       the platform's EVM zero-address native alias to the SDK sentinel.
    2. Every tracked symbol (``--tokens`` / ``config.tokens``) that is NOT native
       and NOT already covered is resolved symbol -> address on ``chain`` through
       the SDK token registry (:func:`get_token_resolver`). Natives skip the
       registry lookup — the provider resolves them registry-side without an
       address (Refinement R1) — but the run chain's own native is still
       registered at its sentinel below.
    3. The run chain's native gas asset, at the native sentinel (ALM-3067).
       This map is not price-lane only: it drives the balance seeder and the
       snapshot symbol-alias bridges, and a strategy guarding its gas reserve
       reads ``market.balance("ETH")`` without ever funding or tracking ETH.
       Excluding the native turned that read into a hold-forever miss reported
       as a plausible zero-trade result. ``setdefault`` keeps explicit entries'
       precedence; :func:`native_token_map_entry` guards non-EVM chains and
       natives without a registry coin id.

    A tracked symbol that is neither native nor registry-resolvable is left out
    of the map entirely. It becomes an honest preflight miss (the engine probe
    surfaces it as unavailable) rather than a fabricated price.

    Args:
        strategy_config: Loaded strategy config dict (may carry ``token_funding``).
        tracked_tokens: Upper-cased tracked symbols for the run.
        chain: Run chain used for both the funding default and registry lookups.

    Returns:
        Map of upper-cased symbol to ``(chain, contract_address)``.
    """
    # Lazy imports keep the lazy CLI group's zero-import contract intact: this
    # helper only runs inside a real backtest body, never at module import.
    from almanak.core.chains._helpers import native_coingecko_ids
    from almanak.framework.backtesting.pnl.initial_portfolio import canonical_token_funding_entries
    from almanak.framework.data.tokens import get_token_resolver
    from almanak.framework.data.tokens.exceptions import TokenResolutionError

    # Source 1: explicit token_funding entries for the ACTIVE chain only. A
    # multi-chain funding config can list the same symbol on several chains;
    # only the entry matching the run chain is relevant, and accepting others
    # would let a different chain's address overwrite the active one (the map is
    # symbol-keyed).
    address_map = _funding_token_address_map(strategy_config, chain, canonical_token_funding_entries)

    # Source 2: registry-resolve any remaining non-native tracked symbol.
    # The skip-set is exactly the provider's native projection
    # (the chain registry projection covers natives + accepted aliases + wrapped
    # natives like WETH); those resolve via the chain registry inside the
    # provider, so they need no address and must not trigger a registry lookup.
    address_map.update(
        _registry_token_address_map(
            tracked_tokens=tracked_tokens,
            chain=chain,
            existing=address_map,
            native_symbols=_native_symbol_set(native_coingecko_ids),
            resolver=get_token_resolver(),
            token_resolution_error=TokenResolutionError,
        )
    )

    # Source 3: the run chain's native gas asset (ALM-3067; see docstring).
    from almanak.framework.backtesting.pnl.data_provider import native_token_map_entry

    native_entry = native_token_map_entry(chain)
    if native_entry is not None:
        symbol, key = native_entry
        address_map.setdefault(symbol, key)
    return address_map


def validate_strategy_is_registered(strategy: str) -> None:
    """Ensure `strategy` is registered, aborting with discovery guidance if not.

    Unifies the richer guidance block previously inline in `pnl_backtest`
    (pnl.py:285-298). `sweep_backtest` (sweep.py:901-906) will adopt this
    variant in 5B.3 to replace its terser message.

    Raises:
        click.Abort: if `strategy` is not registered. Output strings match
            the original pnl inline block verbatim; reviewers grep-assert
            these in smoke tests.
    """
    available_strategies = list_strategies_fn()
    if strategy in available_strategies:
        return

    click.echo(f"Error: Strategy '{strategy}' is not registered.", err=True)
    if available_strategies:
        click.echo(
            f"Available strategies: {', '.join(sorted(available_strategies))}",
            err=True,
        )
    click.echo()
    click.echo("The backtest command discovers strategies by:", err=True)
    click.echo("  1. Importing ./strategy.py in the current working directory", err=True)
    click.echo(
        "  2. Scanning ./strategies/ (or $ALMANAK_STRATEGIES_DIR) for <name>/strategy.py",
        err=True,
    )
    click.echo()
    click.echo("Either cd into the strategy directory or set ALMANAK_STRATEGIES_DIR.", err=True)
    click.echo(
        "See registered strategies with: almanak strat backtest pnl --list-strategies",
        err=True,
    )
    click.echo("Create a new strategy with: almanak strat new --name <name>", err=True)
    raise click.Abort()


def parse_token_list(tokens: str) -> list[str]:
    """Split a comma-separated `--tokens` string into an upper-cased list.

    Matches the inline `[t.strip().upper() for t in tokens.split(",")]`
    pattern used in both pnl and sweep commands.
    """
    return [t.strip().upper() for t in tokens.split(",")]


def ensure_deployment_id(strategy_instance: Any, *, fallback: str) -> None:
    """Ensure a strategy instance has a non-empty `deployment_id`.

    Mirrors the `_deployment_id`-before-`deployment_id` attribute-setter dance
    from the original inline code: some strategies expose `deployment_id` as a
    read-only property backed by `_deployment_id`, so we prefer assigning the
    private attribute when present. Only runs if the instance does not
    already have a truthy `deployment_id`.

    Args:
        strategy_instance: Instantiated strategy object.
        fallback: Value to assign when `deployment_id` is missing or empty.
    """
    existing_id = getattr(strategy_instance, "deployment_id", "")
    if existing_id:
        return
    if hasattr(strategy_instance, "_deployment_id"):
        strategy_instance._deployment_id = fallback
    else:
        strategy_instance.deployment_id = fallback


def resolve_strategy_class_or_mock(strategy: str, *, allow_mock: bool) -> Any:
    """Resolve a strategy class by name.

    Args:
        strategy: Strategy name (must already pass
            `validate_strategy_is_registered` when `allow_mock=False`).
        allow_mock: If True, fall back to the shared
            `MockBacktestStrategy` (bound to id ``mock-sweep``) when the
            factory has no registered strategy — preserves the existing
            sweep fallback path. If False, a missing strategy escalates
            to `click.Abort` so pnl retains its VIB-2917 no-silent-
            fallback contract.

    Returns:
        The strategy class (real or mock).
    """
    try:
        return get_strategy(strategy)
    except ValueError:
        if not allow_mock:
            # pnl path: validate_strategy_is_registered should have already
            # handled this; raising Abort here matches its behaviour and
            # keeps output matching when the registry becomes inconsistent
            # between the pre-check and this call.
            click.echo(f"Error: Strategy '{strategy}' is not registered.", err=True)
            raise click.Abort() from None

        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        # Issue #1701: single consolidated mock. Preserved id "mock-sweep"
        # to keep external output byte-for-byte identical.
        from ...backtesting import make_mock_strategy_class

        return make_mock_strategy_class("mock-sweep")


def build_pnl_config(
    *,
    start_time: datetime,
    end_time: datetime,
    interval_seconds: int,
    timeframe: str | None = None,
    chain: str,
    tokens: list[str],
    token_funding: list[dict[str, Any]] | None = None,
    gas_price_gwei: float | None = None,
    include_gas_costs: bool = True,
    allow_degraded_data: bool | None = None,
    preflight_validation: bool | None = None,
    fail_on_preflight_error: bool | None = None,
) -> PnLBacktestConfig:
    """Construct a `PnLBacktestConfig` from CLI-shaped scalar arguments.

    Centralises the `Decimal(str(gas_price))` coercion repeated inline in
    `pnl_backtest` and `sweep_backtest`.

    The sweep-only robustness kwargs (`allow_degraded_data`,
    `preflight_validation`, `fail_on_preflight_error`) default to ``None`` so
    that pnl callers can omit them entirely — when None, we fall through to the
    `PnLBacktestConfig` dataclass defaults (``True``/``True``/``True``). The
    sweep command overrides these three with values that match the original
    sweep inline construction (``True``/``<first-period-only>``/``False``).

    Note: existing float-coercion behaviour is preserved. Issue #1702 tracks
    moving these to `Decimal` in a dedicated follow-up — 5B.3 is a pure
    refactor and must not change behaviour.
    """
    kwargs: dict[str, Any] = {
        "start_time": start_time,
        "end_time": end_time,
        "interval_seconds": interval_seconds,
        "timeframe": timeframe,
        "chain": chain,
        "tokens": tokens,
        "token_funding": token_funding,
        # None = chain-aware default resolved by PnLBacktestConfig from the
        # chain registry (VIB-5088 -- no silent flat 30 gwei).
        "gas_price_gwei": Decimal(str(gas_price_gwei)) if gas_price_gwei is not None else None,
        "include_gas_costs": include_gas_costs,
    }
    if allow_degraded_data is not None:
        kwargs["allow_degraded_data"] = allow_degraded_data
    if preflight_validation is not None:
        kwargs["preflight_validation"] = preflight_validation
    if fail_on_preflight_error is not None:
        kwargs["fail_on_preflight_error"] = fail_on_preflight_error
    return PnLBacktestConfig(**kwargs)
