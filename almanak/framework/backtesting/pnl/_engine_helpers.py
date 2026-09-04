"""Phase helpers for :class:`PnLBacktester` (Phases 6C.2 + 6C.3).

This module contains phase-level helpers extracted from the main body of
``PnLBacktester`` to reduce cyclomatic complexity and isolate responsibilities.
Every helper preserves the EXACT original behavior captured by the
characterization tests in
``tests/unit/backtesting/pnl/test_engine_characterization.py``.

Extracted surfaces
------------------
* ``_run_backtest`` — preflight, initialization, iteration loop, error path,
  and finalization (Phase 6C.2).
* ``_calculate_token_flows`` — per-intent-type token-inflow / token-outflow
  helpers plus a :func:`calculate_token_flows` dispatch sequencer (Phase
  6C.3).

Design notes
------------
* Helpers are module-level functions (not methods) that take the backtester
  instance explicitly. This mirrors the pattern established by
  ``runner/_run_loop_helpers.py`` (Phase 6A.2) -- keeps ``self.`` noise out
  of the slim orchestrator while still respecting the backtester's private
  state (``self._adapter``, ``self._error_handler`` etc.).
* ``BacktestState`` is a mutable container for all local state that flows
  between the initialization, iteration, error-path, and finalization
  phases. It is NOT part of any public API and is deliberately only a
  dataclass so helpers can mutate it in place exactly like the original
  ``_run_backtest`` body mutated its locals.
* Log messages, ``bt_logger.phase(...)`` boundaries, error-path partial
  ``BacktestResult`` fields, and ``run_started_at`` / ``run_ended_at``
  timestamps are reproduced byte-for-byte from the pre-extraction body.
* ``_engine_helpers`` does NOT import from ``engine`` at module load time --
  it uses ``TYPE_CHECKING`` to avoid a circular import while still offering
  typed signatures.
"""

from __future__ import annotations

import logging
import pickle
import tempfile
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import closing, nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING, Any

from almanak.core.chains import DEFAULT_CHAIN, ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.core.constants import canonical_chain_name
from almanak.core.intent_types import IntentType
from almanak.core.perp_markets import perp_market_base
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    ParameterSourceTracker,
    PreflightReport,
    TradeRecord,
    price_series_display_labels,
)
from almanak.framework.backtesting.numeraire import (
    compute_numeraire_metrics,
    merge_numeraire_canonical,
    numeraire_token_address,
    resolve_numeraire_symbol,
)
from almanak.framework.backtesting.pnl.data_broker import BacktestDataBroker
from almanak.framework.backtesting.pnl.data_manifest import LANE_PRICE, OUTCOME_DEGRADED, OUTCOME_SERVED
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataCapability,
    HistoricalDataConfig,
    MarketState,
    TokenRef,
    is_address_like,
    is_token_key,
    normalize_token_key,
    normalize_token_ref,
    token_ref_display,
)
from almanak.framework.backtesting.pnl.data_quality import DataQualityTracker
from almanak.framework.backtesting.pnl.decision_log import DecisionLog
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
    PreflightValidationError,
)
from almanak.framework.backtesting.pnl.feasibility import ExactPoolOHLCVCost, enforce_window_feasibility
from almanak.framework.backtesting.pnl.initial_portfolio import (
    TokenFundingInitializationError,
    active_token_funding_entries,
    funded_token_refs,
    seed_portfolio_from_token_funding,
)
from almanak.framework.backtesting.pnl.intent_extraction import (
    lp_explicit_pair,
    lp_pool_tokens,
)
from almanak.framework.backtesting.pnl.money import PriceQuote, TokenIdentity, TokenUnits, UsdAmount
from almanak.framework.backtesting.pnl.perp_targets import PerpPriceHistoryTarget
from almanak.framework.backtesting.pnl.portfolio import (
    CASH_EQUIVALENT_STABLECOIN_SYMBOLS,
    SimulatedPortfolio,
)
from almanak.framework.backtesting.pnl.run_context import BacktestRunContext
from almanak.framework.backtesting.pnl.run_validity import classify_run_validity, engine_error_verdict, terminal_errors
from almanak.framework.data.interfaces import DataSourceError
from almanak.framework.market.errors import PoolPriceUnavailableError, PriceUnavailableError

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.engine import (
        BacktestableStrategy,
        PnLBacktester,
    )
    from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
    from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
    from almanak.framework.backtesting.pnl.providers.perp.snapshot_funding import SnapshotFundingRateSource
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import SnapshotPoolStateSource
    from almanak.framework.data.timeframes import OHLCVTimeframe
    from almanak.framework.market import MarketSnapshot


logger = logging.getLogger(__name__)


def _canonical_run_chain(config: PnLBacktestConfig) -> str:
    return canonical_chain_name(config.chain.strip()).strip().lower()


def _registered_token_addresses(backtester: PnLBacktester) -> dict[str, tuple[str, str]]:
    """Return the known provider token-address map, normalized for lookup.

    Attribute access is duck-typed throughout: partially-constructed
    backtesters (unit-test doubles) may lack ``data_provider`` or
    ``token_addresses`` entirely and must read as an empty map.
    """
    token_addresses: dict[str, tuple[str, str]] = {}
    provider_addresses = getattr(getattr(backtester, "data_provider", None), "_token_addresses", None)
    if isinstance(provider_addresses, dict):
        for symbol, entry in provider_addresses.items():
            if is_token_key(entry):
                token_addresses[str(symbol).upper()] = normalize_token_key(entry[0], entry[1])
    for symbol, entry in (getattr(backtester, "token_addresses", None) or {}).items():
        if is_token_key(entry):
            token_addresses[str(symbol).upper()] = normalize_token_key(entry[0], entry[1])
    # The run's funded basket is part of the registered identity plane:
    # token_funding names the wallet's assets by symbol AND
    # address, so a funded symbol must resolve — balance reads, credits and
    # debits, intent sizing — even when neither the provider nor the caller
    # registered it. Explicit same-chain registrations keep precedence
    # (initialize_backtest already refused a funded symbol that disagrees
    # with them); a foreign-chain registration is dead weight on this run's
    # alias bridges and yields to the funded same-chain identity.
    for symbol, entry in (getattr(backtester, "_funding_token_addresses", None) or {}).items():
        if not is_token_key(entry):
            continue
        funded = normalize_token_key(entry[0], entry[1])
        existing = token_addresses.get(str(symbol).upper())
        if existing is None or existing[0] != funded[0]:
            token_addresses[str(symbol).upper()] = funded
    _complete_native_symbol_aliases(token_addresses)
    return token_addresses


def _complete_native_symbol_aliases(token_addresses: dict[str, tuple[str, str]]) -> None:
    """Register a native coin under every symbol its chain accepts.

    The funding canonicalizer and the map builders name a native by its
    chain's canonical symbol (``MATIC`` on Polygon), while strategies and the
    platform name the same coin by an accepted alias (``POL``). One asset,
    one key: once the sentinel is registered on a chain under any symbol, the
    remaining accepted symbols point at it too, unless the caller already
    bound them elsewhere.
    """
    from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

    sentinel = NATIVE_SENTINEL.lower()
    native_chains = {key[0] for key in token_addresses.values() if is_token_key(key) and key[1] == sentinel}
    for run_chain in native_chains:
        native_key = (run_chain, sentinel)
        for symbol in native_symbols_for(run_chain):
            token_addresses.setdefault(symbol.upper(), native_key)


def _funding_token_addresses(
    canonical_funding: list[dict[str, Any]],
    *,
    chain: str,
    registered: Mapping[str, tuple[str, str]],
) -> dict[str, tuple[str, str]]:
    """``{SYMBOL: (chain, address)}`` for the run's funded basket (ALM-3398).

    ``token_funding`` is the only place a user names the wallet's assets by
    symbol AND address, yet the identity map feeding the snapshot and
    market-state alias bridges and the portfolio identity table was built
    from provider and caller registrations alone. A funded-but-unregistered
    symbol therefore read as ``Cannot determine balance`` — or, for a
    stablecoin, as the cash lane's MEASURED zero — while the address-keyed
    balance sat funded (Empty != Zero).

    Refuses, loudly and at init, a funded symbol whose address disagrees
    with a same-chain explicit registration or with another funded entry:
    one symbol must never name one asset on the price plane and another on
    the balance plane.
    """
    run_chain = str(chain).lower()
    funded: dict[str, tuple[str, str]] = {}
    for entry in canonical_funding:
        symbol = str(entry.get("symbol") or "").strip().upper()
        address = entry.get("address")
        if not symbol or not isinstance(address, str) or not address:
            continue
        key = normalize_token_key(str(entry.get("chain") or run_chain), address)
        if key[0] != run_chain:
            continue
        for source, other in (
            ("registered token-address map", registered.get(symbol)),
            ("token_funding", funded.get(symbol)),
        ):
            if other is None or not is_token_key(other):
                continue
            other_key = normalize_token_key(other[0], other[1])
            if other_key[0] == run_chain and other_key != key:
                raise TokenFundingInitializationError(
                    f"token_funding symbol {symbol} names {token_ref_display(key)} but the {source} "
                    f"names {token_ref_display(other_key)}; declare exactly one identity per symbol."
                )
        funded[symbol] = key
    return funded


def _token_address_registrations(
    backtester: PnLBacktester,
    *,
    numeraire_symbol: str | None,
    numeraire_address: tuple[str, str] | None,
) -> dict[str, tuple[str, str]]:
    """Return every token-address mapping the provider registration hook should see."""
    token_addresses = _registered_token_addresses(backtester)
    if numeraire_symbol is not None and numeraire_address is not None:
        token_addresses[numeraire_symbol.upper()] = normalize_token_key(numeraire_address[0], numeraire_address[1])
    return token_addresses


def _append_provider_required_price_tokens(
    backtester: PnLBacktester,
    data_tokens: list[TokenRef],
    data_token_identities: set[TokenRef],
    data_token_labels: set[str],
    *,
    chain: str,
    bt_logger: BacktestLogger,
) -> None:
    """Add dynamically verified provider assets without mutating public config."""
    for provider_token in getattr(backtester.data_provider, "required_price_tokens", ()):
        provider_identity = normalize_token_ref(provider_token, chain)
        if provider_identity in data_token_identities:
            continue
        provider_label = token_ref_display(provider_token).upper()
        data_tokens.append(provider_token)
        data_token_identities.add(provider_identity)
        data_token_labels.add(provider_label)
        bt_logger.debug(f"Added provider-verified index token {provider_label} to the data-fetch token set")


def _expected_price_lookup_label(
    token: TokenRef,
    *,
    token_addresses: dict[str, tuple[str, str]],
    chain: str,
) -> str:
    """Return the display key used to compare expected config tokens to MarketState keys."""
    normalized = normalize_token_ref(token, chain)
    if is_token_key(normalized):
        return token_ref_display(normalized).upper()
    assert isinstance(normalized, str)
    registered_address = token_addresses.get(normalized.upper())
    if registered_address is not None:
        return token_ref_display(registered_address).upper()
    return token_ref_display(normalized).upper()


def declared_perp_price_history_targets(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
) -> tuple[PerpPriceHistoryTarget, ...]:
    """Explicit perp targets that are safe to prewarm before tick 1.

    The strategy-owned ``backtest_perp_price_history_targets()`` hook is the
    only declaration; config keys are never read. A market the hook did not
    declare is prepared at first use from the perp intent that names it
    (ALM-3467).

    Each record carries both spellings: the funding lane uses ``market`` and
    the candle lane uses ``price_market`` (address first).
    """
    hook = getattr(strategy, "backtest_perp_price_history_targets", None)
    if callable(hook):
        declared = hook()
        if isinstance(declared, PerpPriceHistoryTarget):
            declared = (declared,)
        if isinstance(declared, str | bytes | Mapping):
            raise ValueError("backtest_perp_price_history_targets() must return PerpPriceHistoryTarget records")
        try:
            targets = tuple(declared)
        except TypeError as exc:
            raise ValueError(
                "backtest_perp_price_history_targets() must return an iterable of PerpPriceHistoryTarget records"
            ) from exc
        invalid = next((target for target in targets if not isinstance(target, PerpPriceHistoryTarget)), None)
        if invalid is not None:
            raise ValueError(
                "backtest_perp_price_history_targets() must return only PerpPriceHistoryTarget records; "
                f"got {invalid!r}"
            )
        return tuple(dict.fromkeys(targets))

    # No config-key discovery here: the typed hook is the only declaration.
    # Config-shaped markets are PREWARM HINTS (hinted_perp_price_history_targets);
    # a market nobody hinted is prepared at first use from the perp intent
    # that names it.
    return ()


def _hint_value(strategy: Any, strategy_config: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        for source in (strategy_config.get(key), getattr(strategy, key, None)):
            if isinstance(source, str) and source.strip():
                return source.strip()
    return None


def hinted_perp_price_history_targets(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
) -> tuple[PerpPriceHistoryTarget, ...]:
    """Prewarm HINTS read from the perp-market keys generated strategies carry.

    Two shapes: the scalar ``market`` / ``funding_market`` + ``market_address``
    pair, and the generated multi-market basket (``markets`` + ``perp_markets``).
    The venue is any price-history-capable protocol named by config,
    strategy attribute or metadata — there is no precedence rule, so a
    dual-protocol strategy whose top-level ``protocol`` is its LP venue still
    gets its perp plane hinted. Hints only decide what is prewarmed before
    tick 1; they never gate anything and this function never raises.
    """
    from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry

    try:
        basket = _generated_basket_perp_price_history_targets(strategy_config)
        if basket:
            return tuple(target for target in basket if PerpPriceHistoryRegistry.has(target.protocol))
        market = _hint_value(strategy, strategy_config, ("funding_market", "market"))
        if market is None:
            return ()
        candidates: list[str] = []
        for value in (strategy_config.get("protocol"), getattr(strategy, "protocol", None)):
            if isinstance(value, str) and value.strip():
                candidates.append(value)
        candidates.extend(
            str(value) for value in (getattr(_strategy_metadata(strategy), "supported_protocols", None) or ())
        )
        capable = [value for value in candidates if PerpPriceHistoryRegistry.has(value)]
        venues = list(dict.fromkeys(PerpPriceHistoryRegistry.canonical(value) for value in capable))
        if len(venues) != 1 or venues[0] is None:
            return ()  # ambiguous or none: first use resolves it from the intent
        market_address = _hint_value(strategy, strategy_config, ("market_address",))
        if market_address is not None and not is_address_like(market_address):
            market_address = None
        return (PerpPriceHistoryTarget(protocol=venues[0], market=market, market_address=market_address),)
    except Exception as exc:  # noqa: BLE001 — a hint must never refuse a run
        logger.debug("Ignoring unusable perp-market prewarm hint: %s", exc)
        return ()


def _generated_basket_perp_price_history_targets(
    strategy_config: Mapping[str, Any],
) -> tuple[PerpPriceHistoryTarget, ...]:
    """Decode the unchanged generated multi-perp basket config as a hint.

    Pair order comes from ``markets``; identity from matching ``perp_markets``
    entries; the two collections must describe exactly the same bases.
    ``market_order`` is never an input. Anything else is not a basket.
    """
    has_markets = "markets" in strategy_config
    has_perp_markets = "perp_markets" in strategy_config
    if not (has_markets and has_perp_markets):
        return ()
    markets = strategy_config.get("markets")
    perp_markets = strategy_config.get("perp_markets")
    protocol = strategy_config.get("protocol")
    if not isinstance(protocol, str) or not protocol.strip():
        raise ValueError("Generated perp basket config requires one non-empty scalar 'protocol'")
    if not isinstance(markets, list) or not markets:
        raise ValueError("Generated perp basket config requires a non-empty 'markets' list of pair labels")
    if not isinstance(perp_markets, Mapping) or not perp_markets:
        raise ValueError("Generated perp basket config requires a non-empty 'perp_markets' mapping")
    entries: dict[str, Mapping[str, Any]] = {}
    for raw_key, raw_entry in perp_markets.items():
        if not isinstance(raw_key, str) or not raw_key.strip() or not isinstance(raw_entry, Mapping):
            raise ValueError("Generated perp basket 'perp_markets' must map base symbols to identity records")
        base = raw_key.strip().upper()
        if base in entries:
            raise ValueError(f"Generated perp basket contains duplicate market identity for {base}")
        entries[base] = raw_entry
    targets: list[PerpPriceHistoryTarget] = []
    seen_bases: set[str] = set()
    for raw_market in markets:
        market_base = perp_market_base(raw_market)
        if market_base is None:
            raise ValueError(f"Generated perp basket market {raw_market!r} is not a pair label")
        if market_base in seen_bases:
            raise ValueError(f"Generated perp basket contains duplicate pair label for {market_base}")
        seen_bases.add(market_base)
        entry = entries.get(market_base)
        if entry is None:
            raise ValueError(
                f"Generated perp basket market {raw_market!r} has no matching perp_markets[{market_base!r}]"
            )
        market_token = _validated_generated_basket_entry(market_base, entry)
        targets.append(
            PerpPriceHistoryTarget(protocol=protocol, market=str(raw_market).strip(), market_address=market_token)
        )
    extra = sorted(set(entries) - seen_bases)
    if extra:
        raise ValueError(f"Generated perp basket has perp_markets entries not present in markets: {extra}")
    return tuple(targets)


def _validated_generated_basket_entry(base: str, entry: Mapping[str, Any]) -> str:
    """Validate one closed-shape generated market identity and return its market token."""
    index_symbol = entry.get("index_symbol")
    market_token = entry.get("market_token")
    index_token = entry.get("index_token")
    if not isinstance(index_symbol, str) or index_symbol.strip().upper() != base:
        raise ValueError(f"Generated perp basket perp_markets[{base!r}].index_symbol must match pair base {base!r}")
    if not isinstance(market_token, str) or not is_address_like(market_token):
        raise ValueError(f"Generated perp basket perp_markets[{base!r}].market_token must be a token address")
    if not isinstance(index_token, str) or not is_address_like(index_token):
        raise ValueError(f"Generated perp basket perp_markets[{base!r}].index_token must be a token address")
    return market_token


def coverage_aware_default_timeframe(strategy: BacktestableStrategy) -> str | None:
    """Return ``"auto"`` for one atomic connector-native perp target set.

    ``PnLBacktestConfig.timeframe=None`` preserves the legacy contract: the
    provider request is pinned to ``interval_seconds``.  Hosted callers that
    omit a price cadence need a coverage-aware default for connector-native
    perp history, but only when the engine can discover the same atomic target
    set that :func:`prepare_perp_price_history` will route. A protocol name
    alone is insufficient, and cross-protocol declarations remain ambiguous.
    """
    from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
    from almanak.framework.backtesting.pnl.engine import PnLBacktester

    strategy_config = PnLBacktester._get_strategy_config_dict(strategy)
    try:
        targets = tuple(
            dict.fromkeys(
                target
                for target in (
                    declared_perp_price_history_targets(strategy, strategy_config)
                    or hinted_perp_price_history_targets(strategy, strategy_config)
                )
                if PerpPriceHistoryRegistry.has(target.protocol)
            )
        )
    except ValueError:
        # Declaration validation belongs to the engine's strict preflight. A
        # hosted default must not mask or re-spell an invalid target contract.
        return None
    canonical_protocols = tuple(PerpPriceHistoryRegistry.canonical(target.protocol) for target in targets)
    return (
        "auto"
        if targets
        and all(protocol is not None for protocol in canonical_protocols)
        and len(set(canonical_protocols)) == 1
        else None
    )


def _strategy_metadata(strategy: BacktestableStrategy) -> Any:
    """The strategy's registration metadata (``STRATEGY_METADATA`` or ``get_metadata()``), if any."""
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    if metadata is None:
        get_metadata = getattr(strategy, "get_metadata", None)
        metadata = get_metadata() if callable(get_metadata) else None
    return metadata


def _preflight_perp_price_history_targets(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
) -> tuple[PerpPriceHistoryTarget, ...]:
    """Decode strategy targets and render declaration failures as preflight errors."""
    from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry

    try:
        declared_targets = declared_perp_price_history_targets(strategy, strategy_config)
    except ValueError as exc:
        raise PreflightValidationError(
            message=(f"Invalid perp price-history declaration under the address-first market contract: {exc}"),
            failed_checks=["perp_price_history"],
            recommendations=["Return typed PerpPriceHistoryTarget records from backtest_perp_price_history_targets()."],
            error_count=1,
            warning_count=0,
        ) from exc
    return tuple(target for target in declared_targets if PerpPriceHistoryRegistry.has(target.protocol))


def _install_perp_price_history_provider(
    *,
    backtester: PnLBacktester,
    provider_cls: Any,
    canonical: str,
    venue: str,
    chain: str,
    markets: tuple[str, ...],
) -> Any:
    """Reuse or install the singular/plural connector-native provider."""
    current_provider = backtester.data_provider
    provider_targets = tuple((venue, chain, market) for market in markets)
    current_targets = getattr(current_provider, "price_history_targets", None)
    if current_targets is None:
        current_target = getattr(current_provider, "price_history_target", None)
        current_targets = (current_target,) if current_target is not None else ()
    if current_targets == provider_targets:
        if callable(getattr(current_provider, "prepare_backtest", None)):
            return current_provider
        raise PreflightValidationError(
            message=(
                f"Provider claiming price-history targets {provider_targets!r} does not implement "
                "the preparation contract"
            ),
            failed_checks=["perp_price_history"],
            recommendations=["Install a connector-declared historical backtest provider."],
            error_count=1,
            warning_count=0,
        )
    if len(markets) == 1:
        provider = provider_cls.for_backtest(
            fallback=current_provider,
            chain=chain,
            market=markets[0],
            venue=venue,
        )
    else:
        factory = getattr(provider_cls, "for_backtest_many", None)
        if not callable(factory):
            raise PreflightValidationError(
                message=(
                    f"{canonical} declares {len(markets)} perp markets but its price-history provider "
                    "does not implement atomic multi-target preparation"
                ),
                failed_checks=["perp_price_history"],
                recommendations=["Install a connector provider with for_backtest_many() support."],
                error_count=1,
                warning_count=0,
            )
        provider = factory(fallback=current_provider, chain=chain, markets=markets, venue=venue)
    remember_provider = getattr(backtester, "_remember_engine_perp_provider", None)
    if callable(remember_provider):
        remember_provider(provider, current_provider, True)
    backtester.data_provider = provider
    return provider


async def prepare_perp_price_history(
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
) -> None:
    """Install and prepare a connector-declared venue price-history route.

    The engine names no venue. Connector manifests decide whether a protocol
    owns a native historical price plane and lazily publish the provider class.
    """

    strategy_config = backtester._get_strategy_config_dict(strategy)
    # Preparation can be retried on one backtester (readiness followed by a
    # run, or a reused fixture). Never carry accepted identities from an
    # earlier attempt into the funding plane for this one.
    backtester._prepared_perp_price_history_targets = ()
    backtester._prepared_perp_declared_targets = ()
    backtester._prepared_perp_hint_targets = ()

    targets = _preflight_perp_price_history_targets(strategy, strategy_config)
    hinted = False
    if not targets:
        targets = hinted_perp_price_history_targets(strategy, strategy_config)
        hinted = bool(targets)
    if hinted:
        # A hint prewarms the venue plane from tick 0 exactly like a
        # declaration, but it can never refuse the run: on any preflight
        # failure the run keeps its spot provider and the market is prepared
        # at first use from the intent that names it.
        previous_provider = backtester.data_provider
        try:
            await _install_and_prepare_perp_plane(backtester, config, bt_logger, targets)
            backtester._prepared_perp_price_history_targets = tuple(dict.fromkeys(targets))
            backtester._prepared_perp_hint_targets = backtester._prepared_perp_price_history_targets
        except Exception as exc:  # noqa: BLE001 — a hint can never refuse the run
            # Deliberately every exception, not just PreflightValidationError:
            # the provider factory, the venue catalogue and chain resolution
            # each raise their own types, and a config key the strategy never
            # meant as a declaration must not turn one of those into a failed
            # run. The DECLARED path above keeps its strict propagation.
            backtester.data_provider = previous_provider
            detail = getattr(exc, "message", None) or str(exc)
            bt_logger.warning(f"Perp-market prewarm hint skipped ({detail}); markets are prepared at first use instead")
            # Resolve timeframe="auto" against the spot plane exactly as the
            # undeclared route does. Without this the hint's failure would
            # leave the run on the fallback cadence with resolved_timeframe
            # unset — a config key nobody meant as a declaration must not
            # change the cadence either. Still never refuses: a provider that
            # cannot report coverage simply keeps the documented default.
            if config.timeframe == "auto" and callable(getattr(backtester.data_provider, "get_price_coverage", None)):
                preparation = await backtester.prepare_spot_price_history(config)
                bt_logger.info(
                    "Resolved timeframe='auto' to provider-validated spot price cadence "
                    f"{preparation.resolved_timeframe!r} after the perp hint was skipped"
                )
        return
    if not targets:
        # No declared route: perp markets are prepared at first use from the
        # intent that names them (engine._ensure_perp_market_route), so an
        # undeclared market is no longer a preflight failure.
        if config.timeframe == "auto":
            if callable(getattr(backtester.data_provider, "get_price_coverage", None)):
                preparation = await backtester.prepare_spot_price_history(config)
                bt_logger.info(
                    "Resolved timeframe='auto' to provider-validated spot price cadence "
                    f"{preparation.resolved_timeframe!r} "
                    f"without changing the {config.interval_seconds}s simulation tick cadence"
                )
                return
            raise PreflightValidationError(
                message=(
                    "timeframe='auto' requires either a coverage-aware spot token provider or one declared "
                    "connector-native perp market; no eligible price-history route was discovered"
                ),
                failed_checks=["perp_price_history"],
                recommendations=[
                    "Configure a coverage-aware spot historical provider, declare the strategy's perp protocol "
                    "and market, or choose an explicit timeframe."
                ],
                error_count=1,
                warning_count=0,
            )
        return
    await _install_and_prepare_perp_plane(backtester, config, bt_logger, targets)
    backtester._prepared_perp_price_history_targets = tuple(dict.fromkeys(targets))
    backtester._prepared_perp_declared_targets = backtester._prepared_perp_price_history_targets


async def _prewarm_funding_history(
    source: SnapshotFundingRateSource,
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
    *,
    require_complete: bool = False,
    prepared_targets: tuple[PerpPriceHistoryTarget, ...] | None = None,
) -> None:
    """Materialize accepted snapshot funding series before data iteration.

    ``prepared_targets`` is one accepted price-plane class: declarations may
    be required while config-only hints must remain best-effort. ``None``
    preserves declaration discovery for standalone callers.
    """
    if not source.history_capable:
        return
    targets = (
        prepared_targets
        if prepared_targets is not None
        else declared_perp_price_history_targets(strategy, strategy_config)
    )
    for target in targets:
        try:
            point_count = await source.materialize_history(
                target.protocol,
                target.market,
                target.market_address or "",
            )
        except Exception as exc:  # noqa: BLE001 — config hints must never become execution gates
            if require_complete:
                raise
            logger.warning(
                "Snapshot funding prewarm unavailable for %s %s; continuing without prewarm: %s",
                target.protocol,
                target.market,
                exc,
            )
            continue
        if require_complete:
            checked_hours = await source.require_complete_history(
                target.protocol,
                target.market,
                target.market_address or "",
            )
            logger.info(
                "Validated %d hours of snapshot funding coverage for %s %s before tick 1",
                checked_hours,
                target.protocol,
                target.market,
            )
        logger.info(
            "Prewarmed %d snapshot funding points for %s %s before tick 1",
            point_count,
            target.protocol,
            target.market,
        )


async def _prepare_declared_historical_twap(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
    config: PnLBacktestConfig,
    manifest: Any | None,
) -> Any | None:
    """Prewarm explicitly declared exact-pool TWAP dependencies.

    Archive ``observe()`` evidence is execution-grade and has no truthful
    fallback. Any declaration or coverage failure therefore aborts preflight
    before tick 1 instead of producing a plausible zero-trade run.
    """
    from almanak.framework.backtesting.pnl.providers.snapshot_twap import (
        SnapshotTWAPSource,
        declared_historical_twap_targets,
    )

    try:
        targets = declared_historical_twap_targets(
            strategy,
            strategy_config,
            default_chain=config.chain,
        )
    except ValueError as exc:
        raise PreflightValidationError(
            message=f"Historical exact-pool TWAP declaration is invalid: {exc}",
            failed_checks=["historical_exact_pool_twap"],
            recommendations=[
                "Declare HistoricalTWAPTarget values through get_backtest_twap_targets() "
                "with an exact chain, protocol, pool address, and observation window."
            ],
            error_count=1,
            warning_count=0,
        ) from exc
    if not targets:
        return None

    run_chain = _canonical_run_chain(config)
    mismatched = [target.chain for target in targets if target.chain != run_chain]
    if mismatched:
        raise PreflightValidationError(
            message=(
                f"Historical exact-pool TWAP targets must use the backtest chain {run_chain!r}; "
                f"declared={sorted(set(mismatched))!r}"
            ),
            failed_checks=["historical_exact_pool_twap"],
            recommendations=["Run one chain per backtest or update the HistoricalTWAPTarget chain."],
            error_count=1,
            warning_count=0,
        )

    source = SnapshotTWAPSource(
        start_time=config.start_time,
        end_time=config.end_time,
        sample_interval_seconds=config.interval_seconds,
        manifest=manifest,
        first_use_feasibility=_first_use_feasibility(config, strategy_config),
    )
    for target in targets:
        try:
            count = await source.materialize_history(target)
        except (DataSourceError, ValueError) as exc:
            raise PreflightValidationError(
                message=f"Historical exact-pool TWAP preflight failed for {target.manifest_key}: {exc}",
                failed_checks=["historical_exact_pool_twap"],
                recommendations=[
                    "Configure a gateway RPC with archive state for the target chain, verify the exact pool "
                    "existed throughout the requested window, or shorten the backtest window."
                ],
                error_count=1,
                warning_count=0,
            ) from exc
        logger.info(
            "Prewarmed %d exact-pool TWAP observations for %s before tick 1",
            count,
            target.manifest_key,
        )
    return source


async def _prewarm_hinted_pool_state(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
    config: PnLBacktestConfig,
    manifest: Any | None,
) -> Any | None:
    """Prewarm the exact pool a config HINT names, without ever refusing the run.

    Generated strategies carry their pool address under ``pool`` or
    ``swap_pool``. Prewarming it keeps the exact-pool plane
    identical to a declared run from tick 0; if the hint is unusable or the
    archive cannot serve it before tick 1, the run proceeds and the pool is
    authenticated at first use from the intent or read that names it.
    """
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
        SnapshotPoolStateSource,
        hinted_historical_pool_state_target,
    )

    hint = hinted_historical_pool_state_target(strategy, strategy_config, default_chain=config.chain)
    if hint is None or hint.chain != _canonical_run_chain(config):
        return None
    source = SnapshotPoolStateSource(
        start_time=config.start_time,
        end_time=config.end_time,
        sample_interval_seconds=config.interval_seconds,
        manifest=manifest,
        first_use_feasibility=_first_use_feasibility(config, strategy_config),
    )
    try:
        source.enforce_first_use_feasibility()
        count = await source.materialize_history(hint)
    except Exception as exc:  # noqa: BLE001 — a hint must never refuse a run
        logger.warning(
            "Exact-pool prewarm hint %s skipped (%s); the pool is authenticated at first use instead",
            hint.manifest_key,
            exc,
        )
        return None
    logger.info("Prewarmed %d exact-pool state observations for hinted %s before tick 1", count, hint.manifest_key)
    return source


def _strategy_cadence_seconds(strategy_config: Mapping[str, Any] | None) -> int | None:
    """The strategy's declared ``data_granularity`` in seconds, or ``None``.

    Malformed values are left for the granularity resolver to reject with its
    own message; here they simply carry no cadence.
    """
    from almanak.framework.data.timeframes import parse_ohlcv_timeframe

    configured = (strategy_config or {}).get("data_granularity")
    if configured is None:
        return None
    try:
        return parse_ohlcv_timeframe(configured, field_name="strategy config data_granularity").seconds
    except (TypeError, ValueError):
        return None


async def _materialize_declared_historical_pool_state_target(
    source: Any,
    target: Any,
    *,
    state_target_keys: set[tuple[str, str, str]],
    ohlcv_target_keys: set[tuple[str, str, str]],
) -> int:
    """Materialize one pool-state target with declaration-lane failure attribution."""
    try:
        return await source.materialize_history(target)
    except (DataSourceError, ValueError) as exc:
        analytics_only = target.key not in state_target_keys and target.key not in ohlcv_target_keys
        ohlcv_only = target.key not in state_target_keys and not analytics_only
        failed_check = (
            "historical_exact_pool_ohlcv"
            if ohlcv_only
            else "historical_pool_analytics"
            if analytics_only
            else "historical_exact_pool_state"
        )
        lane = (
            "exact-pool OHLCV identity"
            if ohlcv_only
            else "pool-analytics state"
            if analytics_only
            else "exact-pool state"
        )
        raise PreflightValidationError(
            message=f"Historical {lane} preflight failed for {target.manifest_key}: {exc}",
            failed_checks=[failed_check],
            recommendations=[
                "Verify the exact pool existed throughout the requested window and configure an archive-capable gateway RPC."
            ],
            error_count=1,
            warning_count=0,
        ) from exc


def _first_use_feasibility(
    config: PnLBacktestConfig,
    strategy_config: Mapping[str, Any] | None,
) -> Callable[[], Any]:
    """Build the shared one-target feasibility check for lazy data planes."""
    return partial(
        enforce_window_feasibility,
        config,
        target_count=1,
        strategy_cadence_seconds=_strategy_cadence_seconds(strategy_config),
    )


async def _prepare_declared_historical_pool_state(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
    config: PnLBacktestConfig,
    manifest: Any | None,
) -> Any | None:
    """Prewarm exact-address archive pool state before simulation tick 1."""
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import (
        declared_historical_pool_analytics_targets,
    )
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_ohlcv import (
        declared_historical_pool_ohlcv_targets,
    )
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
        HistoricalPoolStateTarget,
        SnapshotPoolStateSource,
        declared_historical_pool_state_targets,
    )

    try:
        state_targets = declared_historical_pool_state_targets(
            strategy,
            strategy_config,
            default_chain=config.chain,
        )
    except ValueError as exc:
        raise PreflightValidationError(
            message=f"Historical exact-pool state declaration is invalid: {exc}",
            failed_checks=["historical_exact_pool_state"],
            recommendations=["Declare exact HistoricalPoolStateTarget values for every required pool."],
            error_count=1,
            warning_count=0,
        ) from exc
    try:
        ohlcv_targets = declared_historical_pool_ohlcv_targets(
            strategy,
            strategy_config,
            default_chain=config.chain,
        )
    except (TypeError, ValueError) as exc:
        raise PreflightValidationError(
            message=f"Historical exact-pool OHLCV declaration is invalid: {exc}",
            failed_checks=["historical_exact_pool_ohlcv"],
            recommendations=[
                "Declare HistoricalPoolOHLCVTarget values with an exact chain, protocol, pool, token "
                "orientation, timeframe, and lookback."
            ],
            error_count=1,
            warning_count=0,
        ) from exc
    try:
        analytics_targets = declared_historical_pool_analytics_targets(
            strategy,
            strategy_config,
            default_chain=config.chain,
        )
    except ValueError as exc:
        raise PreflightValidationError(
            message=f"Historical pool-analytics declaration is invalid: {exc}",
            failed_checks=["historical_pool_analytics"],
            recommendations=[
                "Declare HistoricalPoolAnalyticsTarget values with an exact chain, protocol, pool address, "
                "required fields, and optional freshness limit."
            ],
            error_count=1,
            warning_count=0,
        ) from exc

    # Only analytics fields backed by exact archive state should promote an
    # analytics declaration into ``HistoricalPoolStateTarget`` work. Volume
    # and base fee APY are served by completed pool-history rows; forcing an
    # APY-only Curve target through the V3 state reader rejects it before the
    # measured DefiLlama history can be consulted (ALM-3328).
    state_backed_analytics_fields = frozenset({"tvl_usd"})
    state_target_keys = {target.key for target in state_targets}
    ohlcv_target_keys = {(target.chain, target.protocol, target.pool_address) for target in ohlcv_targets}
    targets_by_key = {target.key: target for target in state_targets}
    for ohlcv_target in ohlcv_targets:
        targets_by_key.setdefault(
            (ohlcv_target.chain, ohlcv_target.protocol, ohlcv_target.pool_address),
            HistoricalPoolStateTarget(
                ohlcv_target.chain,
                ohlcv_target.protocol,
                ohlcv_target.pool_address,
            ),
        )
    for analytics_target in analytics_targets:
        if not analytics_target.required_fields & state_backed_analytics_fields:
            continue
        targets_by_key.setdefault(
            analytics_target.key,
            HistoricalPoolStateTarget(
                analytics_target.chain,
                analytics_target.protocol,
                analytics_target.pool_address,
            ),
        )
    targets = tuple(targets_by_key.values())
    if not targets:
        return await _prewarm_hinted_pool_state(strategy, strategy_config, config, manifest)
    run_chain = _canonical_run_chain(config)
    mismatched_ohlcv = [target.chain for target in ohlcv_targets if target.chain != run_chain]
    if mismatched_ohlcv:
        raise PreflightValidationError(
            message=(
                f"Historical exact-pool OHLCV targets must use backtest chain {run_chain!r}: "
                f"{sorted(set(mismatched_ohlcv))!r}"
            ),
            failed_checks=["historical_exact_pool_ohlcv"],
            recommendations=["Run one chain per backtest or update the pool OHLCV target chain."],
            error_count=1,
            warning_count=0,
        )
    mismatched = [target.chain for target in targets if target.chain != run_chain]
    if mismatched:
        raise PreflightValidationError(
            message=f"Historical pool-state targets must use backtest chain {run_chain!r}: {sorted(set(mismatched))!r}",
            failed_checks=["historical_exact_pool_state"],
            recommendations=["Run one chain per backtest or update the pool-state target chain."],
            error_count=1,
            warning_count=0,
        )
    # Fail fast on windows that cannot finish inside the job budget: the
    # estimate is pure arithmetic, so it lands before the first serial
    # gateway page instead of after ~600s of loading (ALM-3385).
    enforce_window_feasibility(
        config,
        target_count=len(targets),
        strategy_cadence_seconds=_strategy_cadence_seconds(strategy_config),
        exact_pool_ohlcv_costs=tuple(
            ExactPoolOHLCVCost(
                lane_key=target.manifest_key,
                timeframe=target.timeframe,
                lookback_candles=target.lookback_candles,
            )
            for target in ohlcv_targets
        ),
    )
    source = SnapshotPoolStateSource(
        start_time=config.start_time,
        end_time=config.end_time,
        sample_interval_seconds=config.interval_seconds,
        manifest=manifest,
        first_use_feasibility=_first_use_feasibility(config, strategy_config),
    )
    for target in targets:
        count = await _materialize_declared_historical_pool_state_target(
            source,
            target,
            state_target_keys=state_target_keys,
            ohlcv_target_keys=ohlcv_target_keys,
        )
        logger.info("Prewarmed %d exact-pool state observations for %s before tick 1", count, target.manifest_key)
    return source


async def _install_and_prepare_perp_plane(
    backtester: PnLBacktester,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    targets: tuple[PerpPriceHistoryTarget, ...],
) -> None:
    """Install the venue price-history provider for ``targets`` and prepare it; raises preflight errors."""
    from almanak.connectors._strategy_base.perp_price_history_registry import (
        PerpPriceHistoryProvider,
        PerpPriceHistoryRegistry,
    )

    unique_targets = tuple(dict.fromkeys(targets))
    canonical_protocols = {
        canonical
        for target in unique_targets
        if (canonical := PerpPriceHistoryRegistry.canonical(target.protocol)) is not None
    }
    if len(canonical_protocols) != 1:
        raise PreflightValidationError(
            message=(
                "One atomic venue-native price plane requires every declared perp target to use the same "
                f"connector protocol; got {sorted(canonical_protocols)!r}"
            ),
            failed_checks=["perp_price_history"],
            recommendations=["Split cross-protocol baskets into separate runs or provide a composed data provider."],
            error_count=1,
            warning_count=0,
        )

    canonical = next(iter(canonical_protocols))
    markets = tuple(target.price_market for target in unique_targets)
    for target in unique_targets:
        if target.market_address is not None:
            continue
        # Label-only declaration is supported but drift-prone: the pair label
        # is display/signal vocabulary, and a stale or re-labeled string fails
        # runs the declared address would have kept alive.
        bt_logger.warning(
            f"Perp market for {target.protocol} is declared by pair label {target.market!r} only; also declare "
            "'market_address' (the venue's market-token contract address) to pin the market "
            "unambiguously (address-first market contract)."
        )
    chain_descriptor = ChainRegistry.try_resolve(config.chain)
    chain = chain_descriptor.name if chain_descriptor is not None else config.chain.lower()
    declared_chains: set[str] = set()
    for value in PerpPriceHistoryRegistry.declared_chains(canonical):
        declared_chains.add(ChainRegistry.resolve(value).name)
    if declared_chains and chain not in declared_chains:
        raise PreflightValidationError(
            message=f"{canonical} declares venue-native price history on {sorted(declared_chains)}, not {chain!r}",
            failed_checks=["perp_price_history"],
            recommendations=["Choose a connector-declared chain for this market."],
            error_count=1,
            warning_count=0,
        )
    provider_cls = PerpPriceHistoryRegistry.backtest_provider(canonical)
    if provider_cls is None:
        raise PreflightValidationError(
            message=f"{canonical} declares perp price history without a backtest provider",
            failed_checks=["perp_price_history"],
            recommendations=["Install or configure the connector's historical backtest provider."],
            error_count=1,
            warning_count=0,
        )
    venue = PerpPriceHistoryRegistry.venue_for(canonical)
    provider: PerpPriceHistoryProvider = _install_perp_price_history_provider(
        backtester=backtester,
        provider_cls=provider_cls,
        canonical=canonical,
        venue=venue,
        chain=chain,
        markets=markets,
    )
    try:
        resolved = await provider.prepare_backtest(config)
    except (DataSourceError, ValueError) as exc:
        requested = config.resolved_timeframe or config.timeframe or f"{config.interval_seconds}s"
        raise PreflightValidationError(
            message=(f"{venue} price-history preflight failed for {markets!r} at {requested!r}: {exc}"),
            failed_checks=["perp_price_history"],
            recommendations=[
                "Use timeframe='auto' to select the finest complete native cadence, shorten the window, "
                "or choose an explicitly supported market/timeframe."
            ],
            error_count=1,
            warning_count=0,
        ) from exc
    mark_prepared = getattr(backtester, "_mark_perp_provider_prepared", None)
    if callable(mark_prepared):
        mark_prepared(provider)
    bt_logger.info(
        f"Resolved {venue} {markets!r} to one atomic native {resolved} price-candle plane "
        f"for the complete {config.duration_days:.1f}-day window"
    )


def _declared_historical_pool_analytics(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
    config: PnLBacktestConfig,
) -> tuple[Any, ...]:
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import (
        declared_historical_pool_analytics_targets,
    )

    try:
        return declared_historical_pool_analytics_targets(
            strategy,
            strategy_config,
            default_chain=config.chain,
        )
    except ValueError as exc:
        raise PreflightValidationError(
            message=f"Historical pool-analytics declaration is invalid: {exc}",
            failed_checks=["historical_pool_analytics"],
            recommendations=["Declare exact HistoricalPoolAnalyticsTarget values for every required pool field."],
            error_count=1,
            warning_count=0,
        ) from exc


def _validate_declared_historical_pool_analytics(
    reader: Any,
    targets: tuple[Any, ...],
    timestamp: datetime,
) -> int:
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import (
        validate_historical_pool_analytics,
    )

    try:
        return validate_historical_pool_analytics(reader, targets, timestamp)
    except (DataSourceError, PoolPriceUnavailableError, ValueError) as exc:
        raise PreflightValidationError(
            message=f"Historical pool-analytics preflight failed: {exc}",
            failed_checks=["historical_pool_analytics"],
            recommendations=[
                "Ensure pool history measures every required field throughout the requested range. "
                "TVL additionally requires exact archive state and historical USD prices for a pool token."
            ],
            error_count=1,
            warning_count=0,
        ) from exc


async def _spool_validated_historical_pool_analytics_grid(
    *,
    backtester: PnLBacktester,
    data_config: HistoricalDataConfig,
    reader: Any,
    targets: tuple[Any, ...],
    pool_state_source: Any | None,
    token_addresses: Mapping[str, tuple[str, str]],
) -> Any:
    """Validate and disk-spool the exact grid consumed by simulation."""
    spool = tempfile.TemporaryFile(mode="w+b")
    try:
        async for timestamp, market_state in backtester.data_provider.iterate(data_config):
            if token_addresses:
                market_state.register_symbol_aliases(token_addresses)
            reader.bind(
                timestamp,
                market_state=market_state,
                pool_state_view=pool_state_source.view_at(timestamp) if pool_state_source is not None else None,
            )
            _validate_declared_historical_pool_analytics(reader, targets, timestamp)
            pickle.dump((timestamp, market_state), spool, protocol=pickle.HIGHEST_PROTOCOL)
        spool.seek(0)
        return spool
    except BaseException:
        spool.close()
        raise


class _SpooledMarketStateIterator:
    """Async iterator with synchronous, deterministic spool cleanup."""

    def __init__(self, spool: Any) -> None:
        self._spool = spool

    def __aiter__(self) -> _SpooledMarketStateIterator:
        return self

    async def __anext__(self) -> tuple[datetime, MarketState]:
        if self._spool.closed:
            raise StopAsyncIteration
        try:
            return pickle.load(self._spool)
        except EOFError:
            self.close()
            raise StopAsyncIteration from None

    def close(self) -> None:
        self._spool.close()


def _market_state_iterator_scope(iterator: Any) -> Any:
    if isinstance(iterator, _SpooledMarketStateIterator):
        return closing(iterator)
    return nullcontext()


async def _prepare_historical_pool_analytics_iteration(
    *,
    backtester: PnLBacktester,
    data_config: HistoricalDataConfig,
    reader: Any,
    targets: tuple[Any, ...],
    pool_state_source: Any | None,
    token_addresses: Mapping[str, tuple[str, str]],
    bt_logger: BacktestLogger,
) -> AsyncIterator[tuple[datetime, MarketState]]:
    if not targets:
        return backtester.data_provider.iterate(data_config)
    with bt_logger.phase("historical_pool_analytics_preflight"):
        spool = await _spool_validated_historical_pool_analytics_grid(
            backtester=backtester,
            data_config=data_config,
            reader=reader,
            targets=targets,
            pool_state_source=pool_state_source,
            token_addresses=token_addresses,
        )
    return _SpooledMarketStateIterator(spool)


# =============================================================================
# Shared mutable state container
# =============================================================================


@dataclass
class BacktestState:
    """Mutable container for state that flows through ``_run_backtest`` phases.

    Populated by :func:`initialize_backtest`, mutated in place by
    :func:`execute_iteration_loop`, and consumed by
    :func:`finalize_backtest_result` / :func:`build_error_result`.
    """

    # Populated by initialize_backtest
    portfolio: SimulatedPortfolio
    data_config: HistoricalDataConfig
    data_source_capabilities: dict[str, HistoricalDataCapability]
    data_source_warnings: list[str]
    compliance_violations: list[str]
    data_quality_tracker: DataQualityTracker
    indicator_engine: BacktestIndicatorEngine
    strategy_config: dict[str, Any]
    token_funding: list[dict[str, Any]]
    parameter_sources: ParameterSourceTracker
    total_ticks: int
    run_context: BacktestRunContext | None = None
    # Run-scoped data broker + provenance manifest (ALM-2943). Lives here
    # (mutable run state) rather than on the frozen BacktestRunContext; the
    # engine additionally activates it as the ambient broker for lanes that
    # cannot reach run state (see data_broker module docstring).
    data_broker: BacktestDataBroker | None = None
    # Mutated during execute_iteration_loop
    pending_intents: list[tuple[Any, datetime, int]] = field(default_factory=list)
    last_market_state: MarketState | None = None
    tick_count: int = 0
    execution_delayed_at_end: int = 0
    initial_portfolio_seeded: bool = False
    # decide()-time market-data failures aggregated across ticks (ALM-2951):
    # (source, key) -> {"ticks": n, "detail": first message}.
    decision_input_failures: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    no_intent_ticks: int = 0
    # Per-tick decision telemetry — the backtest counterpart of the live
    # runner's iteration_summary. Purely observational: hold reasons and
    # decided intents, aggregated into result.decision_summary at finalize.
    decision_log: DecisionLog = field(default_factory=DecisionLog)


def _failure_pattern(entry: dict[str, Any], total_ticks: int) -> str:
    """Classify a decision-input failure's shape over the run.

    - ``warm_up``: failures stopped within the first 10% of the run (or the
      first 60 ticks, whichever is larger) — indicator windows filling, not a
      data outage.
    - ``persistent``: failed on >=90% of ticks — the input was effectively
      never served.
    - ``intermittent``: everything else.
    """
    last_tick = entry.get("last_tick")
    if total_ticks > 0 and last_tick is not None:
        # Persistence first: a failure covering ~the whole run is persistent
        # even when the run is shorter than the warm-up horizon.
        if entry["ticks"] >= total_ticks * 0.9:
            return "persistent"
        warm_up_horizon = max(60, total_ticks // 10)
        if last_tick <= warm_up_horizon and entry["ticks"] <= warm_up_horizon:
            return "warm_up"
    return "intermittent"


def _decision_input_failure_report(state: BacktestState) -> list[dict[str, Any]]:
    """Sorted decide()-time data-failure report entries (ALM-2951)."""
    return [
        {
            "source": source,
            "key": key,
            "ticks": entry["ticks"],
            "detail": entry["detail"],
            "first_tick": entry.get("first_tick"),
            "last_tick": entry.get("last_tick"),
            "pattern": _failure_pattern(entry, state.tick_count),
        }
        for (source, key), entry in sorted(state.decision_input_failures.items(), key=lambda item: -item[1]["ticks"])
    ]


# =============================================================================
# Preflight
# =============================================================================


async def run_preflight(
    backtester: PnLBacktester,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    strategy: BacktestableStrategy | None = None,
) -> tuple[PreflightReport | None, bool]:
    """Execute preflight validation if enabled.

    Returns ``(preflight_report, preflight_passed)``. ``preflight_passed``
    defaults to ``True`` when validation is disabled, mirroring the
    pre-extraction behavior.

    Raises:
        PreflightValidationError: if the support matrix reports a hard
            failure (unconditional — ``fail_on_preflight_error=False`` does
            not bypass it; a chain that cannot price any token has no
            degraded mode, and disabling ``preflight_validation`` entirely
            is the only escape hatch), or if
            ``config.fail_on_preflight_error`` is True and any check failed.
    """
    preflight_report: PreflightReport | None = None
    preflight_passed: bool = True  # Default to True if validation is disabled
    if config.preflight_validation:
        with bt_logger.phase("preflight_validation"):
            bt_logger.info("Running preflight validation checks...")
            preflight_report = await backtester.run_preflight_validation(config, strategy=strategy)
            preflight_passed = preflight_report.passed

            _log_support_matrix(preflight_report, bt_logger)
            _raise_on_support_hard_failures(preflight_report)

            if preflight_report.passed:
                bt_logger.info(
                    f"Preflight validation passed: "
                    f"{len(preflight_report.tokens_available)} tokens available, "
                    f"estimated coverage {preflight_report.estimated_coverage:.1%}"
                )
            else:
                # Log details about what failed
                bt_logger.warning(
                    f"Preflight validation issues detected: "
                    f"{preflight_report.error_count} errors, "
                    f"{preflight_report.warning_count} warnings"
                )
                for check in preflight_report.failed_checks:
                    bt_logger.warning(f"  - [{check.severity.upper()}] {check.check_name}: {check.message}")

                # Fail fast if configured to do so
                if config.fail_on_preflight_error:
                    failed_check_names = [c.check_name for c in preflight_report.failed_checks]
                    primary_error = next(
                        (check for check in preflight_report.failed_checks if check.severity == "error"),
                        None,
                    )
                    primary_details = primary_error.details if primary_error is not None else {}
                    raise PreflightValidationError(
                        message=(
                            primary_error.message
                            if primary_error is not None and primary_details.get("code")
                            else (
                                f"Preflight validation failed with {preflight_report.error_count} errors "
                                f"and {preflight_report.warning_count} warnings. "
                                "Set fail_on_preflight_error=False to continue with degraded mode."
                            )
                        ),
                        failed_checks=failed_check_names,
                        recommendations=preflight_report.recommendations,
                        error_count=preflight_report.error_count,
                        warning_count=preflight_report.warning_count,
                        code=str(primary_details.get("code", "PreflightValidationError")),
                        details=primary_details,
                    )
                else:
                    bt_logger.warning(
                        "Continuing in degraded mode (fail_on_preflight_error=False). "
                        "Results may be inaccurate due to data quality issues."
                    )
    return preflight_report, preflight_passed


def _log_support_matrix(preflight_report: PreflightReport, bt_logger: BacktestLogger) -> None:
    """Surface the support-matrix table and warnings in the run log.

    Degraded lanes print the table + a WARNING per lane and the run
    continues (default mode); institutional / strict-reproducibility mode
    additionally records them as boot compliance violations in
    ``_run_backtest``.
    """
    support = preflight_report.support
    if support is None or not support.has_signal:
        return
    for line in support.render_table().splitlines():
        bt_logger.info(line)
    for lane in support.degraded_lanes:
        bt_logger.warning(f"Support lane '{lane.label}' is {lane.status}: {lane.detail}")
    for warning in support.warnings:
        bt_logger.warning(f"Support: {warning}")


def _raise_on_support_hard_failures(preflight_report: PreflightReport) -> None:
    """Abort on support-matrix hard failures, before the simulation loop.

    Unconditional by design: ``fail_on_preflight_error=False`` (the
    ``--allow-missing-prices`` escape hatch) opts into degraded DATA, not
    into running on a chain/provider combination that cannot price any
    token. ``preflight_validation=False`` remains the only bypass.
    """
    support = preflight_report.support
    if support is None or not support.hard_failures:
        return
    message = "Backtest support preflight failed: " + "; ".join(support.hard_failures)
    if support.recommendations:
        message += " | Remediation: " + " ".join(support.recommendations)
    raise PreflightValidationError(
        message=message,
        failed_checks=["support_matrix"],
        recommendations=list(support.recommendations),
        error_count=len(support.hard_failures),
        warning_count=len(support.warnings),
    )


# =============================================================================
# Initialization
# =============================================================================


def initialize_backtest(
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
) -> BacktestState:
    """Run the ``bt_logger.phase("initialization")`` block.

    Mirrors the original init block: error handler, MEV simulator,
    strategy adapter, parameter source tracker, portfolio, historical data
    config, data source capabilities, compliance-violation seeding for
    ``CURRENT_ONLY`` providers, gas-price record tracking, data quality
    tracker, indicator engine, strategy config dict, and tick counters.
    """
    with bt_logger.phase("initialization"):
        # Initialize error handler for consistent error classification
        backtester._error_handler = BacktestErrorHandler(BacktestErrorConfig())
        backtester._execution_input_failures = {}
        bt_logger.debug("Initialized BacktestErrorHandler for error classification")

        # Initialize MEV simulator based on config
        backtester._init_mev_simulator(config)

        # One immutable answer to "which chain, which window, how strict" for
        # the whole run; built before the adapter so the adapter is born on the
        # run chain instead of DEFAULT_CHAIN.
        run_context = BacktestRunContext.from_configs(config, backtester.data_config)

        # Initialize strategy adapter for strategy-specific backtesting
        backtester._init_adapter(strategy, chain=run_context.chain)

        strategy_config = backtester._get_strategy_config_dict(strategy) or {}
        token_funding = config.token_funding
        if token_funding is None:
            token_funding = strategy_config.get("token_funding")
        if token_funding is None:
            raise TokenFundingInitializationError(
                "Historical PnL backtests require token_funding in the strategy config."
            )
        # Preserve the effective declared basket on config for hashing and the
        # reproducibility artifact. This conditional assignment is the legacy
        # strategy-config fallback; a caller-supplied config basket is never
        # rewritten, filtered, or canonicalized in place.
        if config.token_funding is None:
            config.token_funding = token_funding

        # Carry a run-local canonical basket through startup consumers. The
        # declared config remains untouched while prefetch, preflight, seeding,
        # coverage, and balance lanes share the SDK-native token identity.
        canonical_funding = [
            entry.model_dump(mode="json") for entry in active_token_funding_entries(token_funding, chain=config.chain)
        ]
        # Run-scoped: a reused engine must not carry a previous run's funded
        # basket into this run's identity plane or its conflict check. The
        # basket itself is registered after the provider hook below.
        backtester._funding_token_addresses = {}

        # Create parameter source tracker for audit trail
        # This must be created after _init_adapter so we can track adapter-specific params
        parameter_sources = backtester._create_parameter_source_tracker(config)
        bt_logger.debug(
            f"Tracked {len(parameter_sources.records)} parameter sources "
            f"({len(parameter_sources.config_sources)} config, "
            f"{len(parameter_sources.liquidation_sources)} liquidation, "
            f"{len(parameter_sources.apy_funding_sources)} apy/funding)"
        )

        # Resolve the strategy's declared numeraire (VIB-5127). None for the USD
        # default; a chain mismatch raises here, before the simulation loop.
        numeraire_symbol = resolve_numeraire_symbol(strategy, config.chain)
        numeraire_address = numeraire_token_address(strategy, config.chain) if numeraire_symbol is not None else None

        # Initialize an empty wallet. Token funding is converted to explicit
        # units at the first market tick, when first historical prices exist.
        if backtester.data_config is not None:
            # One strictness answer for the whole run: no plane can stay soft.
            backtester.data_config.strict_historical_mode = run_context.fidelity.strict
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("0"),
            cash_usd=Decimal("0"),
            chain=config.chain,
            gas_tank_budget_usd=config.gas_funding_usd,
            strict_reproducibility=run_context.fidelity.strict,
        )
        # The portfolio captures the numeraire price per equity point; value_usd
        # stays USD (the conservation core is untouched).
        portfolio._numeraire_symbol = numeraire_symbol
        portfolio._numeraire_token = (
            normalize_token_key(numeraire_address[0], numeraire_address[1]) if numeraire_address is not None else None
        )

        # Ensure the numeraire token is always priced by the data provider, even
        # if the strategy never trades it. Use a local copy -- never mutate
        # config.tokens (it feeds config_hash / the reproducibility audit trail).
        data_tokens: list[TokenRef] = list(config.tokens)
        data_token_labels = {token_ref_display(token).upper() for token in data_tokens}
        data_token_identities = {normalize_token_ref(token, config.chain) for token in data_tokens}
        _append_provider_required_price_tokens(
            backtester,
            data_tokens,
            data_token_identities,
            data_token_labels,
            chain=config.chain,
            bt_logger=bt_logger,
        )
        for funded_token in funded_token_refs(canonical_funding, chain=config.chain):
            funded_identity = normalize_token_ref(funded_token, config.chain)
            if funded_identity not in data_token_identities:
                funded_label = token_ref_display(funded_token).upper()
                data_tokens.append(funded_token)
                data_token_identities.add(funded_identity)
                data_token_labels.add(funded_label)
                bt_logger.debug(f"Added funded token {funded_label} to the data-fetch token set")
        if numeraire_symbol is not None and numeraire_symbol not in data_token_labels:
            data_tokens.append(numeraire_symbol)
            bt_logger.debug(f"Added numeraire token {numeraire_symbol} to the data-fetch token set")
        if config.include_gas_costs and config.gas_eth_price_override is None:
            gas_asset_symbol = _gas_prefetch_symbol(config.chain, data_tokens, backtester.data_provider)
            if gas_asset_symbol is not None:
                data_tokens.append(gas_asset_symbol)
                bt_logger.debug(f"Added gas asset token {gas_asset_symbol} to the data-fetch token set")

        # Register the authoritative contract-address map with the data provider
        # so CoinGecko coin ids resolve by address. CLI / service callers thread
        # traded-token mappings through ``backtester.token_addresses``; the
        # strategy QuoteAsset contributes the numeraire mapping, including
        # numeraires the strategy never trades (VIB-5127). Duck-typed: providers
        # without the hook (custom HistoricalDataProvider impls) are unaffected.
        register_addresses = getattr(backtester.data_provider, "register_token_addresses", None)
        token_address_registrations = _token_address_registrations(
            backtester,
            numeraire_symbol=numeraire_symbol,
            numeraire_address=numeraire_address,
        )
        if token_address_registrations and callable(register_addresses):
            register_addresses(token_address_registrations)
            bt_logger.debug(
                f"Registered {len(token_address_registrations)} token address(es) "
                "with the data provider for coin-id resolution"
            )

        # Put the funded basket on the run's ENGINE-SIDE identity plane:
        # from here on ``_registered_token_addresses`` also names
        # every funded symbol, so the snapshot and market-state alias bridges
        # and the portfolio identity table resolve ``balance("WETH")`` onto
        # the address-native funded key. Deliberately after the provider
        # hook: the price lane already fetches funded tokens by address
        # (``funded_token_refs`` above), and the provider's symbol map stays
        # exactly what CLI / service callers and the numeraire contribute.
        backtester._funding_token_addresses = _funding_token_addresses(
            canonical_funding,
            chain=config.chain,
            registered=_registered_token_addresses(backtester),
        )

        # Create historical data config
        data_config = HistoricalDataConfig(
            start_time=config.start_time,
            end_time=config.end_time,
            interval_seconds=config.interval_seconds,
            price_interval_seconds=config.price_interval_seconds,
            tokens=data_tokens,
            chains=[config.chain],
        )

        # Collect data source capabilities and generate warnings
        data_source_capabilities, data_source_warnings = backtester._collect_data_source_capabilities(bt_logger)

        # Track compliance violations for institutional reporting
        # These indicate potential issues with backtest accuracy/reproducibility
        compliance_violations: list[str] = []

        # Check for CURRENT_ONLY providers which affect historical accuracy
        for provider_name, capability in data_source_capabilities.items():
            if capability == HistoricalDataCapability.CURRENT_ONLY:
                compliance_violations.append(
                    f"CURRENT_ONLY data provider used: '{provider_name}'. "
                    "Historical prices are not available; backtest uses runtime prices."
                )

        # Initialize gas price records tracking (if enabled)
        backtester._gas_price_records = [] if config.track_gas_prices else None

        # Initialize data quality tracker
        data_quality_tracker = DataQualityTracker(
            staleness_threshold_seconds=config.staleness_threshold_seconds,
        )

        # Initialize indicator engine for populating MarketSnapshot with TA indicators
        # This enables strategies using market.rsi(), market.macd(), market.bollinger_bands()
        # to work identically in live and backtest modes.
        indicator_engine = backtester._create_indicator_engine(strategy)

        # Iteration counter for logging
        total_ticks = config.estimated_ticks

    adapter = getattr(backtester, "_adapter", None)
    adapter_config = getattr(adapter, "_config", None) if adapter is not None else None
    if adapter_config is not None and hasattr(adapter_config, "strict_reproducibility"):
        adapter_config.strict_reproducibility = run_context.fidelity.strict

    return BacktestState(
        portfolio=portfolio,
        data_config=data_config,
        data_source_capabilities=data_source_capabilities,
        data_source_warnings=data_source_warnings,
        compliance_violations=compliance_violations,
        data_quality_tracker=data_quality_tracker,
        indicator_engine=indicator_engine,
        strategy_config=strategy_config,
        token_funding=canonical_funding,
        parameter_sources=parameter_sources,
        total_ticks=total_ticks,
        run_context=run_context,
        data_broker=BacktestDataBroker(),
    )


def _gas_prefetch_symbol(chain: str, data_tokens: list[TokenRef], data_provider: Any) -> str | None:
    """Return a gas price symbol to fetch only when no accepted alias is already present."""
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return None

    ordered_symbols = [
        descriptor.native.symbol,
        *descriptor.native.accepted_symbols,
        descriptor.native.wrapped_symbol,
        *native_symbols_for(chain),
    ]
    gas_symbols: list[str] = []
    seen: set[str] = set()
    for symbol in ordered_symbols:
        if not symbol:
            continue
        normalized = str(symbol).upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        gas_symbols.append(normalized)

    token_set = {token_ref_display(token).upper() for token in data_tokens}
    if token_set.intersection(gas_symbols):
        return None

    supported_tokens = getattr(data_provider, "supported_tokens", None)
    if supported_tokens is not None:
        supported = {token_ref_display(token).upper() for token in supported_tokens}
        for symbol in gas_symbols:
            if symbol in supported:
                return symbol

    return gas_symbols[0] if gas_symbols else None


# =============================================================================
# Iteration loop
# =============================================================================


def _populate_snapshot_indicators(
    indicator_engine: BacktestIndicatorEngine,
    snapshot: MarketSnapshot,
    strategy_config: dict[str, Any],
    active_tokens: set[str],
    timeframe: OHLCVTimeframe | None,
) -> None:
    """Populate eager indicators only when the tick has a canonical interval."""
    if timeframe is None:
        return
    indicator_engine.populate_snapshot(
        snapshot,
        strategy_config,
        active_tokens=active_tokens,
        timeframe=timeframe,
    )


def _resolve_tick_ohlcv_timeframes(
    interval_seconds: int,
    bt_logger: BacktestLogger,
    strategy_config: Mapping[str, Any] | None = None,
) -> tuple[str, OHLCVTimeframe | None, OHLCVTimeframe]:
    """Resolve tick cadence and the strategy's snapshot indicator default."""
    from almanak.framework.backtesting.pnl.indicator_engine import (
        ohlcv_timeframe_for_interval,
        timeframe_label,
    )
    from almanak.framework.data.timeframes import OHLCVTimeframe, parse_ohlcv_timeframe

    tick_label = timeframe_label(interval_seconds)
    tick_timeframe = ohlcv_timeframe_for_interval(interval_seconds)
    configured_granularity = (strategy_config or {}).get("data_granularity")
    default_timeframe = (
        parse_ohlcv_timeframe(configured_granularity, field_name="strategy config data_granularity")
        if configured_granularity is not None
        else tick_timeframe or OHLCVTimeframe.FOUR_HOURS
    )
    if tick_timeframe is None:
        if configured_granularity is not None:
            bt_logger.warning(
                f"Backtest tick interval {interval_seconds}s ({tick_label}) is outside the canonical OHLCV vocabulary; "
                "eager indicator prepopulation is disabled and indicator reads without an explicit timeframe use "
                f"the configured strategy data_granularity {default_timeframe.value}."
            )
        else:
            bt_logger.warning(
                f"Backtest tick interval {interval_seconds}s ({tick_label}) is outside the canonical OHLCV vocabulary; "
                "eager indicator prepopulation is disabled and indicator reads without an explicit timeframe fall back "
                f"to {default_timeframe.value}. The fallback is available only when it can be derived exactly from the "
                "tick cadence."
            )
    return tick_label, tick_timeframe, default_timeframe


def _requires_complete_funding_history(backtester: PnLBacktester) -> bool:
    """Whether the execution path must repeat strict funding readiness."""
    data_config = backtester.data_config
    return bool(data_config is not None and data_config.use_historical_funding and data_config.strict_historical_mode)


def _configured_pool_descriptors(
    strategy_config: Mapping[str, Any],
    *,
    chain: str,
) -> tuple[Any, ...]:
    """Resolve connector-owned exact pools into run-scoped LP descriptors."""
    from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY

    if not STRATEGY_RUNNER_HOOK_REGISTRY.has_pool_descriptor_declarations(
        chain=chain,
        config=strategy_config,
    ):
        return ()

    from almanak.framework.backtesting.pnl.providers.perp._gateway_history import get_connected_gateway_client

    client, _ = get_connected_gateway_client()
    return STRATEGY_RUNNER_HOOK_REGISTRY.resolve_pool_descriptors(
        gateway_client=client,
        chain=chain,
        config=strategy_config,
    )


def _bind_historical_pool_descriptors(
    backtester: PnLBacktester,
    source: SnapshotPoolStateSource | None,
    strategy_config: Mapping[str, Any],
    *,
    chain: str,
) -> None:
    """Bind materialized exact-pool identities without growing loop branching."""
    descriptors = list(source.descriptors()) if source is not None else []
    descriptors.extend(_configured_pool_descriptors(strategy_config, chain=chain))
    if descriptors:
        backtester._bind_pool_descriptors(descriptors)


def _ensure_run_twap_source(twap_source: Any | None, config: PnLBacktestConfig, state: BacktestState) -> Any:
    """Keep one exact-pool TWAP plane alive for the run.

    Nothing declared still yields an (empty) source so a pool a decide()-time
    read names at first use has somewhere to materialize; an empty source
    refuses exactly like the missing one did, only after trying the archive.
    """
    if twap_source is not None:
        return twap_source
    from almanak.framework.backtesting.pnl.providers.snapshot_twap import SnapshotTWAPSource

    return SnapshotTWAPSource(
        start_time=config.start_time,
        end_time=config.end_time,
        sample_interval_seconds=config.interval_seconds,
        manifest=state.data_broker.manifest if state.data_broker is not None else None,
        first_use_feasibility=_first_use_feasibility(config, state.strategy_config),
    )


def _bind_run_pool_state_source(
    backtester: PnLBacktester,
    pool_state_source: Any | None,
    config: PnLBacktestConfig,
    state: BacktestState,
) -> Any:
    """Keep one exact-pool state plane alive for the run and hand it to the engine.

    Nothing declared still yields an (empty) source so an exact pool an intent
    names at first use has somewhere to materialize; the per-tick view only
    promotes the source once it holds a series, so an empty one behaves
    exactly like no source did before first-use discovery existed.
    """
    if pool_state_source is None:
        from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import SnapshotPoolStateSource

        pool_state_source = SnapshotPoolStateSource(
            start_time=config.start_time,
            end_time=config.end_time,
            sample_interval_seconds=config.interval_seconds,
            first_use_feasibility=_first_use_feasibility(config, state.strategy_config),
            manifest=state.data_broker.manifest if state.data_broker is not None else None,
        )
    bind = getattr(backtester, "_bind_pool_state_source", None)
    if bind is not None:
        bind(pool_state_source)
    return pool_state_source


def _apply_first_use_overlays(
    backtester: PnLBacktester,
    market_state: MarketState,
) -> None:
    """Overlay perp markets discovered at first use before anything reads the tick.

    The streaming provider cannot be re-wrapped mid-run, so venue index prices
    for markets prepared at first use are merged onto each tick's state here,
    ahead of snapshot creation, fills and marks.
    """
    apply_overlays = getattr(backtester, "_apply_perp_market_overlays", None)
    if apply_overlays is not None:
        apply_overlays(market_state)


def _exact_pool_view_at(pool_state_source: Any | None, timestamp: datetime, *, fallback: Any) -> Any | None:
    """The decide()-time exact-pool view, or None while no exact pool is materialized."""
    if pool_state_source is None or getattr(pool_state_source, "is_empty", False):
        return None
    return pool_state_source.view_at(timestamp, fallback=fallback)


async def _prepare_declared_historical_pool_ohlcv(
    strategy: BacktestableStrategy,
    strategy_config: Mapping[str, Any],
    config: PnLBacktestConfig,
    pool_state_source: Any | None,
    *,
    token_addresses: Mapping[str, tuple[str, str]],
) -> Any | None:
    """Materialize every declared exact-pool candle lane before tick 1."""
    from almanak.framework.backtesting.pnl.providers.snapshot_pool_ohlcv import (
        SnapshotExactPoolOHLCVSource,
        declared_historical_pool_ohlcv_targets,
    )

    try:
        targets = declared_historical_pool_ohlcv_targets(
            strategy,
            strategy_config,
            default_chain=config.chain,
        )
    except (TypeError, ValueError) as exc:
        raise PreflightValidationError(
            message=f"Historical exact-pool OHLCV declaration is invalid: {exc}",
            failed_checks=["historical_exact_pool_ohlcv"],
            recommendations=[
                "Declare HistoricalPoolOHLCVTarget values with an exact chain, protocol, pool, token "
                "orientation, timeframe, and lookback."
            ],
            error_count=1,
            warning_count=0,
        ) from exc
    if not targets:
        return None
    if pool_state_source is None:
        raise PreflightValidationError(
            message="Historical exact-pool OHLCV requires an archive-authenticated pool descriptor",
            failed_checks=["historical_exact_pool_ohlcv"],
            recommendations=["Declare the exact pool and configure an archive-capable gateway RPC."],
            error_count=1,
            warning_count=0,
        )

    source = SnapshotExactPoolOHLCVSource(
        pool_state_source,
        start_time=config.start_time,
        end_time=config.end_time,
        token_addresses=token_addresses,
    )
    targets_by_lane: dict[tuple[Any, ...], Any] = {}
    for target in targets:
        previous = targets_by_lane.get(target.lane_key)
        if previous is None or target.lookback_candles > previous.lookback_candles:
            targets_by_lane[target.lane_key] = target
    for target in targets_by_lane.values():
        try:
            count = await source.materialize_history(target)
        except ValueError as exc:
            raise PreflightValidationError(
                message=f"Historical exact-pool OHLCV preflight failed for {target.manifest_key}: {exc}",
                failed_checks=["historical_exact_pool_ohlcv"],
                recommendations=[
                    "Verify exact-pool candles cover the full requested range, shorten the backtest window, "
                    "or choose a supported timeframe."
                ],
                error_count=1,
                warning_count=0,
            ) from exc
        logger.info("Prewarmed %d exact-pool OHLCV candles for %s before tick 1", count, target.manifest_key)
    return source


async def execute_iteration_loop(
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    state: BacktestState,
) -> None:
    """Run the ``bt_logger.phase("simulation")`` iteration loop.

    Mirrors the async-for loop body: per-tick progress logging, snapshot
    creation, indicator engine warm-up, data-quality tracking, pending
    intent execution, strategy decide (with warm-up + error-handler
    classification), intent queuing, adapter position updates, mark to
    market, and end-of-simulation pending-intent drain.

    Mutates ``state`` in place (``pending_intents``, ``last_market_state``,
    ``tick_count``, ``execution_delayed_at_end``).
    """
    # Local import to avoid cyclic import at module load
    from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
    from almanak.framework.backtesting.pnl.providers.perp.snapshot_funding import SnapshotFundingRateSource

    # Make the run's indicator plane available to first-use connector routes
    # before any pending fill can trigger lazy discovery.
    backtester._active_indicator_engine = state.indicator_engine
    backtester._funding_prepared_perp_markets.clear()

    # Strategy-facing funding lane: one source per run; each tick binds it to
    # the tick's simulated timestamp so decide()'s market.funding_rate(...)
    # resolves the rate in effect at that instant (no look-ahead).
    funding_rate_source = SnapshotFundingRateSource(
        chain=config.chain,
        start_time=config.start_time,
        end_time=config.end_time,
        data_config=backtester.data_config,
        # Snapshot funding reads execute in MarketSnapshot's async bridge
        # worker, where the broker context variable is unavailable. Pass the
        # thread-safe run manifest explicitly so decision-time serves are not
        # lost while position-accrual serves remain broker-routed.
        manifest=state.data_broker.manifest if state.data_broker is not None else None,
    )
    backtester._bind_funding_history_source(funding_rate_source)
    await _prewarm_funding_history(
        funding_rate_source,
        strategy,
        state.strategy_config,
        require_complete=_requires_complete_funding_history(backtester),
        prepared_targets=backtester._prepared_perp_declared_targets,
    )
    await _prewarm_funding_history(
        funding_rate_source,
        strategy,
        state.strategy_config,
        require_complete=False,
        prepared_targets=backtester._prepared_perp_hint_targets,
    )
    twap_source = await _prepare_declared_historical_twap(
        strategy,
        state.strategy_config,
        config,
        state.data_broker.manifest if state.data_broker is not None else None,
    )
    pool_state_source = await _prepare_declared_historical_pool_state(
        strategy,
        state.strategy_config,
        config,
        state.data_broker.manifest if state.data_broker is not None else None,
    )
    twap_source = _ensure_run_twap_source(twap_source, config, state)
    pool_state_source = _bind_run_pool_state_source(backtester, pool_state_source, config, state)
    # Stable for the whole run: provider registrations happen during
    # initialize_backtest, before this loop starts.
    token_addresses = _registered_token_addresses(backtester)
    pool_ohlcv_source = await _prepare_declared_historical_pool_ohlcv(
        strategy,
        state.strategy_config,
        config,
        pool_state_source,
        token_addresses=token_addresses,
    )
    _bind_historical_pool_descriptors(
        backtester,
        pool_state_source,
        state.strategy_config,
        chain=config.chain,
    )
    pool_analytics_targets = _declared_historical_pool_analytics(strategy, state.strategy_config, config)

    # Credits must land on the funding identity plane (ALM-2960) — same map
    # the snapshot registers as symbol aliases.
    state.portfolio.register_token_identities(token_addresses)

    # decide()-time data lanes (ALM-2951): on-demand indicators (any period,
    # tick-derivable timeframes) and engine-modeled gas, per-tick bound.
    from almanak.framework.backtesting.pnl.cadence import cadence_is_coarser
    from almanak.framework.backtesting.pnl.engine import (
        BacktestOHLCVView,
        BacktestPoolAnalyticsReader,
        BacktestPoolHistoryReader,
        BacktestPoolPriceView,
        BacktestRateHistoryReader,
        BacktestVolatilityCalculator,
        SimulatedGasView,
        SimulatedPositionView,
        SimulatedSlippageView,
        _snapshot_token_address_map,
        build_backtest_lending_rates,
        sync_il_calculator_positions,
    )
    from almanak.framework.backtesting.pnl.indicator_engine import native_series_aliases, timeframe_label
    from almanak.framework.data.lp import ILCalculator
    from almanak.framework.data.risk.metrics import PortfolioRiskCalculator

    tick_timeframe, tick_ohlcv_timeframe, default_ohlcv_timeframe = _resolve_tick_ohlcv_timeframes(
        config.interval_seconds,
        bt_logger,
        state.strategy_config,
    )
    # A native-symbol read ("ETH") resolves to the chain's native placeholder,
    # which has no close series of its own — serve it from the run's DECLARED
    # wrapped series (OHLCV_PROXY_MAP; the same identity doctrine as the
    # fill-pricing and perp candle lanes). Registered once per run; resolution
    # happens per read, so a directly served native series keeps precedence.
    state.indicator_engine.register_series_aliases(native_series_aliases(config.chain))
    rsi_provider, indicator_provider = state.indicator_engine.snapshot_providers(
        state.strategy_config, config.interval_seconds
    )
    gas_view = SimulatedGasView(backtester, config)
    # market.position_health / aave_health_factor / lp_position_value served
    # from the engine's own tracked positions (ALM-2943) — leverage-loop and
    # LP-rebalance strategies froze on their own sim positions.
    position_view = SimulatedPositionView(state.portfolio)
    # market.ohlcv() served from the run's own close series (ALM-2962) —
    # candle-reading strategies froze while the same code traded live.
    run_manifest = state.data_broker.manifest if state.data_broker is not None else None
    ohlcv_view = BacktestOHLCVView(
        state.indicator_engine,
        config.interval_seconds,
        token_addresses,
        manifest=run_manifest,
        chain=config.chain,
        pool_ohlcv_source=pool_ohlcv_source,
    )
    # ALM-2943: pool_price / pool_price_by_pair as the labeled pair-ratio
    # proxy, estimate_slippage from the engine's own fill models, and
    # realized_vol / vol_cone over the run's close series — all data the
    # engine already owns; the accessors refused it at decide() time.
    pool_price_view = BacktestPoolPriceView(config.chain, token_addresses)
    slippage_view = SimulatedSlippageView(backtester)
    # Retention for the DEFAULT vol windows (review, #3346) is sized LAZILY by
    # the calculator on the first realized_vol/vol_cone call: sizing it eagerly
    # here for every run made the per-tick indicator plane pay for 90 days of
    # history whether or not the strategy ever reads vol, tripping the 1-year
    # perf SLAs (throughput must not degrade with duration).
    volatility_calculator = BacktestVolatilityCalculator(
        indicator_engine=state.indicator_engine,
        tick_interval_seconds=config.interval_seconds,
    )
    # market.pool_history() served from the run's pool-history lane — the
    # same daily ladder LP fee accrual already consumes internally (parity:
    # the accessor refused data the engine was using). pool_history_provider
    # routes through the run's broker when active, else the legacy singleton.
    from almanak.framework.backtesting.pnl.data_broker import pool_history_provider

    pool_history_reader = BacktestPoolHistoryReader(pool_history_provider(), config.chain)
    # market.pool_analytics() from the same daily plane (best_pool keeps its
    # live-parity refusal — live best_pool is deferred to a gateway RPC).
    pool_analytics_reader = BacktestPoolAnalyticsReader(pool_history_provider(), config.chain)
    # market.funding_rate_history() from the run's funding lane; the reader
    # refuses in fallback-funding mode (constant-series-as-history guard).
    rate_history_reader = BacktestRateHistoryReader(funding_rate_source, config.chain)
    lending_rates = build_backtest_lending_rates(
        [*token_addresses, *(str(token) for token in config.tokens if isinstance(token, str))],
        config.chain,
        config.start_time,
    )
    # Pure-math decision-input lanes (ALM-2943): one run-scoped instance each.
    # The IL calculator accumulates the sim's own LP opens (synced per tick
    # below) so ``il_exposure(position_id)`` serves; the risk calculator is
    # stateless math over the caller-supplied PnL series.
    il_calculator = ILCalculator()
    risk_calculator = PortfolioRiskCalculator()
    # Once-per-RUN dedup for the documented-soft empty lanes
    # (wallet_activity / prediction_price ledger notes).
    soft_empty_noted: set[str] = set()

    market_state_iterator = await _prepare_historical_pool_analytics_iteration(
        backtester=backtester,
        data_config=state.data_config,
        reader=pool_analytics_reader,
        targets=pool_analytics_targets,
        pool_state_source=pool_state_source,
        token_addresses=token_addresses,
        bt_logger=bt_logger,
    )

    with bt_logger.phase("simulation"), _market_state_iterator_scope(market_state_iterator):
        # Iterate through historical data
        async for timestamp, market_state in market_state_iterator:
            state.tick_count += 1
            _apply_first_use_overlays(backtester, market_state)

            if state.tick_count == 1:
                # Prefetch has run by the first yield: thread the vendor's
                # MEASURED data resolution into the indicator engine so
                # finer-than-data timeframes refuse instead of serving
                # values computed from flat upsampled ticks (ALM-2957).
                measured = getattr(backtester.data_provider, "measured_granularity_seconds", None)
                state.indicator_engine.set_data_granularity(measured, config.interval_seconds)
                if measured is not None and cadence_is_coarser(measured, config.interval_seconds):
                    bt_logger.warning(
                        f"Price data resolution is {timeframe_label(measured)} but the backtest ticks at "
                        f"{tick_timeframe}: indicators finer than {timeframe_label(measured)} will refuse "
                        f"hard-stop the run when first read (ALM-2957)"
                    )

            # Bridge engine-internal plain-symbol reads (intent USD sizing,
            # adapter valuation, health-factor collateral) onto the
            # address-native state keys through the run's registered map —
            # the MarketState analogue of the snapshot alias bridge below.
            tick_token_addresses = _snapshot_token_address_map(market_state, token_addresses) or {}
            if token_addresses:
                market_state.register_symbol_aliases(token_addresses)
            ohlcv_view.register_token_addresses(tick_token_addresses)

            # Log progress periodically
            if state.tick_count % 100 == 0 or state.tick_count == 1:
                bt_logger.info(
                    f"Backtest progress: {state.tick_count}/{state.total_ticks} ticks "
                    f"({100 * state.tick_count / state.total_ticks:.1f}%)"
                )

            if not state.initial_portfolio_seeded:
                initial_value = seed_portfolio_from_token_funding(
                    state.portfolio,
                    raw_funding=state.token_funding,
                    chain=config.chain,
                    market_state=market_state,
                )
                state.initial_portfolio_seeded = True
                bt_logger.info(f"Seeded initial portfolio from token_funding: ${initial_value:,.2f}")

            # Execute any pending intents that have waited long enough.
            #
            # Fills run BEFORE this tick's snapshot is built: the
            # snapshot seeds wallet balances EAGERLY (_seed_snapshot_balances)
            # while position reads (SimulatedPositionView) are LAZY over the
            # live portfolio. Filling after the snapshot handed decide() a
            # pre-fill wallet next to a post-fill position — an LP_OPEN's
            # capital counted twice on the tick it landed. One post-fill state
            # now feeds every decide()-time plane; the indicator and data-
            # quality bookkeeping below is fill-independent and stays where it
            # is.
            state.pending_intents = await backtester._process_pending_intents(
                pending_intents=state.pending_intents,
                portfolio=state.portfolio,
                market_state=market_state,
                config=config,
                data_quality_tracker=state.data_quality_tracker,
                strategy=strategy,
            )
            _drain_execution_input_failures(backtester, state)

            # Mirror the sim's open LP positions into the run's IL calculator
            # AFTER fills, BEFORE the snapshot and decide(): a position filled
            # this tick is registered at this tick's (fill-exact) prices, and
            # decide() can immediately read il_exposure on it.
            sync_il_calculator_positions(il_calculator, state.portfolio, market_state, config.chain)

            # Create market snapshot for strategy (post-fill wallet state)
            gas_view.bind(market_state, timestamp)
            ohlcv_view.bind(timestamp)
            position_view.bind(market_state, timestamp)
            pool_price_view.bind(market_state, timestamp)
            slippage_view.bind(market_state, timestamp)
            pool_history_reader.bind(timestamp)
            rate_history_reader.bind(timestamp)
            exact_pool_view = _exact_pool_view_at(pool_state_source, timestamp, fallback=pool_price_view)
            pool_analytics_reader.bind(
                timestamp,
                market_state=market_state,
                pool_state_view=exact_pool_view,
                pool_state_source=pool_state_source,
            )
            _validate_declared_historical_pool_analytics(
                pool_analytics_reader,
                pool_analytics_targets,
                timestamp,
            )
            snapshot = create_market_snapshot_from_state(
                market_state=market_state,
                chain=config.chain,
                portfolio=state.portfolio,
                token_addresses=tick_token_addresses,
                funding_rate_source=funding_rate_source,
                price_aggregator=twap_source.view_at(timestamp) if twap_source is not None else None,
                rsi_provider=rsi_provider,
                indicator_provider=indicator_provider,
                gas_view=gas_view,
                default_timeframe=default_ohlcv_timeframe,
                ohlcv_module=ohlcv_view,
                lending_rates=lending_rates,
                position_view=position_view,
                pool_price_view=exact_pool_view or pool_price_view,
                pool_reader=exact_pool_view,
                slippage_view=slippage_view,
                volatility_calculator=volatility_calculator,
                il_calculator=il_calculator,
                risk_calculator=risk_calculator,
                pool_history_reader=pool_history_reader,
                pool_analytics_reader=pool_analytics_reader,
                rate_history_reader=rate_history_reader,
                soft_empty_noted=soft_empty_noted,
            )

            # Cache available_tokens once per tick: the property returns a
            # fresh list on every access, and we use it in multiple loops
            # below. Also build an upper-case set so the membership check
            # inside the expected_tokens loop is O(1) instead of O(N) per
            # expected token (see #1781).
            available_tokens = market_state.available_tokens

            # Append prices to indicator engine and populate snapshot
            tick_tokens: set[str] = set()
            for token in available_tokens:
                try:
                    price = market_state.get_price(token)
                    state.indicator_engine.append_price(token, price)
                    tick_tokens.add(token)
                except KeyError:
                    pass
            _populate_snapshot_indicators(
                state.indicator_engine,
                snapshot,
                state.strategy_config,
                tick_tokens,
                tick_ohlcv_timeframe,
            )
            state.indicator_engine.enrich_price_data(snapshot, config.interval_seconds, active_tokens=tick_tokens)

            # Track data quality: record successful price lookups
            # Count tokens with available prices in this tick
            # Coverage follows the effective provider universe assembled at
            # initialization (configured + funded + numeraire + gas/provider
            # requirements), not only the public ``config.tokens`` list.
            # Otherwise successfully fetched funded assets disappear from the
            # manifest and a run can report 100% coverage over half its real
            # valuation dependencies (ALM-3232).
            expected_tokens = state.data_config.tokens
            expected_token_addresses = token_addresses
            provider_name = getattr(backtester.data_provider, "provider_name", "unknown")

            # Record successful lookups for each available token
            for expected_token in expected_tokens:
                expected_token_label = _expected_price_lookup_label(
                    expected_token,
                    token_addresses=expected_token_addresses,
                    chain=config.chain,
                )
                if market_state.has_token(expected_token):
                    state.data_quality_tracker.record_lookup(
                        success=True,
                        source=provider_name,
                    )
                    if run_manifest is not None:
                        run_manifest.record(
                            lane=LANE_PRICE,
                            key=expected_token_label,
                            source=provider_name,
                            outcome=OUTCOME_SERVED,
                            at=timestamp,
                        )
                else:
                    state.data_quality_tracker.record_lookup(success=False)
                    if run_manifest is not None:
                        run_manifest.record(
                            lane=LANE_PRICE,
                            key=expected_token_label,
                            source="",
                            outcome=OUTCOME_DEGRADED,
                            at=timestamp,
                            detail="no price in market state for this tick",
                        )

            # Get strategy decision (warm-up + error-handler branch)
            decide_result = _invoke_strategy_decide(
                backtester=backtester,
                strategy=strategy,
                snapshot=snapshot,
                tick_tokens=tick_tokens,
                tick_count=state.tick_count,
                timestamp=timestamp,
                indicator_engine=state.indicator_engine,
                strategy_config=state.strategy_config,
                bt_logger=bt_logger,
                decision_log=state.decision_log,
            )

            # Extract intent from decide result
            intent = backtester._extract_intent(decide_result)

            # Per-tick decision telemetry (iteration_summary counterpart):
            # exactly one record per tick — the engine-side warm-up / error
            # branches inside _invoke_strategy_decide record first with their
            # cause, making this call a no-op for those ticks (first write
            # wins). Explicit holds land here with reason + reason_code.
            state.decision_log.record(
                tick=state.tick_count,
                timestamp=timestamp,
                intent=intent,
                source="strategy",
            )

            # Queue intent for execution (with inclusion delay)
            if intent is not None and not backtester._is_hold_intent(intent):
                state.pending_intents.append((intent, timestamp, config.inclusion_delay_blocks))
            else:
                # Counts every tick without a queued intent: explicit holds,
                # indicator warm-up, and handled decide() errors alike — the
                # hollow-run warning's causal gate is zero fills + recorded
                # failures, not this counter.
                state.no_intent_ticks += 1

            # Aggregate decide()-time data failures for the run report
            # (ALM-2951): the snapshot records every input it could not serve.
            # first/last tick indices let the report tell a warm-up-only gap
            # (indicator windows filling) from a persistent outage — observed
            # on staging: a 14-tick indicator warm-up was blamed as a data
            # outage for a 2161-tick hold.
            for failure_key, detail in getattr(snapshot, "_critical_data_failures", {}).items():
                entry = state.decision_input_failures.setdefault(
                    failure_key,
                    {"ticks": 0, "detail": str(detail), "first_tick": state.tick_count, "last_tick": state.tick_count},
                )
                entry["ticks"] += 1
                entry["last_tick"] = state.tick_count

            # Update positions via adapter if available
            backtester._update_positions_via_adapter(state.portfolio, market_state, timestamp)

            # Mark portfolio to market (uses adapter for valuation if available)
            state.portfolio.mark_to_market(market_state, timestamp, adapter=backtester._adapter)

            # Store the market state for use after simulation completes
            state.last_market_state = market_state  # noqa: F841 (used in US-062b)

        # Execute any remaining pending intents at end of simulation
        # (Use last market state for final execution)
        await _drain_pending_intents_at_end(
            backtester=backtester,
            strategy=strategy,
            config=config,
            bt_logger=bt_logger,
            state=state,
        )
        _drain_execution_input_failures(backtester, state)


def _drain_execution_input_failures(backtester: PnLBacktester, state: BacktestState) -> None:
    """Fold fill-time missing-data refusals into the run's decision-input ledger.

    Same aggregation as the per-tick snapshot drain, so an intent that could
    not be filled for want of a number is nameable in the run report next to
    the decide()-time lanes.
    """
    pending = getattr(backtester, "_execution_input_failures", None)
    if not pending:
        return
    for failure_key, detail in pending.items():
        entry = state.decision_input_failures.setdefault(
            failure_key,
            {"ticks": 0, "detail": detail, "first_tick": state.tick_count, "last_tick": state.tick_count},
        )
        entry["ticks"] += 1
        entry["last_tick"] = state.tick_count
    pending.clear()


def _invoke_strategy_decide(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    snapshot: Any,
    tick_tokens: set[str],
    tick_count: int,
    timestamp: datetime,
    indicator_engine: BacktestIndicatorEngine,
    strategy_config: dict[str, Any],
    bt_logger: BacktestLogger,
    decision_log: DecisionLog | None = None,
) -> Any:
    """Call ``strategy.decide(snapshot)`` with warm-up / error-handler logic.

    Returns the ``decide_result`` (or ``None`` on non-fatal errors / warm-up).
    Raises ``RuntimeError`` if the error handler classifies the error as
    fatal (``should_stop`` True). Engine-side holds (warm-up, handled decide()
    errors) are recorded into ``decision_log`` with their cause so the run's
    decision telemetry can tell them from a strategy's explicit hold.
    """
    try:
        return strategy.decide(snapshot)
    except Exception as e:
        # Check if this is an indicator warm-up error (expected during initial ticks).
        # The indicator engine's is_warming_up() is the authoritative signal:
        # if the engine hasn't accumulated enough data points AND the strategy
        # raised a ValueError, it's almost certainly because indicators aren't
        # ready yet (e.g. "Cannot calculate RSI", "MACD data not available").
        # We only suppress ValueError to avoid masking real bugs (AttributeError,
        # KeyError, etc.).
        is_warmup = isinstance(e, ValueError) and any(
            indicator_engine.is_warming_up(t, strategy_config) for t in tick_tokens
        )
        if is_warmup:
            # Expected: not enough data points yet for indicators.
            # Log at debug (not warning) to avoid alarming users.
            bt_logger.debug(f"Tick {tick_count}: indicator warm-up ({e}) - holding")
            if decision_log is not None:
                decision_log.record(tick=tick_count, timestamp=timestamp, intent=None, source="warm_up", detail=str(e))
        elif backtester._error_handler:
            # Use error handler for consistent classification
            result = backtester._error_handler.handle_error(
                e,
                context=f"strategy_decide:tick_{tick_count}:{timestamp.isoformat()}",
            )
            if result.should_stop:
                raise RuntimeError(f"Fatal error in strategy.decide() at tick {tick_count}: {e}") from e
            # Non-fatal: log warning and continue with hold
            bt_logger.warning(f"Strategy decide() error at tick {tick_count}: {e} - continuing with hold")
            if decision_log is not None:
                decision_log.record(
                    tick=tick_count, timestamp=timestamp, intent=None, source="decide_error", detail=str(e)
                )
        else:
            bt_logger.warning(f"Strategy decide() raised exception at {timestamp}: {e}")
            if decision_log is not None:
                decision_log.record(
                    tick=tick_count, timestamp=timestamp, intent=None, source="decide_error", detail=str(e)
                )
        return None
    finally:
        _raise_on_indicator_timeframe_mismatch(snapshot, tick_count=tick_count)


def _raise_on_indicator_timeframe_mismatch(snapshot: Any, *, tick_count: int) -> None:
    """Abort when an actually-read indicator is finer than historical data.

    This is deliberately demand-driven until strategies declare typed
    indicator requirements.  It catches both an uncaught accessor error and a
    strategy that catches ``ValueError`` and returns HOLD, while avoiding false
    preflight failures for price-only strategies that never read indicators.
    """
    from almanak.framework.backtesting.pnl.cadence import canonical_timeframe_for_cadence
    from almanak.framework.backtesting.pnl.indicator_engine import IndicatorTimeframeMismatchError

    causes = getattr(snapshot, "_critical_data_failure_causes", {})
    mismatch = next(
        (
            (source, cause)
            for (source, _key), cause in causes.items()
            if isinstance(cause, IndicatorTimeframeMismatchError)
        ),
        None,
    )
    if mismatch is None:
        return

    source, cause = mismatch
    compatible_timeframe = canonical_timeframe_for_cadence(cause.native_seconds)
    available_timeframe = compatible_timeframe.value if compatible_timeframe is not None else None
    availability = available_timeframe or f"an unsupported {cause.native_seconds}s cadence"
    recommendations = (
        [
            f"For an explicit indicator timeframe, request {available_timeframe} or coarser in the indicator call.",
            f"For a strategy-default timeframe, set data_granularity to {available_timeframe} or coarser.",
            f"Alternatively select a historical price plane with complete {cause.requested_timeframe} coverage "
            "for the requested window, or shorten the window.",
        ]
        if available_timeframe is not None
        else [
            "Choose a historical provider or window whose native cadence is no coarser than the SDK's 1d "
            "indicator maximum."
        ]
    )
    details: dict[str, Any] = {
        "reason_code": "PRICE_TIMEFRAME_TOO_FINE" if available_timeframe else "PRICE_CADENCE_UNSUPPORTED",
        "source": source,
        "requested_timeframe": cause.requested_timeframe,
        "available_timeframe": available_timeframe,
        "observed_interval_seconds": cause.native_seconds,
    }
    if available_timeframe is not None:
        details.update(
            {
                "suggested_indicator_timeframe_patch": {
                    "indicator": source,
                    "timeframe": available_timeframe,
                },
                "suggested_strategy_config_patch": {"data_granularity": available_timeframe},
            }
        )
    raise PreflightValidationError(
        message=(
            f"Strategy indicator timeframe is incompatible with the resolved historical price data: "
            f"{source} requested {cause.requested_timeframe}, but the data plane provides "
            f"{availability}. The backtest stopped at tick {tick_count} rather than treating "
            "the unavailable indicator as a valid HOLD."
        ),
        failed_checks=["indicator_timeframe_compatibility"],
        recommendations=recommendations,
        error_count=1,
        warning_count=0,
        code="INDICATOR_TIMEFRAME_MISMATCH",
        details=details,
    ) from cause


def notify_intent_outcome(
    backtester: PnLBacktester,
    strategy: Any,
    intent: Any,
    trade_record: TradeRecord,
    log: Any,
) -> None:
    """Invoke ``strategy.on_intent_executed`` with the fill's real outcome.

    ``trade_record.success`` is authoritative: the portfolio records
    rejected fills (insufficient balance, producer-failed) as failed trades
    without mutating state, and the strategy must observe that outcome or
    its state machine advances past a trade that never applied.
    """
    if strategy is None or not hasattr(strategy, "on_intent_executed"):
        return
    applied = trade_record.success
    failure_reason = None if applied else trade_record.metadata.get("failure_reason", "fill rejected")
    try:
        callback_result = backtester._build_callback_result(intent, trade_record, success=applied, error=failure_reason)
        strategy.on_intent_executed(intent, applied, callback_result)
    except Exception as notify_err:
        log.debug(f"on_intent_executed raised: {notify_err}")


async def _drain_pending_intents_at_end(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    state: BacktestState,
) -> None:
    """Execute remaining pending intents through the normal sequence-aware path."""
    if state.pending_intents and state.last_market_state is not None:
        bt_logger.warning(
            f"Executing {len(state.pending_intents)} pending intent(s) at simulation end "
            f"(delayed execution using last market state from {state.last_market_state.timestamp})"
        )
        for intent, decision_time, _ in state.pending_intents:
            terminal_count = await backtester._execute_ready_pending_intent(
                intent=intent,
                decision_time=decision_time,
                portfolio=state.portfolio,
                market_state=state.last_market_state,
                config=config,
                data_quality_tracker=state.data_quality_tracker,
                strategy=strategy,
                delayed_at_end=True,
            )
            state.execution_delayed_at_end += terminal_count
            bt_logger.info(
                f"Finished delayed pending item decided at {decision_time} with "
                f"{terminal_count} terminal trade outcome(s)"
            )
    elif state.pending_intents:
        # No tick ever produced a market state, so nothing can be priced. The
        # intents still need terminal outcomes: a dropped intent reads as a
        # ledger drop, a rejection reads as what it is.
        bt_logger.warning(
            f"Cannot execute {len(state.pending_intents)} remaining pending intents: no valid market state available"
        )
        for intent, decision_time, _ in state.pending_intents:
            unpriced = RuntimeError("no valid market state available at simulation end")
            trade_record = backtester._record_intent_execution_rejection(
                intent,
                state.portfolio,
                decision_time,
                unpriced,
                trades_before_execution=len(state.portfolio.trades),
                delayed_at_end=True,
                rejection_code="no_market_state",
            )
            state.execution_delayed_at_end += 1
            notify_intent_outcome(backtester, strategy, intent, trade_record, bt_logger)


# =============================================================================
# Error-path result
# =============================================================================


def build_error_result(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    backtest_id: str,
    bt_logger: BacktestLogger,
    run_started_at: datetime,
    state: BacktestState,
    preflight_report: PreflightReport | None,
    preflight_passed: bool,
    error: Exception,
) -> BacktestResult:
    """Build the partial ``BacktestResult`` returned on simulation failure.

    Mirrors the ``except Exception as e`` branch exactly -- ``error`` is
    typed as :class:`Exception` (not :class:`BaseException`) because the
    original code only caught ``Exception``. The error handler
    ``handle_error`` call is performed here for consistent classification
    and tracking.
    """
    if backtester._error_handler:
        result = backtester._error_handler.handle_error(
            error,
            context="simulation_phase:main_loop",
        )
        bt_logger.error(
            f"Backtest failed with "
            f"{result.error_record.classification.error_type.value if result.error_record else 'unknown'} "
            f"error: {error}"
        )
    else:
        bt_logger.error(f"Backtest failed with error: {error}")

    run_ended_at = datetime.now(UTC)
    # On error, compliance is False and we add the error as a violation
    error_compliance_violations = state.compliance_violations + [f"Backtest failed with error: {error}"]
    error_fallback_usage = backtester._fallback_usage.copy() if backtester._fallback_usage else {}
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id=strategy.deployment_id,
        start_time=config.start_time,
        end_time=config.end_time,
        metrics=BacktestMetrics(),
        initial_portfolio_value_usd=state.portfolio.initial_capital_usd,
        final_capital_usd=state.portfolio.initial_capital_usd,
        chain=config.chain,
        decision_input_failures=_decision_input_failure_report(state) or None,
        run_validity=engine_error_verdict(error),
        resolved_timeframe=config.resolved_timeframe,
        run_started_at=run_started_at,
        run_ended_at=run_ended_at,
        run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
        config=config.to_dict_with_metadata(data_provider_info=backtester._get_data_provider_info()),
        error=str(error),
        backtest_id=backtest_id,
        phase_timings=[t.to_dict() for t in bt_logger.phase_timings],
        config_hash=config.calculate_config_hash(),
        errors=backtester._error_handler.get_errors_as_dicts() if backtester._error_handler else [],
        data_source_capabilities=state.data_source_capabilities,
        data_source_warnings=state.data_source_warnings,
        data_quality=state.data_quality_tracker.to_data_quality_report(),
        institutional_compliance=False,
        compliance_violations=error_compliance_violations,
        fallback_usage=error_fallback_usage,
        preflight_report=preflight_report,
        preflight_passed=preflight_passed,
        gas_prices_used=backtester._gas_price_records or [],
        gas_price_summary=None,  # No trades on error
        parameter_sources=state.parameter_sources,
        data_manifest=state.data_broker.manifest.to_dict() if state.data_broker is not None else None,
        # Partial telemetry up to the failing tick — often exactly the
        # evidence needed to diagnose the failure.
        decision_summary=state.decision_log.summary(trades=state.portfolio.trades),
        decision_events=state.decision_log.events(),
    )


# =============================================================================
# Finalization
# =============================================================================


def enforce_data_quality_gate(
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    state: BacktestState,
) -> None:
    """Enforce the post-simulation data coverage threshold.

    Appends to ``state.compliance_violations`` when coverage is below the
    configured minimum. Raises ``ValueError`` in institutional mode only;
    otherwise logs a warning.
    """
    coverage_ratio = state.data_quality_tracker.coverage_ratio
    if coverage_ratio is None:
        # Empty != Zero: nothing was looked up, so coverage is unmeasured. An
        # empty run is the NO_TICKS verdict's to name; a ticked run that
        # valued nothing has no price plane at all and cannot pass the gate.
        if state.tick_count == 0:
            return
        unmeasured = f"Data coverage unmeasured: no price lookups were recorded over {state.tick_count} tick(s)"
        state.compliance_violations.append(unmeasured)
        if config.institutional_mode:
            error_msg = f"Data quality gate failed in institutional mode: {unmeasured}"
            bt_logger.error(error_msg)
            raise ValueError(error_msg)
        bt_logger.warning(f"{unmeasured}. Enable institutional_mode=True to enforce data quality requirements.")
        return
    if coverage_ratio < config.min_data_coverage:
        # Track as compliance violation regardless of institutional_mode
        state.compliance_violations.append(
            f"Data coverage below minimum threshold: {coverage_ratio:.2%} < {config.min_data_coverage:.2%} "
            f"({state.data_quality_tracker.successful_lookups}/{state.data_quality_tracker.total_price_lookups} "
            f"successful price lookups)"
        )

        if config.institutional_mode:
            error_msg = (
                f"Data quality gate failed in institutional mode: "
                f"coverage ratio {coverage_ratio:.2%} is below minimum threshold "
                f"{config.min_data_coverage:.2%}. "
                f"({state.data_quality_tracker.successful_lookups}/{state.data_quality_tracker.total_price_lookups} "
                f"successful price lookups)"
            )
            bt_logger.error(error_msg)
            raise ValueError(error_msg)
        else:
            # Not in institutional mode - log warning only
            bt_logger.warning(
                f"Data coverage below threshold: {coverage_ratio:.2%} < {config.min_data_coverage:.2%}. "
                f"({state.data_quality_tracker.successful_lookups}/{state.data_quality_tracker.total_price_lookups} "
                f"successful price lookups). "
                f"Enable institutional_mode=True to enforce data quality requirements."
            )
    elif config.institutional_mode:
        bt_logger.info(
            f"Data quality gate passed in institutional mode: "
            f"coverage ratio {coverage_ratio:.2%} >= {config.min_data_coverage:.2%}"
        )


_PERP_INTEGRITY_INTENT_TYPES = ("PERP_OPEN", "PERP_CLOSE")
_HEDGE_MANDATE_TAGS = frozenset({"hedge", "hedged", "hedged-lp", "delta-neutral"})


def _strategy_declares_hedge(strategy: BacktestableStrategy) -> bool:
    """Whether registration metadata explicitly declares a hedged mandate."""
    metadata = _strategy_metadata(strategy)
    tags = getattr(metadata, "tags", ()) if metadata is not None else ()
    normalized = {str(tag).strip().lower().replace("_", "-").replace(" ", "-") for tag in tags or ()}
    return bool(normalized & _HEDGE_MANDATE_TAGS)


def _hedge_integrity_violation(
    decision_summary: Mapping[str, Any],
    *,
    hedge_mandate: bool = False,
) -> str | None:
    """Report rejected perp decisions as compliance evidence.

    Every partial perp rejection compromises execution integrity. It is a
    *hedge* integrity failure only when strategy metadata explicitly declares
    that mandate; directional perp strategies must not be mislabeled.
    """
    by_type = decision_summary.get("execution_by_intent_type") or {}
    parts: list[str] = []
    for intent_type in _PERP_INTEGRITY_INTENT_TYPES:
        counts = by_type.get(intent_type) or {}
        rejected = int(counts.get("rejected", 0) or 0)
        if rejected <= 0:
            continue
        codes = sorted(
            {
                str(entry.get("rejection_code") or "UNCLASSIFIED")
                for entry in decision_summary.get("rejections") or ()
                if entry.get("intent_type") == intent_type
            }
        )
        parts.append(f"{rejected} {intent_type} rejected ({', '.join(codes) or 'UNCLASSIFIED'})")
    if not parts:
        return None
    prefix = "Hedge integrity" if hedge_mandate else "Perp execution integrity"
    consequence = (
        "the position was not hedged as decided"
        if hedge_mandate
        else "the executed position did not match the strategy's decided perp actions"
    )
    return (
        f"{prefix}: "
        + "; ".join(parts)
        + f" while the run completed — {consequence}, so the headline return does not describe "
        "the intended mandate."
    )


def _append_fallback_compliance_violations(
    fallback_usage: dict[str, int],
    compliance_violations: list[str],
) -> None:
    """Add one compliance-violation entry per known fallback category."""
    if fallback_usage.get("hardcoded_price", 0) > 0:
        count = fallback_usage["hardcoded_price"]
        compliance_violations.append(
            f"Hardcoded price fallback used {count} time(s). "
            "Set strict_reproducibility=True for institutional-grade backtests."
        )
    if fallback_usage.get("default_gas_price", 0) > 0:
        count = fallback_usage["default_gas_price"]
        compliance_violations.append(f"Default gas price fallback used {count} time(s).")
    if fallback_usage.get("default_usd_amount", 0) > 0:
        count = fallback_usage["default_usd_amount"]
        compliance_violations.append(
            f"Default USD amount fallback used {count} time(s). "
            "Set strict_reproducibility=True for institutional-grade backtests."
        )


def _log_decision_input_classification(
    *,
    state: BacktestState,
    decision_summary: dict[str, Any],
    decision_input_failures: list[dict[str, Any]],
    bt_logger: BacktestLogger,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """Log hollow/partially-starved attribution and return its inputs."""
    executed_fills = [trade for trade in state.portfolio.trades if trade.success]
    non_warm_up = [failure for failure in decision_input_failures if failure["pattern"] != "warm_up"]
    terminal_execution_count = sum(decision_summary["executions"].values())
    if decision_summary["intent_ticks"] > 0 and terminal_execution_count == 0:
        bt_logger.warning(
            f"HOLLOW BACKTEST: {decision_summary['intent_ticks']} intent(s) emitted but 0 reached a terminal "
            f"fill-or-rejection outcome; the execution ledger is incomplete and performance metrics are not trustworthy"
        )
    elif non_warm_up and not executed_fills:
        top = "; ".join(
            f"{failure['source']}:{failure['key']} ({failure['ticks']} ticks, {failure['pattern']})"
            for failure in non_warm_up[:3]
        )
        bt_logger.warning(
            f"HOLLOW BACKTEST: 0 executed fills, {state.no_intent_ticks}/{state.tick_count} no-intent ticks, "
            f"and {len(non_warm_up)} decision-input failure(s) — the strategy held because "
            f"inputs were missing, not because it chose to. Top: {top}"
        )
    elif executed_fills:
        starved = [failure for failure in non_warm_up if failure["pattern"] == "persistent"]
        if starved:
            top = "; ".join(
                f"{failure['source']}:{failure['key']} ({failure['ticks']}/{state.tick_count} ticks)"
                for failure in starved[:3]
            )
            bt_logger.warning(
                f"PARTIALLY STARVED BACKTEST: the run traded ({len(executed_fills)} fill(s)) but "
                f"{len(starved)} decision input(s) failed persistently — strategy branches gated on "
                f"them may never have run; the result is NOT a faithful test of the full strategy. "
                f"Starved: {top}"
            )
    return executed_fills, non_warm_up


def finalize_backtest_result(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    backtest_id: str,
    bt_logger: BacktestLogger,
    run_started_at: datetime,
    state: BacktestState,
    preflight_report: PreflightReport | None,
    preflight_passed: bool,
) -> BacktestResult:
    """Run metrics calculation + ``BacktestResult`` assembly on success.

    Mirrors the post-simulation success block:
    ``bt_logger.phase("metrics_calculation")``, final equity lookup,
    phase/error summary logging, fallback compliance violations, and
    ``BacktestResult`` construction (including
    ``data_coverage_metrics`` from the portfolio).
    """
    # Metrics calculation phase
    with bt_logger.phase("metrics_calculation"):
        metrics = backtester._calculate_metrics(state.portfolio, state.portfolio.trades, config)

        # Numeraire reporting projection (VIB-5127). No-op (None) for USD
        # strategies; raises here (after the loop) if the numeraire token was
        # unpriceable at any equity point. For token-quoted strategies the
        # projection is then folded into the metrics as the CANONICAL
        # performance expression (blueprint 31 §7): equity-derived fields move
        # to the numeraire series and USD PnL figures become numeraire amounts
        # converted at the end reference price.
        numeraire_symbol = state.portfolio._numeraire_symbol
        numeraire_metrics, initial_capital_numeraire, final_capital_numeraire = compute_numeraire_metrics(
            state.portfolio.equity_curve,
            numeraire_symbol=numeraire_symbol,
            trading_days_per_year=config.trading_days_per_year,
            risk_free_rate=config.risk_free_rate,
        )
        if numeraire_metrics is not None:
            merge_numeraire_canonical(
                metrics,
                numeraire_metrics,
                state.portfolio.equity_curve,
                state.portfolio.trades,
            )

        # Get final portfolio value
        final_value = (
            state.portfolio.equity_curve[-1].value_usd
            if state.portfolio.equity_curve
            else state.portfolio.initial_capital_usd
        )

    run_ended_at = datetime.now(UTC)

    # Log phase summary
    phase_summary = bt_logger.get_phase_summary()
    bt_logger.info(f"Phase timing summary - Total: {phase_summary['total_duration_seconds']:.2f}s")

    # Log error summary if any non-fatal errors occurred
    if backtester._error_handler and backtester._error_handler.error_count > 0:
        error_summary = backtester._error_handler.get_error_summary()
        bt_logger.info(
            f"Error summary: {error_summary['total_errors']} total "
            f"({error_summary['non_critical_errors']} non-critical, "
            f"{error_summary['recoverable_errors']} recoverable)"
        )

    # Get fallback usage and add compliance violations for any fallbacks used
    fallback_usage = backtester._fallback_usage.copy() if backtester._fallback_usage else {}
    _append_fallback_compliance_violations(fallback_usage, state.compliance_violations)

    # Decision telemetry aggregate (iteration_summary counterpart): one block
    # that answers "what did the strategy decide, and why did it hold" — the
    # per-tick records themselves ship as a separate artifact, not result.json.
    decision_summary = state.decision_log.summary(trades=state.portfolio.trades)

    # decide()-time data-failure report + hollow-run detection (ALM-2951,
    # ALM-3141).
    decision_input_failures = _decision_input_failure_report(state)
    # Attribution rules (each shape observed on real staging runs):
    # - executed fills, not the trades list — a rejections-only run is hollow
    #   too (rejected TradeRecords used to suppress the guard);
    # - warm-up-only failures don't count as "inputs were missing" (a 14-tick
    #   RSI warm-up is not why a 2161-tick run held);
    # - a run that DID trade but starved one input persistently gets its own
    #   warning (a dead strategy leg hides behind a busy one).
    executed_fills, _non_warm_up = _log_decision_input_classification(
        state=state,
        decision_summary=decision_summary,
        decision_input_failures=decision_input_failures,
        bt_logger=bt_logger,
    )

    # The verdict is derived before compliance so a run that did not evaluate
    # the strategy can never certify as institutionally compliant, and its
    # unpublishable reasons become the result error (hosted outcome FAILED).
    verdict = classify_run_validity(
        tick_count=state.tick_count,
        initial_capital_usd=state.portfolio.initial_capital_usd,
        decision_summary=decision_summary,
        decision_input_failures=decision_input_failures,
        executed_fills=len(executed_fills),
    )
    violations = terminal_errors(verdict)
    terminal_error = violations[0] if violations else None
    for violation in violations:
        state.compliance_violations.append(violation)
        bt_logger.error(violation)
    verdict_note = f" ({', '.join(verdict.reason_codes)})" if verdict.reasons else ""
    if verdict.passive_only:
        verdict_note += " — no executed fills; metrics are passive mark-to-market of funded assets"
    bt_logger.info(f"Run validity: {verdict.validity.value}{verdict_note}")
    hedge_integrity = _hedge_integrity_violation(
        decision_summary,
        hedge_mandate=_strategy_declares_hedge(strategy),
    )
    if hedge_integrity is not None:
        state.compliance_violations.append(hedge_integrity)
        bt_logger.warning(hedge_integrity)
    institutional_compliance = len(state.compliance_violations) == 0

    if terminal_error is None:
        bt_logger.info(
            f"Backtest completed for {strategy.deployment_id}: "
            f"PnL=${metrics.net_pnl_usd:,.2f}, "
            f"Return={metrics.total_return_pct:.2f}%, "
            f"Sharpe={metrics.sharpe_ratio:.3f}"
        )

    bt_logger.info(
        f"Decision summary: {decision_summary['ticks']} ticks — "
        f"{decision_summary['intent_ticks']} intent(s), {decision_summary['hold_ticks']} hold(s); "
        f"fills={decision_summary['executions']['fills']}, rejected={decision_summary['executions']['rejected']}"
    )
    for reason in decision_summary["hold_reasons"][:3]:
        bt_logger.info(
            f'  Hold reason [{reason["source"]}] "{reason["example"]}" — '
            f"{reason['ticks']} tick(s), first={reason['first_tick']}, last={reason['last_tick']}"
        )

    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id=strategy.deployment_id,
        start_time=config.start_time,
        end_time=config.end_time,
        metrics=metrics,
        trades=state.portfolio.trades,
        equity_curve=state.portfolio.equity_curve,
        initial_portfolio_value_usd=state.portfolio.initial_capital_usd,
        final_capital_usd=final_value,
        numeraire=numeraire_symbol,
        initial_capital_numeraire=initial_capital_numeraire,
        final_capital_numeraire=final_capital_numeraire,
        price_series=state.portfolio.price_series,
        price_series_display_labels=price_series_display_labels(state.portfolio.price_series),
        chain=config.chain,
        error=terminal_error,
        decision_input_failures=decision_input_failures or None,
        run_validity=verdict,
        resolved_timeframe=config.resolved_timeframe,
        run_started_at=run_started_at,
        run_ended_at=run_ended_at,
        run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
        config=config.to_dict_with_metadata(data_provider_info=backtester._get_data_provider_info()),
        backtest_id=backtest_id,
        phase_timings=[t.to_dict() for t in bt_logger.phase_timings],
        config_hash=config.calculate_config_hash(),
        errors=backtester._error_handler.get_errors_as_dicts() if backtester._error_handler else [],
        execution_delayed_at_end=state.execution_delayed_at_end,
        data_source_capabilities=state.data_source_capabilities,
        data_source_warnings=state.data_source_warnings,
        data_quality=state.data_quality_tracker.to_data_quality_report(),
        institutional_compliance=institutional_compliance,
        compliance_violations=state.compliance_violations,
        fallback_usage=fallback_usage,
        preflight_report=preflight_report,
        preflight_passed=preflight_passed,
        gas_prices_used=backtester._gas_price_records or [],
        gas_price_summary=backtester._create_gas_price_summary(state.portfolio.trades),
        parameter_sources=state.parameter_sources,
        data_coverage_metrics=state.portfolio.calculate_data_coverage_metrics(),
        data_manifest=state.data_broker.manifest.to_dict() if state.data_broker is not None else None,
        decision_summary=decision_summary,
        decision_events=state.decision_log.events(),
    )


# =============================================================================
# Token-flow helpers (Phase 6C.3)
# =============================================================================
#
# Each helper below owns a single ``IntentType`` branch previously inlined in
# ``PnLBacktester._calculate_token_flows``. They share a consistent shape:
#
#     * Accept the intent + scalar USD numbers + market state.
#     * Return a ``(tokens_in, tokens_out)`` tuple of
#       ``dict[str, Decimal]`` — empty dict for the side that does not flow.
#     * Preserve uppercase token normalization (via :func:`_normalize_token`),
#       ``price > 0`` guards, and the ``KeyError`` fallback that substitutes
#       the raw USD amount for tokens whose price is missing from
#       ``market_state`` — byte-for-byte identical to the pre-extraction body
#       (see characterization tests in
#       ``tests/unit/backtesting/pnl/test_engine_characterization.py``).
#
# LP helpers split the USD amount 50/50 and do NOT gate on ``price > 0``
# (matching the original behavior; a zero price would raise
# ``ZeroDivisionError`` exactly as it did before).
#
# Dispatch is performed by :func:`calculate_token_flows` through the
# :data:`_SIMPLE_FLOW_HANDLERS` mapping (plus an explicit SWAP branch, which
# is the only handler that consumes ``fee_usd`` / ``slippage_usd``).


def _market_state_chain(market_state: MarketState) -> str:
    return str(getattr(market_state, "chain", DEFAULT_CHAIN))


def _market_price_or_none(market_state: MarketState, token: Any) -> Decimal | None:
    """Positive market price for ``token``, or None when absent or non-positive.

    A zero/negative price is a data defect masquerading as a measurement
    (Empty != Zero) and is reported as absence, matching the PriceQuote
    construction contract in :mod:`almanak.framework.backtesting.pnl.money`.
    """
    try:
        price = market_state.get_price(token)
    except KeyError:
        return None
    if price is None or price <= 0:
        return None
    return price


def _flow_token_identity(
    token: Any,
    chain: str | None,
    token_addresses: Mapping[str, tuple[str, str]] | None,
) -> TokenIdentity:
    """Canonical :class:`TokenIdentity` for a (normalized) flow-lane token ref.

    Address-shaped refs keep their ``(chain, address)`` key; plain symbols
    resolve through the engine's registered ``{SYMBOL: (chain, address)}``
    map when available, otherwise stay symbol-keyed on the run's chain.
    ``UNRESOLVED`` is a display-only placeholder — identity hashing uses the
    address, never the symbol, when an address exists.
    """
    resolved_chain = (chain or DEFAULT_CHAIN).lower()
    if isinstance(token, tuple) and len(token) == 2:
        token_chain = str(token[0]).lower()
        raw_address = str(token[1])
        address = raw_address.lower()
        for symbol, entry in (token_addresses or {}).items():
            if is_token_key(entry) and normalize_token_key(str(entry[0]), str(entry[1])) == (token_chain, address):
                if is_address_like(address):
                    return TokenIdentity(chain=token_chain, address=address, symbol=symbol)
                # Non-EVM key (e.g. a base58 mint): TokenIdentity's address
                # slot is EVM-only — keep identity via the registered symbol.
                return TokenIdentity(chain=token_chain, address=None, symbol=symbol)
        if is_address_like(address):
            return TokenIdentity(chain=token_chain, address=address, symbol="UNRESOLVED")
        # Unregistered non-EVM key: the raw key doubles as the symbol so two
        # distinct mints never collapse to one identity.
        return TokenIdentity(chain=token_chain, address=None, symbol=raw_address)
    symbol = str(token).strip()
    if is_address_like(symbol):
        return TokenIdentity(chain=resolved_chain, address=symbol.lower(), symbol="UNRESOLVED")
    symbol = symbol.upper()
    registered = (token_addresses or {}).get(symbol)
    if registered is not None and is_token_key(registered):
        reg_chain, reg_address = str(registered[0]), str(registered[1])
        if is_address_like(reg_address):
            return TokenIdentity(chain=reg_chain, address=reg_address, symbol=symbol)
        return TokenIdentity(chain=reg_chain, address=None, symbol=symbol)
    return TokenIdentity(chain=resolved_chain, address=None, symbol=symbol)


def _typed_price_quote(
    identity: TokenIdentity,
    price: Decimal | None,
    context: str,
) -> PriceQuote | None:
    """Build the PriceQuote for a conversion site, or None when truly absent.

    A positive market price is used as-is (provenance ``market_state:...``).
    With no market price, cash-equivalent stables resolve on the deliberate
    $1 cash plane (#3318 doctrine — inside the sim those balances ARE
    ``cash_usd`` at face value); everything else is absence and the caller's
    to_usd/to_units conversion raises instead of guessing.
    """
    if price is not None and price > 0:
        return PriceQuote(token=identity, usd_per_unit=price, source=f"market_state:{context}")
    if identity.symbol in CASH_EQUIVALENT_STABLECOIN_SYMBOLS:
        return PriceQuote(token=identity, usd_per_unit=Decimal("1"), source=f"cash-equivalent-plane:{context}")
    return None


def typed_units_from_usd(
    token: Any,
    price: Decimal | None,
    amount_usd: Decimal,
    *,
    chain: str | None,
    token_addresses: Mapping[str, tuple[str, str]] | None,
    context: str,
) -> Decimal:
    """Convert a USD notional to token units through a typed PriceQuote (ALM-2943).

    A positive market ``price`` yields exactly the pre-migration
    ``amount_usd / price`` — this is a no-op on healthy data. An absent or
    non-positive price is *absence*: cash-equivalent stables stay on the $1
    cash plane (units == USD), and every other token raises
    :class:`PriceUnavailableError` instead of minting units at $1 or
    silently skipping the leg.
    """
    identity = _flow_token_identity(token, chain, token_addresses)
    quote = _typed_price_quote(identity, price, context)
    if quote is None:
        raise PriceUnavailableError(
            identity.display(),
            f"no market price in {context} — refusing to size token units from USD "
            "(a raw-USD-as-units fallback mints value)",
        )
    return UsdAmount(amount_usd).to_units(quote).units


def typed_usd_from_units(
    token: Any,
    price: Decimal | None,
    units: Decimal,
    *,
    chain: str | None,
    token_addresses: Mapping[str, tuple[str, str]] | None,
    context: str,
) -> Decimal:
    """Value token units in USD through a typed PriceQuote (ALM-2943).

    Mirror of :func:`typed_units_from_usd`: positive market prices convert
    exactly as before, cash-equivalent stables fall back to the $1 cash
    plane only when no market price exists, and any other absent price
    raises :class:`PriceUnavailableError` instead of valuing at $1.
    """
    identity = _flow_token_identity(token, chain, token_addresses)
    quote = _typed_price_quote(identity, price, context)
    return TokenUnits(token=identity, units=units).to_usd(quote).value


def _normalize_token(
    token: Any,
    chain: str | None = None,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> Any:
    """Canonicalize a token for address-native flow maps.

    Fully specified contract addresses become ``(chain, address)`` keys.
    Plain symbols resolve through the engine's registered
    ``{SYMBOL: (chain, address)}`` map when provided — the flow lane's
    analogue of the gas lane's registered-address retry (VIB-5508): an
    address-keyed market state keeps symbol reads an honest miss, and an
    unresolved symbol here prices the leg at the $1 fallback and books the
    flow under an unvalued symbol key (silent value minting). Symbols
    outside the map keep their legacy uppercase keys.
    """
    if isinstance(token, str):
        if is_address_like(token):
            return normalize_token_ref(token, chain)
        symbol = token.strip().upper()
        entry = (token_addresses or {}).get(symbol)
        if entry is not None and is_token_key(entry):
            return normalize_token_key(entry[0], entry[1])
        return symbol
    if isinstance(token, tuple) and len(token) == 2:
        token_chain, address = token
        return normalize_token_key(str(token_chain), str(address))
    return token


def _calculate_swap_flows(
    intent: Any,
    amount_usd: Decimal,
    fee_usd: Decimal,
    slippage_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """SWAP: one token leaves (``from_token``), another arrives (``to_token``).

    Outflow uses ``amount_usd`` at ``from_token`` price. Inflow uses
    ``amount_usd - fee_usd - slippage_usd`` at ``to_token`` price. USD↔unit
    conversion goes through a typed PriceQuote (ALM-2943): an absent price
    raises for non-cash tokens instead of booking USD as a unit count.
    """
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    chain = _market_state_chain(market_state)
    from_token = _normalize_token(getattr(intent, "from_token", "USDC"), chain, token_addresses)
    to_token = _normalize_token(getattr(intent, "to_token", "WETH"), chain, token_addresses)

    # Amount out is the trade amount
    tokens_out[from_token] = typed_units_from_usd(
        from_token,
        _market_price_or_none(market_state, from_token),
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="swap.from_leg",
    )

    # Amount in is after fees and slippage
    tokens_in[to_token] = typed_units_from_usd(
        to_token,
        _market_price_or_none(market_state, to_token),
        amount_usd - fee_usd - slippage_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="swap.to_leg",
    )

    return tokens_in, tokens_out


def _resolve_single_token(
    intent: Any,
    default: str,
    chain: str | None = None,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> Any:
    """Look up ``intent.token`` or ``intent.asset`` (first wins), normalized.

    Mirrors ``getattr(intent, "token", getattr(intent, "asset", default))``
    with canonicalization via :func:`_normalize_token`.
    """
    return _normalize_token(getattr(intent, "token", getattr(intent, "asset", default)), chain, token_addresses)


def _calculate_supply_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """SUPPLY: token leaves the wallet into the protocol."""
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    chain = _market_state_chain(market_state)
    token = _resolve_single_token(intent, "WETH", chain, token_addresses)

    tokens_out[token] = typed_units_from_usd(
        token,
        _market_price_or_none(market_state, token),
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="supply",
    )

    return tokens_in, tokens_out


def _calculate_withdraw_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """WITHDRAW: token arrives back from the protocol."""
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    chain = _market_state_chain(market_state)
    token = _resolve_single_token(intent, "WETH", chain, token_addresses)

    tokens_in[token] = typed_units_from_usd(
        token,
        _market_price_or_none(market_state, token),
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="withdraw",
    )

    return tokens_in, tokens_out


def _calculate_borrow_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """BORROW: borrowed token arrives in the wallet.

    BORROW vocabulary intents (``BorrowIntent``) name the received token as
    ``borrow_token`` -- it must win over the generic token/asset scan,
    which would otherwise credit the default symbol (VIB-5098).
    """
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    borrow_token = getattr(intent, "borrow_token", None)
    chain = _market_state_chain(market_state)
    if isinstance(borrow_token, str) and borrow_token:
        token: Any = _normalize_token(borrow_token, chain, token_addresses)
    else:
        token = _resolve_single_token(intent, "USDC", chain, token_addresses)

    tokens_in[token] = typed_units_from_usd(
        token,
        _market_price_or_none(market_state, token),
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="borrow",
    )

    return tokens_in, tokens_out


def _calculate_repay_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """REPAY: token leaves the wallet to pay down debt."""
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    chain = _market_state_chain(market_state)
    token = _resolve_single_token(intent, "USDC", chain, token_addresses)

    tokens_out[token] = typed_units_from_usd(
        token,
        _market_price_or_none(market_state, token),
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="repay",
    )

    return tokens_in, tokens_out


def _resolve_lp_tokens(
    intent: Any,
    chain: str | None = None,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[Any, Any]:
    """Resolve ``(token0, token1)`` for LP intents, uppercased if strings.

    A fully explicit pair (token0/token1, or the token_a/token_b aliases,
    via ``lp_explicit_pair``) wins. LP vocabulary intents (``LPOpenIntent``)
    declare the pair as a single ``pool`` string ("WETH/USDC") instead, so
    that is parsed next -- without it, token flows silently debited
    WETH/USDC for every pool. Unparseable pools (0x... addresses) keep the
    legacy WETH/USDC default. Mirrors ``get_intent_tokens`` so position
    tokens and token flows never diverge.
    """
    token0, token1 = lp_explicit_pair(intent)
    if token0 is not None and token1 is not None:
        return (
            _normalize_token(token0, chain, token_addresses),
            _normalize_token(token1, chain, token_addresses),
        )

    pool_pair = lp_pool_tokens(getattr(intent, "pool", None))
    if pool_pair is not None:
        return (
            _normalize_token(pool_pair[0], chain, token_addresses),
            _normalize_token(pool_pair[1], chain, token_addresses),
        )

    return _normalize_token(token0 if token0 is not None else "WETH", chain, token_addresses), _normalize_token(
        token1 if token1 is not None else "USDC", chain, token_addresses
    )


def _calculate_lp_open_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """LP_OPEN: both tokens leave the wallet, USD split 50/50."""
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    chain = _market_state_chain(market_state)
    token0, token1 = _resolve_lp_tokens(intent, chain, token_addresses)

    # Split the USD amount roughly 50/50
    half_amount = amount_usd / Decimal("2")

    tokens_out[token0] = typed_units_from_usd(
        token0,
        _market_price_or_none(market_state, token0),
        half_amount,
        chain=chain,
        token_addresses=token_addresses,
        context="lp_open.token0",
    )
    tokens_out[token1] = typed_units_from_usd(
        token1,
        _market_price_or_none(market_state, token1),
        half_amount,
        chain=chain,
        token_addresses=token_addresses,
        context="lp_open.token1",
    )

    return tokens_in, tokens_out


def _calculate_lp_close_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """LP_CLOSE: both tokens return to the wallet, USD split 50/50.

    Approximate tokens received (actual depends on impermanent loss).
    """
    tokens_in: dict[TokenRef, Decimal] = {}
    tokens_out: dict[TokenRef, Decimal] = {}

    chain = _market_state_chain(market_state)
    token0, token1 = _resolve_lp_tokens(intent, chain, token_addresses)

    # Approximate tokens received (actual depends on IL)
    half_amount = amount_usd / Decimal("2")

    tokens_in[token0] = typed_units_from_usd(
        token0,
        _market_price_or_none(market_state, token0),
        half_amount,
        chain=chain,
        token_addresses=token_addresses,
        context="lp_close.token0",
    )
    tokens_in[token1] = typed_units_from_usd(
        token1,
        _market_price_or_none(market_state, token1),
        half_amount,
        chain=chain,
        token_addresses=token_addresses,
        context="lp_close.token1",
    )

    return tokens_in, tokens_out


def _calculate_lp_collect_fees_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """LP_COLLECT_FEES: accrued fees return to the wallet, position stays open.

    ``amount_usd`` is the matched position's accrued-uncollected fee value
    (sized by ``_resolve_lp_collect_fees``); the payout mirrors the generic
    LP_CLOSE plane exactly — the USD value split 50/50 across the pair tokens
    at current prices, ONE plane. The per-token ``fees_token0``/``fees_token1``
    attribution units are deliberately NOT paid out: they are valued at the
    position's entry price, so crediting them at current prices would mint or
    burn value relative to the ``fees_earned`` USD the equity curve carried.
    """
    return _calculate_lp_close_flows(intent, amount_usd, market_state, token_addresses)


def resolve_native_wrap_pair(chain: str | None) -> tuple[str, str] | None:
    """Return ``(native_symbol, wrapped_symbol)`` for ``chain``, or None.

    The repo's canonical native↔wrapped mapping is the chain registry's
    native descriptor (the same source the gas lane's price ladder and the
    OHLCV wrapped-proxy use). ``None`` means the chain has no registered
    wrapped-native mapping — WRAP_NATIVE/UNWRAP_NATIVE must refuse, not guess.
    """
    if not chain:
        return None
    descriptor = ChainRegistry.try_resolve(str(chain))
    if descriptor is None or not descriptor.native.wrapped_symbol:
        return None
    return descriptor.native.symbol.upper(), descriptor.native.wrapped_symbol.upper()


def _wrap_conversion_legs(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None,
) -> tuple[Any, Any, Decimal] | None:
    """Shared WRAP/UNWRAP leg resolution: ``(native, wrapped, units)``.

    Both legs are sized from ONE price so the conversion is exactly 1:1 in
    token units. The wrapped side prices first (it is the registered ERC-20;
    the native symbol usually aliases to it), then the native symbol; with
    neither priced the conversion raises :class:`PriceUnavailableError` via
    the typed plane, like every other flow helper (ALM-2943 — a raw-USD-as-
    units fallback would mis-size the wrap and the strict-balance check).
    Returns None when the chain has no registered native↔wrapped mapping
    (the resolution lane has already rejected the fill in that case; flows
    fall through to no-op).
    """
    chain = str(getattr(intent, "chain", None) or _market_state_chain(market_state))
    pair = resolve_native_wrap_pair(chain)
    if pair is None:
        return None
    native_symbol, wrapped_symbol = pair
    declared = getattr(intent, "token", None)
    if isinstance(declared, str) and declared.strip():
        wrapped_symbol = declared.strip().upper()
    wrapped = _normalize_token(wrapped_symbol, chain, token_addresses)
    # Natives are not ERC-20s: the native side keeps its plain symbol key.
    native = native_symbol

    price: Decimal | None = None
    priced_leg: Any = wrapped
    for leg in (wrapped, native):
        try:
            candidate = market_state.get_price(leg)
        except KeyError:
            continue
        if candidate > 0:
            price, priced_leg = candidate, leg
            break
    units = typed_units_from_usd(
        priced_leg,
        price,
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="wrap_native.conversion",
    )
    return native, wrapped, units


def _calculate_wrap_native_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """WRAP_NATIVE: native units leave the wallet, wrapped units arrive 1:1."""
    legs = _wrap_conversion_legs(intent, amount_usd, market_state, token_addresses)
    if legs is None:
        return {}, {}
    native, wrapped, units = legs
    return {wrapped: units}, {native: units}


def _calculate_unwrap_native_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """UNWRAP_NATIVE: wrapped units leave the wallet, native units arrive 1:1."""
    legs = _wrap_conversion_legs(intent, amount_usd, market_state, token_addresses)
    if legs is None:
        return {}, {}
    native, wrapped, units = legs
    return {native: units}, {wrapped: units}


def _resolve_vault_token(
    intent: Any,
    chain: str | None = None,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> Any:
    """Resolve ``intent.deposit_token`` for vault intents, warning on fallback."""
    token = getattr(intent, "deposit_token", None)
    if not token:
        token = "USDC"
        logger.warning(
            "Vault intent missing deposit_token, defaulting to USDC — set deposit_token for accurate backtesting"
        )
    return _normalize_token(token, chain, token_addresses)


def _calculate_vault_token_amount(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[TokenRef, Decimal]:
    """Resolve vault token and convert ``amount_usd`` to token units.

    Mirrors the shared preamble of ``_calculate_vault_deposit_flows`` and
    ``_calculate_vault_redeem_flows``: the only per-branch difference is
    whether the resulting amount lands in ``tokens_in`` or ``tokens_out``.
    """
    chain = _market_state_chain(market_state)
    token = _resolve_vault_token(intent, chain, token_addresses)

    amount = typed_units_from_usd(
        token,
        _market_price_or_none(market_state, token),
        amount_usd,
        chain=chain,
        token_addresses=token_addresses,
        context="vault",
    )

    return token, amount


def _calculate_vault_deposit_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """VAULT_DEPOSIT: deposit token flows out of the wallet into the vault."""
    token, amount = _calculate_vault_token_amount(intent, amount_usd, market_state, token_addresses)
    return {}, {token: amount}


def _calculate_vault_redeem_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """VAULT_REDEEM: deposit token flows back from the vault into the wallet."""
    token, amount = _calculate_vault_token_amount(intent, amount_usd, market_state, token_addresses)
    return {token: amount}, {}


def _calculate_perp_open_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """PERP_OPEN: the declared collateral token leaves the wallet.

    ``amount_usd`` is the position notional, not the collateral leg.  The
    authoring surface declares collateral in token units, so preserve those
    units exactly instead of deriving them from the notional.  The shared
    sizing owner resolves ``"all"`` before this helper is reached.

    Legacy duck-typed perp intents without explicit collateral fields keep
    their historical cash-backed accounting and therefore emit no flow.
    """
    del amount_usd
    token = getattr(intent, "collateral_token", None)
    raw_amount = getattr(intent, "collateral_amount", None)
    if not token or raw_amount is None or str(raw_amount).lower() == "all":
        return {}, {}
    try:
        amount = Decimal(str(raw_amount))
    except (ArithmeticError, TypeError, ValueError):
        return {}, {}
    if not amount.is_finite() or amount <= Decimal("0"):
        return {}, {}
    chain = str(getattr(intent, "chain", None) or _market_state_chain(market_state))
    return {}, {_normalize_token(token, chain, token_addresses): amount}


# Dispatch table used by :func:`calculate_token_flows`. The SWAP handler has
# a distinct signature (it consumes fee/slippage); every other handler takes
# the same ``(intent, amount_usd, market_state)`` shape and is invoked
# through :data:`_SIMPLE_FLOW_HANDLERS`.
#
# Using a module-level mapping (rather than an ``if/elif`` chain) makes the
# sequencer constant-time in the number of intent types and makes it
# mechanically clear which intent types are covered by a dedicated helper
# versus falling through to the collateral-based no-flow default.
_SIMPLE_FLOW_HANDLERS: dict[IntentType, object] = {
    IntentType.SUPPLY: _calculate_supply_flows,
    IntentType.WITHDRAW: _calculate_withdraw_flows,
    IntentType.BORROW: _calculate_borrow_flows,
    IntentType.REPAY: _calculate_repay_flows,
    # DELEVERAGE is structurally a REPAY (lending_intents.DeleverageIntent):
    # same outflow shape, same close resolution — only the recorded
    # intent_type differs so accounting can tell forced unwinds apart.
    IntentType.DELEVERAGE: _calculate_repay_flows,
    IntentType.LP_OPEN: _calculate_lp_open_flows,
    IntentType.LP_CLOSE: _calculate_lp_close_flows,
    IntentType.LP_COLLECT_FEES: _calculate_lp_collect_fees_flows,
    IntentType.WRAP_NATIVE: _calculate_wrap_native_flows,
    IntentType.UNWRAP_NATIVE: _calculate_unwrap_native_flows,
    IntentType.VAULT_DEPOSIT: _calculate_vault_deposit_flows,
    IntentType.VAULT_REDEEM: _calculate_vault_redeem_flows,
    IntentType.PERP_OPEN: _calculate_perp_open_flows,
}


def calculate_token_flows(
    intent: Any,
    intent_type: IntentType,
    amount_usd: Decimal,
    fee_usd: Decimal,
    slippage_usd: Decimal,
    market_state: MarketState,
    token_addresses: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
    """Dispatch ``intent_type`` to the matching per-intent-type flow helper.

    Returns ``({}, {})`` for any intent type not covered by a dedicated
    helper (HOLD, PERP_CLOSE, and future non-flow types).  Explicit
    PERP_OPEN collateral is a wallet outflow; legacy perp intents without a
    declared collateral token/amount retain their cash-backed fallback.
    """
    # SWAP is the only branch that consumes fee / slippage.
    if intent_type == IntentType.SWAP:
        return _calculate_swap_flows(intent, amount_usd, fee_usd, slippage_usd, market_state, token_addresses)

    handler = _SIMPLE_FLOW_HANDLERS.get(intent_type)
    if handler is not None:
        return handler(intent, amount_usd, market_state, token_addresses)  # type: ignore[operator]

    # HOLD, PERP_CLOSE, and other non-flow intent types reach this fallback.
    return {}, {}
