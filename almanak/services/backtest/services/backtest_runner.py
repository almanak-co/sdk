"""Backtest runner — wraps PnLBacktester for HTTP use.

Translates StrategySpec (HTTP input) into the internal BacktestableStrategy +
PnLBacktestConfig, runs the backtest, and reports progress to JobManager.

The runner wires the **full** backtesting engine:
- Protocol-specific fee models (Uniswap V3, Aave V3, GMX, etc.)
- Strategy-type adapters (LP fee accrual, lending interest, perp funding)
- BacktestDataConfig for historical volume / APY / funding rate data
- Strategy metadata so the engine auto-detects LP / lending / perp / swap
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.models import BacktestResult, _decimal_str
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider
from almanak.framework.data.tokens import get_token_resolver
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    HoldIntent,
    LPOpenIntent,
    SupplyIntent,
    SwapIntent,
)
from almanak.framework.market import MarketSnapshot
from almanak.services.backtest.models import (
    BacktestRequest,
    QuickBacktestRequest,
    StrategySpec,
    TimeframeSpec,
)
from almanak.services.backtest.services.job_manager import JobManager

logger = logging.getLogger(__name__)

# Sentinel wallet address for backtesting (not used on-chain)
_BACKTEST_WALLET = "0x0000000000000000000000000000000000000000"

TokenAddressMap = dict[str, tuple[str, str]]

_TOKEN_REF_KEYS = frozenset(
    {
        "from_token",
        "to_token",
        "base_token",
        "quote_token",
        "token0",
        "token1",
        "token",
        "collateral_token",
        "borrow_token",
    }
)

_TOKEN_ADDRESS_KEYS = frozenset(
    {
        "from_token_address",
        "to_token_address",
        "base_token_address",
        "quote_token_address",
        "token0_address",
        "token1_address",
        "collateral_token_address",
        "borrow_token_address",
    }
)

# ---------------------------------------------------------------------------
# Supported actions and their required/optional parameters
# ---------------------------------------------------------------------------
SUPPORTED_ACTIONS = {
    "swap": {"required": [], "optional": ["from_token", "to_token", "amount_usd", "max_slippage"]},
    "provide_liquidity": {
        "required": ["pool"],
        "optional": ["amount0", "amount1", "range_lower", "range_upper"],
    },
    "lend": {"required": [], "optional": ["token", "amount"]},
    "supply": {"required": [], "optional": ["token", "amount"]},
    "borrow": {
        "required": [],
        "optional": ["collateral_token", "collateral_amount", "borrow_token", "borrow_amount"],
    },
}


def _string_token_values(value: Any) -> list[str]:
    """Return non-empty token strings from a scalar/list-ish config value."""
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, bytes | bytearray | str | Mapping):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def _mapping_token_refs(mapping: Mapping[str, Any] | None) -> list[str]:
    """Extract symbol/address token references from strategy config-like mappings."""
    if mapping is None:
        return []

    refs: list[str] = []

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            refs.extend(_string_token_values(value.get("tokens")))
            for key, nested in value.items():
                if isinstance(key, str) and (
                    key in _TOKEN_REF_KEYS or key in _TOKEN_ADDRESS_KEYS or key.endswith("_token_address")
                ):
                    refs.extend(_string_token_values(nested))
                visit(nested)
            return
        if isinstance(value, Iterable) and not isinstance(value, bytes | bytearray | str):
            for item in value:
                if isinstance(item, Mapping) or (
                    isinstance(item, Iterable) and not isinstance(item, bytes | bytearray | str)
                ):
                    visit(item)

    visit(mapping)
    return refs


def _quote_asset_refs(raw_quote_asset: Any, chain: str) -> list[str]:
    if raw_quote_asset is None:
        return []
    try:
        quote_asset = QuoteAsset.parse(raw_quote_asset)
    except (TypeError, ValueError):
        logger.debug("Ignoring invalid quote_asset while building backtest token refs", exc_info=True)
        return []
    if quote_asset.is_usd or quote_asset.address is None:
        return []

    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is not None and quote_asset.chain_id != descriptor.chain_id:
        return []
    return [quote_asset.address]


def _strategy_quote_asset_refs(strategy: object | None, chain: str) -> list[str]:
    if strategy is None:
        return []
    refs = _quote_asset_refs(getattr(strategy, "quote_asset", None), chain)
    metadata = getattr(strategy.__class__, "STRATEGY_METADATA", None)
    return refs + _quote_asset_refs(getattr(metadata, "quote_asset", None), chain)


def _strategy_class_quote_asset_refs(strategy_class: type[Any] | None, chain: str) -> list[str]:
    if strategy_class is None:
        return []
    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    return _quote_asset_refs(getattr(metadata, "quote_asset", None), chain)


def collect_backtest_token_refs(
    *,
    chain: str,
    strategy_config: Mapping[str, Any] | None = None,
    strategy: object | None = None,
    strategy_class: type[Any] | None = None,
    extra_refs: Iterable[str] | None = None,
) -> list[str]:
    """Collect token refs that may need historical price coverage.

    Phase 0 keeps the existing symbol-keyed engine path, but platform-triggered
    runs can supply address-native strategy config fields. Gather both symbol
    refs and address refs here so callers can normalize display symbols for
    ``config.tokens`` and build the existing provider ``token_addresses`` map.
    """
    refs: list[str] = []
    refs.extend(_mapping_token_refs(strategy_config))
    if strategy_config is not None:
        refs.extend(_quote_asset_refs(strategy_config.get("quote_asset"), chain))

    spec = getattr(strategy, "_spec", None)
    refs.extend(_mapping_token_refs(getattr(spec, "parameters", None)))
    refs.extend(_strategy_quote_asset_refs(strategy, chain))
    refs.extend(_strategy_class_quote_asset_refs(strategy_class, chain))

    if extra_refs is not None:
        refs.extend(_string_token_values(list(extra_refs)))

    return refs


def _token_funding_refs(raw_funding: Any) -> list[str]:
    """Extract token refs from a token_funding basket without resolving them."""
    if not isinstance(raw_funding, Iterable) or isinstance(raw_funding, bytes | bytearray | str | Mapping):
        return []

    refs: list[str] = []
    for entry in raw_funding:
        if not isinstance(entry, Mapping):
            continue
        refs.extend(_string_token_values(entry.get("address")))
        refs.extend(_string_token_values(entry.get("symbol")))
    return refs


def _resolve_backtest_token(ref: str, chain: str) -> ResolvedToken | None:
    try:
        return get_token_resolver().resolve(ref, chain, log_errors=False, skip_gateway=True)
    except TokenResolutionError:
        logger.debug("No static token metadata for backtest token ref %s on %s", ref, chain)
        return None


def _display_symbol_for_ref(ref: str, chain: str) -> str:
    resolved = _resolve_backtest_token(ref, chain)
    if resolved is not None:
        return resolved.symbol.upper()
    cleaned = ref.strip()
    return cleaned.lower() if cleaned.lower().startswith("0x") else cleaned


def normalize_backtest_token_refs(token_refs: Iterable[str], chain: str) -> list[str]:
    """Normalize token refs into the symbol-keyed token list expected today."""
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in token_refs:
        if not isinstance(raw, str) or not raw.strip():
            continue
        display = _display_symbol_for_ref(raw.strip(), chain)
        if display not in seen:
            seen.add(display)
            normalized.append(display)
    return normalized


def _should_register_provider_address(token: ResolvedToken) -> bool:
    return not token.is_native


def build_backtest_token_address_map(
    config: PnLBacktestConfig,
    *,
    strategy: object | None = None,
    strategy_config: Mapping[str, Any] | None = None,
    extra_refs: Iterable[str] | None = None,
) -> TokenAddressMap:
    """Build the provider/engine ``SYMBOL -> (chain, address)`` map.

    This mirrors the CLI's Phase-0 bridge shape while deriving coverage from
    both display tokens and address-native strategy config fields. Native
    assets are intentionally skipped because CoinGecko resolves those through
    the chain registry fast path; wrapped-native aliases stay registered for
    engine symbol/address parity.
    """
    refs = list(config.tokens)
    refs.extend(
        collect_backtest_token_refs(
            chain=config.chain,
            strategy_config=strategy_config,
            strategy=strategy,
            extra_refs=extra_refs,
        )
    )

    token_addresses: TokenAddressMap = {}
    for raw in refs:
        if not isinstance(raw, str) or not raw.strip():
            continue
        resolved = _resolve_backtest_token(raw.strip(), config.chain)
        if resolved is None or not _should_register_provider_address(resolved):
            continue
        token_addresses[resolved.symbol.upper()] = (config.chain, resolved.address)
    return token_addresses


class SpecBacktestStrategy:
    """Adapter: converts a StrategySpec into a BacktestableStrategy.

    Translates the HTTP-provided StrategySpec into the appropriate Intent
    type based on the ``action`` field. The PnLBacktester calls
    ``decide(market)`` on each tick and simulates execution + fees.

    Exposes ``get_metadata()`` so the engine's adapter registry auto-detects
    the strategy type (LP / lending / perp / swap) and loads the right
    adapter for position-level simulation (fee accrual, interest, funding).

    Supported actions:
        swap              - SwapIntent  (token exchange)
        provide_liquidity - LPOpenIntent (concentrated LP position)
        lend / supply     - SupplyIntent (lending deposit)
        borrow            - BorrowIntent (collateralized borrow)
    """

    # Maps action → (tags, intent_types) for adapter detection
    _ACTION_METADATA: dict[str, tuple[list[str], list[str]]] = {
        "swap": (["swap", "trading"], ["SWAP"]),
        "provide_liquidity": (["lp", "liquidity", "concentrated-liquidity"], ["LP_OPEN", "LP_CLOSE"]),
        "lend": (["lending", "supply"], ["SUPPLY", "WITHDRAW"]),
        "supply": (["lending", "supply"], ["SUPPLY", "WITHDRAW"]),
        "borrow": (["lending", "borrow"], ["BORROW", "REPAY"]),
    }

    def __init__(self, spec: StrategySpec) -> None:
        self._spec = spec
        self._deployment_id = f"spec_{spec.protocol}_{spec.action}_{spec.chain}"
        self._tick_count = 0
        self._metadata = self._build_metadata(spec)

    def _build_metadata(self, spec: StrategySpec) -> Any:
        """Build StrategyMetadata from the spec for adapter auto-detection."""
        from almanak.framework.strategies.intent_strategy import StrategyMetadata

        action = spec.action.lower()
        if action not in self._ACTION_METADATA:
            raise ValueError(f"Unknown action '{action}'. Supported actions: {', '.join(self._ACTION_METADATA)}")
        tags, intent_types = self._ACTION_METADATA[action]

        return StrategyMetadata(
            name=self._deployment_id,
            description=f"{spec.action} on {spec.protocol} ({spec.chain})",
            tags=tags,
            supported_chains=[spec.chain],
            supported_protocols=[spec.protocol.replace("-", "_")],
            intent_types=intent_types,
        )

    def get_metadata(self) -> Any:
        """Return strategy metadata for adapter auto-detection."""
        return self._metadata

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    # ------------------------------------------------------------------
    # decide() — called by PnLBacktester on every tick
    # ------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Any:
        """Generate an intent from the StrategySpec parameters.

        The first tick opens the position (swap / LP / supply / borrow).
        Subsequent ticks hold — the PnL backtester marks the position
        to market each tick automatically.
        """
        self._tick_count += 1
        params = self._spec.parameters
        action = self._spec.action.lower()

        # Only execute on the first tick — afterwards we hold and let
        # the backtester simulate PnL via fee/slippage models.
        if self._tick_count > 1:
            return HoldIntent(reason=f"Holding {action} position")

        if action == "swap":
            return self._build_swap_intent(params)
        if action == "provide_liquidity":
            return self._build_lp_intent(params)
        if action in ("lend", "supply"):
            return self._build_supply_intent(params)
        if action == "borrow":
            return self._build_borrow_intent(params)

        # Unknown action — hold and let fee model simulate
        logger.warning("Unknown action '%s', falling back to HoldIntent", action)
        return HoldIntent(reason=f"Unsupported action: {action}")

    # ------------------------------------------------------------------
    # Intent builders
    # ------------------------------------------------------------------

    def _build_swap_intent(self, params: dict[str, Any]) -> SwapIntent:
        from_token = params.get("from_token", "USDC")
        to_token = params.get("to_token", "WETH")
        amount_usd = Decimal(str(params.get("amount_usd", "1000")))
        max_slippage = Decimal(str(params.get("max_slippage", "0.005")))
        return SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount_usd=amount_usd,
            max_slippage=max_slippage,
            protocol=self._spec.protocol,
            chain=self._spec.chain,
        )

    def _build_lp_intent(self, params: dict[str, Any]) -> LPOpenIntent:
        pool = params.get("pool", "")
        if not pool:
            # Construct a reasonable pool identifier from tokens
            token0 = params.get("token0", params.get("from_token", "WETH"))
            token1 = params.get("token1", params.get("to_token", "USDC"))
            pool = f"{token0}/{token1}"

        amount0 = Decimal(str(params.get("amount0", "1")))
        amount1 = Decimal(str(params.get("amount1", "1000")))
        range_lower = Decimal(str(params.get("range_lower", "1500")))
        range_upper = Decimal(str(params.get("range_upper", "2500")))

        return LPOpenIntent(
            pool=pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol=self._spec.protocol,
            chain=self._spec.chain,
        )

    def _build_supply_intent(self, params: dict[str, Any]) -> SupplyIntent:
        token = params.get("token", params.get("from_token", "USDC"))
        amount = Decimal(str(params.get("amount", params.get("amount_usd", "10000"))))
        return SupplyIntent(
            protocol=self._spec.protocol,
            token=token,
            amount=amount,
            chain=self._spec.chain,
        )

    def _build_borrow_intent(self, params: dict[str, Any]) -> BorrowIntent:
        collateral_token = params.get("collateral_token", "WETH")
        borrow_token = params.get("borrow_token", "USDC")
        borrow_amount = Decimal(str(params.get("borrow_amount", "1000")))
        # Standalone borrow (collateral_amount=0): a bundled
        # Intent.borrow(collateral_amount>0) is rejected because accounting
        # writes one event per intent and would drop the SUPPLY event + supply
        # cost-basis lot. The "borrow" backtest action models the borrow leg
        # only; collateral_token is retained as metadata. A collateral-supplying
        # backtest should use the "lend"/"supply" action.
        return BorrowIntent(
            protocol=self._spec.protocol,
            collateral_token=collateral_token,
            collateral_amount=Decimal("0"),
            borrow_token=borrow_token,
            borrow_amount=borrow_amount,
            chain=self._spec.chain,
        )


def create_backtester(
    token_addresses: TokenAddressMap | None = None,
    *,
    close_providers_on_finish: bool = True,
    data_config_overrides: Mapping[str, Any] | None = None,
) -> PnLBacktester:
    """Create a PnLBacktester wired with the full engine capabilities.

    Wires:
    - Default fee/slippage models for the generic execution path
    - ``strategy_type="auto"`` so the engine auto-detects LP/lending/perp/swap
      from strategy metadata and loads the right adapter (LPBacktestAdapter,
      LendingBacktestAdapter, PerpBacktestAdapter)
    - ``BacktestDataConfig`` enabling historical volume, APY, and funding rate
      data for adapter-level simulation (LP fee accrual, lending interest,
      perp funding). Falls back to configurable defaults when historical
      data is unavailable (non-strict mode).

    The adapters handle protocol-specific fee calculation internally. The
    ``fee_models`` dict provides the fallback for the generic execution
    path (intents that don't match a registered adapter).

    The CoinGecko provider works standalone (no gateway required). It reads
    COINGECKO_API_KEY from the environment for the pro tier; falls back to the
    free tier when the key is absent.

    ``close_providers_on_finish`` is forwarded to ``PnLBacktester``. The
    default keeps the single-run contract (the engine closes the provider when
    ``backtest()`` finishes); orchestrators that reuse one backtester across
    sequential runs (e.g. the platform sweep runner) pass False and own the
    provider lifetime themselves (VIB-5621).
    """
    data_provider = CoinGeckoDataProvider(token_addresses=token_addresses)

    # Default models for the generic execution path. Protocol-specific fee
    # calculation happens inside the adapters (LP, lending, perp).
    fee_models: dict[str, Any] = {"default": DefaultFeeModel()}
    slippage_models: dict[str, Any] = {"default": DefaultSlippageModel()}

    # Enable historical data sources for adapters:
    # - LP adapter: historical pool volume for fee accrual
    # - Lending adapter: historical APY for interest accrual
    # - Perp adapter: historical funding rates
    # Non-strict mode uses fallback values when historical data is unavailable.
    data_config_kwargs: dict[str, Any] = {
        "use_historical_volume": True,
        "use_historical_funding": True,
        "use_historical_apy": True,
        "use_historical_liquidity": True,
        "strict_historical_mode": False,
    }
    if data_config_overrides:
        # Callers (platform BACKTEST_CONFIG, ALM-2930 #6) may tune the LP
        # data lanes: allow_volume_fallback, explicit_pool_volume_usd_daily,
        # explicit_pool_liquidity_usd, strict_historical_mode, ...
        fields_by_name = {f.name: f for f in dataclasses.fields(BacktestDataConfig)}
        unknown = sorted(set(data_config_overrides) - set(fields_by_name))
        if unknown:
            raise ValueError(f"Unknown BacktestDataConfig overrides: {', '.join(unknown)}")
        for key, value in data_config_overrides.items():
            # JSON delivers numbers as float/str; Decimal-typed fields must be
            # coerced or downstream Decimal arithmetic fails mid-backtest.
            if value is not None and "Decimal" in str(fields_by_name[key].type) and not isinstance(value, Decimal):
                try:
                    value = Decimal(str(value))
                except (InvalidOperation, ValueError) as exc:
                    raise ValueError(f"BacktestDataConfig override {key!r} is not a valid number: {value!r}") from exc
            data_config_kwargs[key] = value
    data_config = BacktestDataConfig(**data_config_kwargs)

    return PnLBacktester(
        data_provider=data_provider,
        fee_models=fee_models,
        slippage_models=slippage_models,
        strategy_type="auto",
        data_config=data_config,
        token_addresses=token_addresses,
        close_providers_on_finish=close_providers_on_finish,
    )


def _extract_tokens(spec: StrategySpec) -> list[str]:
    """Extract the relevant token list from a StrategySpec.

    Different actions reference tokens via different parameter names.
    This helper normalises them into a de-duplicated list.
    """
    params = spec.parameters
    action = spec.action.lower()

    # Explicit tokens list always wins
    if "tokens" in params:
        tokens = params["tokens"]
        return [tokens] if isinstance(tokens, str) else list(tokens)

    token_set: list[str] = []

    if action == "swap":
        token_set = [params.get("from_token", "USDC"), params.get("to_token", "WETH")]
    elif action == "provide_liquidity":
        token_set = [
            params.get("token0", params.get("from_token", "WETH")),
            params.get("token1", params.get("to_token", "USDC")),
        ]
    elif action in ("lend", "supply"):
        token_set = [params.get("token", params.get("from_token", "USDC"))]
    elif action == "borrow":
        token_set = [
            params.get("collateral_token", "WETH"),
            params.get("borrow_token", "USDC"),
        ]
    else:
        token_set = ["WETH", "USDC"]

    # De-duplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in token_set:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


def load_named_strategy(strategy_name: str, chain: str, config: dict | None = None) -> Any:
    """Load a registered strategy by name and instantiate it.

    Tries the same flexible initialization as the CLI:
    1. IntentStrategy signature: (config, chain, wallet_address)
    2. Simple config: (config,)
    3. No-arg constructor
    """
    from almanak.framework.strategies import get_strategy

    strategy_class = get_strategy(strategy_name)
    if strategy_class is None:
        raise ValueError(f"Strategy '{strategy_name}' not found in registry")

    strategy_config = config or {}

    # Try IntentStrategy signature first
    try:
        return strategy_class(strategy_config, chain, _BACKTEST_WALLET)
    except TypeError:
        pass

    # Try simple config
    try:
        return strategy_class(strategy_config)
    except TypeError:
        pass

    # No-arg
    return strategy_class()


def list_available_strategies() -> list[str]:
    """Return names of all registered strategies."""
    from almanak.framework.strategies import list_strategies

    return list_strategies()


#: Sub-hourly candles exist on the price source only near real time
#: (CoinGecko serves 5-minutely data for roughly the trailing day; any
#: earlier window comes back hourly regardless of its length — verified
#: empirically: a 1-day window ending yesterday measured 3604s resolution).
_SUB_HOURLY_MAX_WINDOW_SECONDS = 86_400
_SUB_HOURLY_MAX_END_AGE_SECONDS = 6 * 3_600
_DEFAULT_INTERVAL_SECONDS = 3_600
_MIN_INTERVAL_SECONDS = 300


def _derive_interval_seconds(
    strategy_timeframe: Any,
    window_seconds: float,
    *,
    quick: bool,
    end_age_seconds: float = 0.0,
) -> int:
    """Tick interval derived from the strategy's declared timeframe.

    The strategy author writes ``timeframe: "15m"`` and nothing else — the
    tick choice is the system's, so the system must follow the declaration
    where the data source honestly can, and say so in one sentence when it
    cannot (instead of refusing the author's timeframe tick-by-tick over a
    grid it never chose).
    """
    if quick:
        return 86_400
    if not strategy_timeframe:
        return _DEFAULT_INTERVAL_SECONDS
    from almanak.framework.backtesting.pnl.indicator_engine import _timeframe_seconds

    try:
        declared = int(_timeframe_seconds(str(strategy_timeframe)))
    except ValueError:
        logger.warning("Unrecognized strategy timeframe %r; ticking hourly", strategy_timeframe)
        return _DEFAULT_INTERVAL_SECONDS
    if declared >= _DEFAULT_INTERVAL_SECONDS:
        # Coarser-than-hourly candles derive cleanly from hourly ticks;
        # keeping the finer grid preserves fill timing.
        return _DEFAULT_INTERVAL_SECONDS
    if (
        window_seconds <= _SUB_HOURLY_MAX_WINDOW_SECONDS
        and end_age_seconds <= _SUB_HOURLY_MAX_END_AGE_SECONDS
        and declared >= _MIN_INTERVAL_SECONDS
    ):
        return declared
    logger.warning(
        "Strategy timeframe %s is finer than the price source serves for this window "
        "(sub-hourly data exists only for the trailing ~day): ticking hourly, and %s candle "
        "reads will refuse. Run a <=1-day window ending near now for sub-hourly candles, or "
        "use a 1h+ timeframe.",
        strategy_timeframe,
        strategy_timeframe,
    )
    return _DEFAULT_INTERVAL_SECONDS


def build_backtest_config(
    spec: StrategySpec | None,
    timeframe: TimeframeSpec,
    *,
    quick: bool = False,
    chain: str | None = None,
    token_funding: list[dict[str, Any]] | None = None,
    tokens: list[str] | None = None,
) -> PnLBacktestConfig:
    """Build PnLBacktestConfig from HTTP request parameters.

    Works with both StrategySpec (declarative) and named strategies.
    When using a named strategy, chain must be provided explicitly.
    """
    start = datetime(timeframe.start.year, timeframe.start.month, timeframe.start.day, tzinfo=UTC)
    end = datetime(timeframe.end.year, timeframe.end.month, timeframe.end.day, tzinfo=UTC)

    if spec is not None:
        params = spec.parameters
        resolved_token_funding = token_funding if token_funding is not None else params.get("token_funding")
        if not resolved_token_funding:
            raise ValueError("token_funding is required for PnL backtests")
        resolved_chain = chain or spec.chain
        raw_tokens = list(tokens or _extract_tokens(spec))
        funding_params = {**params, "token_funding": resolved_token_funding}
        raw_tokens.extend(collect_backtest_token_refs(chain=resolved_chain, strategy_config=funding_params))
        raw_tokens.extend(_token_funding_refs(resolved_token_funding))
        resolved_tokens = normalize_backtest_token_refs(raw_tokens, resolved_chain)
        fee_model = spec.protocol.replace("-", "_")
    else:
        if not chain:
            raise ValueError("chain is required when using strategy_name (no strategy_spec to infer it from)")
        if not tokens:
            raise ValueError("tokens is required when using strategy_name (no strategy_spec to infer it from)")
        if not token_funding:
            raise ValueError("token_funding is required when using strategy_name")
        resolved_token_funding = token_funding
        resolved_chain = chain
        raw_tokens = list(tokens)
        raw_tokens.extend(_token_funding_refs(resolved_token_funding))
        resolved_tokens = normalize_backtest_token_refs(raw_tokens, resolved_chain)
        fee_model = "realistic"

    strategy_timeframe = spec.parameters.get("timeframe") if spec is not None else None
    now = datetime.now(UTC)
    # Date-only request models normalize `end` to midnight UTC. When the end
    # DATE is today, the user means "up to now": the WHOLE config (end_time,
    # window, recency) moves to now coherently — deriving sub-hourly ticks
    # while the engine still stopped at midnight would re-create the exact
    # staleness the clamp guards against. Historical end dates keep their
    # midnight bound unchanged.
    if end == end.replace(hour=0, minute=0, second=0, microsecond=0) and end <= now < end + timedelta(days=1):
        end = now
    interval = _derive_interval_seconds(
        strategy_timeframe,
        (end - start).total_seconds(),
        quick=quick,
        end_age_seconds=max(0.0, (now - min(now, end)).total_seconds()),
    )

    return PnLBacktestConfig(
        start_time=start,
        end_time=end,
        interval_seconds=interval,
        token_funding=resolved_token_funding,
        chain=resolved_chain,
        tokens=resolved_tokens,
        fee_model=fee_model,
        slippage_model="realistic",
        include_gas_costs=not quick,
        # Service runs standalone without gateway — use forgiving defaults
        preflight_validation=False,
        allow_hardcoded_fallback=True,
        allow_degraded_data=True,
    )


def _serialize_equity_point(pt: Any) -> dict[str, Any]:
    """Serialize one equity point, keeping the numeraire projection intact.

    For a token-quoted strategy (VIB-5127) every point carries the numeraire
    token's USD price; ``value_numeraire`` (= ``value_usd / numeraire_price_usd``)
    is emitted alongside as a derived convenience so dashboards can plot the
    canonical numeraire equity curve without re-deriving it. USD points are
    unchanged.

    Timestamps deliberately keep the service payload's ``str(datetime)``
    format (matching the trades array and the pre-existing equity points);
    the new Decimal fields are normalized via ``_decimal_str`` so exponent
    artifacts (``0E+17``) never leak into JSON (VIB-5083).
    """
    payload: dict[str, Any] = {"timestamp": str(pt.timestamp), "value_usd": str(pt.value_usd)}
    if pt.numeraire_price_usd is not None and pt.numeraire_price_usd > 0:
        payload["numeraire_price_usd"] = _decimal_str(pt.numeraire_price_usd)
        payload["value_numeraire"] = _decimal_str(pt.value_usd / pt.numeraire_price_usd)
    return payload


def serialize_result(result: BacktestResult) -> dict[str, Any]:
    """Serialize BacktestResult to a JSON-compatible dict.

    Uses BacktestMetrics.to_dict() to expose the full metric set. All Decimal
    values are stringified for JSON safety. The field names match the SDK's
    internal BacktestMetrics dataclass exactly. Numeraire fields (top-level
    descriptors, per-point prices) and the per-tick ``price_series`` are
    passed through emit-when-set, mirroring ``BacktestResult.to_dict``.
    """
    payload: dict[str, Any] = {
        "metrics": result.metrics.to_dict(),
        # decide()-time data-failure report (ALM-2951); [] when clean.
        "decision_input_failures": result.decision_input_failures or [],
        "equity_curve": [_serialize_equity_point(pt) for pt in (result.equity_curve or [])],
        "trades": [
            {
                "timestamp": str(t.timestamp),
                "intent_type": str(t.intent_type),
                "amount_usd": str(t.amount_usd),
                "fee_usd": str(t.fee_usd),
                "slippage_usd": str(t.slippage_usd),
                # None for an opening / inventory-building trade (no realized
                # PnL yet, VIB-5083): serialize JSON null, not the str "None".
                "pnl_usd": str(t.pnl_usd) if t.pnl_usd is not None else None,
                # The trades array records rejected intents alongside fills
                # (result_summary.total_trades counts fills only); without a
                # status the UI blotter renders rejections as $0 trades (ALM-2936).
                "status": "filled" if t.success else "rejected",
                "rejection_reason": t.error if not t.success and t.error else None,
            }
            for t in (result.trades or [])
        ],
        "duration_seconds": result.run_duration_seconds or 0.0,
    }
    if result.numeraire is not None:
        payload["numeraire"] = result.numeraire
    if result.initial_capital_numeraire is not None:
        payload["initial_capital_numeraire"] = str(result.initial_capital_numeraire)
    if result.final_capital_numeraire is not None:
        payload["final_capital_numeraire"] = str(result.final_capital_numeraire)
    if result.price_series:
        payload["price_series"] = [
            {
                # str(datetime), not isoformat: consistent with every other
                # timestamp in this service payload (equity curve, trades).
                "timestamp": str(pt.timestamp),
                "prices": {key: _decimal_str(price) for key, price in pt.prices.items()},
            }
            for pt in result.price_series
        ]
        payload["price_series_display_labels"] = dict(result.price_series_display_labels)
    if result.data_manifest is not None:
        payload["data_manifest"] = result.data_manifest
    return payload


def resolve_strategy(
    request: BacktestRequest | QuickBacktestRequest,
    *,
    quick: bool = False,
) -> tuple[Any, PnLBacktestConfig]:
    """Resolve a strategy and config from a backtest request.

    Supports two modes:
    1. strategy_name — loads a registered strategy class from the SDK registry
    2. strategy_spec — builds a single-action strategy from the declarative spec

    Args:
        request: The backtest request (full or quick).
        quick: Whether this is a quick backtest (1-day intervals, no gas costs).

    Returns (strategy_instance, backtest_config).
    """
    timeframe = getattr(request, "timeframe", None) or build_quick_timeframe()
    # Respect explicit quick param; also check request.mode for BacktestRequest
    is_quick = quick or getattr(request, "mode", "full") == "quick"
    chain = request.chain
    tokens = getattr(request, "tokens", None)
    token_funding = request.token_funding

    if request.strategy_name:
        # Named strategy from registry — chain and tokens are required
        if not chain:
            raise ValueError("chain is required when using strategy_name")
        if not tokens:
            raise ValueError('tokens is required when using strategy_name (e.g. ["WETH", "USDC"])')
        strategy = load_named_strategy(request.strategy_name, chain)
        config = build_backtest_config(
            spec=None,
            timeframe=timeframe,
            quick=is_quick,
            chain=chain,
            token_funding=token_funding,
            tokens=tokens,
        )
        return strategy, config

    if request.strategy_spec:
        strategy = SpecBacktestStrategy(request.strategy_spec)
        config = build_backtest_config(
            spec=request.strategy_spec,
            timeframe=timeframe,
            quick=is_quick,
            chain=chain,
            token_funding=token_funding,
        )
        return strategy, config

    raise ValueError("Either strategy_name or strategy_spec must be provided")


async def run_backtest_job(
    job_id: str,
    request: BacktestRequest,
    job_manager: JobManager,
) -> None:
    """Run a backtest job in the background, updating progress in job_manager.

    This is called from a BackgroundTask. It:
    1. Marks the job as RUNNING
    2. Resolves strategy (named or spec-based) + config
    3. Runs PnLBacktester.backtest()
    4. Stores results (or error) in the job manager
    """
    job_manager.mark_running(job_id)
    job_manager.update_progress(job_id, 5.0, "Initializing backtest...")

    try:
        strategy, config = resolve_strategy(request)

        job_manager.update_progress(job_id, 10.0, "Running simulation...")

        token_addresses = build_backtest_token_address_map(config, strategy=strategy)
        backtester = create_backtester(token_addresses=token_addresses)
        try:
            result = await backtester.backtest(strategy, config)
        finally:
            await backtester.close()

        serialized = serialize_result(result)
        job_manager.complete_job(job_id, serialized)

    except Exception:
        logger.exception("Backtest job %s failed", job_id)
        job_manager.fail_job(job_id, "Backtest failed. Check server logs for details.")


def build_quick_timeframe() -> TimeframeSpec:
    """Build a 7-day timeframe ending today for quick backtests."""
    from datetime import date

    end = date.today()
    start = end - timedelta(days=7)
    return TimeframeSpec(start=start, end=end)
