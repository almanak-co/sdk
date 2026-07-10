"""``almanak strat run`` -- runtime config wiring and component initialization.

Split from run_helpers.py; import via the run_helpers facade externally.
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

import click

from ..strategies.metadata import LEGACY_COMPAT_DATA_REQUIREMENTS, StrategyDataRequirements
from ._run_context import ComponentBundle
from ._run_setup import _DryRunVaultEarlyExit, _runtime_private_key_override

if TYPE_CHECKING:
    pass

logger = logging.getLogger(
    "almanak.framework.cli.run_helpers"
)  # pinned: tests + operator filters key on the historical module path


# ---------------------------------------------------------------------------
# Runtime config wiring
# ---------------------------------------------------------------------------


def _resolve_effective_signing_key(
    runtime_private_key: str | None,
    *,
    config_chain: str | None,
) -> str | None:
    """Apply kwarg-over-env precedence to surface the effective signing key (#2100).

    Used by ``_build_runtime_config`` to drive sidecar-vs-local dispatch with
    the same precedence the downstream ``from_env`` calls use. ``None`` means
    "no kwarg, fall through to env"; ``""`` is the documented force-empty
    override (treated as "no key" by callers, which keeps the legacy
    "empty value -> sidecar" semantic).

    Solana single-chain strategies use ``SOLANA_PRIVATE_KEY`` (base58 Ed25519)
    instead of ``ALMANAK_PRIVATE_KEY`` (hex secp256k1) as the canonical env
    var — mirroring the rule in
    ``almanak.config.runtime._resolve_private_key_from_env``. Without this
    branch, a Solana strategy with ``--no-gateway`` and only
    ``SOLANA_PRIVATE_KEY`` set would falsely take the sidecar branch even
    though ``runtime_config_from_env`` is fully able to load.
    """
    if runtime_private_key is not None:
        # Honour the explicit kwarg before touching the typed config — the
        # kwarg-first contract documented above must hold even when an
        # unrelated submodel would fail validation (PR #2152 review).
        return runtime_private_key

    # Narrow to ``gateway_config_from_env`` rather than ``load_config()`` so
    # a malformed unrelated submodel (backtest, cli, connectors) cannot block
    # signing-key resolution. ``GatewayConfig`` already carries the
    # ``ALMANAK_PRIVATE_KEY`` / ``SOLANA_PRIVATE_KEY`` canonical fallback ladder.
    from almanak.config.env import gateway_config_from_env

    _gw = gateway_config_from_env()
    # VIB-4803: route SVM chains through the ChainFamily adapter.
    from almanak.framework.chain_family import SvmFamily as _SvmFamily
    from almanak.framework.chain_family import family_for as _family_for

    if isinstance(_family_for((config_chain or "").strip()), _SvmFamily):
        # The typed ``GatewayConfig.solana_private_key`` carries
        # SOLANA_PRIVATE_KEY via the canonical env-fallback ladder; falling
        # back to ``private_key`` (ALMANAK_PRIVATE_KEY) preserves the legacy
        # "Solana strategy with hex key" path.
        return _gw.solana_private_key or _gw.private_key or None
    return _gw.private_key or None


def _resolve_runtime_private_key_kwarg(runtime_private_key: str | None) -> str | None:
    """Fall back to the ``_runtime_private_key_override`` ContextVar when no
    explicit kwarg was passed.

    Direct callers (tests, strategy harnesses) keep their kwarg-first semantics
    — passing ``None`` *intentionally* still falls through to the contextvar,
    matching the documented "no kwarg" path. Pass ``""`` to force the
    no-local-key branch; that empty string is preserved verbatim.
    """
    if runtime_private_key is not None:
        return runtime_private_key
    return _runtime_private_key_override.get()


def _build_sidecar_runtime_config(*, config_chain: str | None) -> Any:
    """Build the ``GatewayRuntimeConfig`` used in single-chain sidecar mode.

    Sidecar mode (``--no-gateway`` without a local private key) means the
    gateway handles all signing and RPC; the framework just needs a chain and
    the wallet address it resolves to. ``ALMANAK_SAFE_ADDRESS`` /
    ``ALMANAK_EOA_ADDRESS`` / ``ALMANAK_GATEWAY_WALLETS`` are checked in that
    order; the gateway-wallets path leaves ``wallet_address`` empty for the
    later ``register_chains()`` call to populate.
    """
    from almanak.core.chains import ChainRegistry

    from ..execution.config import GatewayRuntimeConfig
    from ..execution.gas.constants import DEFAULT_GAS_PRICE_CAP_GWEI

    if not config_chain:
        raise click.ClickException(
            "Chain must be specified in config.json or strategy decorator for sidecar deployment mode."
        )
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    _cli = _cli_cfg()
    safe_address = _cli.safe_address
    wallet_address = safe_address or _cli.eoa_address
    if not wallet_address and not _cli.gateway_wallets_configured:
        raise click.ClickException(
            "Sidecar mode (--no-gateway without ALMANAK_PRIVATE_KEY) requires "
            "ALMANAK_SAFE_ADDRESS, ALMANAK_EOA_ADDRESS, or ALMANAK_GATEWAY_WALLETS to be set."
        )
    wallet_address = wallet_address or ""
    descriptor = ChainRegistry.try_resolve(config_chain)
    default_gas_cap = (
        descriptor.gas.price_cap_gwei
        if descriptor is not None and descriptor.gas.price_cap_gwei is not None
        else DEFAULT_GAS_PRICE_CAP_GWEI
    )
    runtime_config = GatewayRuntimeConfig(
        chain=config_chain,
        wallet_address=wallet_address,
        is_safe=bool(safe_address),
        max_gas_price_gwei=default_gas_cap,
    )
    click.echo(f"Sidecar deployment mode: chain={config_chain}, wallet={wallet_address}")
    return runtime_config


def _echo_local_env_help() -> None:
    """User-facing help text shown when single-chain config loading fails."""
    click.echo("Required environment variables:")
    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
    click.echo()
    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
    click.echo("  ALMANAK_ARBITRUM_RPC_URL     - Per-chain RPC URL (highest priority)")
    click.echo("  ALMANAK_RPC_URL              - Generic RPC endpoint URL")
    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
    click.echo()
    click.echo("Optional environment variables:")
    click.echo("  ALMANAK_MAX_GAS_PRICE_GWEI - Max gas price (default: chain-specific; Anvil: 9999)")
    click.echo("  ALMANAK_TX_TIMEOUT_SECONDS - Tx timeout (default: 120)")
    click.echo("  ALMANAK_SIMULATION_ENABLED - Enable simulation (default: false)")


def _echo_multichain_env_help(strategy_chains: list[str]) -> None:
    """User-facing help text shown when multi-chain config loading fails."""
    click.echo("Required environment variables for multi-chain:")
    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
    click.echo()
    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
    for chain in strategy_chains:
        click.echo(f"  ALMANAK_{chain.upper()}_RPC_URL  - Per-chain RPC URL")
    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")


def _accept_anvil_default_wallet_or_exit() -> None:
    """Echo the Anvil-default wallet notice; honour ``isatty`` confirm prompt."""
    from .run import ANVIL_DEFAULT_ADDRESS

    click.echo(f"No ALMANAK_PRIVATE_KEY set. Using default Anvil wallet: {ANVIL_DEFAULT_ADDRESS}")
    if sys.stdin.isatty():
        if not click.confirm("Continue with this wallet?", default=True):
            sys.exit(0)
    else:
        click.echo("(non-interactive, accepting default Anvil wallet)")


def _load_local_runtime_config(
    *,
    config_chain: str | None,
    resolved_network: str,
    runtime_private_key: str | None,
) -> Any:
    """Build a ``LocalRuntimeConfig`` with Anvil-default fallback and verbose errors.

    Routes env reads through :func:`almanak.config.runtime.runtime_config_from_env`
    and converts to the dataclass shape via
    :meth:`LocalRuntimeConfig.from_runtime_config`.
    On ``MissingEnvironmentVariableError`` for ``PRIVATE_KEY`` while on
    Anvil, a second attempt plumbs ``ANVIL_DEFAULT_PRIVATE_KEY`` via the
    typed kwarg (#2100). Anything else exits with the canonical help text.
    """
    from almanak.config.runtime import (
        MissingEnvironmentVariableError,
        runtime_config_from_env,
    )

    from ..execution.config import LocalRuntimeConfig
    from .run import ANVIL_DEFAULT_PRIVATE_KEY

    try:
        rc = runtime_config_from_env(
            chain=config_chain,
            network=resolved_network,
            private_key=runtime_private_key,
        )
        return LocalRuntimeConfig.from_runtime_config(rc)
    except MissingEnvironmentVariableError as e:
        if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
            _accept_anvil_default_wallet_or_exit()
            try:
                rc = runtime_config_from_env(
                    chain=config_chain,
                    network=resolved_network,
                    private_key=ANVIL_DEFAULT_PRIVATE_KEY,
                )
                return LocalRuntimeConfig.from_runtime_config(rc)
            except Exception as retry_err:
                click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                sys.exit(1)
        if e.var_name.endswith("PRIVATE_KEY"):
            click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
            click.echo("Set it in your .env file or environment.", err=True)
        else:
            click.echo(f"Error loading configuration: {e}", err=True)
            click.echo()
            _echo_local_env_help()
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        click.echo()
        _echo_local_env_help()
        sys.exit(1)


def _load_multichain_runtime_config(
    *,
    strategy_chains: list[str],
    strategy_protocols: Any,
    resolved_network: str,
    runtime_private_key: str | None,
    no_gateway: bool,
) -> Any:
    """Build a ``MultiChainRuntimeConfig`` with Anvil-default fallback and verbose errors.

    Routes env reads through :func:`almanak.config.runtime.runtime_config_from_env`
    and converts to the dataclass shape via
    :meth:`MultiChainRuntimeConfig.from_runtime_config`. The Anvil-default
    retry and the multi-chain-sidecar guard match the legacy semantics
    (#2100).
    """
    from almanak.config.runtime import (
        MissingEnvironmentVariableError,
        runtime_config_from_env,
    )

    from ..execution.config import MultiChainRuntimeConfig
    from .run import ANVIL_DEFAULT_PRIVATE_KEY

    try:
        rc = runtime_config_from_env(
            chains=strategy_chains,
            protocols=strategy_protocols,
            network=resolved_network,
            private_key=runtime_private_key,
        )
        runtime_config = MultiChainRuntimeConfig.from_runtime_config(rc)
        click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
        return runtime_config
    except MissingEnvironmentVariableError as e:
        if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
            _accept_anvil_default_wallet_or_exit()
            try:
                rc = runtime_config_from_env(
                    chains=strategy_chains,
                    protocols=strategy_protocols,
                    network=resolved_network,
                    private_key=ANVIL_DEFAULT_PRIVATE_KEY,
                )
                runtime_config = MultiChainRuntimeConfig.from_runtime_config(rc)
            except Exception as retry_err:
                click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                sys.exit(1)
            click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
            return runtime_config
        if no_gateway and e.var_name.endswith("PRIVATE_KEY"):
            click.echo(
                "Error: Multi-chain sidecar mode requires ALMANAK_GATEWAY_WALLETS or ALMANAK_PRIVATE_KEY.",
                err=True,
            )
            click.echo(
                "Set ALMANAK_GATEWAY_WALLETS with per-chain wallet config, or provide ALMANAK_PRIVATE_KEY.",
                err=True,
            )
            sys.exit(1)
        if e.var_name.endswith("PRIVATE_KEY"):
            click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
            click.echo("Set it in your .env file or environment.", err=True)
        else:
            click.echo(f"Error loading multi-chain configuration: {e}", err=True)
            click.echo()
            _echo_multichain_env_help(strategy_chains)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error loading multi-chain configuration: {e}", err=True)
        click.echo()
        _echo_multichain_env_help(strategy_chains)
        sys.exit(1)


def _register_chain_wallets(
    *,
    multi_chain: bool,
    strategy_chains: list[str],
    config_chain: str | None,
    gateway_client: Any,
    runtime_config: Any,
) -> dict[str, str]:
    """Register chains with the gateway and pin ``runtime_config.wallet_address``.

    Only meaningful when ``ALMANAK_GATEWAY_WALLETS`` is set; otherwise returns
    ``{}``. Mutates ``runtime_config.wallet_address`` to the gateway-resolved
    primary wallet so the runtime signs through the same identity the gateway
    uses for accounting.
    """
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    if not _cli_cfg().gateway_wallets_configured:
        return {}
    try:
        register_chain_list = strategy_chains if multi_chain else [str(config_chain)]
        chain_wallets = gateway_client.register_chains(register_chain_list)
        if chain_wallets:
            primary_chain = register_chain_list[0]
            primary_wallet = chain_wallets.get(primary_chain, "")
            runtime_config.wallet_address = primary_wallet
            unique_addrs = {v.lower() for v in chain_wallets.values()}
            if len(unique_addrs) <= 1:
                click.echo(
                    f"Gateway wallet registry: uniform wallet {primary_wallet[:12]}... on {len(chain_wallets)} chain(s)"
                )
            else:
                click.echo("Gateway wallet registry: non-uniform wallets")
                for ch, addr in chain_wallets.items():
                    click.echo(f"  {ch}: {addr}")
        return chain_wallets
    except Exception as e:
        click.secho(f"WARNING: register_chains() failed: {e}", fg="yellow", err=True)
        click.echo("Falling back to legacy wallet resolution.", err=True)
        logger.warning("register_chains() failed: %s", e)
        return {}


def _apply_strategy_config_chain(
    *,
    strategy_config: dict[str, Any],
    multi_chain: bool,
    strategy_chains: list[str],
    runtime_config: Any,
) -> None:
    """Inject ``chain`` into ``strategy_config`` (mutating).

    Sources, in order: existing ``chain`` field (with ``ALMANAK_CHAIN`` env
    override applied for single-chain strategies — keeps the strategy class's
    MarketSnapshot/balance lookups in sync with the runtime); else first
    declared chain when multi-chain; else ``runtime_config.chain``.
    """
    from ..execution.config import GatewayRuntimeConfig, LocalRuntimeConfig

    if "chain" not in strategy_config:
        if multi_chain:
            strategy_config["chain"] = strategy_chains[0]
        else:
            assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)
            strategy_config["chain"] = runtime_config.chain
        return
    if multi_chain:
        return
    # ``ALMANAK_CHAIN`` is the canonical single-chain override; the
    # runtime-config layer reads it via ``runtime_config_from_env`` already.
    # Here we only need the raw value to compare against the strategy
    # config's own ``chain`` field; reading ``os.environ`` would re-introduce
    # the boundary lint hit. Source the value through the runtime-config
    # factory's same lookup path by going through the canonical helper.
    from almanak.config.cli_runtime import _almanak_chain_env

    env_chain = _almanak_chain_env()
    if not env_chain:
        return
    existing = strategy_config.get("chain")
    existing_norm = existing.strip().lower() if isinstance(existing, str) else ""
    if existing_norm != env_chain:
        strategy_config["chain"] = env_chain


def _apply_strategy_config_wallet(
    *,
    strategy_config: dict[str, Any],
    multi_chain: bool,
    strategy_chains: list[str],
    config_chain: str | None,
    runtime_config: Any,
    chain_wallets: dict[str, str],
) -> None:
    """Inject ``wallet_address`` into ``strategy_config`` (mutating).

    Runtime-resolved wallet wins (see #1684) so a stale ``wallet_address`` in
    ``config.json`` never drives ``deployment_id`` when the runtime is signing
    from a different identity. Prefers the gateway-registered chain wallet
    when ``ALMANAK_GATEWAY_WALLETS`` was set.
    """
    if chain_wallets:
        primary = strategy_chains[0] if multi_chain else str(config_chain)
        resolved_wallet = chain_wallets.get(primary, runtime_config.execution_address)
    else:
        resolved_wallet = runtime_config.execution_address
    if resolved_wallet:
        strategy_config["wallet_address"] = resolved_wallet


def _build_runtime_config(
    *,
    no_gateway: bool,
    multi_chain: bool,
    resolved_network: str,
    config_chain: str | None,
    strategy_chains: list[str],
    strategy_protocols: Any,
    gateway_client: Any,
    strategy_config: dict[str, Any],
    runtime_private_key: str | None = None,
) -> tuple[Any, dict[str, str]]:
    """Build the runtime config (Local / MultiChain / Gateway) and register chains.

    Three-way dispatch over the loader helpers
    (``_build_sidecar_runtime_config`` / ``_load_multichain_runtime_config`` /
    ``_load_local_runtime_config``) plus Safe-mode preflight, gateway
    chain-wallet registration, and ``strategy_config`` mutations.

    Args:
        runtime_private_key: Optional explicit private key (kwarg-only). When
            non-None, plumbed into the underlying ``from_env(private_key=...)``
            call so the kwarg wins over env (#2100). Empty-string forces the
            sidecar dispatch path identically to an unset env var. When None,
            falls back to the ``_runtime_private_key_override`` ContextVar set
            by ``almanak strat test`` and by ``_setup_gateway`` after isolated-
            wallet derivation.

    Returns:
        ``(runtime_config, chain_wallets)`` — empty dict when
        ALMANAK_GATEWAY_WALLETS is not set.
    """
    from .run import _validate_safe_mode_preflight

    runtime_private_key = _resolve_runtime_private_key_kwarg(runtime_private_key)
    effective_private_key = _resolve_effective_signing_key(
        runtime_private_key,
        config_chain=config_chain,
    )

    if no_gateway and not effective_private_key and not multi_chain:
        runtime_config = _build_sidecar_runtime_config(config_chain=config_chain)
    elif multi_chain:
        runtime_config = _load_multichain_runtime_config(
            strategy_chains=strategy_chains,
            strategy_protocols=strategy_protocols,
            resolved_network=resolved_network,
            runtime_private_key=runtime_private_key,
            no_gateway=no_gateway,
        )
    else:
        runtime_config = _load_local_runtime_config(
            config_chain=config_chain,
            resolved_network=resolved_network,
            runtime_private_key=runtime_private_key,
        )

    # Safe-mode preflight only when the CLI manages the gateway (env vars
    # are local). Skip when ALMANAK_GATEWAY_WALLETS is set — the gateway's
    # WalletRegistry handles signer configuration per chain.
    from almanak.config import cli_runtime_config_from_env as _cli_cfg_for_wallets

    gateway_wallets_configured = _cli_cfg_for_wallets().gateway_wallets_configured
    if runtime_config.is_safe_mode and not no_gateway and not gateway_wallets_configured:
        error = _validate_safe_mode_preflight(runtime_config.execution_address)
        if error:
            click.secho(f"ERROR: {error}", fg="red", err=True)
            sys.exit(1)

    chain_wallets = _register_chain_wallets(
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        config_chain=config_chain,
        gateway_client=gateway_client,
        runtime_config=runtime_config,
    )
    _apply_strategy_config_chain(
        strategy_config=strategy_config,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        runtime_config=runtime_config,
    )
    _apply_strategy_config_wallet(
        strategy_config=strategy_config,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        config_chain=config_chain,
        runtime_config=runtime_config,
        chain_wallets=chain_wallets,
    )
    return runtime_config, chain_wallets


# ---------------------------------------------------------------------------
# Component initialization
# ---------------------------------------------------------------------------


def _get_data_requirements(strategy_instance: Any) -> StrategyDataRequirements:
    """Return the strategy's declared data requirements, falling back to legacy compat.

    Strategies decorated with @almanak_strategy that omit data_requirements get
    LEGACY_COMPAT_DATA_REQUIREMENTS (all services wired eagerly) to preserve
    pre-VIB-3392 behavior. Strategies without a STRATEGY_METADATA attribute
    (no decorator, test stubs) also fall back to legacy compat.
    """
    metadata = getattr(strategy_instance, "STRATEGY_METADATA", None)
    if metadata is None:
        return LEGACY_COMPAT_DATA_REQUIREMENTS
    dr = getattr(metadata, "data_requirements", None)
    if dr is None:
        return LEGACY_COMPAT_DATA_REQUIREMENTS
    return dr


# crap-allowlist: #2097 replaces direct os.environ.get reads with the typed
# cli_runtime_config_from_env() — no new branches, no new behaviour. Function refactor
# is tracked separately; allowlist matches the documented escape hatch for this
# config-boundary cutover.
def _build_orchestrator_and_providers(  # noqa: C901
    *,
    multi_chain: bool,
    runtime_config: Any,
    strategy_chains: list[str],
    strategy_config: dict[str, Any],
    resolved_network: str,
    gateway_client: Any,
    chain_wallets: dict[str, str],
    strategy_instance: Any,
    components: ComponentBundle,
) -> None:
    """Build orchestrator, price/balance/OHLCV providers, and wire indicators.

    Populates ``components.execution_orchestrator``, ``components.price_oracle``,
    ``components.balance_provider``, ``components.ohlcv_provider``, and
    ``components.solana_fork_mgr`` (Solana + Anvil only). Dispatches between
    multi-chain and single-chain paths preserving the exact click.echo output
    and ordering of the inlined code.
    """
    from decimal import Decimal

    from almanak.gateway.data.balance import Web3BalanceProvider  # noqa: F401 — historical import site
    from almanak.gateway.data.price import CoinGeckoPriceSource, PriceAggregator  # noqa: F401

    from ..data.balance.gateway_provider import GatewayBalanceProvider
    from ..data.ohlcv import create_ohlcv_stack
    from ..data.price.gateway_oracle import GatewayPriceOracle
    from ..execution.multichain import MultiChainOrchestrator
    from .run import (
        _get_orca_pool_accounts,
        _init_prediction_provider,
        _wire_core_providers,
        _wire_indicators,
    )

    requirements = _get_data_requirements(strategy_instance)
    ohlcv_provider: Any = None

    execution_orchestrator: Any
    if multi_chain:
        from ..data.balance.gateway_multichain import MultiChainGatewayBalanceProvider
        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator  # noqa: F401

        # Resolve effective wallet address (from chain_wallets or runtime_config)
        effective_wallet = runtime_config.execution_address
        if chain_wallets:
            effective_wallet = chain_wallets.get(strategy_chains[0], effective_wallet)

        if not effective_wallet:
            raise click.ClickException(
                "No wallet address resolved for multi-chain execution. "
                "Ensure ALMANAK_GATEWAY_WALLETS is configured correctly and the gateway is reachable."
            )

        click.echo("  Using gateway-backed providers for multi-chain...")
        price_oracle = GatewayPriceOracle(gateway_client, default_chain=strategy_chains[0])
        balance_provider = GatewayBalanceProvider(
            client=gateway_client,
            wallet_address=effective_wallet,
            chain=strategy_chains[0],
        )
        execution_orchestrator = MultiChainOrchestrator.from_gateway(
            gateway_client=gateway_client,
            chains=strategy_chains,
            wallet_address=effective_wallet,
            max_gas_price_gwei=runtime_config.max_gas_price_gwei,
            chain_wallets=chain_wallets or None,
        )

        # Create multi-chain balance provider for the strategy
        multi_chain_balance_provider = MultiChainGatewayBalanceProvider(
            client=gateway_client,
            wallet_address=effective_wallet,
            chains=strategy_chains,
            chain_wallets=chain_wallets or None,
        )

        # Set multi-chain providers on strategy if it's an IntentStrategy.
        # VIB-5663: wire the price oracle too — without it the multi-chain
        # MarketSnapshot is built with price_oracle=None (builders.py) and every
        # market.price(..., chain=...) raises "Cannot determine price", halting
        # the runner on the accounting native-gas fold. GatewayPriceOracle.price
        # is chain-aware and routes per-chain through the gateway.
        if hasattr(strategy_instance, "set_multi_chain_providers"):
            strategy_instance.set_multi_chain_providers(
                price_oracle=price_oracle,
                balance_provider=multi_chain_balance_provider,
            )
            click.echo("  Multi-chain providers set on strategy")

        if requirements.indicators:
            # NOTE: In multi-chain mode, OHLCV routing is bound to the first chain.
            # For CEX-listed tokens this is fine (Binance data is chain-agnostic).
            # For DeFi-native tokens on secondary chains, GeckoTerminal pool search
            # may resolve to the wrong network. Per-chain providers would require
            # passing chain context through the indicator callables, which is a larger change.
            ohlcv_stack = create_ohlcv_stack(
                gateway_client=gateway_client,
                chain=strategy_chains[0],
                pool_address=strategy_config.get("pool_address") if strategy_config else None,
            )
            ohlcv_provider = ohlcv_stack.provider
            # VIB-4347: stamp the sync OHLCVRouter on the strategy so
            # ``MarketSnapshot.ohlcv(...)`` resolves to the same routed gateway-backed
            # pipes the indicator path already uses. Shared router instance = shared
            # disk cache + TTL.
            strategy_instance._ohlcv_router = ohlcv_stack.router
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)
        elif requirements.price or requirements.balance:
            # indicators=False: wire price/balance directly without OHLCV or indicator calculators
            _wire_core_providers(strategy_instance, price_oracle, balance_provider)

        # MarketSnapshot needs the gateway client to do gateway-routed eth_calls
        # (e.g. position_health). Wire it unconditionally; methods that need it
        # check for None at call time.
        strategy_instance._gateway_client = gateway_client

        rate_monitor_wired = False
        if requirements.lending_rates:
            try:
                from ..data.rates import RateMonitor

                primary_chain = strategy_chains[0]
                chain_rpc_url = runtime_config.rpc_urls.get(primary_chain)
                # _internal=True: framework wiring of the gateway-backed rate
                # source onto MarketSnapshot is the canonical lending-rate lane,
                # not a deprecated strategy-side bypass (VIB-4869).
                rate_monitor = RateMonitor(chain=primary_chain, rpc_url=chain_rpc_url, _internal=True)
                strategy_instance._rate_monitor = rate_monitor
                rate_monitor_wired = True
            except Exception as e:
                logger.debug(f"Rate monitor not available: {e}")

        funding_wired = False
        if requirements.funding_rates:
            try:
                from ..data.funding import GatewayFundingRateProvider

                primary_chain = strategy_chains[0]
                funding_provider = GatewayFundingRateProvider(gateway_client=gateway_client, chain=primary_chain)
                strategy_instance._funding_rate_provider = funding_provider
                funding_wired = True
            except (ImportError, ValueError, RuntimeError) as e:
                logger.warning(
                    "Funding rate provider init failed for chain=%s: %s",
                    strategy_chains[0],
                    e,
                    exc_info=True,
                )

        _wired = []
        if getattr(strategy_instance, "_price_oracle", None) is not None:
            _wired.append("price")
        if getattr(strategy_instance, "_balance_provider", None) is not None:
            _wired.append("balance")
        if getattr(strategy_instance, "_indicator_provider", None) is not None:
            _wired.append("indicators")
        if rate_monitor_wired:
            _wired.append("lending_rates")
        if funding_wired:
            _wired.append("funding_rates")
        click.echo(f"  Injected strategy data services: {', '.join(_wired)}")
        click.echo(f"  Multi-chain orchestrator created for {len(strategy_chains)} chains")
    else:
        # Single-chain setup - always use gateway-backed providers
        from ..execution.config import GatewayRuntimeConfig, LocalRuntimeConfig
        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

        assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)

        # Resolve effective wallet address (from chain_wallets or runtime_config)
        sc_effective_wallet = runtime_config.execution_address
        if chain_wallets:
            sc_effective_wallet = chain_wallets.get(runtime_config.chain, sc_effective_wallet)

        click.echo("  Using gateway-backed providers...")
        price_oracle = GatewayPriceOracle(gateway_client, default_chain=runtime_config.chain)
        balance_provider = GatewayBalanceProvider(
            client=gateway_client,
            wallet_address=sc_effective_wallet,
            chain=runtime_config.chain,
        )

        # For Solana + --network anvil, start local solana-test-validator.
        # VIB-4803: route through the ChainFamily adapter.
        from almanak.framework.chain_family import SvmFamily as _SvmFamily
        from almanak.framework.chain_family import family_for as _family_for

        if isinstance(_family_for(runtime_config.chain), _SvmFamily) and resolved_network == "anvil":
            from almanak.config import cli_runtime_config_from_env as _solana_cli_cfg

            from ..anvil.solana_fork_manager import SolanaForkManager

            _solana_cli = _solana_cli_cfg()
            solana_rpc_url = _solana_cli.solana_rpc_url
            # Clone any pool/account addresses declared in the strategy config
            _extra_clone = []
            if strategy_config and isinstance(strategy_config, dict):
                for _key in ("pool_address", "pool_a_address", "pool_b_address"):
                    _addr = strategy_config.get(_key)
                    if _addr and isinstance(_addr, str):
                        _extra_clone.append(_addr)
                # For Orca Whirlpool strategies, also pre-clone vault + tick array accounts
                _orca_accounts = _get_orca_pool_accounts(strategy_config)
                if _orca_accounts:
                    click.echo(f"  Pre-cloning {len(_orca_accounts)} Orca pool accounts (vaults + tick arrays)")
                    _extra_clone.extend(_orca_accounts)
            if _extra_clone:
                click.echo(f"  Cloning {len(_extra_clone)} account(s) from mainnet")
            solana_fork_mgr = SolanaForkManager(
                rpc_url=solana_rpc_url,
                validator_port=_solana_cli.solana_validator_port,
                clone_accounts=_extra_clone,
            )
            click.echo("  Starting local solana-test-validator...")
            import asyncio as _aio

            started = _aio.get_event_loop().run_until_complete(solana_fork_mgr.start())
            if not started:
                raise click.ClickException(
                    "Failed to start solana-test-validator. "
                    "Ensure Solana CLI tools are installed: "
                    'sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"'
                )
            click.echo(f"  solana-test-validator running at {solana_fork_mgr.get_rpc_url()}")

            # Fund the wallet
            _aio.get_event_loop().run_until_complete(
                solana_fork_mgr.fund_wallet(runtime_config.wallet_address, Decimal("100"))
            )
            _aio.get_event_loop().run_until_complete(
                solana_fork_mgr.fund_tokens(
                    runtime_config.wallet_address,
                    {"USDC": Decimal("10000"), "USDT": Decimal("10000")},
                )
            )
            click.echo("  Wallet funded with 100 SOL + 10K USDC + 10K USDT")
            components.solana_fork_mgr = solana_fork_mgr

        # All chains (including Solana) use GatewayExecutionOrchestrator
        execution_orchestrator = GatewayExecutionOrchestrator(
            client=gateway_client,
            chain=runtime_config.chain,
            wallet_address=sc_effective_wallet,
            max_gas_price_gwei=runtime_config.max_gas_price_gwei,
        )
        click.echo("  Gateway-backed providers created")

        if requirements.indicators:
            # Create indicator calculators using routed OHLCV provider (CEX + DEX fallback)
            ohlcv_stack = create_ohlcv_stack(
                gateway_client=gateway_client,
                chain=runtime_config.chain,
                pool_address=strategy_config.get("pool_address") if strategy_config else None,
            )
            ohlcv_provider = ohlcv_stack.provider
            # VIB-4347: stamp the sync OHLCVRouter on the strategy so
            # ``MarketSnapshot.ohlcv(...)`` resolves to the same routed gateway-backed
            # pipes the indicator path already uses. Shared router instance = shared
            # disk cache + TTL.
            strategy_instance._ohlcv_router = ohlcv_stack.router
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)
        elif requirements.price or requirements.balance:
            # indicators=False: wire price/balance directly without OHLCV or indicator calculators
            _wire_core_providers(strategy_instance, price_oracle, balance_provider)

        # Initialize prediction market provider for strategies that explicitly
        # declare polymarket support. Non-polymarket strategies skip this
        # entirely (including Polygon runs) to avoid irrelevant warnings.
        if hasattr(strategy_instance, "_prediction_provider"):
            _init_prediction_provider(strategy_instance, chain=runtime_config.chain, gateway_client=gateway_client)

        # MarketSnapshot needs the gateway client to do gateway-routed eth_calls
        # (e.g. position_health). Wire it unconditionally; methods that need it
        # check for None at call time.
        strategy_instance._gateway_client = gateway_client

        rate_monitor_wired = False
        if requirements.lending_rates:
            try:
                from ..data.rates import RateMonitor

                rpc_url = getattr(runtime_config, "rpc_url", None)
                # _internal=True: framework wiring of the gateway-backed rate
                # source onto MarketSnapshot is the canonical lending-rate lane,
                # not a deprecated strategy-side bypass (VIB-4869).
                rate_monitor = RateMonitor(chain=runtime_config.chain, rpc_url=rpc_url, _internal=True)
                strategy_instance._rate_monitor = rate_monitor
                rate_monitor_wired = True
            except Exception as e:
                logger.debug(f"Rate monitor not available: {e}")

        funding_wired = False
        if requirements.funding_rates:
            try:
                from ..data.funding import GatewayFundingRateProvider

                funding_provider = GatewayFundingRateProvider(gateway_client=gateway_client, chain=runtime_config.chain)
                strategy_instance._funding_rate_provider = funding_provider
                funding_wired = True
            except (ImportError, ValueError, RuntimeError) as e:
                logger.warning(
                    "Funding rate provider init failed for chain=%s: %s",
                    runtime_config.chain,
                    e,
                    exc_info=True,
                )

        _wired = []
        if getattr(strategy_instance, "_price_oracle", None) is not None:
            _wired.append("price")
        if getattr(strategy_instance, "_balance_provider", None) is not None:
            _wired.append("balance")
        if getattr(strategy_instance, "_indicator_provider", None) is not None:
            _wired.append("indicators")
        if rate_monitor_wired:
            _wired.append("lending_rates")
        if funding_wired:
            _wired.append("funding_rates")
        click.echo(f"  Injected strategy data services: {', '.join(_wired)}")

    components.execution_orchestrator = execution_orchestrator
    components.price_oracle = price_oracle
    components.balance_provider = balance_provider
    components.ohlcv_provider = ohlcv_provider


def _init_copy_trading(  # noqa: C901
    *,
    strategy_instance: Any,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    gateway_client: Any,
    price_oracle: Any,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    multi_chain: bool,
) -> None:
    """Wire copy-trading components onto the strategy (v1 or v2 config).

    Preserves the inlined block's attribute-injection order and branch
    ordering (v1-only vs v2 coexist: v1 attributes are set first, then v2
    overrides them when valid). Fails fast with ClickException on invalid
    config in strict mode; logs a warning otherwise.

    Runs only in single-chain mode (mirroring the original code placement).
    """
    if not strategy_config.get("copy_trading"):
        return
    if multi_chain:
        # Defense-in-depth guard. `_build_components` pre-validates this same
        # combination BEFORE building any providers, so direct callers of
        # `_init_copy_trading` still get a clear failure instead of a silent
        # skip (see #1683).
        raise click.ClickException(
            "copy_trading is not yet supported for multi-chain strategies. "
            "Remove the copy_trading block or configure the strategy as single-chain."
        )

    from decimal import Decimal

    from almanak.connectors._strategy_base.contract_registry import get_default_registry

    from ..data.wallet_activity import WalletActivityProvider
    from ..services.copy_trading.copy_circuit_breaker import CopyCircuitBreaker
    from ..services.copy_trading.copy_intent_builder import CopyIntentBuilder
    from ..services.copy_trading.copy_ledger import CopyLedger
    from ..services.copy_trading.copy_policy_engine import CopyPolicyEngine
    from ..services.copy_trading.copy_signal_engine import CopySignalEngine
    from ..services.copy_trading.copy_trading_models import (
        CopyTradingConfig,
        CopyTradingConfigError,
        CopyTradingConfigV2,
    )
    from ..services.wallet_monitor import WalletMonitor, WalletMonitorConfig
    from ..testing.copy_replay import CopyReplayRunner
    from .run import create_sync_price_oracle_func

    ct_raw = strategy_config["copy_trading"]
    click.echo("  Copy trading config detected, initializing components...")

    if not isinstance(ct_raw, dict):
        raise click.ClickException("copy_trading config must be an object")

    ct_config = CopyTradingConfig.from_config(ct_raw)
    ct_v2: CopyTradingConfigV2 | None = None
    strict_requested = bool(copy_strict or ct_raw.get("strict") or ct_raw.get("execution_policy", {}).get("strict"))
    try:
        ct_v2 = CopyTradingConfigV2.from_config(ct_raw)
    except CopyTradingConfigError as e:
        if strict_requested:
            raise click.ClickException(f"Invalid copy_trading config in strict mode: {e}") from e
        click.echo("  Warning: copy_trading strict schema validation failed, using legacy-compatible mode")

    registry = get_default_registry()

    # Group leaders by chain for multi-chain monitoring
    leaders_by_chain: dict[str, list[str]] = {}
    if ct_v2 is not None:
        for leader in ct_v2.leaders:
            leader_chain = (leader.chain or runtime_config.chain).lower()
            leaders_by_chain.setdefault(leader_chain, []).append(leader.address)
    else:
        for leader_dict in ct_config.leaders:
            leader_chain = str(leader_dict.get("chain", runtime_config.chain)).lower()
            leaders_by_chain.setdefault(leader_chain, []).append(leader_dict["address"])

    # Create one WalletMonitor per unique chain
    wallet_monitors: dict[str, WalletMonitor] = {}
    for chain, addresses in leaders_by_chain.items():
        monitor_config = WalletMonitorConfig(
            leader_addresses=addresses,
            chain=chain,
            poll_interval_seconds=(
                ct_v2.monitoring.poll_interval_seconds
                if ct_v2 is not None
                else ct_config.monitoring.get("poll_interval_seconds", 12)
            ),
            lookback_blocks=(
                ct_v2.monitoring.lookback_blocks
                if ct_v2 is not None
                else ct_config.monitoring.get("lookback_blocks", 50)
            ),
            confirmation_depth=(
                ct_v2.monitoring.confirmation_depth
                if ct_v2 is not None
                else ct_config.monitoring.get("confirmation_depth", 1)
            ),
        )
        wallet_monitors[chain] = WalletMonitor(config=monitor_config, gateway_client=gateway_client)

    # Create price function for signal engine from gateway price oracle
    copy_price_fn = None
    if price_oracle is not None:
        sync_price = create_sync_price_oracle_func(price_oracle)

        def _copy_price_fn(symbol: str, chain: str) -> Decimal | None:
            try:
                return sync_price(symbol, "USD", chain)
            except Exception:
                return None

        copy_price_fn = _copy_price_fn

    engine = CopySignalEngine(
        registry=registry,
        max_age_seconds=(
            ct_v2.monitoring.max_signal_age_seconds
            if ct_v2 is not None
            else ct_config.monitoring.get("max_signal_age_seconds", 300)
        ),
        price_fn=copy_price_fn,
        strict_token_resolution=strict_requested,
    )

    activity_provider = WalletActivityProvider(
        signal_engine=engine,
        wallet_monitors=wallet_monitors,
    )

    # Runtime-inject copy trading attributes (not declared on IntentStrategy)
    strat_any: Any = strategy_instance
    strat_any._wallet_activity_provider = activity_provider
    strat_any._copy_mode = normalized_copy_mode
    strat_any._copy_replay_file = copy_replay_file
    strat_any._copy_strict = strict_requested

    if ct_v2 is not None:
        strat_any._copy_config_v2 = ct_v2
        strat_any._copy_policy_engine = CopyPolicyEngine(
            config=ct_v2,
            reference_price_fn=copy_price_fn,
        )
        strat_any._copy_intent_builder = CopyIntentBuilder(config=ct_v2)
        strat_any._copy_circuit_breaker = CopyCircuitBreaker.from_copy_config(ct_v2)
        ledger_db_path = ct_raw.get("ledger", {}).get("db_path") if isinstance(ct_raw.get("ledger"), dict) else None
        strat_any._copy_ledger = CopyLedger(ledger_db_path or "./almanak_copy_ledger.db")

        exec_policy = ct_v2.execution_policy
        copy_mode_resolved = str(exec_policy.copy_mode)
        replay_path = exec_policy.replay_file
        if normalized_copy_mode is not None:
            copy_mode_resolved = normalized_copy_mode
        if copy_replay_file:
            replay_path = copy_replay_file
            copy_mode_resolved = "replay"
        if copy_shadow:
            copy_mode_resolved = "shadow"

        strat_any._copy_mode = copy_mode_resolved
        strat_any._copy_replay_file = replay_path

        if copy_mode_resolved == "replay" and replay_path:
            replay_runner = CopyReplayRunner(config=ct_v2)
            replay_signals = replay_runner.load_signals(replay_path)
            activity_provider.inject_signals(replay_signals)
            click.echo(f"  Copy replay loaded: {len(replay_signals)} signal(s) from {replay_path}")

    chains_str = ", ".join(sorted(leaders_by_chain.keys()))
    click.echo(f"  Copy trading initialized: monitoring {len(ct_config.leaders)} leader(s) on {chains_str}")


def _maybe_auto_deploy_vault(
    *,
    strategy_config: dict[str, Any],
    resolved_network: str,
    effective_dry_run: bool,
    config_chain: str | None,
    runtime_config: Any,
    gateway_client: Any,
    execution_orchestrator: Any,
    state_manager: Any,
    strategy_instance: Any,
    deployment_id: str,
) -> Any:
    """Return a VaultLifecycleManager or None, auto-deploying on Anvil if placeholder.

    Mirrors the prior vault-lifecycle block in `run()`. When
    ``strategy_config["vault"]`` is absent, returns None. Otherwise loads the
    Lagoon adapters, auto-deploys on Anvil if the vault_address is a
    placeholder (exiting with 0 under ``--dry-run``), and wires the vault's
    persistence callback so state changes flow back through the strategy's
    state manager.

    CRITICAL: This helper MUST run BEFORE the `StrategyRunner` is constructed,
    so the runner picks up the patched ``vault_address`` on initialization.
    """
    if not strategy_config.get("vault"):
        return None

    from ..vault.capability import default_vault_protocol, get_vault_tool_capability
    from ..vault.config import VaultConfig
    from ..vault.lifecycle import VAULT_STATE_KEY, VaultLifecycleManager
    from .run import _auto_deploy_lagoon_vault, _has_placeholder_vault_address

    vault_raw = strategy_config["vault"]

    # Auto-deploy Lagoon vault on Anvil if placeholder address detected
    if resolved_network == "anvil" and _has_placeholder_vault_address(vault_raw):
        if effective_dry_run:
            click.secho(
                "  [DRY-RUN] Vault has placeholder address -- skipping auto-deploy",
                fg="yellow",
            )
            click.echo("  Deploy manually or run without --dry-run on Anvil")
            # Bubble up to `run()` so cleanup_fn runs before exit-0 (see #1682).
            raise _DryRunVaultEarlyExit()

        click.echo("  Placeholder vault address detected -- auto-deploying on Anvil...")
        vault_raw = _auto_deploy_lagoon_vault(
            vault_raw,
            strategy_config.get("chain") or config_chain or "ethereum",
            runtime_config,
            gateway_client,
            execution_orchestrator,
        )

    vault_config = VaultConfig(**vault_raw)
    vault_chain = strategy_config.get("chain", "")
    vault_protocol = default_vault_protocol()
    vault_capability = get_vault_tool_capability(vault_protocol)
    vault_sdk = vault_capability.build_sdk(gateway_client, vault_chain)
    vault_adapter = vault_capability.build_adapter(vault_sdk)

    # Extract initial vault state from persisted strategy state.
    # State loading is deferred to the async phase for IntentStrategy, so we
    # load the raw state here directly from the state manager (safe to use
    # asyncio.run() because we are still in the sync Click command, before any
    # event loop is started).
    initial_vault_state = None
    try:
        import asyncio as _asyncio

        _raw_state_data = _asyncio.run(state_manager.load_state(deployment_id))
        if _raw_state_data and _raw_state_data.state:
            initial_vault_state = _raw_state_data.state.get(VAULT_STATE_KEY)
    except Exception as _e:  # noqa: BLE001
        logger.debug("Could not load persisted state for vault init (deployment_id=%s): %s", deployment_id, _e)
        # No persisted state — VaultLifecycleManager uses defaults
    # Fallback: also check in-memory strategy state (for StrategyBase subclasses)
    if initial_vault_state is None:
        for attr in ("persistent_state", "state"):
            store = getattr(strategy_instance, attr, None)
            if isinstance(store, dict):
                initial_vault_state = store.get(VAULT_STATE_KEY)
                break

    # Wire persistence callback: vault state changes are saved into the
    # strategy's persistent_state dict and persisted via the gateway state manager.
    def _persist_vault_state(vault_state_dict: dict) -> None:
        for attr in ("persistent_state", "state"):
            store = getattr(strategy_instance, attr, None)
            if isinstance(store, dict):
                store[VAULT_STATE_KEY] = vault_state_dict
                if hasattr(strategy_instance, "save_state"):
                    strategy_instance.save_state()
                return

    vault_lifecycle = VaultLifecycleManager(
        vault_config=vault_config,
        vault_sdk=vault_sdk,
        vault_adapter=vault_adapter,
        execution_orchestrator=execution_orchestrator,
        deployment_id=deployment_id,
        initial_vault_state=initial_vault_state,
        persistence_callback=_persist_vault_state,
        receipt_parser_protocol=vault_protocol,
        # Mode-aware failure semantics for the share-backed AUM guard (VIB-5672):
        # dry-run surfaces a violation loudly but continues; live refuses. The local
        # CLI exposes only dry-run here; live is the safe default for real runs.
        execution_mode="dry_run" if effective_dry_run else "live",
    )
    click.echo(
        f"  Vault lifecycle initialized: "
        f"address={vault_config.vault_address}, "
        f"underlying={vault_config.underlying_token}, "
        f"interval={vault_config.settlement_interval_minutes}min"
    )
    return vault_lifecycle


def _reconciliation_enforcement_from_env() -> bool:
    """Return True iff ``ALMANAK_RECONCILIATION_ENFORCEMENT`` opts the CLI back
    into fail-closed reconciliation.

    Default is observation mode (False) until VIB-3348 block-anchored balance
    reads close the false-positive race. Truthy values: ``1``, ``true``, ``yes``
    (case-insensitive, surrounding whitespace tolerated). Anything else — unset,
    empty, ``0``, ``false``, arbitrary strings — returns False. Read via the
    typed CLI-runtime config during the config-service cutover.
    """
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    return _cli_cfg().reconciliation_enforcement


def _reconciliation_confirmation_from_env() -> tuple[int | None, float]:
    """Return ``(depth, timeout_seconds)`` for the VIB-3350 confirmation-depth
    wait from the typed CLI-runtime config.

    Depth is ``None`` (wait OFF) unless ``ALMANAK_RECONCILIATION_CONFIRMATION_DEPTH``
    is set; timeout defaults to 12.0s. Both are validated (and raise loudly) at
    config-parse time.
    """
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    cfg = _cli_cfg()
    return cfg.reconciliation_confirmation_depth, cfg.reconciliation_confirmation_timeout_seconds


def _build_runner(
    *,
    interval: int,
    effective_dry_run: bool,
    deployment_id: str,
    components: ComponentBundle,
    vault_lifecycle: Any,
) -> Any:
    """Construct the `StrategyRunner` with all safety components wired in.

    Reads orchestrator + price_oracle + balance_provider + state_manager
    from ``components``, creates the safety components
    (CircuitBreaker/StuckDetector/OperatorCardGenerator/EmergencyManager),
    and attaches them all to a new StrategyRunner. The runner and safety
    components are written back into ``components`` for downstream phases.
    """
    from ..execution.circuit_breaker import CircuitBreaker
    from ..runner import RunnerConfig, StrategyRunner
    from ..services.emergency_manager import EmergencyManager
    from ..services.operator_card_generator import OperatorCardGenerator
    from ..services.stuck_detector import StuckDetector

    # Create runner config
    _confirmation_depth, _confirmation_timeout = _reconciliation_confirmation_from_env()
    runner_config = RunnerConfig(
        default_interval_seconds=interval,
        dry_run=effective_dry_run,
        enable_state_persistence=True,
        enable_alerting=False,  # No alert manager configured
        reconciliation_enforcement=_reconciliation_enforcement_from_env(),
        reconciliation_confirmation_depth=_confirmation_depth,
        reconciliation_confirmation_timeout_seconds=_confirmation_timeout,
    )

    # Create safety components for fail-closed execution
    circuit_breaker = CircuitBreaker(deployment_id=deployment_id)
    stuck_detector = StuckDetector()
    operator_card_generator = OperatorCardGenerator()
    emergency_manager = EmergencyManager()

    runner = StrategyRunner(
        price_oracle=components.price_oracle,
        balance_provider=components.balance_provider,
        execution_orchestrator=components.execution_orchestrator,
        state_manager=components.state_manager,
        config=runner_config,
        vault_lifecycle=vault_lifecycle,
        circuit_breaker=circuit_breaker,
        stuck_detector=stuck_detector,
        operator_card_generator=operator_card_generator,
        emergency_manager=emergency_manager,
    )

    components.runner = runner
    components.circuit_breaker = circuit_breaker
    components.stuck_detector = stuck_detector
    components.operator_card_generator = operator_card_generator
    components.emergency_manager = emergency_manager

    return runner


def _build_components(
    *,
    strategy_instance: Any,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    strategy_chains: list[str],
    multi_chain: bool,
    resolved_network: str,
    gateway_client: Any,
    chain_wallets: dict[str, str],
    interval: int,
    effective_dry_run: bool,
    deployment_id: str,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    config_chain: str | None,
) -> ComponentBundle:
    """Construct the full runtime component bundle (orchestrator -> runner).

    Internal ordering is load-bearing:

    1. Build orchestrator + price/balance/OHLCV providers + indicators +
       rate/funding providers + optional Solana fork manager.
    2. Wire copy-trading components (single-chain only) — attaches
       attributes like `_copy_mode`/`_copy_replay_file` that some
       strategies consume on first-iteration setup.
    3. Create the gateway-backed state manager and bind it to the strategy.
    4. Auto-deploy the vault if placeholder and build VaultLifecycleManager.
       MUST run before runner construction so the runner sees the patched
       vault_address.
    5. Build the `StrategyRunner` and safety components.

    Exits with status 1 on any unhandled exception (preserving the original
    top-level except block's behavior).
    """
    components = ComponentBundle()

    # Pre-flight validation: fail BEFORE building any gateway-backed resources
    # so a rejected config doesn't leak providers / orchestrators / sockets.
    # This mirrors the defence-in-depth check inside `_init_copy_trading` but
    # runs at a point where no cleanup_fn has been constructed yet (see #1683
    # and CR comment on PR #1689).
    if multi_chain and strategy_config.get("copy_trading"):
        raise click.ClickException(
            "copy_trading is not yet supported for multi-chain strategies. "
            "Remove the copy_trading block or configure the strategy as single-chain."
        )

    try:
        click.echo("Initializing components...")

        _build_orchestrator_and_providers(
            multi_chain=multi_chain,
            runtime_config=runtime_config,
            strategy_chains=strategy_chains,
            strategy_config=strategy_config,
            resolved_network=resolved_network,
            gateway_client=gateway_client,
            chain_wallets=chain_wallets,
            strategy_instance=strategy_instance,
            components=components,
        )

        # Copy-trading comes AFTER provider wiring so the price_oracle is
        # available to drive the signal-engine price_fn. Single-chain only
        # (the original code places this branch inside the else:).
        _init_copy_trading(
            strategy_instance=strategy_instance,
            strategy_config=strategy_config,
            runtime_config=runtime_config,
            gateway_client=gateway_client,
            price_oracle=components.price_oracle,
            normalized_copy_mode=normalized_copy_mode,
            copy_replay_file=copy_replay_file,
            copy_shadow=copy_shadow,
            copy_strict=copy_strict,
            multi_chain=multi_chain,
        )

        # Create state manager - always use gateway-backed state manager
        from ..state.gateway_state_manager import GatewayStateManager

        state_manager = GatewayStateManager(gateway_client)
        click.echo("  Using gateway-backed state manager")
        components.state_manager = state_manager

        # Inject state manager into strategy for persistence.
        # State loading is deferred to the async setup phase (run_once_with_cleanup /
        # run_loop_with_cleanup) so that load_state_async() can be awaited properly.
        if hasattr(strategy_instance, "set_state_manager"):
            strategy_instance.set_state_manager(state_manager, deployment_id)

        # Vault auto-deploy MUST happen before runner construction so that the
        # StrategyRunner sees the patched vault_address when it initializes.
        vault_lifecycle = _maybe_auto_deploy_vault(
            strategy_config=strategy_config,
            resolved_network=resolved_network,
            effective_dry_run=effective_dry_run,
            config_chain=config_chain,
            runtime_config=runtime_config,
            gateway_client=gateway_client,
            execution_orchestrator=components.execution_orchestrator,
            state_manager=state_manager,
            strategy_instance=strategy_instance,
            deployment_id=deployment_id,
        )

        _build_runner(
            interval=interval,
            effective_dry_run=effective_dry_run,
            deployment_id=deployment_id,
            components=components,
            vault_lifecycle=vault_lifecycle,
        )

        click.echo("Components initialized successfully")

    except click.ClickException:
        # Preserve explicit ClickException (e.g., missing wallet address)
        # so the caller sees the same message as the original code.
        raise
    except _DryRunVaultEarlyExit as e:
        # Attach the partial component bundle so the caller can still run
        # cleanup_fn before exiting 0 (see #1682).
        e.components = components
        raise
    except SystemExit:
        # Preserve any sys.exit() inside the helpers.
        raise
    except Exception as e:
        click.echo(f"Error initializing components: {e}", err=True)
        logger.exception("Component initialization failed")
        sys.exit(1)

    return components
