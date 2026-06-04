"""ChainDescriptor — single source of truth for per-chain configuration.

A ``ChainDescriptor`` consolidates everything the SDK needs to know about a
single chain:

* Identity: ``enum`` (the ``Chain`` enum member), ``name`` (canonical lowercase
  string), ``aliases`` (e.g. ``"bnb"`` for ``Chain.BSC``).
* Wire format: ``chain_id`` (EIP-155). The numeric value is the on-the-wire
  identifier owned by the ``metrics-database`` repo — restructuring how we
  source it in the SDK is fine, **renumbering it is not**.
* Family: ``family`` (EVM vs SOLANA — routes signing / address format / tx model).
* Native token: ``NativeToken`` (symbol, name, decimals, wrapped address).
* Gas profile: ``GasProfile`` (buffer multiplier, price/cost caps, simulation buffer).
* Timeouts: ``Timeouts`` (tx confirmation, gRPC Execute call).

Per-chain descriptor files live as siblings (``ethereum.py``, ``arbitrum.py``,
``base.py``, ...). Each registers itself via ``@register_chain`` into the
singleton ``ChainRegistry`` at import time.

VIB-4801 (parent epic VIB-4800).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from almanak.core.enums import Chain, ChainFamily


@dataclass(frozen=True)
class NativeToken:
    """Native-gas token metadata for a chain.

    Attributes:
        symbol: e.g. ``"ETH"``, ``"AVAX"``, ``"BNB"``.
        name: Human-readable name (e.g. ``"Ethereum"``, ``"BNB"``).
        decimals: Decimal places (18 for every EVM chain, 9 for SOL).
        wrapped_address: Address of the wrapped ERC-20 (or SPL mint for Solana).
            ``None`` is reserved for chains with no canonical wrapper — every
            chain currently registered has one.
        accepted_symbols: Extra symbols that ALSO denote this chain's native coin
            for balance-routing / native-detection (NOT for gas pricing or funding,
            which stay pinned to :attr:`symbol`). Empty for every chain except a
            rename-dual like Polygon, where ``symbol="MATIC"`` and
            ``accepted_symbols=("POL",)`` so both the legacy and post-rename symbol
            route to the native-balance path. The accepted set is derived as
            ``{symbol, *accepted_symbols}`` — see
            ``almanak.core.chains._helpers.native_symbols_for`` (VIB-4851 A1).
    """

    symbol: str
    name: str
    decimals: int
    wrapped_address: str | None = None
    accepted_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class GasProfile:
    """Per-chain gas knobs.

    Every field is :data:`Optional` — ``None`` means "this chain has no
    entry in the corresponding legacy dict; let the consumer's
    ``.get(chain, DEFAULT)`` fall back". The legacy dicts had asymmetric
    coverage (e.g. ``CHAIN_GAS_COST_CAPS_NATIVE`` only covered 12 of 16
    EVM chains), and we preserve that asymmetry byte-for-byte to avoid
    behavior changes at the lookup boundary.

    Attributes:
        buffer: Multiplier applied to raw gas estimates from simulation /
            ``eth_estimateGas`` (mirrors ``CHAIN_GAS_BUFFERS``).
        simulation_buffer: Decimal fraction added on top of post-simulation
            gas (mirrors ``CHAIN_SIMULATION_BUFFERS``; 0.1 == 10%).
        price_cap_gwei: Recommended maximum gas price in gwei
            (mirrors ``CHAIN_GAS_PRICE_CAPS_GWEI``).
        cost_cap_native: Recommended maximum gas cost in native units
            (mirrors ``CHAIN_GAS_COST_CAPS_NATIVE``).
        operation_overrides: Per-operation gas-estimate overrides keyed by
            operation name (e.g. ``"swap_simple"``, ``"lp_mint"``) — mirrors
            the chain half of the legacy
            ``CHAIN_GAS_OVERRIDES[chain]`` dict in
            ``framework/intents/compiler_constants.py``. ``None`` means
            "no chain-specific overrides; use ``DEFAULT_GAS_ESTIMATES``".
            VIB-4857 (W5).
        fallback_base_fee_gwei: Typical base fee in gwei for backtesting
            fallback estimation — mirrors
            ``DEFAULT_GAS_PRICES[chain]["base_fee"]`` in
            ``framework/backtesting/pnl/providers/gas.py``. ``None`` means
            the consumer falls back to the framework-wide ethereum default.
            VIB-4857 (W5).
        fallback_priority_fee_gwei: Typical priority fee (tip) in gwei for
            backtesting fallback estimation — mirrors
            ``DEFAULT_GAS_PRICES[chain]["priority_fee"]``.  VIB-4857 (W5).
    """

    buffer: float | None = None
    simulation_buffer: float | None = None
    price_cap_gwei: int | None = None
    cost_cap_native: float | None = None
    operation_overrides: Mapping[str, int] | None = None
    fallback_base_fee_gwei: float | None = None
    fallback_priority_fee_gwei: float | None = None

    def __post_init__(self) -> None:
        # Freeze the optional operation-overrides mapping so descriptors
        # remain truly immutable. Using ``object.__setattr__`` because the
        # dataclass is ``frozen=True``.
        #
        # We unconditionally wrap a fresh ``dict(...)`` snapshot — even when
        # the caller already passed a ``MappingProxyType``, because a proxy
        # still mirrors its backing dict's mutations. Re-wrapping a copy is
        # the only way to guarantee the descriptor's view never changes
        # after construction (CodeRabbit, VIB-4857).
        if self.operation_overrides is not None:
            object.__setattr__(
                self,
                "operation_overrides",
                MappingProxyType(dict(self.operation_overrides)),
            )


@dataclass(frozen=True)
class Timeouts:
    """Per-chain timeouts.

    Attributes:
        tx_confirmation: Seconds to wait for a tx to land
            (mirrors ``CHAIN_TX_TIMEOUTS``). ``None`` falls back to the
            framework default.
        grpc_execute: Seconds for the gateway gRPC ``Execute`` call
            (mirrors ``CHAIN_GRPC_EXECUTE_TIMEOUTS``). ``None`` falls back to
            the framework default.
        receipt_polling: Seconds for the local ``ChainExecutor`` receipt-
            polling loop — mirrors the legacy
            ``CHAIN_RECEIPT_TIMEOUTS`` dict in
            ``framework/execution/chain_executor.py`` (separate from
            ``tx_confirmation`` because chain_executor is the no-gateway
            local path and has different empirically-measured Anvil-fork
            timings). ``None`` falls back to ``DEFAULT_RECEIPT_TIMEOUT``
            (120s). VIB-4857 (W5).
    """

    tx_confirmation: int | None = None
    grpc_execute: int | None = None
    receipt_polling: int | None = None


@dataclass(frozen=True)
class RpcProfile:
    """Per-chain RPC / Anvil / node-provider metadata.

    Replaces the legacy ``config/rpc_defaults.json`` file. Every field is
    :data:`Optional` so registered chains that have no RPC routing today
    (e.g. ``berachain``, ``blast``) can keep an empty profile without
    pretending coverage exists. Mirrors the asymmetric coverage pattern
    used by :class:`GasProfile`.

    Attributes:
        public_rpc: Free, no-API-key public RPC URL (PublicNode / official
            chain RPC). Used as last-resort fallback when no custom URL or
            API key is configured.
        alchemy_prefix: Prefix that constructs
            ``https://{prefix}-{network}.g.alchemy.com/v2/{api_key}``
            (e.g. ``"eth"``, ``"arb"``, ``"opt"``).
        tenderly_subdomain: Subdomain that constructs
            ``https://{subdomain}.gateway.tenderly.co/{api_key}``
            (e.g. ``"mainnet"``, ``"arbitrum"``). Only set for chains we
            actually route through Tenderly.
        anvil_port: Default port for the chain's managed Anvil fork.
            Picked to avoid collisions across the multi-chain Anvil cluster.
        poa: Whether the chain requires POA middleware (Avalanche, Polygon,
            BSC). When ``True``, ``get_cached_web3`` injects
            ``ExtraDataToPOAMiddleware`` so ``eth.get_block("latest")``
            does not reject the 32-byte ``extraData`` field.
        block_time_seconds: Average block time in seconds. Used by the
            backtesting archive-RPC provider to estimate historical block
            numbers from timestamps. ``None`` means "no archive-RPC support
            for this chain in backtesting"; mirrors the legacy
            ``block_times`` literal in ``framework/backtesting/pnl/
            providers/gas.py`` and the membership of ``ARCHIVE_RPC_CHAINS``.
            VIB-4857 (W5).
    """

    public_rpc: str | None = None
    alchemy_prefix: str | None = None
    tenderly_subdomain: str | None = None
    anvil_port: int | None = None
    poa: bool = False
    block_time_seconds: float | None = None


@dataclass(frozen=True)
class Explorer:
    """Per-chain block-explorer (Etherscan-compatible) API metadata.

    Mirrors the chain half of the legacy ``ETHERSCAN_API_URLS`` and
    ``ETHERSCAN_API_KEY_ENV_VARS`` dicts in
    ``framework/backtesting/pnl/providers/gas.py``. Every field is
    :data:`Optional`; chains without an Etherscan-compatible explorer
    (e.g. Solana, Berachain) leave the profile empty. VIB-4857 (W5).

    Attributes:
        api_url: Etherscan-compatible API endpoint (e.g.
            ``"https://api.etherscan.io/api"``). ``None`` for chains
            without an Etherscan-compatible API.
        api_key_env: Environment-variable name carrying the per-chain
            API key (e.g. ``"ARBISCAN_API_KEY"``). ``None`` for chains
            without an Etherscan-compatible API.
    """

    api_url: str | None = None
    api_key_env: str | None = None


@dataclass(frozen=True)
class SimulationProfile:
    """Tenderly / Alchemy transaction-SIMULATION-API support for this chain (VIB-4851).

    Distinct from RpcProfile.{tenderly_subdomain,alchemy_prefix}, which model READ
    routing through provider RPC gateways — this models SIMULATION-API membership.
    The Tenderly network-id VALUE is NOT stored here; it is always
    str(ChainDescriptor.chain_id) (kills the historical 1648-vs-9745 drift).

    Attributes:
        tenderly_supported: Whether Tenderly's Transaction Simulator covers this chain.
        alchemy_network: Alchemy simulateExecutionBundle network name (e.g.
            "eth-mainnet"), or None when Alchemy simulation is unsupported.
    """

    tenderly_supported: bool = False
    alchemy_network: str | None = None


@dataclass(frozen=True)
class ChainDescriptor:
    """Single source of truth for per-chain configuration.

    Construction is always through a ``@register_chain``-decorated module
    under ``almanak/core/chains/``. Consumers read via ``ChainRegistry``;
    descriptors are immutable.

    Attributes:
        enum: The :class:`Chain` enum member.
        name: Canonical lowercase name (e.g. ``"ethereum"``).
            Always equal to ``enum.name.lower()`` — never diverge.
        chain_id: EIP-155 chain ID. ``0`` is reserved for non-EVM chains
            (Solana).
        family: Execution family (EVM vs SOLANA).
        native: ``NativeToken`` — symbol, decimals, wrapped address.
        gas: ``GasProfile`` — buffer, caps, simulation buffer.
        timeouts: ``Timeouts`` — tx confirmation + gRPC Execute.
        rpc: ``RpcProfile`` — public RPC fallback, Alchemy / Tenderly
            routing keys, Anvil port, POA flag. Default-empty so chains
            with no RPC routing today stay byte-for-byte equivalent.
        explorer: ``Explorer`` — Etherscan-compatible API URL + API-key
            env-var name. Default-empty for chains without an Etherscan-
            compatible explorer (e.g. Solana, Berachain).
            VIB-4857 (W5).
        simulation: ``SimulationProfile`` — Tenderly / Alchemy
            transaction-SIMULATION-API membership. Default-empty
            (``tenderly_supported=False``, ``alchemy_network=None``) for
            chains the simulators do not cover. The framework
            ``simulator/config.py`` maps derive from this field instead of
            hardcoding chain-name literals. VIB-4851.
        tokens: Mapping from lowercase token symbol (e.g. ``"usdc"``,
            ``"weth"``) to its chain-canonical ERC-20 address. Mirrors the
            chain half of the legacy
            ``almanak.framework.intents.compiler_constants.CHAIN_TOKENS``
            dict that drove fee-tier selection + Zodiac permission
            discovery. ``None`` means "the chain has no known-tokens
            catalogue today" (matches ``CHAIN_TOKENS.get(chain, {})``
            semantics — consumers must handle empty / missing lookups).
            Frozen at construction; mutating after returns has no effect.
            VIB-4872 (W6-followup).
        aliases: Extra alternative names that resolve to this chain
            (e.g. ``("bnb", "binance")`` for BSC). The canonical ``name``
            is always implicit and need not be repeated here.
    """

    enum: Chain
    name: str
    chain_id: int
    family: ChainFamily
    native: NativeToken
    gas: GasProfile
    timeouts: Timeouts = field(default_factory=Timeouts)
    rpc: RpcProfile = field(default_factory=RpcProfile)
    explorer: Explorer = field(default_factory=Explorer)
    simulation: SimulationProfile = field(default_factory=SimulationProfile)
    tokens: Mapping[str, str] | None = None
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Strong invariant: ``name`` always equals the lowercase enum name.
        # If they drift, downstream lookups break in subtle ways.
        if self.name != self.enum.name.lower():
            raise ValueError(
                f"ChainDescriptor.name {self.name!r} must equal enum name "
                f"{self.enum.name.lower()!r} (enum: {self.enum.name})"
            )
        # Freeze the optional tokens mapping the same way GasProfile freezes
        # its operation_overrides — wrap a defensive snapshot in
        # MappingProxyType so descriptor immutability survives even if the
        # caller passed a mutable dict.
        if self.tokens is not None:
            object.__setattr__(
                self,
                "tokens",
                MappingProxyType({k.lower(): v for k, v in dict(self.tokens).items()}),
            )
