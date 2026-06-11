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

# Recognised vendor keys for ``ChainDescriptor.external_ids`` (VIB-4851 B1).
# Each key is a third-party data/integration vendor whose per-chain
# identifier the SDK previously kept in a standalone vendor-side map; the
# descriptor now owns the value. ``ChainDescriptor.__post_init__`` rejects any
# external_ids key not in this set so a typo'd vendor (e.g. ``"gecko"``) fails
# loudly at registration rather than silently producing an unreachable id.
#
# ``dexscreener`` and ``geckoterminal`` each collapse two legacy maps that were
# verified value-identical on every shared chain (DexScreener:
# ``CHAIN_TO_DEXSCREENER_PLATFORM`` + ``CHAIN_SLUG_MAP``; GeckoTerminal:
# ``_CHAIN_TO_NETWORK`` + ``_CHAIN_TO_GT_NETWORK``). ``defillama`` (lowercase
# slug) and ``defillama_display`` (Capitalised display name) cover the same
# chains but carry distinct value formats, so they remain separate keys.
KNOWN_VENDORS: frozenset[str] = frozenset(
    {
        "coingecko",
        "dexscreener",
        "geckoterminal",
        "defillama",
        "defillama_display",
        "zerion",
        "moralis",
        "okx",
        # Tenderly DASHBOARD slug for trace URLs
        # (https://dashboard.tenderly.co/tx/{slug}/{hash}). NOT the Tenderly
        # simulation network id — that is always str(chain_id) by
        # SimulationProfile design and is deliberately not stored anywhere.
        "tenderly",
    }
)


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
        coingecko_id: CoinGecko COIN id of the native asset (e.g.
            ``"ethereum"``, ``"avalanche-2"``, ``"sonic-3"``). Distinct from
            ``ChainDescriptor.external_ids["coingecko"]``, which is the
            vendor's PLATFORM id for the chain ("price a token by contract
            address on this chain"); this field prices the gas asset itself.
            ``None`` means "not yet verified against CoinGecko" — the asset
            simply stays absent from derived price maps (legacy miss
            semantics). Mirrors the native rows of the legacy per-chain
            ``*_TOKEN_IDS`` maps (VIB-4851 Phase E, CS-3b; drift precedent
            VIB-3805).
        wrapped_symbol: The wrapped-native ERC-20's actual on-chain symbol
            (``"WETH"``, ``"WMNT"``, ``"wS"``, ``"W0G"``). Stored verbatim,
            including case — NEVER derived as ``"W" + symbol`` (zerog wraps
            ``A0GI`` as ``W0G``; sonic's contract symbol is ``"wS"``).
        wrapped_coingecko_id: CoinGecko COIN id used to price the wrapped
            native. Chains whose wrapper has its own listing use it
            (ethereum-family ``"weth"``, zerog ``"wrapped-0g"``); the rest
            alias the native id (the established WAVAX/WBNB/WSOL/WMNT
            pattern from the legacy maps). ``None`` == unverified/absent.
    """

    symbol: str
    name: str
    decimals: int
    wrapped_address: str | None = None
    accepted_symbols: tuple[str, ...] = ()
    coingecko_id: str | None = None
    wrapped_symbol: str | None = None
    wrapped_coingecko_id: str | None = None


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
        rate_limit_rpm: Gateway-side RPC rate-limit budget in requests per
            minute. Mirrors the chain half of the legacy
            ``gateway/services/rpc_service.py`` ``CHAIN_RATE_LIMITS`` dict.
            ``None`` means "no declared budget" — the gateway lookup site
            falls back to its own conservative default, preserving the
            legacy ``CHAIN_RATE_LIMITS.get(chain, <default>)`` miss
            semantics. VIB-4851 (Phase E, CS-3).
        fork_requires_archive: ``True`` when free-tier public RPCs for this
            chain lack archive state, so managed-Anvil fork operations
            (``eth_getStorageAt`` for ERC-20 approvals, etc.) fail without
            an archive-capable RPC (Alchemy key or chain-specific URL).
            Mirrors the legacy ``gateway/managed.py``
            ``ARCHIVE_RPC_REQUIRED_CHAINS`` membership (VIB-3971,
            VIB-3973 Part B). Drives the pre-fork warning only — not a
            hard gate. VIB-4851 (Phase E, CS-3).
    """

    public_rpc: str | None = None
    alchemy_prefix: str | None = None
    tenderly_subdomain: str | None = None
    anvil_port: int | None = None
    poa: bool = False
    block_time_seconds: float | None = None
    rate_limit_rpm: int | None = None
    fork_requires_archive: bool = False


@dataclass(frozen=True)
class ChainlinkFeeds:
    """Chainlink aggregator addresses for this chain (VIB-4851 Phase E, CS-5).

    A dumb frozen pair→aggregator map — feed-SELECTION policy (USD-first,
    ETH-denominated fallback, staleness thresholds) stays with the
    consumers in ``almanak/core/chainlink.py`` and the price sources; only
    the per-chain ADDRESSES live here. Mirrors the chain half of the
    legacy ``CHAINLINK_PRICE_FEEDS`` / ``ETH_DENOMINATED_FEEDS`` dicts.

    Attributes:
        usd_feeds: ``"TOKEN/USD"`` pair → aggregator address.
        eth_denominated: ``"TOKEN/ETH"`` pair → aggregator address, for
            tokens whose USD price is derived as TOKEN/ETH × ETH/USD.
            Empty for chains without such feeds.
    """

    usd_feeds: Mapping[str, str]
    eth_denominated: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "usd_feeds", MappingProxyType(dict(self.usd_feeds)))
        object.__setattr__(self, "eth_denominated", MappingProxyType(dict(self.eth_denominated)))


# Recognised keys for ``ChainDescriptor.contracts`` (VIB-4851 CS-5). Same
# fail-loudly contract as KNOWN_VENDORS: a typo'd key raises at
# registration. Protocol-owned contract addresses (position managers,
# routers, …) do NOT belong here — they live on the owning connector's
# address tables (AddressRegistry); this map is only for chain-level
# infrastructure contracts the framework itself signs against.
KNOWN_CONTRACT_KEYS: frozenset[str] = frozenset({"safe_multisend"})


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
        browse_url: Human-facing web explorer origin (e.g.
            ``"https://arbiscan.io"``, no trailing slash, no path).
            Consumers compose ``{browse_url}/tx/{hash}`` etc. Mirrors the
            chain half of the legacy dashboard / API timeline explorer-URL
            maps (VIB-4851 Phase E, CS-4). ``None`` means "no known web
            explorer" — consumers keep their legacy miss fallbacks.
    """

    api_url: str | None = None
    api_key_env: str | None = None
    browse_url: str | None = None


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
        external_ids: Sparse, vendor-keyed mapping from a third-party data /
            integration vendor (see :data:`KNOWN_VENDORS`) to that vendor's
            per-chain identifier — e.g. ``{"coingecko": "arbitrum-one",
            "okx": "42161"}``. Mirrors the chain half of the legacy
            vendor-side maps (CoinGecko ``COINGECKO_PLATFORM_IDS``,
            DexScreener, GeckoTerminal, DeFiLlama, Zerion, Moralis, OKX) so a
            vendor identifier derives from the registry instead of a
            standalone dict. **Sparse**: a chain declares only the vendors it
            is actually supported on; ``None`` means "no vendor identifiers
            today" (matches the legacy ``map.get(chain)`` → ``None`` miss
            semantics). Values are stored **verbatim, including case** — the
            distinction between ``"ethereum"`` (DeFiLlama slug) and
            ``"Ethereum"`` (DeFiLlama display) is load-bearing, so only the
            vendor *key* is lowercased, never the value. An unknown vendor key
            raises ``ValueError`` at construction. Frozen at construction;
            mutating after returns has no effect. VIB-4851 (B1).
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
    external_ids: Mapping[str, str] | None = None
    chainlink: ChainlinkFeeds | None = None
    contracts: Mapping[str, str] | None = None
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
                MappingProxyType({k.lower(): v for k, v in self.tokens.items()}),
            )
        # Freeze the optional vendor-keyed external_ids the same way, lowercasing
        # only the vendor KEY (values are verbatim — "ethereum" vs "Ethereum" is
        # meaningful). Reject any key outside KNOWN_VENDORS so a typo'd vendor
        # fails loudly at registration rather than silently producing an id that
        # no lookup will ever find. VIB-4851 (B1).
        if self.external_ids is not None:
            frozen_external_ids = {k.lower(): v for k, v in self.external_ids.items()}
            unknown = sorted(frozen_external_ids.keys() - KNOWN_VENDORS)
            if unknown:
                raise ValueError(
                    f"ChainDescriptor {self.name!r} declares unknown external_ids "
                    f"vendor key(s) {unknown}; known vendors are "
                    f"{sorted(KNOWN_VENDORS)}"
                )
            object.__setattr__(
                self,
                "external_ids",
                MappingProxyType(frozen_external_ids),
            )
        # Freeze + validate the optional chain-infrastructure contracts map
        # (VIB-4851 CS-5). Unknown keys fail loudly at registration.
        if self.contracts is not None:
            frozen_contracts = dict(self.contracts)
            unknown_keys = sorted(frozen_contracts.keys() - KNOWN_CONTRACT_KEYS)
            if unknown_keys:
                raise ValueError(
                    f"ChainDescriptor {self.name!r} declares unknown contracts "
                    f"key(s) {unknown_keys}; known keys are "
                    f"{sorted(KNOWN_CONTRACT_KEYS)}"
                )
            object.__setattr__(self, "contracts", MappingProxyType(frozen_contracts))
