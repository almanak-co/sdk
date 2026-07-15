"""ChainDescriptor — single source of truth for per-chain configuration.

A ``ChainDescriptor`` consolidates everything the SDK needs to know about a
single chain:

* Identity: ``name`` (canonical lowercase string, the sole chain identity —
  VIB-4851), ``aliases`` (e.g. ``"bnb"`` for BSC).
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

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from almanak.core.enums import ChainFamily

# CAIP-2 (https://chainagnostic.org/CAIPs/caip-2) blockchain-id namespace per
# execution family. EVM chains serialize as ``eip155:<chain_id>``; non-EVM
# families (Solana, ...) carry an explicit reference — see
# ``ChainDescriptor.caip2_reference`` — because their ``chain_id`` is the ``0``
# non-EVM sentinel and cannot serve as a CAIP-2 reference. Every ChainFamily
# member MUST appear here or ``ChainDescriptor.caip2`` raises KeyError at first
# use. VIB-5175 (CAIP-2/19 adoption, Phase 1).
CAIP2_NAMESPACE_BY_FAMILY: Mapping[ChainFamily, str] = MappingProxyType(
    {
        ChainFamily.EVM: "eip155",
        ChainFamily.SOLANA: "solana",
    }
)

# CAIP-2 reference grammar (from the spec): ``[-_a-zA-Z0-9]{1,32}``.
_CAIP2_REFERENCE_RE = re.compile(r"^[-_a-zA-Z0-9]{1,32}$")

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
        slip44: SLIP-44 (https://github.com/satoshilabs/slips/blob/master/
            slip-0044.md) registered coin type for this chain's native asset,
            used as the CAIP-19 native-asset reference (``slip44:<coin_type>``).
            ``None`` means "no verified SLIP-44 coin type for this chain yet" —
            ``TokenRef.to_caip19()`` then fails loudly rather than emit a
            non-standard native id, so values can be populated incrementally.
            Populate from the SLIP-44 registry, NEVER guessed (ETH-denominated
            chains are 60; SOL is 501). VIB-5175 (CAIP adoption, Phase 1).
    """

    symbol: str
    name: str
    decimals: int
    wrapped_address: str | None = None
    accepted_symbols: tuple[str, ...] = ()
    coingecko_id: str | None = None
    wrapped_symbol: str | None = None
    wrapped_coingecko_id: str | None = None
    slip44: int | None = None

    def __post_init__(self) -> None:
        # SLIP-44 coin types are non-negative registry indices. A negative
        # value is a copy/paste bug; fail loudly at registration like the
        # sibling descriptor validations.
        if self.slip44 is not None and self.slip44 < 0:
            raise ValueError(f"NativeToken slip44 must be non-negative, got {self.slip44}")


# Recognised L1 fee-oracle mechanism kinds (Plan 026). Each string names the
# on-chain precompile/predeploy mechanism used to fetch L1 data-cost for an L2
# chain. ``GasProfile.__post_init__`` rejects any kind not in this set so a
# typo'd value fails loudly at registration. Idiom mirrors ``KNOWN_CONTRACT_KEYS``
# (:340) and the ``KNOWN_VENDORS`` check (:522-535).
#
# Supported values:
#   "arbitrum_nodeinterface" — Arbitrum ArbGasInfo precompile
#                             (0x000000000000000000000000000000000000006C)
#   "op_gaspriceoracle"     — OP-stack GasPriceOracle predeploy
#                             (0x420000000000000000000000000000000000000F)
KNOWN_L1_FEE_ORACLE_KINDS: frozenset[str] = frozenset(
    {
        "arbitrum_nodeinterface",
        "op_gaspriceoracle",
    }
)


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
        min_priority_fee_gwei: **Live-submit** priority-fee (tip) floor in
            gwei. Distinct from ``fallback_priority_fee_gwei`` (a backtest
            *estimate*): this is the hard minimum the live EIP-1559 fee
            builder floors the RPC's ``eth_maxPriorityFeePerGas`` suggestion
            to, so a node that legitimately returns ``0`` (common on L1) does
            not produce a tip≈0 tx that stalls when the base fee rises
            (VIB-5419). ``None`` / ``0.0`` means "no live floor" — correct
            for L2s whose near-zero base fees let even a zero tip land. L1
            (ethereum) carries a real floor (~2 gwei); validator-gated chains
            (polygon ~30 gwei) and avalanche (~1 gwei) carry their own.
        l1_fee_oracle_kind: L1 fee-oracle mechanism this chain uses for L2
            data-cost estimation. One of ``KNOWN_L1_FEE_ORACLE_KINDS``
            (``"arbitrum_nodeinterface"`` or ``"op_gaspriceoracle"``).
            ``None`` means this chain has no L1 data-cost oracle (i.e. it
            is not an L2 that posts calldata to Ethereum mainnet). Plan 026.
        l1_fee_oracle_address: Address of the precompile / predeploy
            contract corresponding to ``l1_fee_oracle_kind``. Must be set
            iff ``l1_fee_oracle_kind`` is set; must be a ``0x``-prefixed
            40-hex-char string, case-insensitive — EIP-55 checksum is NOT
            enforced here (``almanak.core`` deliberately carries no
            eth-ecosystem imports); consumers canonicalize at the call
            site via ``web3.to_checksum_address`` (see
            ``framework/data/defi/gas.py`` fetchers). Plan 026.
    """

    buffer: float | None = None
    simulation_buffer: float | None = None
    price_cap_gwei: int | None = None
    cost_cap_native: float | None = None
    operation_overrides: Mapping[str, int] | None = None
    fallback_base_fee_gwei: float | None = None
    fallback_priority_fee_gwei: float | None = None
    min_priority_fee_gwei: float | None = None
    l1_fee_oracle_kind: str | None = None
    l1_fee_oracle_address: str | None = None

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
        # ``min_priority_fee_gwei`` feeds ``priority_fee_floor_wei()`` whose
        # contract is ``>= 0``; reject negative descriptors at construction so a
        # bad value can never propagate into live EIP-1559 fee params.
        if self.min_priority_fee_gwei is not None and self.min_priority_fee_gwei < 0:
            raise ValueError(f"GasProfile min_priority_fee_gwei must be non-negative, got {self.min_priority_fee_gwei}")
        # Validate l1_fee_oracle_kind / l1_fee_oracle_address pairing (Plan 026).
        # Kind must be in KNOWN_L1_FEE_ORACLE_KINDS; address must be set iff kind
        # is set and must be a 0x-prefixed 40-hex-char string. Idiom mirrors the
        # KNOWN_VENDORS check in ChainDescriptor.__post_init__ (:522-535).
        if self.l1_fee_oracle_kind is not None:
            if self.l1_fee_oracle_kind not in KNOWN_L1_FEE_ORACLE_KINDS:
                raise ValueError(
                    f"GasProfile declares unknown l1_fee_oracle_kind "
                    f"{self.l1_fee_oracle_kind!r}; known kinds are "
                    f"{sorted(KNOWN_L1_FEE_ORACLE_KINDS)}"
                )
            if self.l1_fee_oracle_address is None:
                raise ValueError(
                    f"GasProfile sets l1_fee_oracle_kind={self.l1_fee_oracle_kind!r} "
                    f"but l1_fee_oracle_address is None — address is required when "
                    f"kind is set"
                )
        if self.l1_fee_oracle_address is not None:
            addr = self.l1_fee_oracle_address
            if not (
                isinstance(addr, str)
                and addr.startswith("0x")
                and len(addr) == 42
                and all(c in "0123456789abcdefABCDEF" for c in addr[2:])
            ):
                raise ValueError(
                    f"GasProfile l1_fee_oracle_address {addr!r} must be a 0x-prefixed 40-hex-character string"
                )
            if self.l1_fee_oracle_kind is None:
                raise ValueError(
                    f"GasProfile sets l1_fee_oracle_address={addr!r} "
                    f"but l1_fee_oracle_kind is None — kind is required when "
                    f"address is set"
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
class AnvilProfile:
    """Anvil fork-TEST funding facts (VIB-4851 Phase E, CS-6).

    Test-infrastructure data deliberately kept OUT of the production
    structures (``GasProfile`` / ``RpcProfile`` / ``tokens``): the funding
    catalogue here is a superset of the production token catalogue and
    must never widen intent compilation or Zodiac permission discovery.

    Keys are stored VERBATIM, never lowercased — display case is
    load-bearing (``"USDC.e"``, ``"wstETH"``): the cross-table invariant
    tests compare case-sensitively, and every runtime lookup is already
    case-normalized by the consumer (fork_manager builds ``*_ci`` maps).

    Attributes:
        funding_tokens: Display-cased symbol → ERC-20 address for managed
            Anvil funding (legacy ``fork_manager.TOKEN_ADDRESSES``).
        balance_slots: Display-cased symbol → ``balanceOf`` storage slot
            for slot-patch funding (legacy ``KNOWN_BALANCE_SLOTS``).
        whale_funded_tokens: UPPERCASE symbol → whale address fallback for
            impersonation funding when slot-patching fails (legacy
            ``WHALE_FUNDED_TOKENS``).
        wrapped_native_deposit: ``True`` when the chain's wrapped-native
            contract is verified WETH9-style deposit()-fundable on a fork
            (legacy ``WRAPPED_NATIVE_TOKENS`` membership). Deliberately a
            separate gate from ``NativeToken.wrapped_symbol`` — gating on
            the production field would silently widen the deposit path to
            chains whose wrappers are unverified.
        block_gas_limit: ``anvil --gas-limit`` override. Only Mantle —
            non-standard gas accounting (VIB-3666/VIB-3746).
    """

    funding_tokens: Mapping[str, str] | None = None
    balance_slots: Mapping[str, int] | None = None
    whale_funded_tokens: Mapping[str, str] | None = None
    wrapped_native_deposit: bool = False
    block_gas_limit: int | None = None

    def __post_init__(self) -> None:
        for attr in ("funding_tokens", "balance_slots", "whale_funded_tokens"):
            value = getattr(self, attr)
            if value is not None:
                object.__setattr__(self, attr, MappingProxyType(dict(value)))


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
KNOWN_CONTRACT_KEYS: frozenset[str] = frozenset(
    {
        "safe_multisend",
        "safe_proxy_factory_v1_4_1",
        "safe_l2_singleton_v1_4_1",
        "zodiac_module_proxy_factory",
        "zodiac_roles_modifier_singleton",
        "enso_delegate_primary",
        "enso_delegate_secondary",
    }
)


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
        name: Canonical lowercase name (e.g. ``"ethereum"``) — the sole
            chain identity (the Chain enum was removed, VIB-4851). Must
            equal the descriptor module's file stem.
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
        canonical_stable: The chain's canonical USD stablecoin SYMBOL (e.g.
            ``"USDG"``) — the dollar that actually has routable liquidity here
            and that protocols on this chain denominate in. **Sparse and
            deliberately so**: declare it ONLY where the chain has a single
            verifiable answer that a registry-ordering heuristic gets WRONG.
            ``None`` (the default, and the case for every chain that has a
            liquid Circle-USDC) means "no chain-specific override" — consumers
            fall back to their own ordering, which is already correct there.

            This is a PICK, never an existence check. It answers "which dollar
            do we mean on this chain?", NOT "is token X registered here?" — the
            latter is ``TokenResolver``'s job, and ``TokenResolver`` does not
            read this field or ``tokens``. Do not infer one from the other:
            ``tokens`` is ``None`` on several chains where USDC nonetheless
            resolves fine (berachain, solana), because symbol resolution is
            driven by an independent catalogue.

            The declared symbol MUST resolve on this chain. ``core`` cannot
            import the ``framework`` token layer to check that at construction
            (backward import), so the invariant is enforced by a unit test
            (``tests/unit/core/test_chain_canonical_stable.py``). VIB-5727.
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
        anvil: ``AnvilProfile`` — managed-Anvil fork-test funding facts
            (token addresses, balance slots, whale fallbacks, gas-limit
            quirk). Default-empty; test infra only. VIB-4851 (CS-6).
        bridged_stablecoin_variants: Display-cased bridged-stable symbols
            (``"USDC.e"``, ``"USDbC"``) seeded at $1 by the teardown
            fallback pricer (legacy ``runner_teardown.
            _CHAIN_BRIDGED_STABLECOINS``). PRODUCTION teardown surface,
            hence top-level, not AnvilProfile. Empty tuple == no variants
            (legacy ``.get(chain, ())`` miss; absence is load-bearing —
            VIB-3814). VIB-4851 (CS-6).
        reorg_safe_depth: Number of block confirmations past a tx's receipt
            block after which that block is unlikely to be re-orged away on
            this chain — i.e. how far the chain head should advance before a
            block-pinned reconciliation read is safe against a lagging replica
            (VIB-3350). ``None`` means "use the framework default" (generic L2,
            3 blocks). Chains with deeper reorg windows override it (Ethereum
            12, Polygon 10, Avalanche 5). This is chain physics, owned by the
            descriptor, not hardcoded in framework code (blueprint 22).
        aliases: Extra alternative names that resolve to this chain
            (e.g. ``("bnb", "binance")`` for BSC). The canonical ``name``
            is always implicit and need not be repeated here.
        color: Brand hex color for dashboard/UI chain badges (e.g.
            ``"#627eea"`` for Ethereum). Must be a ``#``-prefixed 3- or
            6-digit lowercase hex string. ``None`` means the consumer falls
            back to a neutral default (e.g. ``"#9e9e9e"``). Sparse: only
            chains with a verified brand color declare this field. Plan 027.
        default_display_tokens: Ordered list of token symbols shown by
            default in the wallet-overview ``ax`` command for this chain.
            ``None`` means "no chain-specific defaults; use the framework
            fallback list". Mirrors the legacy ``_CHAIN_DEFAULT_TOKENS``
            class dict in ``almanak/framework/agent_tools/executor.py``.
            Lookup is EXACT-NAME (canonical chain name only — alias inputs
            deliberately fall through to the fallback). Plan 027.
        caip2_reference: Explicit CAIP-2 reference for non-EVM chains (e.g.
            Solana's mainnet genesis-hash prefix
            ``"5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"``). MUST be ``None`` for EVM
            chains — their CAIP-2 reference derives from ``chain_id`` — and is
            REQUIRED for non-EVM families, whose ``chain_id`` is the ``0``
            non-EVM sentinel. Validated against the CAIP-2 reference grammar.
            Consumed by the :attr:`caip2` property and ``ChainRegistry``'s
            CAIP-2 lookups. VIB-5175 (CAIP adoption, Phase 1).
    """

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
    canonical_stable: str | None = None
    external_ids: Mapping[str, str] | None = None
    chainlink: ChainlinkFeeds | None = None
    contracts: Mapping[str, str] | None = None
    anvil: AnvilProfile = field(default_factory=AnvilProfile)
    bridged_stablecoin_variants: tuple[str, ...] = ()
    reorg_safe_depth: int | None = None
    aliases: tuple[str, ...] = ()
    color: str | None = None
    default_display_tokens: tuple[str, ...] | None = None
    caip2_reference: str | None = None

    @property
    def caip2(self) -> str:
        """CAIP-2 blockchain id for this chain (e.g. ``"eip155:42161"``).

        EVM chains serialize as ``eip155:<chain_id>``; non-EVM chains use
        ``<namespace>:<caip2_reference>`` (e.g. ``solana:5eykt4UsFv8P8…``).

        Raises ``ValueError`` for a non-EVM chain with no ``caip2_reference``
        (its ``chain_id`` is the 0 sentinel and cannot serve as a reference).
        ``ChainRegistry.register`` guarantees registered non-EVM chains always
        have one, so this only fires on synthetic (unregistered) descriptors.
        """
        namespace = CAIP2_NAMESPACE_BY_FAMILY[self.family]
        if self.caip2_reference is not None:
            reference = self.caip2_reference
        elif self.family is ChainFamily.EVM:
            reference = str(self.chain_id)
        else:
            raise ValueError(
                f"ChainDescriptor {self.name!r} is non-EVM and has no caip2_reference; cannot form a CAIP-2 id"
            )
        return f"{namespace}:{reference}"

    def __post_init__(self) -> None:
        # VIB-3350: a confirmation depth is a non-negative block count. A negative
        # value would make ``receipt_block + depth`` nonsensical and the
        # confirmation-wait would treat the target as trivially already-reached.
        # Fail loudly at registration like the sibling field validations.
        if self.reorg_safe_depth is not None and self.reorg_safe_depth < 0:
            raise ValueError(
                f"ChainDescriptor {self.name!r} reorg_safe_depth must be non-negative, got {self.reorg_safe_depth}"
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
        # Validate the optional brand color (Plan 027). Must be a #-prefixed
        # 3- or 6-digit lowercase hex string; None is explicitly allowed (sparse
        # field -- chains without a declared brand color fall back to the
        # consumer's DEFAULT_COLOR). Fail loudly at registration so a typo'd hex
        # value does not silently produce an invisible or wrong-colored badge.
        if self.color is not None:
            c = self.color
            valid = (
                isinstance(c, str)
                and c.startswith("#")
                and len(c) in (4, 7)
                and all(ch in "0123456789abcdef" for ch in c[1:])
            )
            if not valid:
                raise ValueError(
                    f"ChainDescriptor {self.name!r} color {c!r} must be a "
                    f"#-prefixed 3- or 6-digit lowercase hex string (e.g. '#627eea')"
                )
        # CAIP-2 reference (VIB-5175). EVM derives the reference from
        # ``chain_id`` and must NOT set ``caip2_reference``. Non-EVM families
        # MAY omit it at construction so synthetic test descriptors stay
        # buildable; its presence on a *registered* non-EVM chain is enforced
        # in ``ChainRegistry.register``. Here we only reject it on EVM and
        # validate the grammar when it is provided.
        if self.family is ChainFamily.EVM:
            if self.caip2_reference is not None:
                raise ValueError(
                    f"ChainDescriptor {self.name!r} is EVM; caip2_reference must be None "
                    f"(the CAIP-2 reference is derived from chain_id={self.chain_id}), "
                    f"got {self.caip2_reference!r}"
                )
        elif self.caip2_reference is not None and not _CAIP2_REFERENCE_RE.match(self.caip2_reference):
            raise ValueError(
                f"ChainDescriptor {self.name!r} caip2_reference {self.caip2_reference!r} "
                f"must match the CAIP-2 reference grammar [-_a-zA-Z0-9]{{1,32}}"
            )
