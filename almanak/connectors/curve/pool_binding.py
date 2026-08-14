"""Deployment-scoped identity binding for exact Curve pools.

An exact pool address is strategy input, not SDK registry data.  This module
turns that input into an immutable, live-verified description that permission
discovery can compile without any SDK-global pool catalog.

The binding is also serialisable into an intent's connector parameters.  The
Curve compiler revalidates that description against the pool it resolves at
compile time, so a stale or substituted pool shape fails before calldata is
emitted.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from almanak.framework.data.tokens.address_resolution import looks_like_evm_address
from almanak.framework.permissions.hints import PermissionBindingError

_MARKER_KEY = "_permission_binding"
_RESOURCE_TYPE = "curve_pool"
PoolTypeName = Literal["stableswap", "cryptoswap", "tricrypto"]


@dataclass(frozen=True)
class CurvePoolPermissionBinding:
    """Immutable identity of one deployment-selected Curve pool."""

    chain: str
    pool_address: str
    coin_symbols: tuple[str, ...]
    coin_addresses: tuple[str, ...]
    coin_decimals: tuple[int, ...]
    lp_token: str
    n_coins: int
    pool_type: PoolTypeName
    abi_families: tuple[str, ...]
    is_metapool: bool
    base_pool: str | None = None
    base_pool_coin_addresses: tuple[str, ...] | None = None

    @classmethod
    def from_metadata(cls, chain: str, metadata: Any) -> CurvePoolPermissionBinding:
        """Freeze a fully resolved ``CurvePoolMetadata`` value."""
        pool_type = str(metadata.pool_type).lower()
        if pool_type not in {"stableswap", "cryptoswap", "tricrypto"}:
            raise PermissionBindingError(
                f"Curve pool {metadata.address} on {chain} resolved an unsupported pool_type={pool_type!r}"
            )
        abi_families = ("stableswap_legacy", "stableswap_ng") if pool_type == "stableswap" else (pool_type,)
        binding = cls(
            chain=chain,
            pool_address=_normalise_address(metadata.address, field="pool address"),
            coin_symbols=tuple(str(value) for value in metadata.coin_symbols),
            coin_addresses=tuple(
                _normalise_address(value, field=f"pool coin {index}")
                for index, value in enumerate(metadata.coin_addresses)
            ),
            coin_decimals=tuple(int(value) for value in metadata.coin_decimals),
            lp_token=_normalise_address(metadata.lp_token, field="LP token"),
            n_coins=int(metadata.n_coins),
            pool_type=cast(PoolTypeName, pool_type),
            abi_families=abi_families,
            is_metapool=bool(metadata.is_metapool),
            base_pool=(
                _normalise_address(metadata.base_pool, field="base pool") if metadata.base_pool is not None else None
            ),
            base_pool_coin_addresses=(
                tuple(
                    _normalise_address(value, field=f"base-pool coin {index}")
                    for index, value in enumerate(metadata.base_pool_coin_addresses)
                )
                if metadata.base_pool_coin_addresses is not None
                else None
            ),
        )
        binding._validate_shape()
        return binding

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CurvePoolPermissionBinding:
        """Parse the serialised intent marker, rejecting partial identities."""
        if value.get("resource_type") != _RESOURCE_TYPE:
            raise ValueError(f"Unsupported Curve permission binding resource_type={value.get('resource_type')!r}")
        try:
            pool_type = str(value["pool_type"]).lower()
            if pool_type not in {"stableswap", "cryptoswap", "tricrypto"}:
                raise ValueError(f"unsupported pool_type={pool_type!r}")
            binding = cls(
                chain=str(value["chain"]),
                pool_address=_normalise_address(str(value["pool_address"]), field="pool address"),
                coin_symbols=tuple(str(symbol) for symbol in value["coin_symbols"]),
                coin_addresses=tuple(
                    _normalise_address(str(address), field=f"pool coin {index}")
                    for index, address in enumerate(value["coin_addresses"])
                ),
                coin_decimals=tuple(int(decimals) for decimals in value["coin_decimals"]),
                lp_token=_normalise_address(str(value["lp_token"]), field="LP token"),
                n_coins=int(value["n_coins"]),
                pool_type=cast(PoolTypeName, pool_type),
                abi_families=tuple(str(family) for family in value["abi_families"]),
                is_metapool=bool(value["is_metapool"]),
                base_pool=(
                    _normalise_address(str(value["base_pool"]), field="base pool")
                    if value.get("base_pool") is not None
                    else None
                ),
                base_pool_coin_addresses=(
                    tuple(
                        _normalise_address(str(address), field=f"base-pool coin {index}")
                        for index, address in enumerate(value["base_pool_coin_addresses"])
                    )
                    if value.get("base_pool_coin_addresses") is not None
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Malformed Curve pool permission binding: {exc}") from exc
        binding._validate_shape()
        required_targets = value.get("required_targets")
        if required_targets != [binding.pool_address]:
            raise ValueError(
                "Malformed Curve pool permission binding: required_targets must contain only the exact pool address"
            )
        return binding

    def _validate_shape(self) -> None:
        if self.n_coins < 2 or self.n_coins != len(self.coin_addresses):
            raise ValueError(
                f"Curve pool binding n_coins={self.n_coins} does not match "
                f"coin_addresses length={len(self.coin_addresses)}"
            )
        if self.n_coins != len(self.coin_symbols) or any(not symbol.strip() for symbol in self.coin_symbols):
            raise ValueError(
                f"Curve pool binding n_coins={self.n_coins} does not match a complete "
                f"coin_symbols vector of length={len(self.coin_symbols)}"
            )
        if self.n_coins != len(self.coin_decimals):
            raise ValueError(
                f"Curve pool binding n_coins={self.n_coins} does not match "
                f"coin_decimals length={len(self.coin_decimals)}"
            )
        if any(decimals < 0 or decimals > 36 for decimals in self.coin_decimals):
            raise ValueError(f"Curve pool binding has implausible coin decimals: {self.coin_decimals}")
        expected_families = (
            ("stableswap_legacy", "stableswap_ng") if self.pool_type == "stableswap" else (self.pool_type,)
        )
        if self.abi_families != expected_families:
            raise ValueError(
                f"Curve pool binding ABI families {self.abi_families} do not match pool_type={self.pool_type}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Return the stable marker placed on discovery intents."""
        return {
            "resource_type": _RESOURCE_TYPE,
            # Framework discovery reads only this generic admission contract;
            # every remaining field is connector-owned identity evidence.
            "required_targets": [self.pool_address],
            "chain": self.chain,
            "pool_address": self.pool_address,
            "coin_symbols": list(self.coin_symbols),
            "coin_addresses": list(self.coin_addresses),
            "coin_decimals": list(self.coin_decimals),
            "lp_token": self.lp_token,
            "n_coins": self.n_coins,
            "pool_type": self.pool_type,
            "abi_families": list(self.abi_families),
            "is_metapool": self.is_metapool,
            "base_pool": self.base_pool,
            "base_pool_coin_addresses": (
                list(self.base_pool_coin_addresses) if self.base_pool_coin_addresses is not None else None
            ),
        }

    def marker_params(self) -> dict[str, Any]:
        """Return connector-parameter fields for a bound synthetic intent."""
        return {_MARKER_KEY: self.to_dict()}

    def pool_data(self) -> dict[str, Any]:
        """Return the registry-shaped subset needed to construct vectors."""
        return {
            "address": self.pool_address,
            "lp_token": self.lp_token,
            # ``CurveAdapter`` indexes a pool by the resolved token symbol first
            # and falls back to ``coin_addresses`` for address-form callers.
            # Keep both aligned, exactly like ``PoolInfo.to_dict()``. Feeding
            # addresses into ``coins`` makes a token resolved as e.g. ``DAI``
            # impossible to locate during permission compilation.
            "coins": list(self.coin_symbols),
            "coin_addresses": list(self.coin_addresses),
            "coin_decimals": list(self.coin_decimals),
            "pool_type": self.pool_type,
            "n_coins": self.n_coins,
            "is_metapool": self.is_metapool,
            "base_pool": self.base_pool,
            "base_pool_coin_addresses": (
                list(self.base_pool_coin_addresses) if self.base_pool_coin_addresses is not None else None
            ),
        }

    def assert_matches_pool_data(self, *, chain: str, pool_data: Mapping[str, Any]) -> None:
        """Fail if runtime resolution differs from the admitted identity."""
        actual_address = _normalise_address(str(pool_data.get("address", "")), field="resolved pool address")
        actual_lp_token = _normalise_address(str(pool_data.get("lp_token", "")), field="resolved LP token")
        actual_coins = tuple(
            _normalise_address(str(address), field=f"resolved pool coin {index}")
            for index, address in enumerate(pool_data.get("coin_addresses") or ())
        )
        actual_decimals = tuple(int(decimals) for decimals in pool_data.get("coin_decimals") or ())
        actual_pool_type = str(pool_data.get("pool_type", "")).lower()
        actual_n_coins = int(pool_data.get("n_coins") or 0)
        actual_is_metapool = bool(pool_data.get("is_metapool", False))
        actual_base_pool = (
            _normalise_address(str(pool_data["base_pool"]), field="resolved base pool")
            if pool_data.get("base_pool") is not None
            else None
        )
        actual_base_coins = (
            tuple(
                _normalise_address(str(address), field=f"resolved base-pool coin {index}")
                for index, address in enumerate(pool_data["base_pool_coin_addresses"])
            )
            if pool_data.get("base_pool_coin_addresses") is not None
            else None
        )

        mismatches: list[str] = []
        if chain.lower() != self.chain.lower():
            mismatches.append(f"chain expected {self.chain}, got {chain}")
        if actual_address != self.pool_address:
            mismatches.append(f"pool expected {self.pool_address}, got {actual_address}")
        if actual_lp_token != self.lp_token:
            mismatches.append(f"LP token expected {self.lp_token}, got {actual_lp_token}")
        if actual_coins != self.coin_addresses:
            mismatches.append(f"ordered coins expected {self.coin_addresses}, got {actual_coins}")
        if actual_decimals != self.coin_decimals:
            mismatches.append(f"coin decimals expected {self.coin_decimals}, got {actual_decimals}")
        if actual_n_coins != self.n_coins:
            mismatches.append(f"n_coins expected {self.n_coins}, got {actual_n_coins}")
        if actual_pool_type != self.pool_type:
            mismatches.append(f"pool_type expected {self.pool_type}, got {actual_pool_type}")
        if actual_is_metapool != self.is_metapool:
            mismatches.append(f"is_metapool expected {self.is_metapool}, got {actual_is_metapool}")
        if actual_base_pool != self.base_pool:
            mismatches.append(f"base pool expected {self.base_pool}, got {actual_base_pool}")
        if actual_base_coins != self.base_pool_coin_addresses:
            mismatches.append(
                f"ordered base-pool coins expected {self.base_pool_coin_addresses}, got {actual_base_coins}"
            )
        if mismatches:
            raise ValueError(
                f"Curve pool binding revalidation failed for {self.pool_address} on {chain}: " + "; ".join(mismatches)
            )


def permission_binding_from_intent(intent: Any) -> CurvePoolPermissionBinding | None:
    """Read a Curve pool binding from LP ``protocol_params`` or swap params."""
    bindings: list[tuple[str, CurvePoolPermissionBinding]] = []
    for carrier in ("protocol_params", "swap_params"):
        params = getattr(intent, carrier, None)
        raw = params.get(_MARKER_KEY) if isinstance(params, Mapping) else None
        if raw is None:
            continue
        if not isinstance(raw, Mapping):
            raise ValueError(f"{carrier}.{_MARKER_KEY} must be a mapping")
        bindings.append((carrier, CurvePoolPermissionBinding.from_dict(raw)))

    if not bindings:
        return None
    first_carrier, first_binding = bindings[0]
    for carrier, binding in bindings[1:]:
        if binding != first_binding:
            raise ValueError(
                f"Conflicting Curve permission bindings in {first_carrier} and {carrier}; "
                "an intent may identify only one exact pool"
            )
    return first_binding


def resolve_configured_pool_bindings(
    *,
    chain: str,
    config: Mapping[str, Any],
    gateway_client: Any = None,
    rpc_url: str | None = None,
    include_legacy: bool = True,
) -> tuple[CurvePoolPermissionBinding, ...]:
    """Resolve every exact Curve pool declared by this deployment.

    The canonical multi-resource form is::

        "permission_bindings": [
          {
            "protocol": "curve",
            "resource_type": "pool",
            "chain": "ethereum",
            "address": "0x...",
            "coin_addresses": ["0x...", "0x..."]
          }
        ]

    For existing strategies, top-level ``pool`` plus ordered ``token0``,
    ``token1``, ... fields is accepted as the same declaration.  Only exact
    address-form pools participate; nicknames are deliberately not admission
    identities because the SDK has no global alias-to-address catalog.
    """
    declarations = _binding_declarations(chain=chain, config=config, include_legacy=include_legacy)
    if not declarations:
        return ()
    if not ((gateway_client is not None and getattr(gateway_client, "is_connected", False)) or rpc_url):
        pools = ", ".join(address for address, _ in declarations)
        raise PermissionBindingError(
            f"Cannot verify exact Curve pool binding(s) {pools} on {chain}: no gateway/RPC read transport is available. "
            "Deployment-grade permission generation must resolve the pool through Curve's MetaRegistry; "
            "provide a connected gateway client or --rpc-url."
        )

    from almanak.connectors.curve.pool_resolver import resolution_is_definitive, resolve_pool_metadata

    resolved: dict[str, CurvePoolPermissionBinding] = {}
    for pool_address, expected_coins in declarations:
        metadata = resolve_pool_metadata(
            chain=chain,
            pool_address=pool_address,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
        )
        if metadata is None:
            state = (
                "MetaRegistry confirmed that it is not a supported plain Curve pool"
                if resolution_is_definitive(chain, pool_address)
                else "the MetaRegistry read was unavailable or inconclusive"
            )
            raise PermissionBindingError(
                f"Cannot verify exact Curve pool {pool_address} on {chain}: {state}. "
                "Permission generation refuses to omit or guess the pool target."
            )

        binding = CurvePoolPermissionBinding.from_metadata(chain, metadata)
        if binding.coin_addresses != expected_coins:
            raise PermissionBindingError(
                f"Exact Curve pool {pool_address} on {chain} has ordered coins {binding.coin_addresses}, "
                f"but deployment config declares {expected_coins}. Refusing to bind amounts or permissions "
                "to a different pool identity."
            )
        existing = resolved.get(binding.pool_address)
        if existing is not None and existing != binding:
            raise PermissionBindingError(f"Conflicting Curve permission bindings declared for {binding.pool_address}")
        resolved[binding.pool_address] = binding
    return tuple(resolved[address] for address in sorted(resolved))


def resolve_pool_binding(
    *,
    chain: str,
    pool_address: str,
    coin_addresses: list[str] | tuple[str, ...],
    gateway_client: Any = None,
    rpc_url: str | None = None,
) -> CurvePoolPermissionBinding:
    """Public exact-pool validation API for admission and runtime callers.

    Resolves the address through Curve's MetaRegistry and verifies the caller's
    canonical coin order. The returned immutable value includes the LP token,
    decimals, pool family and ABI families and can be serialised into an
    intent with :meth:`CurvePoolPermissionBinding.marker_params`.
    """
    bindings = resolve_configured_pool_bindings(
        chain=chain,
        config={
            "permission_bindings": [
                {
                    "protocol": "curve",
                    "resource_type": "pool",
                    "chain": chain,
                    "address": pool_address,
                    "coin_addresses": list(coin_addresses),
                }
            ]
        },
        gateway_client=gateway_client,
        rpc_url=rpc_url,
        include_legacy=False,
    )
    if len(bindings) != 1:
        raise PermissionBindingError(
            f"Expected one Curve pool binding for {pool_address} on {chain}, resolved {len(bindings)}"
        )
    return bindings[0]


def _binding_declarations(
    *, chain: str, config: Mapping[str, Any], include_legacy: bool
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    declarations: list[tuple[str, tuple[str, ...]]] = []
    explicit_addresses: set[str] = set()

    raw_bindings = config.get("permission_bindings")
    if raw_bindings is not None:
        if not isinstance(raw_bindings, list):
            raise PermissionBindingError("config.permission_bindings must be a list")
        for index, entry in enumerate(raw_bindings):
            if not isinstance(entry, Mapping):
                raise PermissionBindingError(f"config.permission_bindings[{index}] must be an object")
            if str(entry.get("protocol", "")).lower() != "curve":
                continue
            if str(entry.get("resource_type", "")).lower() != "pool":
                raise PermissionBindingError(
                    f"config.permission_bindings[{index}] for Curve must use resource_type='pool'"
                )
            entry_chain = entry.get("chain")
            if entry_chain is not None and not _same_chain(str(entry_chain), chain):
                continue
            declaration = _parse_declaration(
                address=entry.get("address"),
                coin_addresses=entry.get("coin_addresses"),
                location=f"config.permission_bindings[{index}]",
            )
            declarations.append(declaration)
            explicit_addresses.add(declaration[0])

    pool = config.get("pool")
    if (
        include_legacy
        and looks_like_evm_address(pool)
        and str(pool).lower() not in explicit_addresses
        and _config_applies_to_chain(config, chain)
        and _legacy_config_can_target_curve(config)
    ):
        coins_value = config.get("coin_addresses")
        if coins_value is None:
            sequential: list[Any] = []
            for index in range(8):
                key = f"token{index}"
                if key not in config:
                    break
                sequential.append(config[key])
            coins_value = sequential
        if not coins_value and isinstance(config.get("tokens"), list):
            coins_value = [token.get("address") if isinstance(token, Mapping) else None for token in config["tokens"]]
        declarations.append(
            _parse_declaration(address=pool, coin_addresses=coins_value, location="top-level config.pool")
        )

    deduped: dict[str, tuple[str, ...]] = {}
    for address, coins in declarations:
        previous = deduped.get(address)
        if previous is not None and previous != coins:
            raise PermissionBindingError(f"Conflicting ordered coin declarations for Curve pool {address}")
        deduped[address] = coins
    return tuple((address, deduped[address]) for address in sorted(deduped))


def _parse_declaration(*, address: Any, coin_addresses: Any, location: str) -> tuple[str, tuple[str, ...]]:
    if not isinstance(address, str) or not looks_like_evm_address(address):
        raise PermissionBindingError(f"{location}.address must be an exact EVM pool address")
    if not isinstance(coin_addresses, list | tuple) or len(coin_addresses) < 2:
        raise PermissionBindingError(
            f"{location} must declare at least two ordered coin_addresses (or token0/token1 for top-level config)"
        )
    coins: list[str] = []
    for index, value in enumerate(coin_addresses):
        if not isinstance(value, str) or not looks_like_evm_address(value):
            raise PermissionBindingError(
                f"{location}.coin_addresses[{index}] must be an exact EVM token address; symbols are not pool identity"
            )
        coins.append(value.lower())
    return address.lower(), tuple(coins)


def _normalise_address(value: str, *, field: str) -> str:
    if not looks_like_evm_address(value):
        raise ValueError(f"Curve pool binding {field} is not an exact EVM address: {value!r}")
    return value.lower()


def _config_applies_to_chain(config: Mapping[str, Any], chain: str) -> bool:
    configured_chain = config.get("chain")
    if isinstance(configured_chain, str):
        return _same_chain(configured_chain, chain)
    configured_chains = config.get("chains")
    if isinstance(configured_chains, list):
        matching = [value for value in configured_chains if isinstance(value, str) and _same_chain(value, chain)]
        return len(configured_chains) == 1 and bool(matching)
    return True


def _legacy_config_can_target_curve(config: Mapping[str, Any]) -> bool:
    """Do not claim another connector's top-level exact-pool declaration."""
    configured_protocol = config.get("protocol")
    if isinstance(configured_protocol, str) and configured_protocol.strip():
        return configured_protocol.strip().lower() == "curve"
    configured_protocols = config.get("protocols")
    if isinstance(configured_protocols, list) and configured_protocols:
        names = {value.strip().lower() for value in configured_protocols if isinstance(value, str)}
        return "curve" in names
    # Historical Curve configs often predate either protocol discriminator.
    return True


def _same_chain(left: str, right: str) -> bool:
    from almanak.core.constants import canonical_chain_name

    return canonical_chain_name(left) == canonical_chain_name(right)


__all__ = [
    "CurvePoolPermissionBinding",
    "permission_binding_from_intent",
    "resolve_configured_pool_bindings",
    "resolve_pool_binding",
]
