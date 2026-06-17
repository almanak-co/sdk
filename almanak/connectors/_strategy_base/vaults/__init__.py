"""Generic ERC-4626 vault adapter registry.

Provides a single dispatch surface for vault connectors (MetaMorpho, Beefy,
Yearn V3, Sommelier, ...) so the intent compiler, permission synthesiser, and
portfolio valuer don't import any specific vault SDK directly.

A vault connector registers itself by calling :func:`register_vault_adapter`
with a factory callable that builds an adapter instance. The intent compiler
calls :func:`build_vault_adapter` keyed by the intent's ``protocol`` field.

The adapter Protocol below is the minimum surface area required by the framework
(compile deposit/redeem, query asset/decimals, query max-redeemable). Vault SDKs
typically expose more (curator, supply queue, harvest history) — those stay
behind the protocol-specific adapter and are not part of the generic primitive.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from almanak.framework.data.tokens import TokenResolver


class VaultAdapter(Protocol):
    """Minimum interface every ERC-4626 vault adapter must satisfy.

    The compiler treats this Protocol as the contract; concrete adapters
    (MetaMorphoAdapter, BeefyAdapter, ...) extend it with protocol-specific
    methods that the compiler does not depend on.
    """

    @property
    def sdk(self) -> Any:
        """Adapter exposes its SDK (must implement VaultSdk contract)."""
        ...


class VaultSdk(Protocol):
    """ERC-4626 SDK surface required by the compiler and portfolio valuer."""

    def get_vault_asset(self, vault_address: str) -> str: ...
    def get_balance_of(self, vault_address: str, owner: str) -> int: ...
    def get_max_redeem(self, vault_address: str, owner: str) -> int: ...
    def convert_to_assets(self, vault_address: str, shares: int) -> int: ...
    def get_decimals(self, address: str) -> int: ...
    def build_deposit_tx(self, *, vault_address: str, assets: int, receiver: str) -> dict[str, Any]: ...
    def build_redeem_tx(self, *, vault_address: str, shares: int, receiver: str, owner: str) -> dict[str, Any]: ...


VaultAdapterFactory = Any  # Callable[..., VaultAdapter] — kept loose to avoid
# pinning kwarg names across heterogeneous vault SDKs (MetaMorpho takes a
# MetaMorphoConfig dataclass; Beefy will likely take a BeefyConfig). Concrete
# factories accept (chain, wallet_address, gateway_client, token_resolver).


class _VaultRegistration:
    """Internal record holding the factory + declared chain support for a protocol.

    The chain set lets the compiler answer "is this vault×chain combination
    supported?" WITHOUT instantiating the adapter (which would require a
    gateway client and would raise inside the chain check, fragmenting the
    error surface). Empty/None means "chain support is opaque — adapter will
    decide at instantiation time" (legacy behaviour, preserved for adapters
    registered without an explicit set).
    """

    __slots__ = ("factory", "supported_chains")

    def __init__(
        self,
        factory: VaultAdapterFactory,
        supported_chains: frozenset[str] | None,
    ) -> None:
        self.factory = factory
        self.supported_chains = supported_chains


_REGISTRY: dict[str, _VaultRegistration] = {}


def register_vault_adapter(
    protocol: str,
    factory: VaultAdapterFactory,
    *,
    supported_chains: Iterable[str] | None = None,
) -> None:
    """Register a vault adapter factory for a protocol name.

    Idempotent: re-registering the same protocol overwrites the previous factory
    (useful for tests and for hot-reload scenarios). The protocol key is
    case-insensitive — stored lowercased.

    Args:
        protocol: Protocol name (e.g. "metamorpho"). Case-insensitive.
        factory: Callable that builds the adapter instance.
        supported_chains: Optional iterable of chain names this adapter
            supports. When provided, the intent compiler uses it for a fail-fast
            chain check at compile time (VIB-3827) so unsupported vault×chain
            combinations classify as ``COMPILATION_PERMANENT`` instead of
            burning retries on a deterministic mis-configuration. Chain names
            are stored lowercased.

    NOTE: Registering here enables intent compilation and portfolio valuation.
    For Safe/Zodiac permission discovery to work you must ALSO expose a
    connector-owned representative vault entry and include it in
    ``almanak.framework.permissions.constants.VAULT_PROTOCOL_REPRESENTATIVE``.
    """
    chains: frozenset[str] | None
    if supported_chains is None:
        chains = None
    else:
        chains = frozenset(c.lower() for c in supported_chains)
    _REGISTRY[protocol.lower()] = _VaultRegistration(factory, chains)


def supported_vault_protocols() -> frozenset[str]:
    """Return the set of registered vault protocol names (lowercased)."""
    return frozenset(_REGISTRY.keys())


def supported_vault_chains(protocol: str) -> frozenset[str] | None:
    """Return the declared chain set for ``protocol``.

    Returns:
        A frozenset of supported chain names (lowercased) when the adapter
        registered with an explicit ``supported_chains``. Returns ``None``
        when the protocol declared no chain set (legacy adapters whose chain
        gating happens inside the adapter constructor). Raises ``KeyError``
        if the protocol is not registered.
    """
    record = _REGISTRY.get(protocol.lower())
    if record is None:
        raise KeyError(protocol)
    return record.supported_chains


def is_vault_chain_supported(protocol: str, chain: str) -> bool:
    """Return True if ``protocol`` is registered AND ``chain`` is in its declared set.

    Returns False for unknown protocols and for protocols whose declared chain
    set excludes ``chain``. Returns True for legacy adapters that did not
    declare a chain set (we cannot statically prove non-support without
    instantiating, and the adapter's own constructor is the fallback gate).
    """
    record = _REGISTRY.get(protocol.lower())
    if record is None:
        return False
    if record.supported_chains is None:
        return True
    return chain.lower() in record.supported_chains


def build_vault_adapter(
    protocol: str,
    *,
    chain: str,
    wallet_address: str,
    gateway_client: Any,
    token_resolver: TokenResolver | None = None,
) -> VaultAdapter:
    """Build a vault adapter instance for ``protocol``.

    Raises:
        ValueError: if ``protocol`` is not registered.
    """
    record = _REGISTRY.get(protocol.lower())
    if record is None:
        raise ValueError(f"Unknown vault protocol: {protocol!r}. Registered: {sorted(_REGISTRY.keys()) or '[]'}")
    return record.factory(
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        token_resolver=token_resolver,
    )


def _register_builtin_adapters() -> None:
    """Register adapters that ship with the framework."""

    def _build_metamorpho(
        *,
        chain: str,
        wallet_address: str,
        gateway_client: Any,
        token_resolver: TokenResolver | None,
    ) -> VaultAdapter:
        from almanak.connectors.morpho_vault.adapter import MetaMorphoAdapter, MetaMorphoConfig

        config = MetaMorphoConfig(chain=chain, wallet_address=wallet_address)
        return MetaMorphoAdapter(
            config,
            gateway_client=gateway_client,
            token_resolver=token_resolver,
        )

    # MetaMorpho ships on Ethereum and Base. Sonic vaults are blocked on
    # VIB-2281 (Silo V2 native connector) — strategies that target Sonic via
    # the metamorpho protocol key will fail-fast at compile time (VIB-3827).
    from almanak.connectors.morpho_vault.sdk import SUPPORTED_CHAINS as _METAMORPHO_CHAINS

    register_vault_adapter(
        "metamorpho",
        _build_metamorpho,
        supported_chains=_METAMORPHO_CHAINS,
    )
    register_vault_adapter(
        "morpho_vault",
        _build_metamorpho,
        supported_chains=_METAMORPHO_CHAINS,
    )


_register_builtin_adapters()


__all__ = [
    "VaultAdapter",
    "VaultSdk",
    "build_vault_adapter",
    "is_vault_chain_supported",
    "register_vault_adapter",
    "supported_vault_chains",
    "supported_vault_protocols",
]
