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

_REGISTRY: dict[str, VaultAdapterFactory] = {}


def register_vault_adapter(protocol: str, factory: VaultAdapterFactory) -> None:
    """Register a vault adapter factory for a protocol name.

    Idempotent: re-registering the same protocol overwrites the previous factory
    (useful for tests and for hot-reload scenarios). The protocol key is
    case-insensitive — stored lowercased.

    NOTE: Registering here enables intent compilation and portfolio valuation.
    For Safe/Zodiac permission discovery to work you must ALSO add a
    representative vault entry to
    ``almanak.framework.permissions.constants.VAULT_PROTOCOL_REPRESENTATIVE``.
    See its inline comment for the required format.
    """
    _REGISTRY[protocol.lower()] = factory


def supported_vault_protocols() -> frozenset[str]:
    """Return the set of registered vault protocol names (lowercased)."""
    return frozenset(_REGISTRY.keys())


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
    factory = _REGISTRY.get(protocol.lower())
    if factory is None:
        raise ValueError(f"Unknown vault protocol: {protocol!r}. Registered: {sorted(_REGISTRY.keys()) or '[]'}")
    return factory(
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
        from ..morpho_vault.adapter import MetaMorphoAdapter, MetaMorphoConfig

        config = MetaMorphoConfig(chain=chain, wallet_address=wallet_address)
        return MetaMorphoAdapter(
            config,
            gateway_client=gateway_client,
            token_resolver=token_resolver,
        )

    register_vault_adapter("metamorpho", _build_metamorpho)


_register_builtin_adapters()


__all__ = [
    "VaultAdapter",
    "VaultSdk",
    "build_vault_adapter",
    "register_vault_adapter",
    "supported_vault_protocols",
]
