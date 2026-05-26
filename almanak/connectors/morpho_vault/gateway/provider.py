"""Gateway-side connector binding for Morpho Vault (VIB-4810 / VIB-4817).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Morpho vault token metadata lookup without hand-wiring
an import in :mod:`almanak.gateway.services.token_service`.

VIB-4817 — adds ``GatewayDefillamaSlugCapability``. Morpho's DefiLlama
project slug (``"morpho-blue"``) is published under the historical
``morpho`` alias via ``defillama_slug_aliases`` — the morpho_vault
connector covers vault metadata but ``DefiLlama`` indexes the
underlying lending market under the ``morpho`` key, mirroring the
pre-Phase-3 dispatch.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
    GatewayMarketLookupCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from .vault_lookup import get_morpho_lookup


class MorphoVaultGatewayConnector(
    GatewayConnector,
    GatewayMarketLookupCapability,
    GatewayDefillamaSlugCapability,
):
    """Gateway-side connector for Morpho Vault."""

    protocol: ClassVar[ProtocolName] = ProtocolName("morpho_vault")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    def market_lookup(self):
        """Return the awaitable Morpho vault-lookup singleton factory."""
        return get_morpho_lookup

    def defillama_slug(self) -> str | None:
        """No standalone slug — the canonical Morpho slug is published via the alias."""
        return None

    def defillama_slug_aliases(self) -> dict[str, str]:
        """Publish the ``morpho`` alias for the morpho-blue DefiLlama project.

        The strategy/runner historically uses ``morpho`` as the
        protocol identifier for Morpho lending markets (the vault
        connector ships vault metadata, but the underlying lending
        product is "morpho_blue" in DefiLlama's catalog). Mapping the
        alias here preserves the byte-identical dispatch the legacy
        ``_PROTOCOL_TO_LLAMA_TODO_FALLBACK`` row produced.
        """
        return {"morpho": "morpho-blue"}


__all__ = ["MorphoVaultGatewayConnector"]
