"""Gateway-side connector binding for Pendle (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Pendle PT / YT / LP token metadata lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_pendle_lookup`` directly. Phase 4 collapses the
per-protocol accessor methods on ``TokenService`` into a registry-driven
loop.

Phase 3 (VIB-4811) adds ``GatewayPriceIdCapability`` — the PENDLE
governance token's CoinGecko slug (``pendle``). Moved verbatim from
``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.

W1 (VIB-4853) adds:

* ``GatewayAddressCapability`` — per-chain Pendle Router + market
  factory + dynamic ``market_*`` addresses, moved verbatim from
  ``almanak.core.contracts``. Non-connector callers (teardown
  discovery, ContractRegistry, CLI support matrix) resolve Pendle
  addresses through this capability instead of importing the dict by
  name.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
    GatewayPrincipalTokenPriceCapability,
    PrincipalTokenMarketRef,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import PENDLE
from .market_lookup import get_pendle_lookup


class PendleGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
    GatewayPrincipalTokenPriceCapability,
):
    """Gateway-side connector for Pendle."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Pendle contract addresses for ``chain`` (or empty).

        Includes the dynamic ``market_*`` / ``pt_*`` / ``yt_*`` / ``sy_*``
        entries — the strategy-side ``ContractRegistry`` scans for keys
        with the ``market_`` prefix to register per-market routers.
        """
        return PENDLE.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Pendle addresses are registered."""
        return frozenset(PENDLE.keys())

    def market_lookup(self):
        """Return the awaitable Pendle market-lookup singleton factory."""
        return get_pendle_lookup

    # ------------------------------------------------------------------
    # GatewayPrincipalTokenPriceCapability (VIB-5310 / epic VIB-5299, M1)
    #
    # Connector-owned resolution of a PT/YT symbol → (market, underlying)
    # from STATIC metadata only (no egress). The gateway's MarketService
    # composes the PT/YT-USD price from this ref + on-chain rate + the
    # underlying/USD aggregator; this method holds no pricing logic.
    # ------------------------------------------------------------------

    def principal_token_price_chains(self) -> frozenset[str]:
        """Chains for which a PT/YT symbol can be resolved to a market."""
        from ..sdk import MARKET_BY_PT_TOKEN, MARKET_BY_YT_TOKEN

        return frozenset(MARKET_BY_PT_TOKEN) | frozenset(MARKET_BY_YT_TOKEN)

    def resolve_principal_token_ref(
        self,
        *,
        symbol: str,
        chain: str,
        maturity_ts: int = 0,
    ) -> PrincipalTokenMarketRef | None:
        """Resolve a Pendle PT/YT symbol to its market + underlying (no egress).

        Uses the connector's static ``MARKET_BY_PT_TOKEN`` /
        ``MARKET_BY_YT_TOKEN`` (symbol → market) and ``MARKET_TOKEN_MINT_SY``
        (market → SY-mint / underlying token). Returns ``None`` — which the
        gateway maps to ``UNMEASURED`` (Empty≠Zero) — when the symbol is
        unknown on this chain OR the market has no statically-known underlying
        to price.

        Orchestration only; each step is a focused module-level helper:
        ``_pt_family`` (PT/YT), ``_lookup_market_address`` (case-insensitive
        symbol → market), ``_lookup_underlying_token`` (market → underlying).
        """
        sym = (symbol or "").strip()
        if not sym:
            return None

        family = _pt_family(sym)
        market_address = _lookup_market_address(sym, chain, family)
        if market_address is None:
            return None

        underlying = _lookup_underlying_token(market_address, chain)
        if not underlying:
            # No statically-known underlying to price → unmeasured, not zero.
            return None

        return PrincipalTokenMarketRef(
            protocol=str(self.protocol),
            market_address=market_address,
            underlying_token=underlying,
            family=family,
            maturity_ts=maturity_ts,
        )

    def build_principal_token_market_reader(self, *, chain: str, rpc_url: str):
        """Build a Pendle on-chain market reader in direct (rpc_url) mode.

        Returns ``None`` for a chain without a RouterStatic mapping (the gateway
        then composes with the at-par ESTIMATED rate). Direct mode reuses the
        on-chain reader's EXISTING ``# vib-2986-exempt`` web3 path — no new
        egress, no new exempt marker. A bounded request timeout is passed so a
        hung RPC can't wedge the gateway's worker thread indefinitely.
        """
        from ..on_chain_reader import ROUTER_STATIC_ADDRESSES, PendleOnChainReader

        if chain not in ROUTER_STATIC_ADDRESSES:
            return None
        return PendleOnChainReader(
            chain=chain,
            rpc_url=rpc_url,
            request_timeout_seconds=_GATEWAY_PT_RPC_TIMEOUT_SECONDS,
        )

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Pendle governance token."""
        return {"PENDLE": "pendle"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """PENDLE is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    # The CLI support matrix renders Pendle as a single ``yield`` row
    # (overriding the strategy-side SWAP/LP/WITHDRAW intent → category
    # derivation). The override lives on the strategy-side manifest's
    # ``matrix_entries`` field (see ``almanak/connectors/pendle/__init__.py``).


# Bound on each gateway-driven Pendle on-chain rate read (direct mode). Mirrors
# the gateway's own eth_call timeout (RpcService / on_chain_reader gateway mode
# both use 30s) so a hung RPC can't hold a worker thread open forever.
_GATEWAY_PT_RPC_TIMEOUT_SECONDS = 30.0


def _pt_family(symbol: str) -> str:
    """Classify a Pendle synthetic symbol as ``"YT"`` or ``"PT"``."""
    return "YT" if symbol.upper().startswith("YT-") else "PT"


def _lookup_market_address(symbol: str, chain: str, family: str) -> str | None:
    """Resolve a PT/YT symbol to its market address on ``chain`` (no egress).

    The static dicts already carry common case variants; a case-folded scan is
    the fallback so a canonical-cased symbol (config is maturity-less and may
    differ in case from the indexed variants) still resolves. Returns ``None``
    when the symbol is unknown on the chain.
    """
    from ..sdk import MARKET_BY_PT_TOKEN, MARKET_BY_YT_TOKEN

    market_map = (MARKET_BY_YT_TOKEN if family == "YT" else MARKET_BY_PT_TOKEN).get(chain, {})

    market_address = market_map.get(symbol)
    if market_address is not None:
        return market_address

    sym_lower = symbol.lower()
    for key, addr in market_map.items():
        if key.lower() == sym_lower:
            return addr
    return None


def _lookup_underlying_token(market_address: str, chain: str) -> str | None:
    """Resolve a market address to its SY-mint / underlying token (no egress).

    Returns ``None`` when no underlying is statically known for the market
    (the gateway then reports the position as unmeasured, never a fabricated 0).
    """
    from ..sdk import MARKET_TOKEN_MINT_SY

    return MARKET_TOKEN_MINT_SY.get(chain, {}).get(market_address.lower())


__all__ = ["PendleGatewayConnector"]
