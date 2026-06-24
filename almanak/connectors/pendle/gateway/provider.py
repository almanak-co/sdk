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

    def build_principal_token_market_reader(self, *, chain: str, rpc_client):
        """Build a Pendle on-chain market reader in **gateway mode** (VIB-5348).

        ``rpc_client`` is a gateway-supplied transport that duck-types the
        reader's ``gateway_client`` seam (it exposes ``rpc.Call(RpcRequest,
        timeout=...) -> resp`` with ``.success`` / ``.result`` / ``.error``); in
        practice the gateway passes a
        :class:`~almanak.gateway.services.pt_rpc_adapter.GatewayPtRpcClient`
        backed by the gateway's own async ``aiohttp`` eth_call. The reader then
        runs with ``web3 is None`` — it instantiates NO raw ``HTTPProvider`` and
        relies on NO ``# vib-2986-exempt`` web3 path. (The reader's direct-web3
        branch remains for the separate local-dev strategy-container consumer,
        unchanged.) The bounded request timeout now lives on the gateway-native
        transport (aiohttp ``ClientTimeout``), not on a web3 ``request_kwargs``.

        Returns ``None`` for a chain without a PT-oracle mapping. With no reader
        the gateway cannot read ``pt_to_asset_rate``, so the price is UNMEASURED
        (no price) — it is NEVER fabricated to an at-par (1.0) ESTIMATED rate, which
        would overvalue the PT to its maximum redemption value (PT trades at ≤ par
        before maturity). See ``market_service.py`` §5 (``rate is None`` →
        UNMEASURED) and ``_read_pt_market`` for the reject-at-par contract.
        """
        from ..on_chain_reader import PT_ORACLE_ADDRESSES, PendleOnChainReader

        if chain not in PT_ORACLE_ADDRESSES:
            return None
        return PendleOnChainReader(chain=chain, gateway_client=rpc_client)

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
