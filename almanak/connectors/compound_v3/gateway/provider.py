"""Gateway-side connector binding for Compound v3 (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Compound v3 cToken metadata lookup without hand-wiring
an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_compound_lookup`` directly. Phase 4 collapses the
per-protocol accessor methods on ``TokenService`` into a registry-driven
loop.

W7 (VIB-4859) adds:

* ``GatewayLendingRateHistoryCapability`` — live supply / borrow / utilisation
  via on-chain ``eth_call`` to the Comet contract's
  ``getUtilization()`` + ``getSupplyRate(util)`` / ``getBorrowRate(util)``.
  Migrates the ``_fetch_compound_v3_rate_onchain`` body that used to live
  strategy-side in ``framework/data/rates/monitor.py`` (and opened its own
  ``httpx.AsyncClient``); egress now happens through the
  ``RateHistoryService`` servicer's shared HTTP session.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
    GatewayLendingRateHistoryCapability,
    GatewayMarketLookupCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from .market_lookup import get_compound_lookup

logger = logging.getLogger(__name__)

# =============================================================================
# W7 / VIB-4859 — Compound V3 on-chain rate fetch (live path)
# =============================================================================
#
# Migrated verbatim from
# ``almanak/framework/data/rates/monitor.py`` (W7 / VIB-4859). The
# strategy container no longer holds these — every byte of HTTP egress
# moves into the gateway sidecar via ``RateHistoryService``.

# Function selector for Comet.getUtilization() -> returns uint256 (1e18 scale)
# keccak256("getUtilization()")[:4].hex() == "7eb71131"
_COMPOUND_V3_GET_UTILIZATION_SELECTOR = "7eb71131"

# Function selector for Comet.getSupplyRate(uint256 utilization) -> returns uint64
# keccak256("getSupplyRate(uint256)")[:4].hex() == "d955759d"
_COMPOUND_V3_GET_SUPPLY_RATE_SELECTOR = "d955759d"

# Function selector for Comet.getBorrowRate(uint256 utilization) -> returns uint64
# keccak256("getBorrowRate(uint256)")[:4].hex() == "9fa83b5a"
_COMPOUND_V3_GET_BORROW_RATE_SELECTOR = "9fa83b5a"

# Compound V3 rate scaling: per-second rates are scaled by 1e18
_COMPOUND_V3_RATE_SCALE = Decimal("1000000000000000000")  # 1e18

# Compound V3 utilization scaling: 1e18 = 100%
_COMPOUND_V3_UTIL_SCALE = Decimal("1000000000000000000")  # 1e18

# Seconds per year for APY calculations
_SECONDS_PER_YEAR = 365 * 24 * 60 * 60

# Map token symbol to Comet market key (lowercase, matching COMPOUND_V3_COMET_ADDRESSES)
_COMPOUND_V3_TOKEN_TO_MARKET: dict[str, str] = {
    "USDC": "usdc",
    "USDC.e": "usdc_bridged",
    "USDT": "usdt",
    "WETH": "weth",
    "wstETH": "wsteth",
    "USDS": "usds",
}


def _compound_v3_resolve_comet_address(chain: str, asset_symbol: str) -> str:
    """Resolve the Comet contract address for ``asset_symbol`` on ``chain``.

    Raises ``RateHistoryUnavailable`` when the token is not mapped or the
    chain has no Comet market for it.
    """
    from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    market_key = _COMPOUND_V3_TOKEN_TO_MARKET.get(asset_symbol)
    if not market_key:
        raise RateHistoryUnavailable(
            "compound_v3",
            f"Token {asset_symbol!r} has no Comet market mapping on {chain!r}",
        )

    chain_comets = COMPOUND_V3_COMET_ADDRESSES.get(chain, {})
    comet_address = chain_comets.get(market_key)
    if not comet_address:
        raise RateHistoryUnavailable(
            "compound_v3",
            f"No Comet market for {asset_symbol!r} on {chain!r}",
        )
    return comet_address


def _compound_v3_resolve_rpc_url(servicer: Any, chain: str) -> str:
    """Resolve the RPC URL for ``chain`` via the servicer's settings.

    Raises ``RateHistoryUnavailable`` when no RPC URL is configured.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable
    from almanak.gateway.utils import get_rpc_url

    try:
        return get_rpc_url(chain, network=servicer.settings.network)
    except ValueError as exc:
        raise RateHistoryUnavailable(
            "compound_v3",
            f"No RPC URL configured for chain {chain!r}: {exc}",
        ) from exc


def _compound_v3_build_rate_calldata(side: str, utilization_raw: int) -> str:
    """Build the ``eth_call`` data field for ``getSupplyRate`` / ``getBorrowRate``."""
    padded_util = f"{utilization_raw:064x}"
    selector = _COMPOUND_V3_GET_SUPPLY_RATE_SELECTOR if side == "supply" else _COMPOUND_V3_GET_BORROW_RATE_SELECTOR
    return f"0x{selector}{padded_util}"


async def _compound_v3_eth_call_uint256(
    session: Any,
    rpc_url: str,
    to_addr: str,
    data: str,
    *,
    request_id: int,
    label: str,
    chain: str,
) -> int:
    """POST a single ``eth_call`` and decode the uint256 ``result`` as an int.

    All RPC / decode failure modes are normalised to ``RateHistoryUnavailable``
    with ``label`` distinguishing call sites in the error message.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": to_addr, "data": data}, "latest"],
        "id": request_id,
    }
    try:
        async with session.post(rpc_url, json=payload) as response:
            response.raise_for_status()
            result = await response.json()
    except Exception as exc:
        raise RateHistoryUnavailable(
            "compound_v3",
            f"{label} RPC request / decode failed for chain {chain!r}: {exc}",
        ) from exc

    if "error" in result:
        msg = result["error"].get("message", "RPC error")
        raise RateHistoryUnavailable("compound_v3", f"{label} failed: {msg}")

    result_hex = result.get("result", "")
    if not result_hex or result_hex == "0x":
        raise RateHistoryUnavailable("compound_v3", f"{label} returned empty")

    return int(result_hex, 16)


class CompoundV3GatewayConnector(
    GatewayConnector,
    GatewayLendingRateHistoryCapability,
    GatewayMarketLookupCapability,
    GatewayDefillamaSlugCapability,
):
    """Gateway-side connector for Compound v3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("compound_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self):
        """Return the awaitable Compound market-lookup singleton factory."""
        return get_compound_lookup

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Compound v3."""
        return "compound-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where Compound V3 lending rates are queryable.

        Equal to the chains the connector ships Comet addresses for.
        """
        from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES

        return frozenset(COMPOUND_V3_COMET_ADDRESSES.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        market_id: str | None = None,  # noqa: ARG002 — not market-scoped: see below
    ) -> Any:
        """Fetch live Compound V3 supply / borrow / utilisation via on-chain
        ``eth_call`` to ``Comet.getUtilization()`` + ``getSupplyRate(util)`` /
        ``getBorrowRate(util)``.

        Migrated verbatim from
        ``framework/data/rates/monitor.py:_fetch_compound_v3_rate_onchain``.
        ``servicer`` is the gateway-side ``RateHistoryServiceServicer`` —
        we read its shared aiohttp session + settings.

        ``market_id`` is accepted-and-ignored (VIB-5729): a Comet is already
        selected by its base ``asset_symbol``, and the returned point leaves
        ``market_id`` unset, so a market-scoped caller sees no echo and falls
        closed to unmeasured rather than trusting this rate.
        """
        from almanak.gateway.services.rate_history_service import LendingRatePoint

        comet_address = _compound_v3_resolve_comet_address(chain, asset_symbol)
        rpc_url = _compound_v3_resolve_rpc_url(servicer, chain)
        session = await servicer._get_http_session()

        # Step 1: getUtilization()
        utilization_raw = await _compound_v3_eth_call_uint256(
            session,
            rpc_url,
            comet_address,
            f"0x{_COMPOUND_V3_GET_UTILIZATION_SELECTOR}",
            request_id=1,
            label="getUtilization",
            chain=chain,
        )

        # Step 2: getSupplyRate(utilization) or getBorrowRate(utilization)
        rate_data = _compound_v3_build_rate_calldata(side, utilization_raw)
        rate_per_second = Decimal(
            await _compound_v3_eth_call_uint256(
                session,
                rpc_url,
                comet_address,
                rate_data,
                request_id=2,
                label="getRate",
                chain=chain,
            )
        )

        # Convert per-second rate to APY percentage:
        # APY = rate_per_second * SECONDS_PER_YEAR / 1e18 * 100
        apy_percent = rate_per_second * Decimal(_SECONDS_PER_YEAR) / _COMPOUND_V3_RATE_SCALE * Decimal("100")

        # Convert utilization to percentage
        utilization_percent = Decimal(utilization_raw) / _COMPOUND_V3_UTIL_SCALE * Decimal("100")

        logger.debug(
            "Compound V3 %s/%s/%s: %.4f%% (util: %.2f%%, on-chain)",
            asset_symbol,
            side,
            chain,
            float(apy_percent),
            float(utilization_percent),
        )

        # Side selection means the OTHER side is unmeasured by this call —
        # the framework caller passed ``side`` and only that side is what
        # we expose. Empty fields on the wire encode that ("Empty != Zero").
        supply = apy_percent if side == "supply" else None
        borrow = apy_percent if side == "borrow" else None

        return LendingRatePoint(
            timestamp=0,
            supply_apy_pct=supply,
            borrow_apy_pct=borrow,
            utilization_pct=utilization_percent,
        )

    async def fetch_lending_history(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        start_ts: int,
        end_ts: int,
    ) -> Any:
        """Historical lending series.

        Compound V3 historical APY series is sourced from the dedicated
        ``LendingAPYProvider`` (see ``framework/backtesting/pnl/providers/
        lending_apy.py``) which continues to consume TheGraph subgraph data
        through the shared ``SubgraphClient``. Surface lands in W7 step 4
        once the consumer rewrite wires through the gRPC service.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "compound_v3",
            "lending-history surface lands once the framework consumer rewrite ships (W7 step 4)",
        )


__all__ = ["CompoundV3GatewayConnector"]
