"""Gateway-side connector binding for Benqi (VIB-4811).

Contributes:

* ``GatewayPriceIdCapability`` — ``QI`` (governance token, Avalanche).
  Moved verbatim from ``AVALANCHE_TOKEN_IDS`` in
  ``almanak.gateway.data.price.coingecko``.
* ``GatewayLendingRateHistoryCapability`` — live base supply / borrow APY
  from each qiToken's ``supplyRatePerTimestamp()`` /
  ``borrowRatePerTimestamp()`` view through the gateway's RPC session.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayLendingRateHistoryCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

logger = logging.getLogger(__name__)

_SUPPLY_RATE_PER_TIMESTAMP_SELECTOR = "d3bd2c72"
_BORROW_RATE_PER_TIMESTAMP_SELECTOR = "cd91801c"
_RATE_SCALE = Decimal(10**18)
_SECONDS_PER_DAY = Decimal(86_400)
_DAYS_PER_YEAR = 365
_SYNTHETIC_MARKET_ID = "benqi"


def _resolve_qi_token(asset_symbol: str) -> str:
    """Resolve a BENQI underlying symbol or ERC-20 address to its qiToken.

    Connector-owned market metadata is the only authority. Unknown assets fail
    closed. AVAX is native and therefore has no underlying contract address;
    ``WAVAX`` is accepted as its priceable symbol spelling.
    """
    from almanak.connectors.benqi.adapter import BENQI_QI_TOKENS
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    requested = str(asset_symbol).strip()
    folded = requested.casefold()
    from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

    if folded in {"wavax", WRAPPED_NATIVE["avalanche"].casefold()}:
        folded = "avax"
    for symbol, market in BENQI_QI_TOKENS.items():
        if symbol.casefold() == folded:
            return str(market["qi_token"])
        underlying = market.get("underlying")
        if underlying and str(underlying).casefold() == folded:
            return str(market["qi_token"])
    raise RateHistoryUnavailable("benqi", f"No qiToken market for asset {asset_symbol!r} on Avalanche")


def _resolve_rpc_url(servicer: Any, chain: str) -> str:
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable
    from almanak.gateway.utils import get_rpc_url

    try:
        return get_rpc_url(chain, network=servicer.settings.network)
    except ValueError as exc:
        logger.warning("BENQI RPC configuration unavailable for %s", chain, exc_info=True)
        raise RateHistoryUnavailable("benqi", f"No RPC URL configured for chain {chain!r}") from exc


async def _eth_call_uint256(servicer: Any, *, chain: str, to: str, selector: str, label: str) -> int:
    """Execute one qiToken view through the gateway session and decode uint256."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    rpc_url = _resolve_rpc_url(servicer, chain)
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": to, "data": f"0x{selector}"}, "latest"],
        "id": 1,
    }
    try:
        session = await servicer._get_http_session()
        async with session.post(rpc_url, json=payload) as response:
            response.raise_for_status()
            result = await response.json()
    except Exception as exc:
        logger.warning("BENQI %s RPC request or JSON decode failed", label, exc_info=True)
        raise RateHistoryUnavailable("benqi", f"{label} RPC request failed") from exc
    if not isinstance(result, dict):
        logger.warning("BENQI %s RPC returned %s instead of an object", label, type(result).__name__)
        raise RateHistoryUnavailable("benqi", f"{label} RPC returned an invalid response")
    if "error" in result:
        # RPC error messages are remote-controlled and may contain an upstream
        # URL. Keep them gateway-side so provider credentials never cross the
        # strategy-container security boundary.
        logger.warning("BENQI %s RPC returned an error: %r", label, result["error"])
        raise RateHistoryUnavailable("benqi", f"{label} RPC call failed")
    if "result" not in result:
        logger.warning("BENQI %s RPC response omitted result", label)
        raise RateHistoryUnavailable("benqi", f"{label} RPC returned an invalid response")
    result_hex = result.get("result", "")
    if not isinstance(result_hex, str):
        logger.warning("BENQI %s RPC returned non-string result type %s", label, type(result_hex).__name__)
        raise RateHistoryUnavailable("benqi", f"{label} returned malformed uint256")
    if not result_hex or result_hex == "0x":
        raise RateHistoryUnavailable("benqi", f"{label} returned empty")
    try:
        return int(result_hex, 16)
    except (TypeError, ValueError) as exc:
        logger.warning("BENQI %s RPC returned malformed uint256 %r", label, result_hex)
        raise RateHistoryUnavailable("benqi", f"{label} returned malformed uint256") from exc


def _rate_per_timestamp_to_apy_percent(raw_rate: int) -> Decimal:
    """Match BENQI's published daily-compounded APY convention exactly."""
    daily_rate = Decimal(raw_rate) / _RATE_SCALE * _SECONDS_PER_DAY
    return ((Decimal(1) + daily_rate) ** _DAYS_PER_YEAR - Decimal(1)) * Decimal(100)


class BenqiGatewayConnector(GatewayConnector, GatewayLendingRateHistoryCapability, GatewayPriceIdCapability):
    """Gateway-side connector for Benqi (Avalanche lending)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("benqi")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Benqi governance token."""
        return {"QI": "benqi"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """QI is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    def lending_supported_chains(self) -> frozenset[str]:
        return frozenset({"avalanche"})

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        market_id: str | None = None,
    ) -> Any:
        """Read and annualise the selected qiToken's live base rate.

        BENQI's contracts define the value as a per-timestamp rate scaled by
        1e18. The published BENQI adapter convention compounds the per-second
        rate daily for 365 days; this method preserves that convention. Reward
        APY is deliberately excluded because L5 records protocol base rates.

        BENQI is a whole-account lending venue. ``market_id='benqi'`` is the
        canonical synthetic account scope used by accounting; it is not a
        qiToken identity. The requested ``asset_symbol`` independently and
        deterministically selects the qiToken whose rate is read.
        """
        from almanak.gateway.services.rate_history_service import LendingRatePoint, RateHistoryUnavailable

        if chain != "avalanche":
            raise RateHistoryUnavailable("benqi", f"BENQI lending rates are unavailable on {chain!r}")
        if side not in {"supply", "borrow"}:
            raise RateHistoryUnavailable("benqi", f"Unknown BENQI lending-rate side {side!r}")
        if market_id is not None and market_id.casefold() != _SYNTHETIC_MARKET_ID:
            raise RateHistoryUnavailable("benqi", f"Unknown BENQI market id {market_id!r}")

        qi_token = _resolve_qi_token(asset_symbol)
        selector = _SUPPLY_RATE_PER_TIMESTAMP_SELECTOR if side == "supply" else _BORROW_RATE_PER_TIMESTAMP_SELECTOR
        raw_rate = await _eth_call_uint256(
            servicer,
            chain=chain,
            to=qi_token,
            selector=selector,
            label=f"{side}RatePerTimestamp",
        )
        apy_percent = _rate_per_timestamp_to_apy_percent(raw_rate)
        logger.debug("BENQI %s/%s: %s%% base APY", asset_symbol, side, apy_percent)
        return LendingRatePoint(
            timestamp=0,
            supply_apy_pct=apy_percent if side == "supply" else None,
            borrow_apy_pct=apy_percent if side == "borrow" else None,
            utilization_pct=None,
            market_id=market_id,
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
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable("benqi", "Historical BENQI lending-rate series is not available")


__all__ = ["BenqiGatewayConnector"]
