"""Gateway-side connector binding for Morpho Blue (VIB-4853 / W1).

Minimal Phase-3-scaffold-style binding so Morpho Blue can publish its
on-chain contract addresses through :class:`GatewayAddressCapability`
without forcing every consumer to import the connector by name. The
strategy-side connector code (adapter, compiler, flash-loan provider,
SDK) still lives under ``almanak/connectors/morpho_blue/``; this module
contributes the gateway-side address surface only.

Contributes:

* ``GatewayAddressCapability`` — per-chain Morpho Blue Morpho + Bundler
  addresses (per-chain because Arbitrum / Polygon / Monad each deployed
  at a distinct address that differs from the universal vanity address).
  Moved verbatim from the entries previously held in
  ``almanak.core.contracts``.

W7 (VIB-4859) adds:

* ``GatewayLendingRateHistoryCapability`` — live supply / borrow APY +
  utilisation. **VIB-5040** lights up the live path: the connector reads
  the market state from the Morpho singleton (``market(id)``) and the
  per-second borrow rate from the market's Adaptive-Curve IRM
  (``borrowRateView(MarketParams, Market)``), composes the supply rate
  (``borrowRate · utilisation · (1 − fee)``) and annualises with
  continuous compounding. Egress happens through the
  ``RateHistoryService`` servicer's shared aiohttp session — the gateway
  sidecar is the correct layer for outbound RPC (AGENTS.md §Gateway
  boundary); the strategy container never makes this call.

  Morpho Blue is per-*market*, not per-asset: a single "USDC supply rate"
  does not exist on-chain. For the strategy-facing
  ``lending_rate(protocol, asset_symbol, side)`` contract we select every
  registered market whose loan token is ``asset_symbol`` and return the
  best rate (highest supply APY / lowest borrow APY) as the asset's
  representative rate. That is a deterministic, real, on-chain number —
  never a fabricated placeholder.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayLendingRateHistoryCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import MORPHO_BLUE, MORPHO_MARKETS

logger = logging.getLogger(__name__)

# =============================================================================
# VIB-5040 — Morpho Blue on-chain live rate read (gateway-internal egress)
# =============================================================================
#
# Two eth_calls per market:
#   1. Morpho.market(bytes32 id) -> (totalSupplyAssets, totalSupplyShares,
#      totalBorrowAssets, totalBorrowShares, lastUpdate, fee) — six uint128.
#   2. IRM.borrowRateView(MarketParams, Market) -> uint256 borrow rate per
#      second (WAD, 1e18). MarketParams = (loanToken, collateralToken, oracle,
#      irm, lltv); Market is the six-field struct from call (1). Both tuples are
#      static, so the calldata is the selector + 11 concatenated 32-byte words.
#
# Selectors (keccak256(signature)[:4]):
_MORPHO_BLUE_MARKET_SELECTOR = "5c60e39a"  # market(bytes32)
_MORPHO_BLUE_BORROW_RATE_VIEW_SELECTOR = "8c00bf6b"
# ^ borrowRateView((address,address,address,address,uint256),
#                  (uint128,uint128,uint128,uint128,uint128,uint128))

# WAD (1e18) — Morpho rate / fee scale.
_WAD = Decimal("1000000000000000000")
# Seconds per year for APY compounding.
_SECONDS_PER_YEAR = 365 * 24 * 60 * 60
# Market struct has six uint128 words; that is also the minimum the IRM view
# call needs to be re-encoded.
_MARKET_STRUCT_WORDS = 6


def _morpho_blue_resolve_morpho_address(chain: str) -> str:
    """Resolve the Morpho singleton address for ``chain``.

    Raises ``RateHistoryUnavailable`` when the chain has no deployment.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    morpho = MORPHO_BLUE.get(chain, {}).get("morpho")
    if not morpho:
        raise RateHistoryUnavailable(
            "morpho_blue",
            f"No Morpho deployment configured on chain {chain!r}",
        )
    return morpho


def _morpho_blue_markets_for_asset(chain: str, asset_symbol: str) -> list[tuple[str, dict[str, Any]]]:
    """Return ``[(market_id, params), …]`` whose loan token is ``asset_symbol``.

    Raises ``RateHistoryUnavailable`` when no registered market lends the asset.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    chain_markets = MORPHO_MARKETS.get(chain, {})
    matches = [
        (market_id, params)
        for market_id, params in chain_markets.items()
        if str(params.get("loan_token", "")).upper() == asset_symbol.upper()
    ]
    if not matches:
        raise RateHistoryUnavailable(
            "morpho_blue",
            f"No registered Morpho Blue market lends {asset_symbol!r} on {chain!r}",
        )
    return matches


def _morpho_blue_resolve_rpc_url(servicer: Any, chain: str) -> str:
    """Resolve the RPC URL for ``chain``, raising ``RateHistoryUnavailable`` on failure."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable
    from almanak.gateway.utils import get_rpc_url

    try:
        return get_rpc_url(chain, network=servicer.settings.network)
    except ValueError as exc:
        raise RateHistoryUnavailable(
            "morpho_blue",
            f"No RPC URL configured for chain {chain!r}: {exc}",
        ) from exc


async def _morpho_blue_eth_call(
    session: Any,
    *,
    rpc_url: str,
    to_addr: str,
    data: str,
    chain: str,
    label: str,
    request_id: int = 1,
) -> str:
    """POST a single ``eth_call`` and return the raw 0x-prefixed hex result.

    All transport / decode / RPC failure modes normalise to
    ``RateHistoryUnavailable`` with ``label`` distinguishing the call site.
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
        # Do NOT interpolate ``exc`` into the client-facing reason: aiohttp /
        # RPC errors can embed the RPC URL (provider credentials). The full
        # cause is preserved via ``from exc`` for server-side logs only.
        raise RateHistoryUnavailable(
            "morpho_blue",
            f"{label} RPC request / decode failed for chain {chain!r}",
        ) from exc

    if "error" in result:
        msg = result["error"].get("message", "RPC error")
        raise RateHistoryUnavailable("morpho_blue", f"{label} failed: {msg}")

    result_hex = result.get("result", "") or ""
    if not result_hex or result_hex == "0x":
        raise RateHistoryUnavailable("morpho_blue", f"{label} returned empty")
    return result_hex


def _morpho_blue_split_words(hex_data: str) -> list[bytes]:
    """Split a 0x-prefixed hex blob into 32-byte words."""
    raw = bytes.fromhex(hex_data[2:])
    return [raw[i : i + 32] for i in range(0, len(raw), 32)]


def _morpho_blue_decode_market(hex_data: str) -> tuple[int, ...]:
    """Decode ``market(id)`` into its six uint128 fields.

    Raises ``RateHistoryUnavailable`` on a short response or an all-zero
    struct (the market id is not created on-chain — never a silent zero).
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    words = _morpho_blue_split_words(hex_data)
    if len(words) < _MARKET_STRUCT_WORDS:
        raise RateHistoryUnavailable(
            "morpho_blue",
            f"unexpected market() response: {len(words)} words (need {_MARKET_STRUCT_WORDS})",
        )
    values = tuple(int.from_bytes(word, "big") for word in words[:_MARKET_STRUCT_WORDS])
    if all(v == 0 for v in values):
        raise RateHistoryUnavailable(
            "morpho_blue",
            "market() returned an all-zero struct (market not created on-chain)",
        )
    return values


def _morpho_blue_encode_borrow_rate_view_calldata(
    params: dict[str, Any],
    market: tuple[int, ...],
) -> str:
    """ABI-encode ``borrowRateView(MarketParams, Market)`` calldata.

    Both arguments are static tuples, so the encoding is the selector
    followed by 5 (MarketParams) + 6 (Market) concatenated 32-byte words.
    """

    def _addr_word(value: str) -> str:
        return value[2:].lower().zfill(64) if value.startswith("0x") else value.lower().zfill(64)

    param_words = [
        _addr_word(str(params["loan_token_address"])),
        _addr_word(str(params["collateral_token_address"])),
        _addr_word(str(params["oracle"])),
        _addr_word(str(params["irm"])),
        f"{int(params['lltv']):064x}",
    ]
    market_words = [f"{value:064x}" for value in market]
    return f"0x{_MORPHO_BLUE_BORROW_RATE_VIEW_SELECTOR}{''.join(param_words)}{''.join(market_words)}"


def _morpho_blue_compute_apys(
    *,
    borrow_rate_per_second_wad: int,
    market: tuple[int, ...],
) -> tuple[Decimal, Decimal, Decimal]:
    """Compose ``(supply_apy_pct, borrow_apy_pct, utilisation_pct)`` from raw reads.

    APY uses continuous compounding (``exp(rate_ps · seconds_per_year) − 1``)
    to match Morpho's displayed rate; the supply side nets the protocol fee.
    """
    total_supply_assets = Decimal(market[0])
    total_borrow_assets = Decimal(market[2])
    fee = Decimal(market[5])

    borrow_rate_ps = Decimal(borrow_rate_per_second_wad) / _WAD
    utilisation = total_borrow_assets / total_supply_assets if total_supply_assets > 0 else Decimal(0)
    fee_fraction = fee / _WAD
    supply_rate_ps = borrow_rate_ps * utilisation * (Decimal(1) - fee_fraction)

    def _annualise(rate_ps: Decimal) -> Decimal:
        if rate_ps <= 0:
            return Decimal(0)
        return ((rate_ps * Decimal(_SECONDS_PER_YEAR)).exp() - Decimal(1)) * Decimal("100")

    return _annualise(supply_rate_ps), _annualise(borrow_rate_ps), utilisation * Decimal("100")


class MorphoBlueGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayLendingRateHistoryCapability,
):
    """Gateway-side connector for Morpho Blue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("morpho_blue")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Morpho Blue contract addresses for ``chain`` (or empty)."""
        return MORPHO_BLUE.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Morpho Blue addresses are registered."""
        return frozenset(MORPHO_BLUE.keys())

    # The CLI support matrix consumes Morpho Blue's matrix surface via
    # ``ConnectorManifest.matrix_entries`` on the strategy side
    # (see ``almanak/connectors/morpho_blue/__init__.py``).

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability (VIB-4859 / W7 · VIB-5040 live read)
    # ---------------------------------------------------------------------

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where Morpho Blue lending rates are queryable on-chain.

        Equal to the chains that ship a registered market catalogue — the
        ``market(id)`` + IRM ``borrowRateView`` reads need a known market id.
        """
        return frozenset(MORPHO_MARKETS.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
    ) -> Any:
        """Fetch live Morpho Blue supply / borrow / utilisation on-chain (VIB-5040).

        Reads ``market(id)`` from the Morpho singleton and ``borrowRateView``
        from each registered market's Adaptive-Curve IRM, then returns the best
        rate across the markets that lend ``asset_symbol`` (highest supply APY /
        lowest borrow APY). ``servicer`` is the gateway-side
        ``RateHistoryServiceServicer`` — we read its shared aiohttp session +
        settings, so no egress happens in the strategy container.
        """
        from almanak.gateway.services.rate_history_service import LendingRatePoint, RateHistoryUnavailable

        morpho_address = _morpho_blue_resolve_morpho_address(chain)
        markets = _morpho_blue_markets_for_asset(chain, asset_symbol)
        rpc_url = _morpho_blue_resolve_rpc_url(servicer, chain)
        session = await servicer._get_http_session()

        best: tuple[Decimal, Decimal, Decimal] | None = None
        last_error: RateHistoryUnavailable | None = None
        for market_id, params in markets:
            try:
                market_hex = await _morpho_blue_eth_call(
                    session,
                    rpc_url=rpc_url,
                    to_addr=morpho_address,
                    data=f"0x{_MORPHO_BLUE_MARKET_SELECTOR}{market_id[2:]}",
                    chain=chain,
                    label="market",
                    request_id=1,
                )
                market = _morpho_blue_decode_market(market_hex)
                borrow_rate_hex = await _morpho_blue_eth_call(
                    session,
                    rpc_url=rpc_url,
                    to_addr=str(params["irm"]),
                    data=_morpho_blue_encode_borrow_rate_view_calldata(params, market),
                    chain=chain,
                    label="borrowRateView",
                    request_id=2,
                )
                supply_apy, borrow_apy, utilisation = _morpho_blue_compute_apys(
                    borrow_rate_per_second_wad=int(borrow_rate_hex, 16),
                    market=market,
                )
            except RateHistoryUnavailable as exc:
                last_error = exc
                continue
            except (ArithmeticError, ValueError) as exc:
                # A single market with an unreadable / pathological rate (a
                # malformed borrowRateView hex that fails ``int(.., 16)``, or an
                # APY that overflows ``Decimal.exp``) must not crash the whole
                # multi-market scan — skip it like any other unreadable market
                # and fall through to ``last_error`` only if NONE are readable.
                last_error = RateHistoryUnavailable(
                    "morpho_blue",
                    f"market {market_id} rate compute failed: {exc}",
                )
                continue

            if best is None:
                best = (supply_apy, borrow_apy, utilisation)
            elif side == "supply" and supply_apy > best[0]:
                best = (supply_apy, borrow_apy, utilisation)
            elif side == "borrow" and borrow_apy < best[1]:
                best = (supply_apy, borrow_apy, utilisation)

        if best is None:
            raise last_error or RateHistoryUnavailable(
                "morpho_blue",
                f"No readable Morpho Blue market for {asset_symbol!r} on {chain!r}",
            )

        supply_apy, borrow_apy, utilisation = best
        logger.debug(
            "Morpho Blue %s/%s/%s: supply=%.4f%% borrow=%.4f%% util=%.2f%%",
            asset_symbol,
            side,
            chain,
            float(supply_apy),
            float(borrow_apy),
            float(utilisation),
        )

        # Side selection means the OTHER side is unmeasured by this call —
        # Empty fields on the wire encode that ("Empty != Zero").
        return LendingRatePoint(
            timestamp=0,
            supply_apy_pct=supply_apy if side == "supply" else None,
            borrow_apy_pct=borrow_apy if side == "borrow" else None,
            utilization_pct=utilisation,
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

        Morpho Blue historical APY series is sourced from the dedicated
        ``MorphoBlueAPYProvider`` (see ``framework/backtesting/pnl/providers/
        lending/morpho_apy.py``) which continues to consume TheGraph subgraph
        data through the shared ``SubgraphClient``. Surface lands in W7 step 4
        once the consumer rewrite wires through the gRPC service.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "morpho_blue",
            "lending-history surface lands once the framework consumer rewrite ships (W7 step 4)",
        )


__all__ = ["MorphoBlueGatewayConnector"]
