"""Gateway-side connector binding for Uniswap V3.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. Uniswap V3 contributes:

* ``GatewayPoolHistoryCapability`` — pool history is supported on
  Ethereum, Arbitrum, Base, Optimism, and Polygon (the chains with a
  registered Uniswap V3 subgraph). Previously this set lived in
  ``almanak.gateway.services.pool_history_service.SUPPORTED_POOL_PAIRS``.
* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"uniswap-v3"``).
* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs for the chains
  where Uniswap V3 pool history is available. Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
* ``GatewayPriceIdCapability`` — Uniswap governance token CoinGecko
  slug (``UNI`` → ``uniswap``). Moved verbatim from
  ``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.
* ``GatewayDexQuoteCapability`` — DEX quote function for the multi-DEX
  price service. The simulation logic stays on
  ``MultiDexPriceService`` (where it shares state with siblings);
  this connector only delegates dispatch.

W7 (VIB-4859) adds:

* ``GatewayDexTwapCapability`` — TWAP price observation via the pool's
  ``observe(secondsAgos)`` function. Migrates the
  ``_query_observe`` / ``_query_observe_at_block`` / ``_tick_to_price``
  bodies that used to live strategy-side in
  ``framework/backtesting/pnl/providers/twap.py`` (and instantiated
  ``Web3(Web3.HTTPProvider(rpc_url))`` directly). The egress now happens
  through the ``RateHistoryService`` servicer's per-chain ``AsyncWeb3``
  cache.

W7-followup (VIB-4870) adds:

* ``GatewayDexVolumeCapability`` — daily trading-volume history via the
  Uniswap V3 ``poolDayDatas`` subgraph. Migrates
  ``framework/backtesting/pnl/providers/dex/uniswap_v3_volume.py`` (which
  opened its own ``aiohttp`` session against TheGraph). Egress now runs
  on the ``RateHistoryService`` servicer's shared HTTP session via the
  ``_dex_volume_subgraph`` helper.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
    GatewayDexLwapCapability,
    GatewayDexQuoteCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
    GatewayPoolHistoryCapability,
    GatewayPriceIdCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import UNISWAP_V3

logger = logging.getLogger(__name__)

# =============================================================================
# W7 / VIB-4859 — Uniswap V3 ``observe()`` / ``slot0()`` selectors
# =============================================================================
#
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/twap.py``. The strategy
# container no longer holds these — the gateway servicer's per-chain
# ``AsyncWeb3`` cache invokes them server-side.

# observe(uint32[] secondsAgos) -> (int56[] tickCumulatives, uint160[] secondsPerLiquidityX128s)
_OBSERVE_SELECTOR = "883bdbfd"

# slot0() -> (sqrtPriceX96, tick, observationIndex, observationCardinality, ...)
_SLOT0_SELECTOR = "3850c7bd"

# liquidity() -> uint128 in-range liquidity (VIB-4948 — LWAP weighting).
_LIQUIDITY_SELECTOR = "1a686502"

# 2^96 — the Uniswap V3 sqrtPriceX96 fixed-point denominator.
_Q96 = Decimal(2) ** 96

# token0() / token1() — used for decimal-aware tick→price conversion.
_TOKEN0_SELECTOR = "0dfe1681"
_TOKEN1_SELECTOR = "d21220a7"

# ERC20 decimals() selector.
_DECIMALS_SELECTOR = "313ce567"

# Subgraph URLs for Uniswap V3. Keyed by the public alias the strategy
# caller passes (``"uniswap-v3-<chain>"``). Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_UNISWAP_V3_SUBGRAPHS: dict[str, str] = {
    "uniswap-v3-ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    "uniswap-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
    "uniswap-v3-optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
    "uniswap-v3-polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
    "uniswap-v3-base": "https://api.studio.thegraph.com/query/48211/uniswap-v3-base/version/latest",
}

# =============================================================================
# W7-followup / VIB-4870 — Uniswap V3 daily-volume subgraph spec
# =============================================================================
#
# Deployment IDs migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/uniswap_v3_volume.py``
# (``UNISWAP_V3_SUBGRAPH_IDS``). The strategy container no longer holds
# these — the ``RateHistoryService`` servicer queries TheGraph
# server-side via the shared ``_dex_volume_subgraph`` helper.
#
# Built lazily (a module-level constant import of the gateway-side
# ``DexVolumeSubgraphSpec`` would couple this connector module to the
# gateway services at import time). See ``_volume_spec`` below.
_UNISWAP_V3_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "base": "43Hwfi3dJSoGpyas9VwNoDAv28rqtbnqUk3EYCRr3j6i",
    "optimism": "Gc2DPCVq5UkBfyHjZDMbKTc7ynrjoSKxc6sHLKY9Pmjc",
    "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
}


class UniswapV3GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayPoolHistoryCapability,
    GatewayDefillamaSlugCapability,
    GatewaySubgraphCapability,
    GatewayPriceIdCapability,
    GatewayDexQuoteCapability,
    GatewayDexTwapCapability,
    GatewayDexLwapCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for Uniswap V3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Uniswap V3 contract addresses for ``chain`` (or empty)."""
        return UNISWAP_V3.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Uniswap V3 addresses are registered."""
        return frozenset(UNISWAP_V3.keys())

    def pool_history_supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 pool history is available.

        Matches the historical
        ``SUPPORTED_POOL_PAIRS`` Uniswap V3 entries in
        ``pool_history_service.py`` (Ethereum, Arbitrum, Base, Optimism,
        Polygon). The set is closed: a new chain requires a new
        subgraph URL contribution AND adding it here.
        """
        return frozenset(
            {
                "ethereum",
                "arbitrum",
                "base",
                "optimism",
                "polygon",
            }
        )

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Uniswap V3."""
        return "uniswap-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        """No alias variants ride this connector."""
        return {}

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Uniswap V3 (one per supported chain)."""
        return dict(_UNISWAP_V3_SUBGRAPHS)

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Uniswap governance token."""
        return {"UNI": "uniswap"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """UNI is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``Dex.UNISWAP_V3`` string."""
        return "uniswap_v3"

    def supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 quotes are available via the multi-DEX service.

        Matches the historical ``DEX_CHAINS`` entries that listed
        ``"uniswap_v3"`` (Ethereum, Arbitrum, Optimism, Polygon, Base).
        """
        return frozenset(
            {
                "ethereum",
                "arbitrum",
                "optimism",
                "polygon",
                "base",
            }
        )

    async def quote(
        self,
        service: Any,
        token_in: str,
        token_out: str,
        amount_in: Any,
    ) -> Any:
        """Delegate to ``MultiDexPriceService._get_uniswap_v3_quote``.

        The simulation helpers (default-price lookup, price-impact +
        slippage curves, mock-quote hooks) stay on the service so they
        keep their shared state. This capability layer only owns
        dispatch.
        """
        return await service._get_uniswap_v3_quote(token_in, token_out, amount_in)

    # ---------------------------------------------------------------------
    # GatewayDexTwapCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def twap_supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 ``observe()`` is queryable.

        Same set as ``pool_history_supported_chains`` — wherever we have
        a V3 deployment, the pool exposes ``observe`` (it's the standard
        V3 pool ABI).
        """
        return frozenset(
            {
                "ethereum",
                "arbitrum",
                "base",
                "optimism",
                "polygon",
            }
        )

    async def fetch_twap(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        secs_ago_start: int,
        secs_ago_end: int,
        as_of_block: int | None = None,
    ) -> Any:
        """Fetch a single TWAP observation via ``observe(secondsAgos)``.

        Migrated from
        ``framework/backtesting/pnl/providers/twap.py:_query_observe`` /
        ``_query_observe_at_block``. ``servicer`` is the
        ``RateHistoryServiceServicer`` — we use its per-chain
        ``AsyncWeb3`` cache so every TWAP query on the same chain reuses
        one provider instance.
        """
        return await _fetch_uniswap_v3_twap_observation(
            servicer,
            chain=chain,
            pool_address=pool_address,
            secs_ago_start=secs_ago_start,
            secs_ago_end=secs_ago_end,
            as_of_block=as_of_block,
            protocol="uniswap_v3",
        )

    async def fetch_twap_series(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any:
        """TWAP series.

        The pre-W7 framework code (``twap.py``) only computes a single
        observation at a time; building a series at ``interval_secs``
        spacing requires the block-by-block bisect that lives in
        ``_query_observe_at_block``. That fan-out arrives in Step 3 of
        the W7 plan (DEX TWAP cluster). For Step 2 (this PR), the series
        lane raises ``RateHistoryUnavailable``.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "uniswap_v3",
            "DEX TWAP series fan-out lands in W7 step 3 (DEX cluster); see plan PR #2473 §5.3",
        )

    # ---------------------------------------------------------------------
    # GatewayDexLwapCapability (VIB-4948 / L3 of ALM-2770)
    # ---------------------------------------------------------------------

    def lwap_supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 ``slot0()`` + ``liquidity()`` are queryable.

        Same set as ``twap_supported_chains`` — the LWAP read uses the
        standard V3 pool ABI present on every V3 deployment.
        """
        return frozenset(
            {
                "ethereum",
                "arbitrum",
                "base",
                "optimism",
                "polygon",
            }
        )

    async def fetch_lwap(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_addresses: list[str],
        min_liquidity: str = "",
        as_of_block: int | None = None,
        base_token: str = "",
        quote_token: str = "",
    ) -> Any:
        """Liquidity-weighted spot price across the supplied pools.

        ``servicer`` is the ``RateHistoryServiceServicer`` — we use its
        per-chain ``AsyncWeb3`` cache so all pool reads on the same chain
        reuse one provider. Pool resolution is done framework-side; this
        body reads ``slot0()`` + ``liquidity()`` + ``token0/token1`` per pool,
        filters to the requested ``{base_token, quote_token}`` pair (when
        supplied), and weights the survivors.
        """
        return await _fetch_uniswap_v3_lwap(
            servicer,
            chain=chain,
            pool_addresses=list(pool_addresses),
            min_liquidity=min_liquidity,
            as_of_block=as_of_block,
            protocol="uniswap_v3",
            base_token=base_token,
            quote_token=quote_token,
        )

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 daily-volume history is available.

        = the chains with a registered volume subgraph (migrated from the
        pre-W7 ``uniswap_v3_volume.UNISWAP_V3_SUBGRAPH_IDS`` keys).
        """
        return frozenset(_UNISWAP_V3_VOLUME_SUBGRAPH_IDS)

    async def fetch_volume_history(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any:
        """Daily trading-volume history via the V3 ``poolDayDatas`` subgraph.

        Migrated from
        ``framework/backtesting/pnl/providers/dex/uniswap_v3_volume.py``.
        The subgraph egress now runs on the servicer's shared HTTP session
        through the ``_dex_volume_subgraph`` helper — no strategy-side
        ``aiohttp`` session.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            _uniswap_v3_volume_spec(),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


def _uniswap_v3_volume_spec() -> Any:
    """Build the V3 daily-volume subgraph spec.

    Lazy (function-local import) so the connector module stays importable
    without eagerly pulling in the gateway services package.
    """
    from almanak.gateway.services._dex_volume_subgraph import DexVolumeSubgraphSpec

    return DexVolumeSubgraphSpec(
        dex_name="uniswap_v3",
        subgraph_ids=dict(_UNISWAP_V3_VOLUME_SUBGRAPH_IDS),
        entity="poolDayDatas",
        id_field="pool",
        volume_field="volumeUSD",
        source="uniswap_v3_subgraph",
    )


# =============================================================================
# ``observe()`` codec helpers — pure functions, no I/O
# =============================================================================
#
# Migrated from
# ``framework/backtesting/pnl/providers/twap.py``'s
# ``_encode_observe_call`` / ``_query_observe`` decode block. Living at
# module scope (not as connector methods) makes them callable from the
# AgniFinanceGatewayConnector when the V3-fork sibling lands its TWAP
# support, AND keeps the methods on the class purely about wiring.


def _encode_observe_call(seconds_agos: list[int]) -> str:
    """ABI-encode ``observe(uint32[] secondsAgos)`` calldata.

    Returns a 0x-prefixed hex string suitable for ``eth_call`` /
    ``web3.eth.call``.
    """
    offset = 32  # 0x20: dynamic data offset (points to array start)
    length = len(seconds_agos)

    calldata = f"0x{_OBSERVE_SELECTOR}"
    calldata += offset.to_bytes(32, byteorder="big").hex()
    calldata += length.to_bytes(32, byteorder="big").hex()
    for sec in seconds_agos:
        calldata += sec.to_bytes(32, byteorder="big").hex()
    return calldata


def _decode_observe_response(result: bytes) -> tuple[list[int], list[int]]:
    """Decode ``observe`` return data into ``(tickCumulatives, secondsPerLiquidityX128s)``.

    The pool's ``observe`` returns two parallel ``uint`` arrays; we
    only consume ``tickCumulatives`` to compute TWAP, but
    ``secondsPerLiquidity`` is returned alongside for future callers
    that may want it (liquidity-weighted price impact, etc.).
    """
    if len(result) < 128:
        raise ValueError(f"observe() response too short: {len(result)} bytes")

    offset_ticks = int.from_bytes(result[0:32], byteorder="big")
    offset_liquidity = int.from_bytes(result[32:64], byteorder="big")

    # tickCumulatives array.
    tick_array_start = offset_ticks
    tick_array_len = int.from_bytes(result[tick_array_start : tick_array_start + 32], byteorder="big")
    tick_cumulatives: list[int] = []
    for i in range(tick_array_len):
        element_start = tick_array_start + 32 + (i * 32)
        # int56 stored signed in the low 7 bytes; read as int256 with
        # sign extension. Empirically, V3 pools return values that fit
        # comfortably in int56 but the codec is int256 on the wire.
        raw_value = int.from_bytes(
            result[element_start : element_start + 32],
            byteorder="big",
            signed=True,
        )
        tick_cumulatives.append(raw_value)

    # secondsPerLiquidityCumulativeX128s array.
    liq_array_start = offset_liquidity
    liq_array_len = int.from_bytes(result[liq_array_start : liq_array_start + 32], byteorder="big")
    liquidity_cumulatives: list[int] = []
    for i in range(liq_array_len):
        element_start = liq_array_start + 32 + (i * 32)
        raw_value = int.from_bytes(result[element_start : element_start + 32], byteorder="big")
        liquidity_cumulatives.append(raw_value)

    return tick_cumulatives, liquidity_cumulatives


def _tick_to_price(
    tick: int,
    token0_decimals: int = 18,
    token1_decimals: int = 6,
) -> Decimal:
    """Convert a Uniswap V3 tick to token1/token0 price in human units.

    Tick formula: ``price = 1.0001^tick * 10^(token0_dec - token1_dec)``.
    The decimal adjustment converts the raw on-chain ratio to
    human-readable price (e.g. ``$3000`` for WETH/USDC instead of
    ``3e-15``).
    """
    base_price = Decimal(str(math.pow(1.0001, tick)))
    decimal_adjustment = Decimal(10 ** (token0_decimals - token1_decimals))
    return base_price * decimal_adjustment


async def _fetch_pool_tokens_and_decimals(
    web3: Any,
    pool_address: str,
    block_identifier: int | str,
) -> tuple[str, str, int, int]:
    """Read ``(token0_addr, token1_addr, token0_decimals, token1_decimals)``.

    Four ``eth_call`` round-trips (token0, token1, t0.decimals(), t1.decimals()).
    Cheap enough for the prototype Step 2; Step 3 introduces a per-pool
    decimals cache in the servicer to amortise across repeated calls.

    The token0/token1 ADDRESSES are returned alongside the decimals (lowercased)
    so the LWAP caller can filter a multi-pool set down to the requested pair
    without a second set of reads (VIB-4924 B2 follow-on).
    """
    t0_data = await web3.eth.call(
        {"to": pool_address, "data": f"0x{_TOKEN0_SELECTOR}"},
        block_identifier=block_identifier,
    )
    t1_data = await web3.eth.call(
        {"to": pool_address, "data": f"0x{_TOKEN1_SELECTOR}"},
        block_identifier=block_identifier,
    )

    # Each token() return is a single 32-byte word: address right-padded.
    t0_address = web3.to_checksum_address("0x" + t0_data[-20:].hex())
    t1_address = web3.to_checksum_address("0x" + t1_data[-20:].hex())

    t0_decimals_data = await web3.eth.call(
        {"to": t0_address, "data": f"0x{_DECIMALS_SELECTOR}"},
        block_identifier=block_identifier,
    )
    t1_decimals_data = await web3.eth.call(
        {"to": t1_address, "data": f"0x{_DECIMALS_SELECTOR}"},
        block_identifier=block_identifier,
    )

    # An empty return from ``decimals()`` (token address isn't a
    # contract, or the contract doesn't implement the ERC20 ABI) would
    # silently decode to ``0``, throwing the tick→price math off by
    # ``10^(t0_dec - t1_dec)`` of magnitude. Raise loudly so the caller
    # surfaces a typed ``RateHistoryUnavailable`` rather than emitting a
    # wildly wrong price. Gemini PR-review feedback (PR #2474).
    if not t0_decimals_data or not t1_decimals_data:
        raise ValueError(
            f"decimals() returned empty data for pool {pool_address!r} (token0={t0_address}, token1={t1_address})"
        )
    t0_decimals = int.from_bytes(t0_decimals_data, byteorder="big")
    t1_decimals = int.from_bytes(t1_decimals_data, byteorder="big")
    # ERC-20 ``decimals()`` is a ``uint8`` on-chain (0..255). A malicious
    # or non-ERC20 contract can return a much larger value, which would
    # trigger pathological ``10 ** (t0_dec - t1_dec)`` exponentiation in
    # the tick->price math. Bound to the on-chain type and raise loudly.
    # CodeRabbit PR-review feedback (PR #2474).
    if not (0 <= t0_decimals <= 255 and 0 <= t1_decimals <= 255):
        raise ValueError(
            f"Invalid ERC20 decimals for pool {pool_address!r}: token0={t0_decimals}, token1={t1_decimals}"
        )
    return t0_address.lower(), t1_address.lower(), t0_decimals, t1_decimals


async def _fetch_pool_token_decimals(
    web3: Any,
    pool_address: str,
    block_identifier: int | str,
) -> tuple[int, int]:
    """Read ``(token0_decimals, token1_decimals)`` for a pool (TWAP path)."""
    _t0, _t1, t0_decimals, t1_decimals = await _fetch_pool_tokens_and_decimals(web3, pool_address, block_identifier)
    return t0_decimals, t1_decimals


async def _twap_resolve_web3_and_pool(
    servicer: Any,
    chain: str,
    pool_address: str,
    *,
    protocol: str,
) -> tuple[Any, str]:
    """Return ``(web3, pool_checksum)`` for a TWAP call.

    Raises ``RateHistoryUnavailable`` when the chain has no RPC URL or
    the pool address fails the checksum decode.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        web3 = await servicer._get_web3(chain)
    except ValueError as exc:
        raise RateHistoryUnavailable(protocol, f"No RPC URL configured for chain {chain!r}: {exc}") from exc

    try:
        pool_checksum = web3.to_checksum_address(pool_address)
    except ValueError as exc:
        raise RateHistoryUnavailable(protocol, f"Invalid pool address {pool_address!r}: {exc}") from exc
    return web3, pool_checksum


async def _twap_call_observe(
    web3: Any,
    *,
    pool_checksum: str,
    seconds_agos: list[int],
    block_identifier: int | str,
    protocol: str,
    pool_address: str,
) -> tuple[list[int], list[int]]:
    """Encode + execute ``observe(secondsAgos)`` and decode the tick cumulatives.

    Failures are normalised to ``RateHistoryUnavailable`` with ``protocol``
    distinguishing call sites.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    calldata = _encode_observe_call(seconds_agos)
    try:
        result = await web3.eth.call(
            {"to": pool_checksum, "data": calldata},
            block_identifier=block_identifier,
        )
        # Decode inside the try so a malformed ``observe()`` payload
        # (raw ``ValueError`` from ``_decode_observe_response``) surfaces
        # as a typed ``RateHistoryUnavailable`` rather than leaking as a
        # gRPC INTERNAL error. CodeRabbit PR-review feedback (PR #2474).
        tick_cumulatives, liquidity_cumulatives = _decode_observe_response(result)
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"observe() request/decode failed on pool {pool_address!r}: {exc}",
        ) from exc

    if len(tick_cumulatives) < 2:
        raise RateHistoryUnavailable(
            protocol,
            f"observe() returned {len(tick_cumulatives)} tick(s); need >= 2",
        )
    return tick_cumulatives, liquidity_cumulatives


async def _twap_resolve_pool_decimals(
    web3: Any,
    pool_checksum: str,
    block_identifier: int | str,
    *,
    protocol: str,
    pool_address: str,
) -> tuple[int, int]:
    """Read pool decimals, wrapping failures as ``RateHistoryUnavailable``."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        return await _fetch_pool_token_decimals(web3, pool_checksum, block_identifier)
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"Failed to read token decimals for pool {pool_address!r}: {exc}",
        ) from exc


async def _fetch_uniswap_v3_twap_observation(
    servicer: Any,
    *,
    chain: str,
    pool_address: str,
    secs_ago_start: int,
    secs_ago_end: int,
    as_of_block: int | None,
    protocol: str,
) -> Any:
    """Shared single-observation TWAP fetch for Uniswap V3 + forks.

    ``protocol`` ("uniswap_v3" / "pancakeswap_v3" / "sushiswap_v3") is
    used only for error-message attribution — the on-chain ABI is
    identical across V3 forks.
    """
    from almanak.gateway.services.rate_history_service import (
        DexTwapPoint,
        RateHistoryUnavailable,
    )

    web3, pool_checksum = await _twap_resolve_web3_and_pool(servicer, chain, pool_address, protocol=protocol)

    seconds_elapsed = secs_ago_start - secs_ago_end
    if seconds_elapsed <= 0:
        raise RateHistoryUnavailable(
            protocol,
            f"non-positive window (start={secs_ago_start}, end={secs_ago_end})",
        )

    block_identifier: int | str = as_of_block if as_of_block is not None else "latest"
    tick_cumulatives, _liquidity = await _twap_call_observe(
        web3,
        pool_checksum=pool_checksum,
        seconds_agos=[secs_ago_start, secs_ago_end],
        block_identifier=block_identifier,
        protocol=protocol,
        pool_address=pool_address,
    )

    tick_diff = tick_cumulatives[1] - tick_cumulatives[0]
    tick_twap = tick_diff // seconds_elapsed

    t0_decimals, t1_decimals = await _twap_resolve_pool_decimals(
        web3,
        pool_checksum,
        block_identifier,
        protocol=protocol,
        pool_address=pool_address,
    )
    price = _tick_to_price(tick_twap, t0_decimals, t1_decimals)

    return DexTwapPoint(
        timestamp=int(time.time()),
        price=price,
        tick_observation_count=len(tick_cumulatives),
    )


# =============================================================================
# VIB-4948 — Uniswap V3 LWAP (liquidity-weighted spot across pools)
# =============================================================================


def _sqrt_price_x96_to_price(
    sqrt_price_x96: int,
    token0_decimals: int = 18,
    token1_decimals: int = 6,
) -> Decimal:
    """Decode a Uniswap V3 ``slot0().sqrtPriceX96`` to token1/token0 human price.

    ``price = (sqrtPriceX96 / 2^96)^2 * 10^(token0_dec - token1_dec)`` — the
    spot analogue of ``_tick_to_price``. Mirrors
    ``framework/data/pools/reader.decode_sqrt_price_x96`` (kept inline so the
    connector stays self-contained, same as ``_tick_to_price``).
    """
    sqrt_price = Decimal(sqrt_price_x96) / _Q96
    raw_price = sqrt_price * sqrt_price
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return raw_price * decimal_adjustment


async def _read_pool_spot_price(
    web3: Any,
    pool_address: str,
    block_identifier: int | str,
) -> tuple[Decimal, int, str, str] | None:
    """Read ``(price, in_range_liquidity, token0_lower, token1_lower)`` for one pool.

    ``price`` is the pool-native ``token1/token0`` human price. The token0 /
    token1 addresses are returned (lowercased) so the LWAP caller can filter a
    multi-pool set to the requested pair (a single foreign-pair pool would
    otherwise poison the liquidity-weighted average — VIB-4924 B2 follow-on).

    Returns ``None`` (skip this pool) when the address is not a readable V3
    pool — an uninitialised / non-existent pool, a malformed ``slot0()`` /
    ``liquidity()`` payload, or a decimals read that fails. The caller treats
    a fully-unreadable pool set as a structured ``RateHistoryUnavailable``;
    a single bad pool must not poison the weighted average.
    """
    try:
        pool_checksum = web3.to_checksum_address(pool_address)
        slot0_data = await web3.eth.call(
            {"to": pool_checksum, "data": f"0x{_SLOT0_SELECTOR}"},
            block_identifier=block_identifier,
        )
        if not slot0_data or len(slot0_data) < 32:
            return None
        sqrt_price_x96 = int.from_bytes(slot0_data[0:32], byteorder="big")
        if sqrt_price_x96 == 0:
            return None

        liquidity_data = await web3.eth.call(
            {"to": pool_checksum, "data": f"0x{_LIQUIDITY_SELECTOR}"},
            block_identifier=block_identifier,
        )
        if not liquidity_data:
            return None
        liquidity = int.from_bytes(liquidity_data, byteorder="big")

        t0_addr, t1_addr, t0_decimals, t1_decimals = await _fetch_pool_tokens_and_decimals(
            web3, pool_checksum, block_identifier
        )
    except Exception as exc:  # noqa: BLE001 — one bad pool is skipped, not fatal
        logger.debug("lwap pool read failed for %s: %s", pool_address, exc)
        return None

    price = _sqrt_price_x96_to_price(sqrt_price_x96, t0_decimals, t1_decimals)
    return price, liquidity, t0_addr, t1_addr


async def _fetch_uniswap_v3_lwap(
    servicer: Any,
    *,
    chain: str,
    pool_addresses: list[str],
    min_liquidity: str,
    as_of_block: int | None,
    protocol: str,
    base_token: str = "",
    quote_token: str = "",
) -> Any:
    """Shared liquidity-weighted spot fetch for Uniswap V3 + V3-style forks.

    Reads every pool concurrently (one ``AsyncWeb3`` per chain, reused), then
    computes ``LWAP = Σ(price·liquidity) / Σ(liquidity)`` over the pool-native
    ``token1/token0`` prices. ``protocol`` is used only for error-message
    attribution — the slot0/liquidity ABI is identical across V3 forks.

    Pair filtering (VIB-4924 B2 follow-on): when both ``base_token`` and
    ``quote_token`` addresses are supplied, every pool whose ``{token0, token1}``
    is not exactly ``{base, quote}`` is DROPPED. All surviving pools therefore
    share the same ``token0 = min(base, quote)`` orientation (a V3 invariant for
    a given pair), so the native-price weighting — and the single framework-side
    quote/base inversion — are well-defined. Without this guard a stale
    known-pools entry pointing at a different pair (observed live: an Ethereum
    PancakeSwap "USDC/WETH" entry that was actually a WETH/USDT pool) would
    dominate Σ(price·liq) and corrupt the result.

    Pools below ``min_liquidity`` (raw uint) are dropped; if that empties the
    set we fall back to the (pair-filtered) readable pools.
    """
    from almanak.gateway.services.rate_history_service import (
        DexLwapPoint,
        RateHistoryUnavailable,
    )

    if not pool_addresses:
        raise RateHistoryUnavailable(protocol, "no pool addresses supplied for LWAP")

    try:
        web3 = await servicer._get_web3(chain)
    except ValueError as exc:
        raise RateHistoryUnavailable(protocol, f"No RPC URL configured for chain {chain!r}: {exc}") from exc

    block_identifier: int | str = as_of_block if as_of_block is not None else "latest"

    try:
        min_liq = int(Decimal(min_liquidity)) if min_liquidity else 0
    except (ValueError, ArithmeticError) as exc:
        raise RateHistoryUnavailable(protocol, f"invalid min_liquidity {min_liquidity!r}: {exc}") from exc

    results = await asyncio.gather(*(_read_pool_spot_price(web3, addr, block_identifier) for addr in pool_addresses))
    readable = [r for r in results if r is not None]
    if not readable:
        raise RateHistoryUnavailable(
            protocol,
            f"no readable V3 pools among {pool_addresses} on {chain}",
        )

    # Pair filter: keep only pools that contain EXACTLY the requested pair.
    if base_token and quote_token:
        want = {base_token.lower(), quote_token.lower()}
        matched = [(price, liq) for (price, liq, t0, t1) in readable if {t0, t1} == want]
        if not matched:
            raise RateHistoryUnavailable(
                protocol,
                f"no readable pool among {pool_addresses} on {chain} contains the requested pair "
                f"{{{base_token}, {quote_token}}}",
            )
    else:
        matched = [(price, liq) for (price, liq, _t0, _t1) in readable]

    filtered = [(price, liq) for price, liq in matched if liq >= min_liq]
    # If the liquidity floor removed everything, fall back to the pair-matched
    # readable pools rather than fabricating a failure — mirrors the framework
    # PriceAggregator.lwap "all pools below threshold" path.
    pools = filtered or matched

    total_liquidity = sum(liq for _, liq in pools)
    if total_liquidity <= 0:
        # VIB-4924 I1: every readable pool has zero in-range liquidity. Equal-
        # weighting their spot prices here would fabricate an EXECUTION_GRADE
        # price out of economically unbacked (and trivially manipulable) data.
        # Fail closed — the framework surfaces this as PoolPriceUnavailableError
        # and the strategy HOLDs rather than trading on a phantom price.
        raise RateHistoryUnavailable(
            protocol,
            f"all readable V3 pools have zero in-range liquidity ({pool_addresses} on {chain}); "
            "refusing to fabricate an equal-weighted price",
        )

    weighted = sum((price * Decimal(liq) for price, liq in pools), Decimal(0))
    lwap_price = weighted / Decimal(total_liquidity)

    return DexLwapPoint(
        timestamp=int(time.time()),
        price=lwap_price,
        pool_count=len(pools),
    )


__all__ = ["UniswapV3GatewayConnector"]
