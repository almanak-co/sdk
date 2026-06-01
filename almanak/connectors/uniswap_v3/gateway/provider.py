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

import logging
import math
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
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


async def _fetch_pool_token_decimals(
    web3: Any,
    pool_address: str,
    block_identifier: int | str,
) -> tuple[int, int]:
    """Read token0 / token1 from the pool and their ``decimals()``.

    Four ``eth_call`` round-trips (token0, token1, t0.decimals(), t1.decimals()).
    Cheap enough for the prototype Step 2; Step 3 introduces a per-pool
    decimals cache in the servicer to amortise across repeated calls.
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


__all__ = ["UniswapV3GatewayConnector"]
