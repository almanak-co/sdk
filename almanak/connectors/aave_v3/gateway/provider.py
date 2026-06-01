"""Gateway-side connector binding for Aave v3.

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Aave v3 receipt-token (aToken / vToken) lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 (VIB-4810) — the capability is declared but ``token_service``
continues to call ``get_aave_lookup`` directly. Phase 4 collapses the
explicit per-protocol accessor methods on ``TokenService`` into a loop
over ``GATEWAY_REGISTRY.capability_providers(GatewayMarketLookupCapability)``.

Phase 3 (VIB-4811) adds:

* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"aave-v3"``).
* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs (Ethereum,
  Arbitrum, Optimism, Polygon). Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
* ``GatewayPriceIdCapability`` — Aave governance token CoinGecko slug
  (``AAVE`` → ``aave``). Moved verbatim from
  ``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.

W1 (VIB-4853) adds:

* ``GatewayAddressCapability`` — per-chain Pool + PoolDataProvider +
  AaveOracle addresses, moved verbatim from
  ``almanak.core.contracts``. Non-connector callers (teardown
  discovery, valuation, rate monitor, ContractRegistry, CLI support
  matrix) resolve Aave addresses through this capability instead of
  importing the dict by name.

W7 (VIB-4859) adds:

* ``GatewayLendingRateHistoryCapability`` — live + historical supply /
  borrow APY + utilisation. The live path migrates the
  ``_fetch_aave_v3_rate_onchain`` body that used to live strategy-side
  in ``framework/data/rates/monitor.py`` (and opened its own
  ``httpx.AsyncClient``); the egress now happens through the
  ``RateHistoryService`` servicer's shared HTTP session, which is the
  correct layer for outbound network traffic.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
    GatewayLendingRateHistoryCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import AAVE_V3, AAVE_V3_TOKENS
from .market_lookup import get_aave_lookup

logger = logging.getLogger(__name__)

# =============================================================================
# W7 / VIB-4859 — Aave V3 on-chain rate fetch (live path)
# =============================================================================
#
# Migrated verbatim from
# ``almanak/framework/data/rates/monitor.py`` (W7 / VIB-4859). The
# strategy container no longer holds these — every byte of HTTP egress
# moves into the gateway sidecar via ``RateHistoryService``.

# Function selector for AaveProtocolDataProvider.getReserveData(address)
# Computed: keccak256("getReserveData(address)")[:4].hex() == "35ea6a75"
_AAVE_V3_GET_RESERVE_DATA_SELECTOR = "35ea6a75"

# Return value indices for AaveProtocolDataProvider.getReserveData()
# Returns: (unbacked, accruedToTreasuryScaled, totalAToken, totalStableDebt,
#           totalVariableDebt, liquidityRate, variableBorrowRate, stableBorrowRate,
#           averageStableBorrowRate, liquidityIndex, variableBorrowIndex, lastUpdateTimestamp)
_AAVE_IDX_TOTAL_ATOKEN = 2  # totalAToken (for utilization numerator)
_AAVE_IDX_TOTAL_VARIABLE_DEBT = 4  # totalVariableDebt (for utilization denominator)
_AAVE_IDX_LIQUIDITY_RATE = 5  # currentLiquidityRate = supply APY in ray
_AAVE_IDX_VARIABLE_BORROW_RATE = 6  # currentVariableBorrowRate = borrow APY in ray
_AAVE_MIN_RESPONSE_WORDS = 7  # Minimum words needed to read both rates

# Ray unit for Aave (1e27)
_RAY = Decimal("1000000000000000000000000000")


def _aave_v3_resolve_data_provider(chain: str) -> str:
    """Resolve the PoolDataProvider address for ``chain``.

    Raises ``RateHistoryUnavailable`` when the chain has no provider mapping.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    chain_contracts = AAVE_V3.get(chain, {})
    data_provider = chain_contracts.get("pool_data_provider")
    if not data_provider:
        raise RateHistoryUnavailable(
            "aave_v3",
            f"No PoolDataProvider configured on chain {chain!r}",
        )
    return data_provider


def _aave_v3_resolve_token_address(chain: str, asset_symbol: str) -> str:
    """Resolve the on-chain token address for ``asset_symbol`` on ``chain``.

    Tries the curated Aave registry first, then falls back to the global
    ``TokenResolver`` (in-process registry — no network call).
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    chain_tokens = AAVE_V3_TOKENS.get(chain, {})
    token_address = chain_tokens.get(asset_symbol)
    if token_address:
        return token_address

    from almanak.framework.data.tokens import get_token_resolver
    from almanak.framework.data.tokens.exceptions import TokenNotFoundError

    try:
        return get_token_resolver().resolve(asset_symbol, chain).address
    except TokenNotFoundError:
        raise RateHistoryUnavailable(
            "aave_v3",
            f"Token {asset_symbol!r} not in Aave v3 catalogue on {chain!r}",
        ) from None


def _aave_v3_resolve_rpc_url(servicer: Any, chain: str) -> str:
    """Resolve the RPC URL for ``chain``, raising ``RateHistoryUnavailable`` on failure."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable
    from almanak.gateway.utils import get_rpc_url

    try:
        return get_rpc_url(chain, network=servicer.settings.network)
    except ValueError as exc:
        raise RateHistoryUnavailable(
            "aave_v3",
            f"No RPC URL configured for chain {chain!r}: {exc}",
        ) from exc


async def _aave_v3_post_get_reserve_data(
    session: Any,
    *,
    rpc_url: str,
    data_provider: str,
    calldata: str,
    chain: str,
) -> str:
    """POST ``getReserveData(asset)`` and return the raw hex result.

    Normalises transport / decode / RPC failures to ``RateHistoryUnavailable``.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": data_provider, "data": calldata}, "latest"],
        "id": 1,
    }
    try:
        async with session.post(rpc_url, json=payload) as response:
            response.raise_for_status()
            rpc_result = await response.json()
    except Exception as exc:
        raise RateHistoryUnavailable(
            "aave_v3",
            f"RPC request / decode failed for chain {chain!r}: {exc}",
        ) from exc

    if "error" in rpc_result:
        msg = rpc_result["error"].get("message", "RPC error")
        raise RateHistoryUnavailable("aave_v3", f"eth_call error: {msg}")

    return rpc_result.get("result", "") or ""


def _aave_v3_split_hex_words(hex_data: str) -> list[bytes]:
    """Split a 0x-prefixed hex blob into 32-byte words."""
    raw = bytes.fromhex(hex_data[2:])
    word_size = 32
    return [raw[i : i + word_size] for i in range(0, len(raw), word_size)]


def _aave_v3_words_all_zero(words: list[bytes]) -> bool:
    """Return True when every word in ``words`` is the 32-byte zero word."""
    zero = b"\x00" * 32
    return all(word == zero for word in words)


def _aave_v3_decode_reserve_words(
    hex_data: str,
    *,
    chain: str,
    asset_symbol: str,
) -> list[bytes]:
    """Decode the ``getReserveData`` hex blob to a list of 32-byte words.

    Raises ``RateHistoryUnavailable`` on empty results, short responses,
    or all-zero structs (which signals "not a listed reserve").
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    if not hex_data or hex_data == "0x":
        raise RateHistoryUnavailable(
            "aave_v3",
            f"Token {asset_symbol!r} not a registered Aave reserve on {chain!r}",
        )

    words = _aave_v3_split_hex_words(hex_data)

    if len(words) < _AAVE_MIN_RESPONSE_WORDS:
        raise RateHistoryUnavailable(
            "aave_v3",
            f"unexpected getReserveData response: {len(words)} words (need {_AAVE_MIN_RESPONSE_WORDS})",
        )

    if _aave_v3_words_all_zero(words):
        raise RateHistoryUnavailable(
            "aave_v3",
            f"Token {asset_symbol!r} resolved but is not a listed Aave reserve on {chain!r}",
        )
    return words


def _aave_v3_compute_apy_and_utilization(
    words: list[bytes],
    *,
    side: str,
) -> tuple[Decimal, Decimal | None]:
    """Compute APY percentage (ray -> %) and utilisation (variable-debt/aToken)."""
    if side == "supply":
        apy_ray = Decimal(int.from_bytes(words[_AAVE_IDX_LIQUIDITY_RATE], "big"))
    else:
        apy_ray = Decimal(int.from_bytes(words[_AAVE_IDX_VARIABLE_BORROW_RATE], "big"))
    apy_percent = apy_ray / _RAY * Decimal("100")

    total_atoken = Decimal(int.from_bytes(words[_AAVE_IDX_TOTAL_ATOKEN], "big"))
    total_variable_debt = Decimal(int.from_bytes(words[_AAVE_IDX_TOTAL_VARIABLE_DEBT], "big"))
    utilization: Decimal | None = None
    if total_atoken > 0:
        utilization = total_variable_debt / total_atoken * Decimal("100")
    return apy_percent, utilization


# Aave v3 subgraph URLs. Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_AAVE_V3_SUBGRAPHS: dict[str, str] = {
    "aave-v3-ethereum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3",
    "aave-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum",
    "aave-v3-optimism": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism",
    "aave-v3-polygon": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon",
}


class AaveV3GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayLendingRateHistoryCapability,
    GatewayMarketLookupCapability,
    GatewayDefillamaSlugCapability,
    GatewaySubgraphCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for Aave v3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Aave v3 contract addresses for ``chain`` (or empty)."""
        return AAVE_V3.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Aave v3 addresses are registered."""
        return frozenset(AAVE_V3.keys())

    # The CLI support matrix consumes connector-level matrix data through
    # ``ConnectorManifest.matrix_entries`` on the strategy side
    # (see ``almanak/connectors/aave_v3/__init__.py``); declaring a
    # parallel gateway capability would duplicate the source of truth.

    def market_lookup(self):
        """Return the awaitable Aave market-lookup singleton factory.

        The underlying ``get_aave_lookup`` is a coroutine factory that
        returns a lazily-loaded singleton with disk-cache + retry
        plumbing (see ``ProtocolTokenLookup``). Phase 4 will swap this
        for an ``async`` capability contract; for Phase 1+2 the provider
        method just returns the callable so the capability registration
        is visible without coupling to the lookup's async lifecycle.
        """
        return get_aave_lookup

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Aave v3."""
        return "aave-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Aave v3 (one per supported chain)."""
        return dict(_AAVE_V3_SUBGRAPHS)

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Aave governance token."""
        return {"AAVE": "aave"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """Aave token addresses are resolved via ``TokenResolver`` on EVM chains."""
        return {}

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where Aave v3 lending rates are queryable.

        Equal to the chains the connector ships addresses for — anywhere
        we have a ``PoolDataProvider`` address we can do the on-chain
        ``getReserveData`` call.
        """
        return frozenset(AAVE_V3.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
    ) -> Any:
        """Fetch live Aave v3 supply / borrow / utilisation via on-chain
        ``eth_call`` to ``AaveProtocolDataProvider.getReserveData(asset)``.

        Migrated verbatim from
        ``framework/data/rates/monitor.py:_fetch_aave_v3_rate_onchain``.
        ``servicer`` is the gateway-side ``RateHistoryServiceServicer`` —
        we read its shared aiohttp session + settings.
        """
        from almanak.gateway.services.rate_history_service import LendingRatePoint

        data_provider = _aave_v3_resolve_data_provider(chain)
        token_address = _aave_v3_resolve_token_address(chain, asset_symbol)
        rpc_url = _aave_v3_resolve_rpc_url(servicer, chain)

        # Encode eth_call: getReserveData(address)
        padded_addr = token_address[2:].lower().zfill(64)
        calldata = f"0x{_AAVE_V3_GET_RESERVE_DATA_SELECTOR}{padded_addr}"

        session = await servicer._get_http_session()
        hex_data = await _aave_v3_post_get_reserve_data(
            session,
            rpc_url=rpc_url,
            data_provider=data_provider,
            calldata=calldata,
            chain=chain,
        )

        words = _aave_v3_decode_reserve_words(
            hex_data,
            chain=chain,
            asset_symbol=asset_symbol,
        )

        apy_percent, utilization = _aave_v3_compute_apy_and_utilization(words, side=side)

        logger.debug(
            "Aave V3 %s/%s/%s on %s: %.4f%% (utilization=%s)",
            asset_symbol,
            side,
            chain,
            data_provider,
            float(apy_percent),
            f"{float(utilization):.2f}%" if utilization is not None else "n/a",
        )

        # Side selection means the OTHER side is unmeasured by this call —
        # the framework caller passed ``side`` and only that side is what
        # we expose. Empty fields on the wire encode that ("Empty != Zero").
        supply = apy_percent if side == "supply" else None
        borrow = apy_percent if side == "borrow" else None

        # Timestamp 0 == "now" sentinel from the caller; the framework
        # client substitutes ``datetime.now(UTC)`` when packaging the
        # response into ``LendingRate`` to preserve pre-W7 semantics.
        return LendingRatePoint(
            timestamp=0,
            supply_apy_pct=supply,
            borrow_apy_pct=borrow,
            utilization_pct=utilization,
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

        Migration of ``framework/data/rates/history.py``'s TheGraph crawl
        + ``backtesting/pnl/providers/lending/aave_v3_apy.py`` arrives in
        Step 3 (lending cluster) of the W7 plan. For Step 2 (this PR),
        the historical lane raises ``RateHistoryUnavailable`` so the
        dispatcher surfaces a clean ``success=False`` envelope rather
        than fabricating data.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "aave_v3",
            "lending-history fan-out lands in W7 step 3 (lending cluster); see plan PR #2473 §5.3",
        )


__all__ = ["AaveV3GatewayConnector"]
