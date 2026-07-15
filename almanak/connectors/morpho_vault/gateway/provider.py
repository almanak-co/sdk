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

VIB-5040 — adds ``GatewayLendingRateHistoryCapability`` so MetaMorpho
(ERC-4626) vault APY becomes a live, gateway-backed ``lending_rate(...)``
read instead of an unavailable-raise that pins the demo at a permanent
HOLD. The provider measures the vault's realised APY directly on-chain:
``convertToAssets(1e18)`` (assets per share) at the latest block and at a
block-window earlier, annualised by the exact elapsed seconds between the
two blocks. No subgraph, no new egress — the read uses the ERC-4626
standard surface every MetaMorpho vault exposes, through the
``RateHistoryService`` servicer's shared aiohttp session (the gateway
sidecar is the correct egress layer; the strategy container never makes
this call).

The connector's ``protocol`` is ``morpho_vault`` but strategies/demos call
``lending_rate("metamorpho", …)``; ``lending_aliases()`` registers the
``metamorpho`` dispatch slug so the request routes here.
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

from ..addresses import METAMORPHO_VAULTS
from .vault_lookup import get_morpho_lookup

logger = logging.getLogger(__name__)

# =============================================================================
# VIB-5040 — MetaMorpho (ERC-4626) on-chain APY read (gateway-internal egress)
# =============================================================================
#
# APY is measured from the vault share price (assets per share):
#   r1 = convertToAssets(1e18) at the latest block
#   r0 = convertToAssets(1e18) at (latest - window) blocks
#   apy = (r1 / r0) ^ (seconds_per_year / elapsed_seconds) - 1
# where elapsed_seconds is the exact gap between the two blocks' timestamps.
# A pure on-chain read of a standard ERC-4626 surface — works on an Anvil
# fork (the historical block proxies to the fork's upstream archive).
#
# Selector: keccak256("convertToAssets(uint256)")[:4]
_METAMORPHO_CONVERT_TO_ASSETS_SELECTOR = "07a2d13a"
# 1e18 shares — the probe amount fed to convertToAssets.
_METAMORPHO_PROBE_SHARES = 10**18
# Block window over which the share-price delta is measured. Annualisation
# uses the measured elapsed time, so the window size only trades precision
# (too small = noisy) against archive depth — ~7200 blocks is hours of data
# on every supported chain.
_METAMORPHO_APY_WINDOW_BLOCKS = 7200
_SECONDS_PER_YEAR = 365 * 24 * 60 * 60


def _metamorpho_resolve_vault(chain: str, asset_symbol: str) -> str:
    """Resolve the representative MetaMorpho vault address for ``chain``.

    Soft-validates ``asset_symbol`` against the vault's underlying so a USDC
    request never silently returns a WETH vault's rate. Raises
    ``RateHistoryUnavailable`` when the chain has no vault or the asset clearly
    mismatches.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    entry = METAMORPHO_VAULTS.get(chain)
    if not entry or not entry.get("vault"):
        raise RateHistoryUnavailable(
            "metamorpho",
            f"No MetaMorpho vault configured on chain {chain!r}",
        )

    underlying = entry.get("underlying", "")
    if underlying:
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolved = get_token_resolver().resolve(asset_symbol, chain).address
        except Exception as exc:  # noqa: BLE001 — resolver failure must fail closed
            # If the requested asset symbol cannot be resolved, we cannot prove
            # it matches the vault's underlying — fail closed (UNAVAILABLE)
            # rather than return the vault's APY for a possibly-unsupported asset.
            raise RateHistoryUnavailable(
                "metamorpho",
                f"Cannot resolve MetaMorpho asset {asset_symbol!r} on {chain!r}",
            ) from exc
        if resolved and resolved.lower() != underlying.lower():
            raise RateHistoryUnavailable(
                "metamorpho",
                f"MetaMorpho vault on {chain!r} holds a different underlying than {asset_symbol!r}",
            )
    return entry["vault"]


def _metamorpho_resolve_rpc_url(servicer: Any, chain: str) -> str:
    """Resolve the RPC URL for ``chain``, raising ``RateHistoryUnavailable`` on failure."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable
    from almanak.gateway.utils import get_rpc_url

    try:
        return get_rpc_url(chain, network=servicer.settings.network)
    except ValueError as exc:
        raise RateHistoryUnavailable(
            "metamorpho",
            f"No RPC URL configured for chain {chain!r}: {exc}",
        ) from exc


async def _metamorpho_rpc(
    session: Any,
    *,
    rpc_url: str,
    method: str,
    params: list[Any],
    chain: str,
    label: str,
    request_id: int,
) -> Any:
    """POST a single JSON-RPC call and return the ``result`` field.

    Normalises transport / RPC failures to ``RateHistoryUnavailable``.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": request_id}
    try:
        async with session.post(rpc_url, json=payload) as response:
            response.raise_for_status()
            result = await response.json()
    except Exception as exc:
        # Do NOT interpolate ``exc`` into the client-facing reason: aiohttp /
        # RPC errors can embed the RPC URL (provider credentials). The full
        # cause is preserved via ``from exc`` for server-side logs only.
        raise RateHistoryUnavailable(
            "metamorpho",
            f"{label} RPC request / decode failed for chain {chain!r}",
        ) from exc

    if "error" in result:
        msg = result["error"].get("message", "RPC error")
        raise RateHistoryUnavailable("metamorpho", f"{label} failed: {msg}")
    return result.get("result")


async def _metamorpho_block_number(session: Any, *, rpc_url: str, chain: str) -> int:
    """Return the latest block number via ``eth_blockNumber``."""
    raw = await _metamorpho_rpc(
        session,
        rpc_url=rpc_url,
        method="eth_blockNumber",
        params=[],
        chain=chain,
        label="eth_blockNumber",
        request_id=1,
    )
    return int(str(raw), 16)


async def _metamorpho_block_timestamp(session: Any, *, rpc_url: str, block_hex: str, chain: str) -> int:
    """Return the unix timestamp of ``block_hex`` via ``eth_getBlockByNumber``."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    block = await _metamorpho_rpc(
        session,
        rpc_url=rpc_url,
        method="eth_getBlockByNumber",
        params=[block_hex, False],
        chain=chain,
        label="eth_getBlockByNumber",
        request_id=2,
    )
    if not block or "timestamp" not in block:
        raise RateHistoryUnavailable("metamorpho", f"block {block_hex} not found on {chain!r}")
    return int(str(block["timestamp"]), 16)


async def _metamorpho_convert_to_assets(
    session: Any,
    *,
    rpc_url: str,
    vault: str,
    block_hex: str,
    chain: str,
    request_id: int,
) -> int:
    """Read ``convertToAssets(1e18)`` from ``vault`` at ``block_hex``."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    calldata = f"0x{_METAMORPHO_CONVERT_TO_ASSETS_SELECTOR}{_METAMORPHO_PROBE_SHARES:064x}"
    raw = await _metamorpho_rpc(
        session,
        rpc_url=rpc_url,
        method="eth_call",
        params=[{"to": vault, "data": calldata}, block_hex],
        chain=chain,
        label="convertToAssets",
        request_id=request_id,
    )
    result_hex = str(raw or "")
    if not result_hex or result_hex == "0x":
        raise RateHistoryUnavailable("metamorpho", f"convertToAssets returned empty on {chain!r}")
    return int(result_hex, 16)


class MorphoVaultGatewayConnector(
    GatewayConnector,
    GatewayMarketLookupCapability,
    GatewayDefillamaSlugCapability,
    GatewayLendingRateHistoryCapability,
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

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability (VIB-5040)
    # ---------------------------------------------------------------------

    def lending_aliases(self) -> tuple[str, ...]:
        """Dispatch slug strategies/demos pass to ``lending_rate(...)``.

        The gateway dispatcher keys lending providers by ``protocol`` plus
        these aliases (see ``rate_history_service._lending_dispatch_keys``).
        The vault's strategy-facing protocol identity is ``metamorpho``.
        """
        return ("metamorpho",)

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where a representative MetaMorpho vault is registered."""
        return frozenset(METAMORPHO_VAULTS.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        market_id: str | None = None,  # noqa: ARG002 — not market-scoped: see below
    ) -> Any:
        """Fetch live MetaMorpho supply APY from the ERC-4626 share price (VIB-5040).

        Measures ``convertToAssets(1e18)`` at the latest block and a
        block-window earlier, then annualises the realised growth by the exact
        elapsed time. MetaMorpho vaults are supply-only, so ``side="borrow"``
        is unavailable. ``servicer`` is the gateway-side
        ``RateHistoryServiceServicer`` — we read its shared aiohttp session +
        settings, so no egress happens in the strategy container.

        ``market_id`` is accepted-and-ignored (VIB-5729): a MetaMorpho vault is
        selected by ``asset_symbol``, and its APY is a blended share-price rate
        across the vault's allocations — NOT a single Morpho Blue market's rate.
        The returned point leaves ``market_id`` unset, so a market-scoped caller
        sees no echo and falls closed to unmeasured.
        """
        from almanak.gateway.services.rate_history_service import LendingRatePoint, RateHistoryUnavailable

        if side == "borrow":
            raise RateHistoryUnavailable(
                "metamorpho",
                "MetaMorpho vaults are supply-only; no borrow rate exists",
            )

        vault = _metamorpho_resolve_vault(chain, asset_symbol)
        rpc_url = _metamorpho_resolve_rpc_url(servicer, chain)
        session = await servicer._get_http_session()

        latest = await _metamorpho_block_number(session, rpc_url=rpc_url, chain=chain)
        start_block = latest - _METAMORPHO_APY_WINDOW_BLOCKS
        if start_block < 1:
            raise RateHistoryUnavailable(
                "metamorpho",
                f"chain {chain!r} has too few blocks ({latest}) to measure a window",
            )
        latest_hex = hex(latest)
        start_hex = hex(start_block)

        assets_now = await _metamorpho_convert_to_assets(
            session, rpc_url=rpc_url, vault=vault, block_hex=latest_hex, chain=chain, request_id=3
        )
        assets_then = await _metamorpho_convert_to_assets(
            session, rpc_url=rpc_url, vault=vault, block_hex=start_hex, chain=chain, request_id=4
        )
        if assets_now <= 0 or assets_then <= 0:
            raise RateHistoryUnavailable("metamorpho", f"non-positive share price on {chain!r}")

        ts_now = await _metamorpho_block_timestamp(session, rpc_url=rpc_url, block_hex=latest_hex, chain=chain)
        ts_then = await _metamorpho_block_timestamp(session, rpc_url=rpc_url, block_hex=start_hex, chain=chain)
        elapsed = ts_now - ts_then
        if elapsed <= 0:
            raise RateHistoryUnavailable("metamorpho", f"non-positive elapsed window on {chain!r}")

        growth = Decimal(assets_now) / Decimal(assets_then)
        # apy = growth ^ (seconds_per_year / elapsed) - 1, via ln/exp on Decimal.
        apy_fraction = (growth.ln() * Decimal(_SECONDS_PER_YEAR) / Decimal(elapsed)).exp() - Decimal(1)
        apy_percent = apy_fraction * Decimal("100")

        logger.debug(
            "MetaMorpho %s/%s on %s: %.4f%% APY (share %s->%s over %ss, vault %s)",
            asset_symbol,
            side,
            chain,
            float(apy_percent),
            assets_then,
            assets_now,
            elapsed,
            vault,
        )

        return LendingRatePoint(
            timestamp=0,
            supply_apy_pct=apy_percent,
            borrow_apy_pct=None,
            utilization_pct=None,
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
        """Historical MetaMorpho APY series.

        The forward-path live read (``fetch_lending_current``) is what unblocks
        the demo APY gate; the historical share-price series (sampling
        ``convertToAssets`` across a block grid) is a follow-on and raises a
        clean ``success=False`` envelope until it ships.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "metamorpho",
            "MetaMorpho lending-history series is not yet implemented (live current-rate read only)",
        )


__all__ = ["MorphoVaultGatewayConnector"]
