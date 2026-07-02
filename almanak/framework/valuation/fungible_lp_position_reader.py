"""On-chain fungible-LP position reader (Phase 4 / VIB-5032).

Values a fungible ERC-20-share, two-token LP position (no NFT, no tick range)
from live on-chain state: share balance → per-share (token0, token1) claim via
the connector's resolver. The framework LP valuer is hardcoded to V3-NFT
``positions(uint256)`` + tick math and cannot value a share-balance position;
this reader fills that gap, mirroring ``VaultPositionReader`` (ERC-4626
share→assets) but two-legged.

Per-protocol logic stays connector-local: a connector declares
``position_read=PositionReadDecl(kind="fungible_lp", builder=ImportRef(...))``
on its ``CONNECTOR`` manifest, and this reader dispatches through
:class:`~almanak.connectors._strategy_base.position_read_registry.PositionReadRegistry`
(VIB-5126 — promotes the old hardcoded ``_BOOTSTRAP`` map onto the manifest, so
adding a fungible-LP connector needs no framework edit). The reader returns
``None`` on any failure (Empty ≠ Zero — the valuer then flags
``no_path``/``UNAVAILABLE``, never a fabricated zero). All reads route through
the gateway.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from almanak.connectors._strategy_base.position_read_base import FUNGIBLE_LP
from almanak.connectors._strategy_base.position_read_registry import PositionReadRegistry

logger = logging.getLogger(__name__)


@dataclass
class FungibleLpPosition:
    """On-chain state of a fungible two-token LP position."""

    wrapper: str
    token0_symbol: str
    token1_symbol: str
    token0_decimals: int
    token1_decimals: int
    amount0_wei: int
    amount1_wei: int
    shares_wei: int
    # Token contract addresses (VIB-5032): the valuer prices legs by ADDRESS
    # (engages the oracle's CoinGecko-by-address + DexScreener-by-address paths,
    # which a bare-symbol lookup does NOT — the gateway only builds a
    # ``ResolvedToken`` for address-form inputs). Default "" → valuer falls back
    # to symbol pricing, preserving behaviour for any builder not yet supplying
    # addresses.
    token0_address: str = ""
    token1_address: str = ""

    @property
    def is_active(self) -> bool:
        return self.shares_wei > 0


class FungibleLpPositionReader:
    """Reads fungible two-token LP positions via manifest-declared connector builders.

    Capability-gated by :meth:`supports`, which asks
    :class:`PositionReadRegistry` whether a protocol declares the ``fungible_lp``
    kind — so the valuer dispatches fungible-LP positions here (and NOT into the
    V3-NFT read) without a hardcoded protocol-name set.
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client

    def set_gateway_client(self, gateway_client: object | None) -> None:
        self._gateway = gateway_client

    def supports(self, protocol: str) -> bool:
        return PositionReadRegistry.kind(protocol) == FUNGIBLE_LP

    def read_position(
        self,
        *,
        protocol: str,
        chain: str,
        wrapper: str,
        wallet_address: str,
    ) -> FungibleLpPosition | None:
        """Read share balance + per-share token amounts. None on any failure."""
        if self._gateway is None or not self.supports(protocol):
            return None
        builder = PositionReadRegistry.builder(protocol)
        if builder is None:
            return None
        try:
            return builder(self._gateway, chain, wrapper, wallet_address)
        except Exception:  # noqa: BLE001
            logger.debug(
                "Fungible-LP on-chain read failed for protocol=%s wrapper=%s wallet=%s",
                protocol,
                wrapper,
                wallet_address,
                exc_info=True,
            )
            return None


__all__ = ["FungibleLpPosition", "FungibleLpPositionReader"]
