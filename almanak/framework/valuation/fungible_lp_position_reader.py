"""On-chain fungible-LP position reader (Phase 4 / VIB-5032).

Values a fungible ERC-20-share, two-token LP position (no NFT, no tick range)
from live on-chain state: share balance → per-share (token0, token1) claim via
the connector's resolver. The framework LP valuer is hardcoded to V3-NFT
``positions(uint256)`` + tick math and cannot value a share-balance position;
this reader fills that gap, mirroring ``VaultPositionReader`` (ERC-4626
share→assets) but two-legged.

Per-protocol logic stays connector-local: a connector registers a builder via
:func:`register_fungible_lp_reader`. The reader returns ``None`` on any failure
(Empty ≠ Zero — the valuer then flags ``no_path``/``UNAVAILABLE``, never a
fabricated zero). All reads route through the gateway.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

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


# Builder signature: (gateway_client, chain, wrapper, wallet) -> FungibleLpPosition | None
_Builder = Callable[[object, str, str, str], "FungibleLpPosition | None"]

_BUILDERS: dict[str, _Builder] = {}

# Lazy bootstrap: importing the module registers the protocol's builder. One
# entry per fungible-LP connector (v1: fluid_dex_lp). This is the single
# framework→connector PROTOCOL_STRING the coupling ratchet records as an
# intentional, dated exception (see VIB-5126 + the coupling baseline): with one
# fungible-LP connector, a manifest-field generalization is premature
# (rule-of-three). Promote to a ``fungible_lp_reader=ImportRef(...)`` manifest
# decl — resolved from the registry generically — when a 2nd fungible-LP
# connector lands (VIB-5126), which removes this map and the baseline entry.
_BOOTSTRAP: dict[str, str] = {
    "fluid_dex_lp": "almanak.connectors.fluid.dex_lp_valuation",
}


def register_fungible_lp_reader(protocol: str, builder: _Builder) -> None:
    """Register a connector-local fungible-LP position builder."""
    _BUILDERS[protocol.lower()] = builder


def _ensure_bootstrapped(protocol: str) -> None:
    key = protocol.lower()
    if key in _BUILDERS:
        return
    module_path = _BOOTSTRAP.get(key)
    if module_path is None:
        return
    try:
        import importlib

        importlib.import_module(module_path)
    except Exception:  # noqa: BLE001
        logger.debug("Fungible-LP reader bootstrap failed for protocol=%s", protocol, exc_info=True)


class FungibleLpPositionReader:
    """Reads fungible two-token LP positions via registered connector builders."""

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client

    def set_gateway_client(self, gateway_client: object | None) -> None:
        self._gateway = gateway_client

    def supports(self, protocol: str) -> bool:
        _ensure_bootstrapped(protocol)
        return protocol.lower() in _BUILDERS

    def read_position(
        self,
        *,
        protocol: str,
        chain: str,
        wrapper: str,
        wallet_address: str,
    ) -> FungibleLpPosition | None:
        """Read share balance + per-share token amounts. None on any failure."""
        if self._gateway is None:
            return None
        _ensure_bootstrapped(protocol)
        builder = _BUILDERS.get(protocol.lower())
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


__all__ = ["FungibleLpPosition", "FungibleLpPositionReader", "register_fungible_lp_reader"]
