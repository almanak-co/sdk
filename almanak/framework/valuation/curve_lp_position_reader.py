"""On-chain Curve LP position reader (VIB-5420).

Values a Curve StableSwap LP position (a single fungible ERC-20 LP token, N
underlying coins, no NFT and no tick range) from live on-chain state. The
framework LP valuer's V3-NFT ``positions(uint256)`` read cannot value a Curve
LP-token balance — it would corrupt the LP-token address into a wrong decode
(the same class of bug as the V4 case). This reader fills that gap.

**Valuation model.** The canonical Curve mark is::

    value_usd = lp_balance(human) * virtual_price * peg

``virtual_price`` (1e18-scaled, read LIVE from the pool's ``get_virtual_price()``
getter) is the number of *underlying invariant units* one LP token is worth; it
grows above 1.0 as the pool accrues fees. For a USD-pegged StableSwap pool every
underlying coin is a ~$1 stablecoin, so the numeraire ``peg`` is ``$1`` and the
mark is ``lp_balance * virtual_price``. Pools whose numeraire is NOT USD (e.g.
``steth`` — ETH-denominated — or cryptoswap/tricrypto pools holding WETH/WBTC)
have ``peg != 1`` and are **out of scope for v1**: this reader fails closed
(returns ``None`` → the valuer flags ``UNAVAILABLE``) rather than mis-mark them
at ``peg=1`` (Empty ≠ Zero — a wrong mark is worse than no mark).

**Gateway boundary.** Both the LP-token ``balanceOf`` and the pool
``get_virtual_price()`` are read live through the gateway's generic ``eth_call``
(via the framework :class:`LPPositionReader`); this module opens no sockets and
holds no RPC URL. Pool metadata (address / coins) is resolved from a READ-ONLY
lookup of the Curve connector's static ``CURVE_POOLS`` registry — a pure data
read, no connector logic, no egress — reached via a LAZY
``importlib.import_module`` (the same seam ``FungibleLpPositionReader._BOOTSTRAP``
uses) so the framework→concrete-connector static-import ratchet stays clean.

Returns ``None`` on any failure (Empty ≠ Zero — the valuer then flags
``no_path`` / ``UNAVAILABLE``, never a fabricated zero); returns a measured
``lp_balance_wei = 0`` only for a genuinely empty (zero-balance) position.

VIB-5420. Mirrors :class:`VaultPositionReader` / :class:`FungibleLpPositionReader`.
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Curve StableSwap pools expose ``get_virtual_price()``; some newer NG / crypto
# pools use the alias ``virtual_price()``. Try the canonical getter first, then
# the alias, before failing closed.
_VIRTUAL_PRICE_SELECTORS = ("0xbb7b8b80", "0x0c46b72a")  # get_virtual_price(), virtual_price()
_VIRTUAL_PRICE_SCALE = Decimal(10**18)

# USD-pegged StableSwap numeraire: every coin in the pool tracks ~$1, so the LP
# token's underlying-invariant unit IS a USD unit (peg = $1). A pool whose coins
# are NOT all in this set is non-USD-numeraire (peg != 1) and out of v1 scope.
# The allowlist is deliberately conservative — adding a coin asserts "this token
# is a ~$1 USD stablecoin" and must be true on every supported chain.
_USD_STABLE_SYMBOLS = frozenset(
    {
        "USDC",
        "USDC.E",  # bridged USDC (Arbitrum/Optimism/Polygon) — 1:1 USDC
        "USDT",
        "DAI",
        "FRAX",
        "CRVUSD",
        "USDD",
        "TUSD",
        "BUSD",
        "GUSD",
        "LUSD",
        "MIM",
        "SUSD",
        "USDP",
        "DOLA",
        "GHO",
        "PYUSD",
        "USDE",
        # Bridged / wrapped USDC variants held by PLAIN USD-stable Curve pools
        # (audit P0-3). Each is a 1:1 USD-pegged wrapper of canonical USDC, so the
        # peg = $1 numeraire holds exactly as for native USDC:
        "USDBC",  # USD Base Coin — native-bridge bridged USDC on Base, 1:1 USDC
        "AXLUSDC",  # Axelar-wrapped USDC, 1:1 backed by USDC. Used by Base 4pool.
    }
)


@dataclass
class CurveLpPosition:
    """On-chain state of a Curve LP position (single LP token, N coins)."""

    lp_token: str
    pool_address: str
    lp_balance_wei: int
    virtual_price: Decimal
    # Curve LP tokens are an 18-decimal ERC-20 by protocol invariant (the pool's
    # ``totalSupply`` / ``balanceOf`` are 1e18-scaled, matching the 1e18-scaled
    # ``virtual_price``), so the value math ``lp_balance_wei / 1e18 × virtual_price``
    # is exact regardless of the underlying coins' decimals.
    decimals: int = 18  # decimal-policy-exempt: Curve LP token is always 18-dec (VIB-5420)
    coins: list[str] = field(default_factory=list)

    @property
    def is_active(self) -> bool:
        return self.lp_balance_wei > 0


def _resolve_curve_pool_meta(chain: str, *, pool: str, lp_token: str) -> dict[str, Any] | None:
    """Resolve a Curve pool's static metadata from the connector's pool registry.

    READ-ONLY lookup of the connector's ``CURVE_POOLS`` (static pool DATA only —
    no connector logic, no egress) to map a pool name / LP-token / pool address to
    its `{address, lp_token, coins}`. Matches by pool NAME first, then by LP-token
    / pool ADDRESS, so a discovered position (which may carry only an address) and
    a strategy-reported one (which may carry only a name) both resolve. Returns
    ``None`` when the pool is unknown.

    Resolved via a LAZY ``importlib.import_module`` (not a static ``import``) so
    the framework→concrete-connector static-import ratchet stays clean — the same
    seam ``FungibleLpPositionReader._BOOTSTRAP`` uses to reach the fluid connector
    without a top-level connector import. Recorded as the dated CONNECTOR_IMPORT
    exception in the coupling baseline (VIB-5420).
    """
    try:
        adapter = importlib.import_module("almanak.connectors.curve.adapter")
        curve_pools: dict[str, dict[str, dict[str, Any]]] = adapter.CURVE_POOLS
    except Exception:  # noqa: BLE001 — connector data optional; fail closed
        logger.debug("Curve pool registry resolution failed", exc_info=True)
        return None

    chain_pools = curve_pools.get(chain, {})
    if not chain_pools:
        return None

    # 1) by pool name (e.g. "3pool")
    if pool:
        meta = chain_pools.get(pool)
        if meta is not None:
            return meta

    # 2) by LP-token or pool address — try BOTH candidate addresses sequentially
    # (a stale `lp_token` detail must not mask a resolvable `pool` address, and
    # vice-versa).
    for addr in (lp_token, pool):
        if not addr:
            continue
        needle = addr.lower()
        if not needle.startswith("0x"):
            continue
        for meta in chain_pools.values():
            if str(meta.get("lp_token", "")).lower() == needle or str(meta.get("address", "")).lower() == needle:
                return meta
    return None


class CurveLpPositionReader:
    """Reads Curve LP positions (balance + live virtual_price) via the gateway.

    Capability-gated by :meth:`supports` so the valuer dispatches Curve LP
    positions here (and NOT into the V3-NFT read) without an inline protocol-name
    branch — mirroring the registry seam of :class:`FungibleLpPositionReader`.
    """

    # The single framework→connector protocol-string this reader keys on. Recorded
    # as an intentional, dated exception in the coupling baseline (VIB-5420),
    # mirroring the ``fluid_dex_lp`` precedent for the fungible-LP reader.
    _SUPPORTED_PROTOCOLS = frozenset({"curve"})

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        self._lp_reader = LPPositionReader(gateway_client)

    def set_gateway_client(self, gateway_client: object | None) -> None:
        self._gateway = gateway_client
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        self._lp_reader = LPPositionReader(gateway_client)

    def supports(self, protocol: str) -> bool:
        return bool(protocol) and protocol.lower() in self._SUPPORTED_PROTOCOLS

    def read_position(
        self,
        *,
        protocol: str,
        chain: str,
        pool: str,
        lp_token: str,
        wallet_address: str,
        coins: list[str] | None = None,
    ) -> CurveLpPosition | None:
        """Read LP-token balance + live virtual_price for a Curve LP position.

        Returns ``None`` on any failure (Empty ≠ Zero), a measured
        ``lp_balance_wei = 0`` position for a genuinely empty wallet, or the full
        :class:`CurveLpPosition` otherwise. Pools whose coins are not all
        USD-pegged stablecoins return ``None`` (v1 scope — peg must be $1).
        """
        if self._gateway is None or not self.supports(protocol):
            return None

        meta = _resolve_curve_pool_meta(chain, pool=pool, lp_token=lp_token)
        if meta is None:
            logger.debug("Curve pool meta unknown for pool=%s lp_token=%s on %s", pool, lp_token, chain)
            return None

        pool_address = str(meta.get("address", ""))
        lp_token_address = str(meta.get("lp_token", "") or lp_token)
        pool_coins = [str(c) for c in (coins or meta.get("coins") or [])]
        if not pool_address or not lp_token_address:
            return None

        # v1 scope: only USD-pegged StableSwap pools (peg = $1). A pool with any
        # non-USD-stable coin (steth, tricrypto, …) is fail-closed, not mis-marked.
        if not pool_coins or not all(c.upper() in _USD_STABLE_SYMBOLS for c in pool_coins):
            logger.debug(
                "Curve pool %s coins %s not all USD-pegged — out of v1 valuation scope",
                pool_address,
                pool_coins,
            )
            return None

        # LP-token balance for the wallet (live, gateway eth_call). None → fail
        # closed (unmeasured). A measured zero means an empty position.
        lp_balance_wei = self._lp_reader.read_erc20_balance(chain, lp_token_address, wallet_address)
        if lp_balance_wei is None:
            return None
        if lp_balance_wei == 0:
            return CurveLpPosition(
                lp_token=lp_token_address,
                pool_address=pool_address,
                lp_balance_wei=0,
                virtual_price=Decimal("0"),
                coins=pool_coins,
            )

        virtual_price = self._read_virtual_price(chain, pool_address)
        if virtual_price is None or virtual_price <= 0:
            # Empty ≠ Zero: an unreadable / non-positive virtual_price is
            # unmeasured, never a fabricated mark.
            return None

        return CurveLpPosition(
            lp_token=lp_token_address,
            pool_address=pool_address,
            lp_balance_wei=lp_balance_wei,
            virtual_price=virtual_price,
            coins=pool_coins,
        )

    def _read_virtual_price(self, chain: str, pool_address: str) -> Decimal | None:
        """Read the pool's live ``get_virtual_price()`` (1e18-scaled) via gateway.

        Tries ``get_virtual_price()`` then the ``virtual_price()`` alias. Returns
        the human-scaled Decimal, or ``None`` if neither getter resolves.
        """
        for selector in _VIRTUAL_PRICE_SELECTORS:
            raw = self._lp_reader.read_uint256_call(chain, pool_address, selector)
            if raw is not None and raw > 0:
                return Decimal(raw) / _VIRTUAL_PRICE_SCALE
        return None
