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

# Crypto / non-USD pool spot-reserves reads (VIB-5428). A pool's tracked balance
# of coin ``i`` is read from its ``balances(i)`` getter — NOT an ERC-20
# ``balanceOf`` on the coin (which would miss native ETH that the pool holds
# directly, e.g. the steth pool's coin 0). Old pools (steth, tricrypto2) declare
# ``balances(int128)``; NG pools declare ``balances(uint256)`` — same ABI word
# layout for index 0..N, so we probe ``uint256`` first then ``int128``.
_BALANCES_SELECTORS = ("0x4903b0d1", "0x065a80d8")  # balances(uint256), balances(int128)
_TOTAL_SUPPLY_SELECTOR = "0x18160ddd"  # totalSupply()
_DECIMALS_SELECTOR = "0x313ce567"  # decimals()

# Native-ETH placeholder addresses a Curve pool's coin list uses for raw ETH
# (the steth pool holds native ETH as coin 0). These have no ``decimals()``
# getter — ETH is 18-decimal by definition. Source literals are kept in
# EIP-55-checksummed form (the static checksum scanner reads source literals)
# but lowercased INTO the frozenset, because every membership test lowercases
# the candidate address first — a checksummed set member would never match.
_NATIVE_ETH_ADDRESSES = frozenset(
    a.lower()
    for a in (
        "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",  # Curve native-ETH sentinel
        "0x0000000000000000000000000000000000000000",
    )
)
_NATIVE_ETH_DECIMALS = 18  # decimal-policy-exempt: native ETH is always 18-dec (VIB-5428)

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
    # Coin contract addresses (same order as ``coins``), resolved from the pool
    # registry. Carried so the valuer can price each coin by ADDRESS against the
    # independent oracle for the depeg cross-check (VIB-5426) — address-form
    # pricing engages the oracle's by-address market sources a bare symbol skips,
    # which is what makes a real depeg visible. Empty when the registry has no
    # addresses for the pool; the valuer then falls back to symbol pricing.
    coin_addresses: list[str] = field(default_factory=list)

    # ── Per-pool-family valuation dispatch (VIB-5427 / VIB-5428) ──────────────
    # Which valuation family the valuer marks this position with:
    #   "usd_stable"    — lp × virtual_price × $1, depeg-checked vs $1 (existing).
    #   "metapool_usd"  — USD metapool (meta coin + base-pool coins all USD);
    #                     lp × metapool virtual_price × $1, depeg-checked over the
    #                     EXPANDED underlying set (VIB-5427).
    #   "crypto"        — non-USD / volatile pool (tricrypto, cryptoswap, steth);
    #                     valued from spot reserves × independent oracle prices
    #                     (VIB-5428). No $1 peg check (no peg to hold).
    family: str = "usd_stable"

    # Metapool (VIB-5427). The base pool's live ``get_virtual_price()`` (USD per
    # base-LP unit) — read as a diagnostic / model anchor; ``None`` when
    # unreadable. ``underlying_*`` are the EXPANDED underlying coin set
    # ([meta coins] + [base-pool coins]) the valuer prices for the depeg
    # cross-check, since the base-LP coin itself (e.g. 3CRV) is not an
    # oracle-priceable symbol.
    base_pool_virtual_price: Decimal | None = None
    underlying_coins: list[str] = field(default_factory=list)
    underlying_coin_addresses: list[str] = field(default_factory=list)

    # Crypto / non-USD pool (VIB-5428) spot-reserves inputs. ``reserves_wei`` is
    # the pool's tracked balance of each coin (same order as ``coins``),
    # ``coin_decimals`` each coin's decimals, ``total_supply_wei`` the LP token's
    # total supply. The valuer marks
    #   value = (lp_balance / total_supply) × Σ (reserve_i / 10^dec_i) × oracle_price_i.
    total_supply_wei: int | None = None
    reserves_wei: list[int] = field(default_factory=list)
    coin_decimals: list[int] = field(default_factory=list)

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
        """Read LP-token balance + the family-specific on-chain inputs needed to
        mark a Curve LP position.

        Returns ``None`` on any failure (Empty ≠ Zero), a measured
        ``lp_balance_wei = 0`` position for a genuinely empty wallet, or the full
        :class:`CurveLpPosition` otherwise. The pool is classified into a
        valuation FAMILY (:meth:`_classify_family`) — USD-stable, USD metapool,
        or crypto/non-USD — and the inputs that family needs are read live; a
        pool that fits no safely-valuable family returns ``None`` (fail closed,
        never mis-marked).
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

        family = self._classify_family(meta, pool_coins, coins_overridden=coins is not None)
        if family is None:
            logger.debug(
                "Curve pool %s coins %s fits no safely-valuable family — fail closed (not mis-marked)",
                pool_address,
                pool_coins,
            )
            return None

        # Coin addresses (registry, same order as ``meta["coins"]``) for the
        # depeg cross-check's by-address oracle pricing (VIB-5426). Carry them
        # ONLY when they align 1:1 with the resolved coins — a caller-supplied
        # ``coins`` override that reorders/subsets the pool must not let an
        # address map to the wrong coin; the valuer falls back to symbol pricing.
        meta_coins = [str(c) for c in (meta.get("coins") or [])]
        meta_coin_addresses = [str(a) for a in (meta.get("coin_addresses") or [])]
        coin_addresses = (
            meta_coin_addresses if (pool_coins == meta_coins and len(meta_coin_addresses) == len(pool_coins)) else []
        )

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
                coin_addresses=coin_addresses,
                family=family,
            )

        if family == "crypto":
            return self._read_crypto_position(
                chain=chain,
                pool_address=pool_address,
                lp_token_address=lp_token_address,
                lp_balance_wei=lp_balance_wei,
                coins=pool_coins,
                coin_addresses=coin_addresses,
            )

        # USD-stable + USD-metapool both mark off the pool's own virtual_price.
        virtual_price = self._read_virtual_price(chain, pool_address)
        if virtual_price is None or virtual_price <= 0:
            # Empty ≠ Zero: an unreadable / non-positive virtual_price is
            # unmeasured, never a fabricated mark.
            return None

        if family == "metapool_usd":
            return self._build_metapool_position(
                chain=chain,
                meta=meta,
                pool_address=pool_address,
                lp_token_address=lp_token_address,
                lp_balance_wei=lp_balance_wei,
                virtual_price=virtual_price,
                pool_coins=pool_coins,
                coin_addresses=coin_addresses,
            )

        return CurveLpPosition(
            lp_token=lp_token_address,
            pool_address=pool_address,
            lp_balance_wei=lp_balance_wei,
            virtual_price=virtual_price,
            coins=pool_coins,
            coin_addresses=coin_addresses,
            family="usd_stable",
        )

    @staticmethod
    def _classify_family(meta: dict[str, Any], pool_coins: list[str], *, coins_overridden: bool) -> str | None:
        """Classify a pool into its valuation family, or ``None`` (fail closed).

        * ``"metapool_usd"`` — a USD metapool: ``is_metapool`` with a resolvable
          ``base_pool``, every non-base (meta) coin AND every ``base_pool_coins``
          entry a USD stable. Valued at ``lp × metapool virtual_price × $1`` (the
          metapool's own ``get_virtual_price`` already rate-incorporates the base
          pool), with the depeg cross-check run over the expanded underlying set.
        * ``"usd_stable"`` — a plain pool whose coins are all USD stables.
        * ``"crypto"`` — a non-USD / volatile pool (steth, tricrypto, cryptoswap)
          with a registry address for every coin, so the valuer can price each by
          address against the independent oracle and mark from spot reserves.
        * ``None`` — fits none safely (e.g. a metapool with a non-USD base, or a
          non-USD pool missing coin addresses). Fail closed, never mis-marked.

        A caller-supplied ``coins`` override is honoured ONLY for the all-USD
        check (it cannot reclassify the pool's structure): metapool / crypto
        structure is read from the registry ``meta``, never from the override.
        """
        if not pool_coins:
            return None

        if bool(meta.get("is_metapool")):
            base_pool = str(meta.get("base_pool") or "")
            base_coins = [str(c).upper() for c in (meta.get("base_pool_coins") or [])]
            meta_native_coins = [str(c).upper() for c in (meta.get("coins") or [])]
            if len(meta_native_coins) < 2 or not base_pool or not base_coins:
                return None
            # Standard Curve metapool layout: coins = [meta coin(s)…, base-LP].
            # Every meta coin and every base-pool coin must be a USD stable for
            # the $1 numeraire to hold end to end.
            meta_coins = meta_native_coins[:-1]
            if not meta_coins or not all(c in _USD_STABLE_SYMBOLS for c in meta_coins):
                return None
            if not all(c in _USD_STABLE_SYMBOLS for c in base_coins):
                return None
            return "metapool_usd"

        if all(c.upper() in _USD_STABLE_SYMBOLS for c in pool_coins):
            return "usd_stable"

        # Non-USD / volatile: valuable from spot reserves × oracle prices only
        # when every coin has a registry address to price by. A coins override
        # that breaks the 1:1 address alignment forfeits this family (the valuer
        # would mis-map an address to the wrong coin).
        meta_addresses = [str(a) for a in (meta.get("coin_addresses") or [])]
        meta_coins_canon = [str(c) for c in (meta.get("coins") or [])]
        if coins_overridden and pool_coins != meta_coins_canon:
            return None
        if len(meta_addresses) == len(pool_coins) and all(meta_addresses):
            return "crypto"
        return None

    def _build_metapool_position(
        self,
        *,
        chain: str,
        meta: dict[str, Any],
        pool_address: str,
        lp_token_address: str,
        lp_balance_wei: int,
        virtual_price: Decimal,
        pool_coins: list[str],
        coin_addresses: list[str],
    ) -> CurveLpPosition:
        """Assemble a USD-metapool position (VIB-5427).

        Marked at ``lp × virtual_price × $1`` like a plain USD pool, but the depeg
        cross-check must run over the EXPANDED underlying set — [meta coins] +
        [base-pool coins] — because the base-LP coin (e.g. 3CRV) is itself not an
        oracle-priceable symbol. The base pool's live ``get_virtual_price()`` is
        read as a diagnostic / model anchor (a miss does not fail the position —
        the mark uses the metapool's own vp, not the base vp).
        """
        meta_native_coins = [str(c) for c in (meta.get("coins") or [])]
        meta_native_addresses = [str(a) for a in (meta.get("coin_addresses") or [])]
        base_coins = [str(c) for c in (meta.get("base_pool_coins") or [])]
        base_addresses = [str(a) for a in (meta.get("base_pool_coin_addresses") or [])]
        # Meta coins are all but the trailing base-LP coin; expand the base-LP
        # leg into the base pool's underlying coins for the depeg cross-check.
        underlying_coins = meta_native_coins[:-1] + base_coins
        underlying_addresses = (
            meta_native_addresses[:-1] + base_addresses
            if len(meta_native_addresses) == len(meta_native_coins) and len(base_addresses) == len(base_coins)
            else []
        )
        base_pool_address = str(meta.get("base_pool") or "")
        base_vp = self._read_virtual_price(chain, base_pool_address) if base_pool_address else None

        return CurveLpPosition(
            lp_token=lp_token_address,
            pool_address=pool_address,
            lp_balance_wei=lp_balance_wei,
            virtual_price=virtual_price,
            coins=pool_coins,
            coin_addresses=coin_addresses,
            family="metapool_usd",
            base_pool_virtual_price=base_vp,
            underlying_coins=underlying_coins,
            underlying_coin_addresses=underlying_addresses,
        )

    def _read_crypto_position(
        self,
        *,
        chain: str,
        pool_address: str,
        lp_token_address: str,
        lp_balance_wei: int,
        coins: list[str],
        coin_addresses: list[str],
    ) -> CurveLpPosition | None:
        """Read spot-reserves inputs for a crypto / non-USD pool (VIB-5428).

        Reads the LP token ``totalSupply()``, each coin's pool ``balances(i)``,
        and each coin's ``decimals()`` (native ETH → 18 without a call). Returns
        ``None`` (fail closed, Empty ≠ Zero) if any read misses or the total
        supply is non-positive — the valuer then flags UNAVAILABLE rather than
        mark from a partial read.

        ⚠️ COIN-ORDER INVARIANT (VIB-5539). The crypto mark pairs ``balances(i)``
        (read by on-chain index ``i``) with ``coin_addresses[i]`` / ``coins[i]``
        from the static registry, and the valuer prices reserve ``i`` with the
        oracle price of ``coin_addresses[i]``. This TRUSTS the registry
        ``coin_addresses`` order to match the pool's on-chain ``coins(i)`` order.
        It is hand-verified for the in-scope pools (steth, tricrypto2, base
        WETH/cbETH) but is NOT yet validated on-chain. A wrong order pairs a
        reserve with the wrong coin's price → a ~10^10 mis-mark (e.g. WBTC reserve
        priced as USDT). **VIB-5539 (on-chain ``coins(i)`` validation, sibling of
        VIB-5424) MUST land before any new crypto pool is added to the registry.**
        """
        if not coin_addresses or len(coin_addresses) != len(coins):
            # Crypto pricing is by address; without a full address set we cannot
            # safely map reserves to oracle prices.
            return None

        total_supply = self._lp_reader.read_uint256_call(chain, lp_token_address, _TOTAL_SUPPLY_SELECTOR)
        if total_supply is None or total_supply <= 0:
            return None

        reserves: list[int] = []
        for i in range(len(coins)):
            reserve = self._read_pool_balance(chain, pool_address, i)
            if reserve is None:
                return None
            reserves.append(reserve)

        decimals: list[int] = []
        for address in coin_addresses:
            dec = self._read_coin_decimals(chain, address)
            if dec is None:
                return None
            decimals.append(dec)

        return CurveLpPosition(
            lp_token=lp_token_address,
            pool_address=pool_address,
            lp_balance_wei=lp_balance_wei,
            virtual_price=Decimal("0"),  # unused for crypto (spot-reserves mark)
            coins=coins,
            coin_addresses=coin_addresses,
            family="crypto",
            total_supply_wei=total_supply,
            reserves_wei=reserves,
            coin_decimals=decimals,
        )

    def _read_pool_balance(self, chain: str, pool_address: str, index: int) -> int | None:
        """Read a pool's tracked ``balances(index)`` (probes uint256 then int128).

        Returns the raw wei reserve, or ``None`` if neither getter resolves
        (Empty ≠ Zero — the caller fails closed). A measured ``0`` reserve is a
        valid empty-leg reading and is returned as ``0``.
        """
        index_word = hex(index)[2:].zfill(64)
        for selector in _BALANCES_SELECTORS:
            raw = self._lp_reader.read_uint256_call(chain, pool_address, selector + index_word)
            if raw is not None:
                return raw
        return None

    def _read_coin_decimals(self, chain: str, coin_address: str) -> int | None:
        """Read a coin's ERC-20 ``decimals()`` (native ETH → 18, no call).

        Returns ``None`` (fail closed) on an unreadable / implausible value so the
        valuer never scales a reserve by a fabricated decimals count.
        """
        if coin_address.lower() in _NATIVE_ETH_ADDRESSES:
            return _NATIVE_ETH_DECIMALS
        raw = self._lp_reader.read_uint256_call(chain, coin_address, _DECIMALS_SELECTOR)
        if raw is None or raw < 0 or raw > 36:
            return None
        return raw

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
