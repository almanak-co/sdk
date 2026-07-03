"""Gateway-backed dynamic Curve pool resolution (VIB-5628).

Resolve an **arbitrary (uncurated) Curve pool's metadata from just its address**
so the compiler / valuer / receipt-parser work for pools that are NOT in the
hand-curated static ``CURVE_POOLS`` registry. The static registry becomes a
cache / fast-path; this module is the fallback that reads pool shape live from
Curve's on-chain MetaRegistry.

**No new gateway proto / no perimeter change.** Every read rides the existing
gateway-first ``eth_call`` seam (``_strategy_base.rpc.eth_call``,
gateway-first + ``# vib-2986-exempt`` rpc_url fallback) — the same seam the
Curve adapter's ``_refresh_pool_info_from_chain`` (VIB-5423) already uses.

## MetaRegistry mechanics (verified — see the design doc)

- ``AddressProvider.get_address(7)`` → the chain's MetaRegistry. Resolved live
  (never hardcoded per chain); a ``0x0`` result fails closed. The AddressProvider
  itself is the same deployment on every EVM chain.
- ``get_coins(address)(address[8])`` / ``get_decimals(address)(uint256[8])`` return
  FIXED [8] arrays with trailing zeros — sliced by ``get_n_coins(address)`` (never
  by counting non-zero slots: a native-ETH placeholder / a legitimate 0-decimals
  coin would break counting).
- On a NON-pool address every MetaRegistry read REVERTS ("no registry"), NOT
  all-zeros — so any revert ⇒ this resolver returns ``None`` (fail closed).

## pool_type discriminator (SAFETY-CRITICAL)

Picking the wrong add/remove ABI mis-marks ~10^10. ``get_pool_asset_type`` is
UNSAFE (stETH stableswap → 1; tricryptoUSDC crypto → 3; neither bound holds) and
``price_oracle()`` does not separate families (StableSwap-NG exposes it too). The
reliable rule is to probe ``gamma()(uint256)`` — a Cryptoswap-only invariant — on
the POOL:

- ``gamma()`` succeeds → crypto family (``n_coins == 3 → tricrypto`` else
  ``cryptoswap``).
- ``gamma()`` reverts → stableswap (metapool-ness is the separate ``is_meta``).

``gamma()`` is probed **only after** the MetaRegistry reads have already
succeeded; a ``gamma()`` failure is confirmed against a pool-independent
transport-health probe (``_transport_healthy``) before it is read as a genuine
contract revert (⇒ stableswap). If transport health can't be confirmed the
failure is ambiguous → ``_TransientTransport`` (do not classify, do not cache).

## Transient vs definitive (the root-cause invariant, VIB-5628)

The ``eth_call`` seam cannot distinguish a genuine contract revert from a
transport error (both raise / return ``None``; see ``adapter.py`` "Transport
error and contract revert are indistinguishable at this seam"). So EVERY
safety-critical negative this resolver infers from a failed read
(gamma-``None`` → stableswap; underlying-``None`` → aave gate; required-read
failure → not-a-pool) is CONFIRMED against ``_transport_healthy`` — a
pool-independent read (``AddressProvider.get_address(0)``) that must succeed on
any healthy transport:

- transport healthy ⇒ the failure is a genuine revert ⇒ DEFINITIVE negative.
- transport unhealthy ⇒ ambiguous blip ⇒ ``_TransientTransport``.

``resolve_pool_metadata`` caches ONLY definitive outcomes (a resolved shape or a
transport-confirmed not-a-pool ``None``). A transient blip is NEVER cached, so
the pool self-heals on the next call rather than being poisoned by one timeout.

## Fail-closed contract

``resolve_pool_metadata`` returns ``None`` (never raises to the caller, never
fabricates zeros / partial shapes) on: no read transport; ``get_address(7) ==
0x0``; any MetaRegistry read revert / failure; implausible coin decimals; an
aave-type / wrapped-lending pool — INCLUDING a non-meta pool whose underlying
read can't be confirmed (out of scope — MetaRegistry can't cleanly signal the
wrapped-aToken ABI, and a mis-resolve there is a money-path hazard). Cases where
transport health can't be confirmed return ``None`` too, but are not cached.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from eth_utils import function_signature_to_4byte_selector

from almanak.connectors._strategy_base.pool_validation_base import ZERO_ADDRESS, decode_address
from almanak.connectors._strategy_base.rpc import eth_call

if TYPE_CHECKING:
    # Type-only: the leaf resolver must not import GatewayClient at runtime
    # (keeps this module free of a framework-package dependency). The
    # ``eth_call`` seam expects exactly this type for its ``gateway_client``.
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

__all__ = ["CurvePoolMetadata", "resolve_pool_metadata"]

# Universal Curve AddressProvider — the SAME deployment on every EVM chain
# (documented in ``CURVE_ADDRESSES[*]["address_provider"]``; kept here as a
# constant so this leaf module has no import cycle with the adapter). The chain's
# MetaRegistry is resolved from it live via ``get_address(7)`` and fails closed on
# ``0x0`` — the id/address is never hardcoded per chain.
_ADDRESS_PROVIDER = "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98"
_META_REGISTRY_ADDRESS_ID = 7
# AddressProvider id 0 -> the StableSwap registry: a fixed, always-present,
# pool-independent read used purely as a transport-health probe (see
# ``_transport_healthy``). A healthy transport MUST answer it; a failure means
# the transport is degraded, not that a target-pool read is a genuine revert.
_STABLESWAP_REGISTRY_ADDRESS_ID = 0

_MAX_COINS = 8  # MetaRegistry fixed-array width
# Plausibility bound on MetaRegistry-reported coin decimals. Real ERC-20s never
# exceed this; a value outside it means a malformed / non-pool read — fail closed
# (defense-in-depth on the valuer path, which consumes these decimals directly).
_MAX_COIN_DECIMALS = 36


def _selector(signature: str) -> str:
    """0x-prefixed 4-byte selector derived from a function signature.

    Derived (not hand-typed hex) so calldata can never drift from the ABI the
    contract actually exposes.
    """
    return "0x" + function_signature_to_4byte_selector(signature).hex()


_GET_ADDRESS_SEL = _selector("get_address(uint256)")
_GET_N_COINS_SEL = _selector("get_n_coins(address)")
_GET_COINS_SEL = _selector("get_coins(address)")
_GET_DECIMALS_SEL = _selector("get_decimals(address)")
_GET_LP_TOKEN_SEL = _selector("get_lp_token(address)")
_IS_META_SEL = _selector("is_meta(address)")
_GET_BASE_POOL_SEL = _selector("get_base_pool(address)")
_GET_UNDERLYING_COINS_SEL = _selector("get_underlying_coins(address)")
_GAMMA_SEL = _selector("gamma()")


@dataclass(frozen=True)
class CurvePoolMetadata:
    """Live-resolved shape of an uncurated Curve pool (VIB-5628).

    Every field is either fully MetaRegistry-sourced (+ gamma-discriminated for
    ``pool_type``) or the whole object is ``None`` — there is no partial /
    fabricated shape. ``coin_addresses`` / ``coin_decimals`` / ``coin_symbols``
    are positionally aligned and sliced to ``n_coins``.
    """

    address: str
    lp_token: str
    coin_addresses: list[str]
    coin_decimals: list[int]
    coin_symbols: list[str]
    n_coins: int
    pool_type: str  # "stableswap" | "cryptoswap" | "tricrypto"
    is_metapool: bool
    base_pool: str | None
    base_pool_coin_addresses: list[str] | None
    base_pool_coins: list[str] | None  # base-pool coin SYMBOLS (metapool only)


# Per-process memo: (chain, addr_lc) -> metadata | None. Only DEFINITIVE outcomes
# are stored — a resolved shape, or a transport-confirmed not-a-pool ``None`` (a
# cheap, correct miss). A transient blip is never written (see
# ``resolve_pool_metadata`` / ``_TransientTransport``), so a single timeout can't
# poison the memo. Mirrors the adapter's ``_pool_refresh_cache`` immutability
# rationale — a Curve pool's shape is fixed after deployment.
_METADATA_CACHE: dict[tuple[str, str], CurvePoolMetadata | None] = {}


def _clear_cache() -> None:
    """Test hook — drop the per-process resolution memo."""
    _METADATA_CACHE.clear()


def _pad_address(addr: str) -> str:
    """Left-pad a 20-byte address to a 32-byte ABI word (hex, no 0x)."""
    return addr.lower().removeprefix("0x").zfill(64)


def _pad_uint256(value: int) -> str:
    """Left-pad a uint256 to a 32-byte ABI word (hex, no 0x)."""
    return hex(value)[2:].zfill(64)


def _decode_address_at(data: bytes, index: int) -> str:
    """Decode the address in ABI word ``index`` of ``data`` (rightmost 20 bytes)."""
    start = index * 32
    word = data[start : start + 32]
    if len(word) < 32:
        return ZERO_ADDRESS
    return "0x" + word[12:32].hex()


def _decode_uint_at(data: bytes, index: int) -> int:
    """Decode the uint256 in ABI word ``index`` of ``data``."""
    start = index * 32
    word = data[start : start + 32]
    if len(word) < 32:
        return 0
    return int.from_bytes(word, "big")


def _has_transport(gateway_client: GatewayClient | None, rpc_url: str | None) -> bool:
    if gateway_client is not None and getattr(gateway_client, "is_connected", False):
        return True
    return bool(rpc_url)


class _ReadFailed(Exception):
    """Internal sentinel — a required MetaRegistry read reverted or errored."""


class _TransientTransport(Exception):
    """Internal sentinel — a read failed but transport health could NOT be confirmed.

    The ``eth_call`` seam cannot distinguish a genuine contract revert from a
    transport error (both raise / return ``None``; see ``adapter.py`` "Transport
    error and contract revert are indistinguishable at this seam"). When a
    safety-critical negative is being inferred from a failed read, we confirm the
    transport with a pool-independent probe first: if that probe ALSO fails the
    failure is ambiguous, so we raise this sentinel instead of classifying — the
    caller returns ``None`` WITHOUT caching, so the pool self-heals on the next
    call rather than being poisoned by a single blip.
    """


def _transport_healthy(
    *,
    chain: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> bool:
    """Whether the read transport itself is healthy, independent of the target pool.

    Reads ``AddressProvider.get_address(0)`` — the StableSwap registry, a fixed,
    always-present read on any healthy transport. Returns ``True`` iff it reads
    back a non-empty result (we care only that the transport ANSWERED, not what
    address it returned). Any exception / empty return ⇒ ``False`` (transport not
    confirmed healthy). This is the seam that turns an ambiguous read failure into
    either a definitive revert (transport healthy ⇒ genuine "no registry") or a
    transient blip (transport unhealthy ⇒ do not classify / do not cache).
    """
    try:
        raw = eth_call(
            chain=chain,
            to=_ADDRESS_PROVIDER,
            data=_GET_ADDRESS_SEL + _pad_uint256(_STABLESWAP_REGISTRY_ADDRESS_ID),
            rpc_url=rpc_url,
            gateway_client=gateway_client,
            timeout=timeout,
        )
        return raw is not None
    except Exception:  # noqa: BLE001 — any failure ⇒ transport not confirmed healthy
        return False


def _read(
    *,
    chain: str,
    to: str,
    data: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> bytes:
    """One eth_call that MUST succeed with data, else ``_ReadFailed``.

    A revert ("no registry" on a non-pool address) surfaces through the seam as
    an exception; an empty ``0x`` return surfaces as ``None``. Both mean the read
    did not produce a usable value → fail closed.
    """
    try:
        raw = eth_call(
            chain=chain,
            to=to,
            data=data,
            rpc_url=rpc_url,
            gateway_client=gateway_client,
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001 — revert / transport error → fail closed
        raise _ReadFailed(str(exc)) from exc
    if raw is None:
        raise _ReadFailed("empty result")
    return raw


def _try_read(
    *,
    chain: str,
    to: str,
    data: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> bytes | None:
    """A best-effort read that returns ``None`` on any revert / empty (never raises).

    Used for the ``gamma()`` discriminator and the ``get_underlying_coins`` gate.
    A ``None`` here is AMBIGUOUS (genuine revert vs transport blip); the caller
    disambiguates with ``_transport_healthy`` before inferring any safety-critical
    negative from it.
    """
    try:
        return eth_call(
            chain=chain,
            to=to,
            data=data,
            rpc_url=rpc_url,
            gateway_client=gateway_client,
            timeout=timeout,
        )
    except Exception:  # noqa: BLE001 — genuine revert (transport already confirmed healthy)
        return None


def _resolve_symbol(address: str, chain: str) -> str:
    """Address → token symbol via the shared TokenResolver, truncated-address fallback.

    Reuses the framework TokenResolver (the same path the adapter's
    ``_get_token_symbol`` uses) with ``skip_gateway=True`` to avoid a 30s gateway
    timeout for LP tokens that are valid ERC-20s but not in the static registry.
    Symbols are display / coin-index metadata only, never transaction-critical
    math, so a truncated-address fallback is safe.
    """
    if not address.startswith("0x"):
        return address
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolved = get_token_resolver().resolve(address, chain, skip_gateway=True, log_errors=False)
        return resolved.symbol
    except Exception:  # noqa: BLE001 — unknown token → truncated address (display only)
        return f"{address[:10]}..."


def resolve_pool_metadata(
    chain: str,
    pool_address: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> CurvePoolMetadata | None:
    """Resolve an uncurated Curve pool's metadata from the on-chain MetaRegistry.

    Returns ``None`` (fail closed, never raises) when the pool cannot be safely
    and fully resolved — see the module docstring's fail-closed contract.
    """
    cache_key = (chain, pool_address.lower())
    if cache_key in _METADATA_CACHE:
        return _METADATA_CACHE[cache_key]

    try:
        result = _resolve_uncached(
            chain=chain,
            pool_address=pool_address,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )
    except _TransientTransport as exc:
        # A read failed AND transport health could not be confirmed — the failure
        # is ambiguous (blip vs genuine revert). Return None WITHOUT caching so the
        # pool re-resolves on the next call (self-heals). Caching this would poison
        # the per-process memo permanently on a single timeout (blocker #2) and, on
        # the teardown lane, strand an uncurated LP whose ``LP_CLOSE`` must
        # re-resolve after a blip.
        logger.debug(
            "Curve MetaRegistry resolve transient-failed for %s on %s (%s); not cached",
            pool_address,
            chain,
            exc,
        )
        return None

    # Cache only DEFINITIVE outcomes — a fully-resolved shape, or a
    # transport-confirmed "not a Curve pool" ``None`` (a cheap, correct miss).
    _METADATA_CACHE[cache_key] = result
    return result


def _resolve_uncached(
    *,
    chain: str,
    pool_address: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> CurvePoolMetadata | None:
    if not _has_transport(gateway_client, rpc_url):
        # No transport at all is the ultimate transient state: we cannot even
        # confirm health, so the answer is unknown, not "not a pool". Raise so the
        # caller returns None WITHOUT caching (a later call with a live transport
        # must be able to resolve).
        logger.debug("Curve MetaRegistry resolve skipped for %s on %s: no read transport", pool_address, chain)
        raise _TransientTransport("no read transport")

    def read(to: str, selector: str, arg_hex: str) -> bytes:
        return _read(
            chain=chain,
            to=to,
            data=selector + arg_hex,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    try:
        # 1) AddressProvider.get_address(7) -> MetaRegistry (live, fail-closed on 0x0).
        mr_raw = read(_ADDRESS_PROVIDER, _GET_ADDRESS_SEL, _pad_uint256(_META_REGISTRY_ADDRESS_ID))
        meta_registry = decode_address(mr_raw)
        if meta_registry == ZERO_ADDRESS:
            logger.debug("Curve MetaRegistry unresolved on %s (get_address(7)=0x0)", chain)
            return None

        pool_arg = _pad_address(pool_address)

        # 2) n_coins — the authoritative slice width for the fixed [8] arrays.
        n_coins = _decode_uint_at(read(meta_registry, _GET_N_COINS_SEL, pool_arg), 0)
        if n_coins <= 0 or n_coins > _MAX_COINS:
            logger.debug("Curve pool %s on %s: implausible n_coins=%s; fail closed", pool_address, chain, n_coins)
            return None

        # 3) coins[:n] / 4) decimals[:n]
        coins_raw = read(meta_registry, _GET_COINS_SEL, pool_arg)
        decimals_raw = read(meta_registry, _GET_DECIMALS_SEL, pool_arg)
        coin_addresses = [_decode_address_at(coins_raw, i) for i in range(n_coins)]
        coin_decimals = [_decode_uint_at(decimals_raw, i) for i in range(n_coins)]
        if any(a == ZERO_ADDRESS for a in coin_addresses):
            logger.debug("Curve pool %s on %s: zero coin in first %d slots; fail closed", pool_address, chain, n_coins)
            return None
        # #5 decimals plausibility bound (defense-in-depth): a real coin's decimals
        # are always 0..36. An out-of-range value means a malformed / non-pool read
        # — fail closed rather than feed a bogus decimal into the valuer's
        # MetaRegistry-decimals path.
        if any(not (0 <= d <= _MAX_COIN_DECIMALS) for d in coin_decimals):
            logger.debug(
                "Curve pool %s on %s: implausible coin decimals %s; fail closed",
                pool_address,
                chain,
                coin_decimals,
            )
            return None

        # 5) lp_token (essential — an unresolved LP token can't be valued/closed).
        lp_token = decode_address(read(meta_registry, _GET_LP_TOKEN_SEL, pool_arg))
        if lp_token == ZERO_ADDRESS:
            logger.debug("Curve pool %s on %s: zero lp_token; fail closed", pool_address, chain)
            return None

        # 6) is_meta
        is_metapool = _decode_uint_at(read(meta_registry, _IS_META_SEL, pool_arg), 0) != 0

        # 7) underlying coins — base-pool coins (meta) + the aave/wrapped gate (non-meta).
        underlying = _resolve_underlying(
            chain=chain,
            pool_address=pool_address,
            meta_registry=meta_registry,
            pool_arg=pool_arg,
            read=read,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
            n_coins=n_coins,
            coin_addresses=coin_addresses,
            is_metapool=is_metapool,
        )
        if underlying is _AAVE_SENTINEL:
            return None  # aave / wrapped-lending pool — out of scope, fail closed
        base_pool, base_pool_coin_addresses = underlying

        # 8) pool_type discriminator — gamma() probe (see ``_discriminate_pool_type``).
        pool_type = _discriminate_pool_type(
            chain=chain,
            pool_address=pool_address,
            n_coins=n_coins,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    except _TransientTransport:
        raise  # ambiguous read failure — propagate so the caller returns None uncached
    except _ReadFailed as exc:
        # A required MetaRegistry read failed. Distinguish a genuine "no registry"
        # revert (definitive None, cacheable) from a transport blip (transient,
        # uncached) by confirming the transport with a pool-independent probe.
        if _transport_healthy(chain=chain, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout):
            logger.debug(
                "Curve MetaRegistry resolve failed for %s on %s: %s; transport healthy -> definitive None",
                pool_address,
                chain,
                exc,
            )
            return None
        logger.debug(
            "Curve MetaRegistry read failed for %s on %s: %s; transport unhealthy -> transient",
            pool_address,
            chain,
            exc,
        )
        raise _TransientTransport(str(exc)) from exc
    except Exception:  # noqa: BLE001 — never raise to the caller
        logger.debug("Curve MetaRegistry resolve raised unexpectedly for %s on %s", pool_address, chain, exc_info=True)
        return None

    coin_symbols = [_resolve_symbol(addr, chain) for addr in coin_addresses]
    # Base-pool coin SYMBOLS (metapool only) — the valuer's ``_classify_family`` /
    # ``_build_metapool_position`` key the USD-metapool classification on these
    # symbols, so a dynamic metapool that only carried addresses would fail the
    # ``metapool_usd`` classification and fall through. Resolved from the same
    # symbol path as the top-level coins.
    base_pool_coins = (
        [_resolve_symbol(addr, chain) for addr in base_pool_coin_addresses] if base_pool_coin_addresses else None
    )

    return CurvePoolMetadata(
        address=pool_address,
        lp_token=lp_token,
        coin_addresses=coin_addresses,
        coin_decimals=coin_decimals,
        coin_symbols=coin_symbols,
        n_coins=n_coins,
        pool_type=pool_type,
        is_metapool=is_metapool,
        base_pool=base_pool,
        base_pool_coin_addresses=base_pool_coin_addresses,
        base_pool_coins=base_pool_coins,
    )


def _discriminate_pool_type(
    *,
    chain: str,
    pool_address: str,
    n_coins: int,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> str:
    """Resolve ``pool_type`` by probing the pool's ``gamma()`` — the SAFETY-CRITICAL
    discriminator that picks the add/remove ABI and valuation family (a wrong call
    is a ~10^10 mis-mark). ``gamma()`` is a Cryptoswap-only invariant.

    A returned value ⇒ crypto family (``n_coins == 3 → tricrypto`` else
    ``cryptoswap``). A ``None`` is AMBIGUOUS — a genuine revert (⇒ stableswap) or a
    transport blip — so before inferring "stableswap" we both (a) confirm the
    transport with a pool-independent probe AND (b) RE-READ gamma itself: a genuine
    revert is DETERMINISTIC (the re-read also reverts), an isolated blip is not (the
    re-read recovers ⇒ crypto). A crypto→stableswap mis-mark therefore requires TWO
    consecutive gamma blips bracketed by healthy transport confirms, not one dropped
    packet. Raises ``_TransientTransport`` when the transport can't be confirmed.
    """

    def _probe_gamma() -> bytes | None:
        return _try_read(
            chain=chain,
            to=pool_address,
            data=_GAMMA_SEL,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    def _transport_ok() -> bool:
        return _transport_healthy(chain=chain, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)

    gamma = _probe_gamma()
    if gamma is None:
        if not _transport_ok():
            raise _TransientTransport("gamma() probe inconclusive; transport unhealthy")
        gamma = _probe_gamma()  # deterministic genuine revert re-reverts; a blip recovers
        if gamma is None and not _transport_ok():
            raise _TransientTransport("gamma() re-probe inconclusive; transport unhealthy")
    if gamma is not None:
        return "tricrypto" if n_coins == 3 else "cryptoswap"
    return "stableswap"


# Sentinel returned by ``_resolve_underlying`` when the pool is an aave/wrapped
# lending pool (out of scope) — distinct from the ``(base_pool, base_coins)``
# tuple so a genuine "no base pool" doesn't read as an aave fail.
_AAVE_SENTINEL = object()


def _resolve_underlying(
    *,
    chain: str,
    pool_address: str,
    meta_registry: str,
    pool_arg: str,
    read,  # noqa: ANN001 — the _resolve_uncached ``read`` closure (raises _ReadFailed)
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
    n_coins: int,
    coin_addresses: list[str],
    is_metapool: bool,
):
    """Resolve ``(base_pool, base_pool_coin_addresses)`` for a metapool, or run the
    aave/wrapped gate for a non-meta pool.

    Returns ``_AAVE_SENTINEL`` when a NON-meta pool's MetaRegistry underlying
    coins differ from its ``coins`` — a wrapped-aToken pool whose coins are
    aTokens (out of scope; a mis-resolve there is a money-path hazard, so the
    caller fails closed). Otherwise returns a ``(base_pool, base_coins)`` tuple
    (both ``None`` for a plain pool).
    """
    underlying_raw = _try_read(
        chain=chain,
        to=meta_registry,
        data=_GET_UNDERLYING_COINS_SEL + pool_arg,
        gateway_client=gateway_client,
        rpc_url=rpc_url,
        timeout=timeout,
    )

    if is_metapool:
        base_pool: str | None = decode_address(read(meta_registry, _GET_BASE_POOL_SEL, pool_arg))
        if base_pool == ZERO_ADDRESS:
            base_pool = None
        base_pool_coin_addresses: list[str] | None = None
        if underlying_raw is not None:
            # Combined underlying = [meta coin, base_coin_1..N]. USD metapools carry
            # no native-ETH placeholder, so counting non-zero leading slots is safe;
            # base coins are index 1..end.
            combined = _leading_nonzero_addresses(underlying_raw)
            if len(combined) > 1:
                base_pool_coin_addresses = combined[1:]
        return base_pool, base_pool_coin_addresses

    if underlying_raw is not None:
        underlying = [_decode_address_at(underlying_raw, i) for i in range(n_coins)]
        differs = [u.lower() for u in underlying] != [a.lower() for a in coin_addresses]
        if any(u != ZERO_ADDRESS for u in underlying) and differs:
            logger.debug(
                "Curve pool %s on %s looks aave/wrapped (underlying != coins); out of scope, fail closed",
                pool_address,
                chain,
            )
            return _AAVE_SENTINEL
        return None, None

    # underlying_raw is None — the best-effort read couldn't confirm the underlying
    # set. This is the aave/wrapped gate on a NON-meta pool, so a ``None`` here must
    # NOT silently pass as a plain pool (a fail-OPEN hole): a wrapped-aToken pool
    # whose underlying we can't read would then be mis-executed with the plain ABI.
    if not _transport_healthy(chain=chain, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout):
        # Transport can't be confirmed → the failed underlying read is ambiguous.
        raise _TransientTransport("underlying-coins read inconclusive; transport unhealthy")
    # Transport healthy but the underlying read still returned nothing → we cannot
    # confirm the pool is a plain (non-wrapped) pool → fail CLOSED, never plain.
    logger.debug(
        "Curve pool %s on %s: non-meta underlying unreadable on healthy transport; cannot confirm safe, fail closed",
        pool_address,
        chain,
    )
    return _AAVE_SENTINEL


def _leading_nonzero_addresses(data: bytes) -> list[str]:
    """Leading non-zero addresses of a fixed ``address[8]`` word array.

    Stops at the first zero slot. Safe for USD metapool underlying sets (no
    native-ETH placeholder among stablecoins); NOT used for the primary
    ``coins`` slice, which is sliced by the authoritative ``get_n_coins``.

    #6 (P1-4 out of scope): a non-USD / native-ETH metapool underlying set could
    carry a native-ETH ``0x0`` placeholder that would truncate this early. That
    only degrades metapool *underlying* routing (base_pool_coin_addresses), which
    is already fail-closed (a shorter/None list is safe, never a mis-mark); it does
    not affect the primary coin slice or pool_type. Native-ETH metapool underlying
    is deferred to the P1-4 asset-set resolver.
    """
    out: list[str] = []
    for i in range(_MAX_COINS):
        addr = _decode_address_at(data, i)
        if addr == ZERO_ADDRESS:
            break
        out.append(addr)
    return out
