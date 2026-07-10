"""Curve pair→pool dynamic resolution via the MetaRegistry (VIB-5716).

Resolve a **token pair** (two coin addresses) to a deployable Curve pool when
the static ``CURVE_POOLS`` curation and the address-based dynamic resolver
(VIB-5628, ``pool_resolver.py``) both miss. This is the "P1-4" follow-on that
VIB-5628 deferred: MetaRegistry ``find_pool_for_coins(coin_a, coin_b, i)`` IS
Curve's pairwise ``factory.getPool``-style resolver — enumerated until it
returns the zero address, then each candidate is resolved through the existing
``resolve_pool_metadata`` seam. Same transport boundary as VIB-5628: the
gateway-first ``_strategy_base.rpc.eth_call`` seam, no proto change.

## Why naive enumeration is WORSE than failing (ALM-2931)

Verified on-chain for crvUSD/WBTC on Ethereum: ``find_pool_for_coins`` returns
7 pools, none of which a plain Curve LP strategy can use — two Yield Basis
pools whose ``add_liquidity`` is whitelist-gated (reverts ``!wl`` for anyone
but the YB leveraged-token contract; ~$60M TVL, so it would WIN a naive
liquidity ranking), two empty "Testy Test" pools, and three dust tricryptos.
Yield Basis pools are registered in the MetaRegistry and shape-resolve like
normal twocrypto pools, so shape resolution alone cannot reject them. Hence
the three-stage pipeline this module implements:

1. **Liquidity floor** — rank candidates by MetaRegistry ``get_balances``
   priced in USD; reject below a floor (kills dust/test pools with zero
   chain-state subtlety).
2. **Provenance corroboration** — MetaRegistry ``get_pool_name`` REVERTS for
   the Yield Basis pools but succeeds for genuine factory pools (empirical
   tell, verified on-chain). Recorded per candidate as ``provenance_suspect``;
   corroboration, never the sole gate.
3. **Deployability probe** (consumer-side; see
   :func:`classify_add_liquidity_probe`) — static-call the REAL adapter-built
   ``add_liquidity`` calldata from the wallet and classify the revert reason.

## Probe classification (the sound shape, from the VIB-5716 design)

At compile time the wallet holds no approvals (approvals are txs 1-2 of the
same bundle), so a static ``add_liquidity`` from the real wallet reverts for
LEGIT pools too. Classification:

- call succeeds, or reverts with an allowance / balance / funds-shaped reason
  → expected pre-funding state → PASS;
- reverts with any OTHER decodable reason (``!wl``, unknown custom errors)
  → deposit-gated or incompatible → DISQUALIFY (fail closed);
- reverts with NO decodable reason → inconclusive, because legacy tokens
  (e.g. USDT) revert reasonlessly on a missing allowance — indistinguishable
  from a reasonless gate. Disqualifying here would false-reject every USDT
  pool, while passing is fail-SAFE (a wrongly passed gated pool compiles a
  bundle that reverts on-chain with no funds moved — exactly today's
  behaviour). So an inconclusive probe defers to the provenance tell:
  ``provenance_suspect`` disqualifies, clean provenance passes.
- transport outcome → same inconclusive handling (says nothing about the
  pool).

## Fail-closed / transient contract

Mirrors ``pool_resolver``: enumeration that cannot be confirmed against a
healthy transport is INDETERMINATE (``PairCandidateSet.indeterminate`` — the
caller falls back to its legacy miss path and may retry), never a fabricated
"no pools". Nothing in this module is cached: candidate sets, balances and
floors are re-read per resolution so a pool deployed or funded after strategy
boot is picked up, and a probe verdict can never go stale.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.pool_validation_base import ZERO_ADDRESS, decode_address
from almanak.connectors.curve.pool_resolver import (
    _ADDRESS_PROVIDER,
    _GET_ADDRESS_SEL,
    _META_REGISTRY_ADDRESS_ID,
    CurvePoolMetadata,
    _decode_uint_at,
    _has_transport,
    _pad_address,
    _pad_uint256,
    _selector,
    _TransientTransport,
    _transport_healthy,
    _try_read,
    resolution_is_definitive,
    resolve_pool_metadata,
)

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.rpc import StaticCallProbe
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_PAIR_LIQUIDITY_FLOOR_USD",
    "PairCandidate",
    "PairCandidateSet",
    "build_pair_candidates",
    "classify_add_liquidity_probe",
    "format_pair_miss",
    "pool_provenance_suspect",
]

_FIND_POOL_FOR_COINS_SEL = _selector("find_pool_for_coins(address,address,uint256)")
_GET_BALANCES_SEL = _selector("get_balances(address)")
_GET_POOL_NAME_SEL = _selector("get_pool_name(address)")

# Enumeration bound. find_pool_for_coins terminates with the zero address; the
# cap is a runaway guard, far above any real pair's pool count (crvUSD/WBTC,
# the busiest observed, has 7).
_MAX_PAIR_POOLS = 32

# Pools below this USD liquidity are rejected as dust/test pools. The pair
# resolver is CHOOSING a pool for the user; an explicit pool address bypasses
# pair resolution entirely, so the floor never blocks a deliberate choice.
DEFAULT_PAIR_LIQUIDITY_FLOOR_USD = Decimal("10000")

# Chains where the get_pool_name provenance tell has been VERIFIED on-chain
# against genuine factory pools (reads succeed) AND known gated pools (reads
# revert). The tell is chain-empirical — on an unverified chain a MetaRegistry
# whose get_pool_name reverts for ordinary factory pools would false-suspect
# every candidate, and combined with reasonless legacy-token (USDT-style)
# prefund reverts that would false-REJECT legitimate pools (an availability
# regression vs VIB-5628's address path). Off this list the tell is simply
# not consulted: inconclusive probes pass, restoring the pre-VIB-5716
# fail-safe baseline (compile SUCCESS → gated pool reverts on-chain with no
# funds moved). Extend per chain only with fresh on-chain verification.
_PROVENANCE_TELL_VERIFIED_CHAINS = frozenset({"ethereum"})

# Revert reasons that mean "the wallet just isn't funded/approved yet" — the
# EXPECTED state at compile time (approvals are part of the same bundle).
# Substring-matched against the lowercased decoded reason.
_EXPECTED_PREFUND_MARKERS: tuple[str, ...] = (
    "allowance",
    "exceeds balance",
    "insufficient balance",
    "balance too low",
    "insufficient funds",
    "transferfrom failed",
    "transfer_from_failed",
    "safeerc20",
)
# Uniswap TransferHelper's terse markers — matched exactly, not as substrings,
# so they can't fire inside an unrelated word.
_EXPECTED_PREFUND_EXACT: tuple[str, ...] = ("stf", "tf")
# ERC-6093 custom errors (OpenZeppelin v5 tokens) — surfaced by the probe seam
# as "custom error 0x<selector>". Derived, never hand-typed.
_EXPECTED_PREFUND_CUSTOM_ERRORS: tuple[str, ...] = (
    f"custom error {_selector('ERC20InsufficientAllowance(address,uint256,uint256)')}",
    f"custom error {_selector('ERC20InsufficientBalance(address,uint256,uint256)')}",
)


@dataclass(frozen=True)
class PairCandidate:
    """One MetaRegistry match for a pair, with its screening verdicts.

    ``rejection`` is ``None`` for candidates that survived shape resolution and
    the liquidity floor (probe-eligible, carried in ``PairCandidateSet.ranked``)
    and a human-readable reason otherwise. ``provenance_suspect`` is the
    ``get_pool_name``-revert tell — corroboration for an inconclusive probe,
    never a rejection by itself.
    """

    address: str
    metadata: CurvePoolMetadata | None
    liquidity_usd: Decimal | None
    provenance_suspect: bool
    rejection: str | None


@dataclass(frozen=True)
class PairCandidateSet:
    """Screened, liquidity-ranked candidates for one (chain, coin pair).

    ``indeterminate=True`` means enumeration could not be confirmed against a
    healthy transport — the caller must treat the pair as UNRESOLVED (fall back
    to its legacy miss path), never as "no pools exist".
    """

    chain: str
    pair_label: str
    ranked: list[PairCandidate]
    rejected: list[PairCandidate]
    indeterminate: bool = False


def build_pair_candidates(
    chain: str,
    pair_label: str,
    coin_a: str,
    coin_b: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    usd_price: Callable[[str], Decimal | None],
    liquidity_floor_usd: Decimal = DEFAULT_PAIR_LIQUIDITY_FLOOR_USD,
    timeout: float = 10.0,
) -> PairCandidateSet:
    """Enumerate + screen every MetaRegistry pool holding both coins.

    ``usd_price`` maps a coin SYMBOL to its USD price (``None`` = unpriceable);
    the caller supplies it from whatever oracle its lane has (the compiler uses
    ``ctx.services.require_token_price``). Never raises: transport failures
    yield ``indeterminate=True``.
    """
    ranked: list[PairCandidate] = []
    rejected: list[PairCandidate] = []
    try:
        meta_registry, addresses = _enumerate_pair_pools(
            chain=chain,
            coin_a=coin_a,
            coin_b=coin_b,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )
        for address in addresses:
            candidate = _screen_candidate(
                chain=chain,
                meta_registry=meta_registry,
                address=address,
                coin_a=coin_a,
                coin_b=coin_b,
                gateway_client=gateway_client,
                rpc_url=rpc_url,
                usd_price=usd_price,
                liquidity_floor_usd=liquidity_floor_usd,
                timeout=timeout,
            )
            (rejected if candidate.rejection else ranked).append(candidate)
    except _TransientTransport as exc:
        # An unconfirmable read anywhere in the pipeline poisons the whole
        # answer (a partial candidate set would mislead the honest-miss text),
        # so the entire resolution is indeterminate — the caller falls back to
        # its legacy miss path and a later compile retries fresh.
        logger.debug("Curve pair resolution indeterminate for %s on %s (%s)", pair_label, chain, exc)
        return PairCandidateSet(chain=chain, pair_label=pair_label, ranked=[], rejected=[], indeterminate=True)

    ranked.sort(key=lambda c: c.liquidity_usd or Decimal(0), reverse=True)
    return PairCandidateSet(chain=chain, pair_label=pair_label, ranked=ranked, rejected=rejected)


def _confirmed_read(
    *,
    chain: str,
    to: str,
    data: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> bytes | None:
    """A ``_try_read`` whose ``None`` is a CONFIRMED deterministic revert.

    The VIB-5628 discriminator lesson, applied to every read this module
    infers something from: a single failed read is ambiguous (genuine revert
    vs a transport blip — a cold lazy-fork read can time out while the cheap
    transport-health probe still answers). So a failure is confirmed by (a)
    the pool-independent health probe AND (b) re-reading the SAME call — a
    genuine revert is deterministic and re-fails; a blip recovers. Raises
    ``_TransientTransport`` when the failure can't be confirmed.
    """

    def read() -> bytes | None:
        return _try_read(
            chain=chain,
            to=to,
            data=data,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    raw = read()
    if raw is not None:
        return raw
    if not _transport_healthy(chain=chain, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout):
        raise _TransientTransport("read failed and transport health unconfirmed")
    return read()


def _enumerate_pair_pools(
    *,
    chain: str,
    coin_a: str,
    coin_b: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    timeout: float,
) -> tuple[str, list[str]]:
    """MetaRegistry address + every pool holding both coins, in registry order.

    Raises ``_TransientTransport`` when the result cannot be trusted (no
    transport / unconfirmable read failure / persistent read failure).
    ``find_pool_for_coins`` never legitimately reverts — its terminator is the
    ZERO ADDRESS, a successful read — so a read failure, even a confirmed one,
    is NEVER read as "no pools exist": a definitive empty requires the
    registry to actually answer with the zero address at index 0. (Confirmed
    empty results ARE definitive; a chain whose registry cannot answer this
    method stays honestly on the legacy miss path.)
    """
    if not _has_transport(gateway_client, rpc_url):
        raise _TransientTransport("no read transport")

    def confirmed(to: str, data: str) -> bytes | None:
        return _confirmed_read(
            chain=chain,
            to=to,
            data=data,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    mr_raw = confirmed(_ADDRESS_PROVIDER, _GET_ADDRESS_SEL + _pad_uint256(_META_REGISTRY_ADDRESS_ID))
    if mr_raw is None:
        raise _TransientTransport("MetaRegistry address unreadable")
    meta_registry = decode_address(mr_raw)
    if meta_registry == ZERO_ADDRESS:
        # No MetaRegistry on this chain — pair resolution is definitively
        # unavailable, not transient. An empty candidate set says so honestly.
        logger.debug("Curve MetaRegistry unresolved on %s (get_address(7)=0x0)", chain)
        return meta_registry, []

    pair_args = _pad_address(coin_a) + _pad_address(coin_b)
    addresses: list[str] = []
    seen: set[str] = set()
    for i in range(_MAX_PAIR_POOLS):
        raw = confirmed(meta_registry, _FIND_POOL_FOR_COINS_SEL + pair_args + _pad_uint256(i))
        if raw is None:
            # Confirmed persistent failure — but a revert is not part of this
            # method's contract, so it still cannot mean "no more pools".
            raise _TransientTransport(f"find_pool_for_coins({i}) unreadable; refusing a fabricated empty")
        address = decode_address(raw)
        if address == ZERO_ADDRESS:
            return meta_registry, addresses
        if address not in seen:  # a pool may be registered by several handlers
            seen.add(address)
            addresses.append(address)
    # Cap reached WITHOUT the zero-address terminator: the universe is bigger
    # than we enumerated, so the set is incomplete — treating it as complete
    # could silently hide a deployable pool past the cap. Indeterminate.
    raise _TransientTransport(f"find_pool_for_coins enumeration exceeded the {_MAX_PAIR_POOLS}-pool cap")


def _screen_candidate(
    *,
    chain: str,
    meta_registry: str,
    address: str,
    coin_a: str,
    coin_b: str,
    gateway_client: GatewayClient | None,
    rpc_url: str | None,
    usd_price: Callable[[str], Decimal | None],
    liquidity_floor_usd: Decimal,
    timeout: float,
) -> PairCandidate:
    """Shape-resolve one candidate and apply the exact-pair, floor + provenance screens."""
    metadata = resolve_pool_metadata(
        chain,
        address,
        gateway_client=gateway_client,
        rpc_url=rpc_url,
        timeout=timeout,
    )
    if metadata is None:
        # ``resolve_pool_metadata`` returns None for BOTH a confirmed
        # not-a-plain-pool (cached, definitive) and an unconfirmable transient
        # blip (uncached). Only the former is a determinate rejection; a blip
        # must poison the whole resolution as indeterminate, or an
        # all-candidates blip would fabricate a determinate honest-miss
        # (Codex + pr-auditor high-confidence finding).
        if not resolution_is_definitive(chain, address):
            raise _TransientTransport(f"candidate {address} metadata unresolved (transient)")
        return PairCandidate(
            address=address,
            metadata=None,
            liquidity_usd=None,
            provenance_suspect=False,
            rejection="not a plain Curve pool (wrapped/aave-type or not a pool; transport-confirmed)",
        )

    # Exact-pair screen (VIB-3946 discipline): find_pool_for_coins matches any
    # pool CONTAINING both coins — including 3-coin pools (tricryptos). A pair
    # request must never resolve to a superset pool: LP_OPEN amounts map
    # positionally to pool coin indices, so "WETH/WBTC" landing on tricrypto2
    # ([USDT, WBTC, WETH]) would silently deposit amount0 as USDT.
    requested = {coin_a.lower(), coin_b.lower()}
    if metadata.n_coins != 2 or {a.lower() for a in metadata.coin_addresses} != requested:
        return PairCandidate(
            address=address,
            metadata=metadata,
            liquidity_usd=None,
            provenance_suspect=False,
            rejection=(
                f"holds {metadata.coin_symbols} — not exactly the requested pair "
                f"(a pair request never matches a superset pool; target it by address with coin_amounts)"
            ),
        )

    def confirmed(data: str) -> bytes | None:
        return _confirmed_read(
            chain=chain,
            to=meta_registry,
            data=data,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    pool_arg = _pad_address(address)
    liquidity_usd, liquidity_rejection = _liquidity_usd(
        # The reserves read is only needed when the floor screen is active.
        # ``None`` from a confirmed read = deterministic registry revert.
        balances_raw=confirmed(_GET_BALANCES_SEL + pool_arg) if liquidity_floor_usd > 0 else None,
        metadata=metadata,
        usd_price=usd_price,
        liquidity_floor_usd=liquidity_floor_usd,
    )
    # A CONFIRMED get_pool_name revert is the Yield-Basis-style provenance
    # tell (an unconfirmable failure raises instead — the whole resolution
    # goes indeterminate rather than mis-labelling a pool). Suspicion only
    # ever DOWNGRADES an inconclusive probe, never passes anything — and is
    # consulted only on chains where the tell is verified (see
    # ``_PROVENANCE_TELL_VERIFIED_CHAINS``).
    provenance_suspect = chain in _PROVENANCE_TELL_VERIFIED_CHAINS and confirmed(_GET_POOL_NAME_SEL + pool_arg) is None

    return PairCandidate(
        address=address,
        metadata=metadata,
        liquidity_usd=liquidity_usd,
        provenance_suspect=provenance_suspect,
        rejection=liquidity_rejection,
    )


def _liquidity_usd(
    *,
    balances_raw: bytes | None,
    metadata: CurvePoolMetadata,
    usd_price: Callable[[str], Decimal | None],
    liquidity_floor_usd: Decimal,
) -> tuple[Decimal | None, str | None]:
    """Price the pool's reserves in USD and apply the floor.

    Sums only the coins the oracle can price — an under-count, which errs
    toward rejecting (never inflates a dust pool over the floor). A pool where
    NO coin is priceable cannot be floor-checked or ranked → rejected with a
    reason that says so. A non-positive floor DISABLES the screen entirely
    (the close lane selects by wallet holdings, not pool quality — a position
    in a dust pool must still be findable).
    """
    if liquidity_floor_usd <= 0:
        return None, None
    # A truncated payload would decode as zero balances and produce a
    # misleading "~$0 below floor" — reject it as unreadable instead
    # (CodeRabbit #3236).
    if balances_raw is None or len(balances_raw) < 32 * metadata.n_coins:
        return None, "pool reserves unreadable (MetaRegistry get_balances failed or returned truncated data)"

    total = Decimal(0)
    any_priced = False
    for i in range(metadata.n_coins):
        price = usd_price(metadata.coin_symbols[i])
        if price is None or price <= 0:
            continue
        any_priced = True
        raw_balance = _decode_uint_at(balances_raw, i)
        total += Decimal(raw_balance) * price / (Decimal(10) ** metadata.coin_decimals[i])

    if not any_priced:
        return None, f"cannot price pool reserves (no USD price for any of {metadata.coin_symbols})"
    if total < liquidity_floor_usd:
        return total, f"~${total:,.0f} liquidity below the ${liquidity_floor_usd:,.0f} floor"
    return total, None


def classify_add_liquidity_probe(
    probe: StaticCallProbe,
    *,
    provenance_suspect: bool,
) -> tuple[bool, str]:
    """Fuse a static ``add_liquidity`` probe with the provenance tell.

    Returns ``(deployable, detail)``. Decision table (rationale in the module
    docstring): an explicit non-prefund revert reason always disqualifies; an
    explicit prefund-shaped reason always passes; everything inconclusive
    (reasonless revert, transport) is decided by ``provenance_suspect``.
    """
    if probe.outcome == "success":
        return True, "add_liquidity static-call succeeded"

    if probe.outcome == "revert" and probe.revert_reason is not None:
        reason = probe.revert_reason.strip()
        lowered = reason.lower()
        if (
            any(marker in lowered for marker in _EXPECTED_PREFUND_MARKERS)
            or lowered in _EXPECTED_PREFUND_EXACT
            or lowered in _EXPECTED_PREFUND_CUSTOM_ERRORS
        ):
            return True, f"expected pre-funding revert ({reason!r})"
        return False, f"add_liquidity reverted {reason!r} — deposit-gated or incompatible pool"

    if probe.outcome == "revert":
        detail = "add_liquidity reverted without a reason"
    else:
        detail = f"add_liquidity probe got no answer ({probe.error or 'transport'})"
    if provenance_suspect:
        return False, f"{detail}; MetaRegistry get_pool_name also reverts (non-factory provenance) — fail closed"
    return True, f"{detail}; treating as pre-funding revert (factory provenance confirmed)"


def pool_provenance_suspect(
    chain: str,
    pool_address: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> bool:
    """The ``get_pool_name``-revert provenance tell for a SINGLE pool address.

    Used by the uncurated-ADDRESS compile path, which bypasses
    :func:`build_pair_candidates` screening but still runs the deployability
    probe. Best-effort and one-sided: ``True`` only for a CONFIRMED
    deterministic ``get_pool_name`` revert (health-probed + re-read) on a
    chain where the tell is verified (``_PROVENANCE_TELL_VERIFIED_CHAINS``);
    every other state is ``False`` (suspicion only ever downgrades an
    inconclusive probe, so a missed tell is fail-safe — the pool falls back to
    the probe's own verdict).
    """
    if chain not in _PROVENANCE_TELL_VERIFIED_CHAINS:
        return False
    if not _has_transport(gateway_client, rpc_url):
        return False

    def confirmed(to: str, data: str) -> bytes | None:
        return _confirmed_read(
            chain=chain,
            to=to,
            data=data,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            timeout=timeout,
        )

    try:
        mr_raw = confirmed(_ADDRESS_PROVIDER, _GET_ADDRESS_SEL + _pad_uint256(_META_REGISTRY_ADDRESS_ID))
        if mr_raw is None:
            return False
        meta_registry = decode_address(mr_raw)
        if meta_registry == ZERO_ADDRESS:
            return False
        return confirmed(meta_registry, _GET_POOL_NAME_SEL + _pad_address(pool_address)) is None
    except _TransientTransport:
        return False


def _short(address: str) -> str:
    return f"{address[:8]}…{address[-4:]}" if len(address) == 42 else address


def format_pair_miss(
    candidate_set: PairCandidateSet,
    probe_rejections: list[tuple[str, str]] | None = None,
) -> str:
    """Honest-miss error text: WHY no pool was returned, per candidate.

    ``probe_rejections`` are ``(address, detail)`` pairs for ranked candidates
    the consumer's deployability probe disqualified.
    """
    pair = candidate_set.pair_label
    chain = candidate_set.chain
    probe_rejections = probe_rejections or []
    total = len(candidate_set.ranked) + len(candidate_set.rejected)
    if total == 0:
        return (
            f"No Curve pool holds both sides of {pair!r} on {chain} (MetaRegistry find_pool_for_coins returned none)."
        )

    reasons = [f"{_short(address)} — {detail}" for address, detail in probe_rejections]
    reasons += [f"{_short(c.address)} — {c.rejection}" for c in candidate_set.rejected]
    return (
        f"No deployable Curve pool for {pair!r} on {chain}: MetaRegistry matched "
        f"{total} pool(s), none passed screening: " + "; ".join(reasons) + ". "
        "Pass an explicit pool address as intent.pool to target a specific pool "
        "(deposit-gated pools remain non-deployable)."
    )
