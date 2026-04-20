"""DexScreener symbol-to-address lookup with scam-resistant gating.

Second-tier dynamic fallback for EVM symbol resolution. Called from
``TokenServiceServicer._try_evm_symbol_lookup`` after CoinGecko misses,
or as the only dynamic source on chains CoinGecko does not list
(Blast, Linea, Plasma, ...).

Why not use DexScreener naively
-------------------------------
A bare ``/latest/dex/search?q=LUME`` returns whichever scam token was indexed
first for a colliding symbol. The motivating smoke test (2026-04-17, VIB-2983)
showed three different Base contracts claiming "LUME" with $25k / $11k / $10k
liquidity and essentially zero volume — picking one by liquidity would silently
resolve to a dead pool.

The 4-gate policy
-----------------
A candidate pair on the requested chain is accepted only if ALL pass:

1. Liquidity floor -- ``pair.liquidity.usd >= MIN_LIQUIDITY_USD`` (default $10k).
   Below this is measurement noise or deliberate dust.
2. Volume floor -- ``pair.volume.h24 >= MIN_VOLUME_USD`` (default $1k).
   Proves real trading, not a seeded-then-abandoned pool.
3. Turnover ratio -- ``volume.h24 / liquidity.usd >= MIN_TURNOVER_RATIO``
   (default 0.05 = 5% daily). Catches honeypots where the creator added
   liquidity but nobody can sell.
4. Dominance -- if multiple candidates pass 1-3 on the same chain, the top
   pair must have at least ``DOMINANCE_MULTIPLE`` (default 3.0) times the
   runner-up's liquidity. Otherwise the symbol is ambiguous and the caller
   is forced to disambiguate with an explicit address.

Thresholds are tunable via environment variables — see module constants.

The defaults are calibrated for the primary use case (new DEX launches on
chains CoinGecko hasn't indexed), not for established tokens (those are
already in the static JSON registry shipped in VIB-2950).
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

from almanak.framework.data.tokens.exceptions import AmbiguousTokenError

logger = logging.getLogger(__name__)

# =============================================================================
# Endpoint
# =============================================================================

DEXSCREENER_BASE_URL = "https://api.dexscreener.com"
DEXSCREENER_SEARCH_PATH = "/latest/dex/search"

# HTTP timeout for a single DexScreener request, in seconds.
# DexScreener is a search endpoint that returns quickly on cache hits (<300ms
# observed) but can slow down under load. 8s leaves headroom for the caller's
# ~10s gateway on-chain-confirm step without pushing total latency past 30s.
DEFAULT_HTTP_TIMEOUT_S = 8.0

# Public rate limit is 300 req/min. One-level exponential backoff handles the
# rare 429; beyond that we fail through to TokenNotFoundError.
BACKOFF_INITIAL_S = 0.5
BACKOFF_MAX_RETRIES = 1  # total tries = 1 initial + 1 retry

# =============================================================================
# Gating thresholds (env-configurable)
# =============================================================================


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float in env %s=%r; using default %s", name, raw, default)
        return default


MIN_LIQUIDITY_USD = _float_env("ALMANAK_DEXSCREENER_MIN_LIQUIDITY_USD", 10_000.0)
MIN_VOLUME_USD = _float_env("ALMANAK_DEXSCREENER_MIN_VOLUME_USD", 1_000.0)
MIN_TURNOVER_RATIO = _float_env("ALMANAK_DEXSCREENER_MIN_TURNOVER_RATIO", 0.05)
DOMINANCE_MULTIPLE = _float_env("ALMANAK_DEXSCREENER_DOMINANCE_MULTIPLE", 3.0)

# =============================================================================
# Chain-slug mapping
# =============================================================================

# SDK chain name -> DexScreener chainId. Keys are lowercase SDK-canonical names.
# DexScreener's slugs mostly match ours but a few need translation.
# Verify new entries with: curl "https://api.dexscreener.com/latest/dex/search?q=USDC" | jq '.pairs[].chainId' | sort -u
#
# IMPORTANT: keep this map as a subset of ``almanak.gateway.validation.ALLOWED_CHAINS``
# (plus ``"solana"``, which routes through Jupiter instead). Listing a chain here
# that the gateway rejects upstream is dead code and misleading.
CHAIN_SLUG_MAP: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "polygon": "polygon",
    "avalanche": "avalanche",
    "bsc": "bsc",
    "sonic": "sonic",
    "mantle": "mantle",
    "berachain": "berachain",
    "monad": "monad",
    "xlayer": "xlayer",
    "zerog": "zerog",
    "blast": "blast",
    "linea": "linea",
    "plasma": "plasma",
    "solana": "solana",  # Informational; Solana path goes through Jupiter.
}


# =============================================================================
# Exceptions
# =============================================================================


class DexScreenerError(RuntimeError):
    """DexScreener API returned an unexpected status or malformed payload.

    Distinct from ``LookupError`` (no match found) so callers can log
    API-level failures separately and treat them as transient.
    """


# =============================================================================
# Public data type
# =============================================================================


@dataclass(frozen=True)
class DexScreenerResult:
    """A single DexScreener match that passed all four gates.

    Attributes:
        address: The token contract address (checksum-preserving case).
        chain: The SDK chain name (lowercased) the address lives on.
        symbol: The symbol as reported by DexScreener (case-preserved).
        liquidity_usd: Top pair's liquidity in USD.
        volume_24h_usd: Top pair's 24h volume in USD.
        pair_url: DexScreener URL for the winning pair (for logging/ops).
    """

    address: str
    chain: str
    symbol: str
    liquidity_usd: float
    volume_24h_usd: float
    pair_url: str | None = None


@dataclass(frozen=True)
class _Candidate:
    """Intermediate representation of a pair + extracted candidate token."""

    address: str
    symbol: str
    chain_slug: str
    liquidity_usd: float
    volume_24h_usd: float
    pair_url: str | None


# =============================================================================
# Core lookup
# =============================================================================


def chain_slug_for(chain: str) -> str | None:
    """Translate an SDK chain name to a DexScreener chainId.

    Returns None if DexScreener does not index the chain.
    """
    return CHAIN_SLUG_MAP.get(chain.lower())


async def find_token_address(
    symbol: str,
    chain: str,
    *,
    session: Any | None = None,
    http_timeout_s: float = DEFAULT_HTTP_TIMEOUT_S,
) -> DexScreenerResult | None:
    """Resolve a symbol to a token address on ``chain`` via DexScreener.

    Args:
        symbol: Token symbol (case-insensitive).
        chain: SDK chain name (e.g. "arbitrum", "base", "linea").
        session: Optional ``aiohttp.ClientSession`` for connection reuse.
            If None, a temporary session is created per call.
        http_timeout_s: Per-request timeout.

    Returns:
        DexScreenerResult if exactly one candidate passes all four gates
        (dominantly, if there are multiple gate-passing candidates on-chain).
        None if no candidates exist, all candidates fail the gates, or the
        chain is not indexed by DexScreener.

    Raises:
        AmbiguousTokenError: Multiple candidates pass the first three gates
            but no candidate is dominant (runner-up within DOMINANCE_MULTIPLE
            of the leader). The error includes the full candidate list.
        DexScreenerError: API returned HTTP != 200 after retries, or the
            response payload is malformed.
    """
    chain_slug = chain_slug_for(chain)
    if chain_slug is None:
        logger.debug("dexscreener_chain_not_indexed chain=%s symbol=%s", chain, symbol)
        return None

    _record_metric("requests_total")
    pairs = await _fetch_pairs(symbol, session=session, http_timeout_s=http_timeout_s)
    _record_metric("pairs_returned_total", len(pairs))
    if not pairs:
        logger.debug("dexscreener_no_pairs chain=%s symbol=%s", chain, symbol)
        return None

    candidates = _extract_candidates_on_chain(pairs, symbol=symbol, chain_slug=chain_slug)
    if not candidates:
        logger.debug(
            "dexscreener_no_matches_on_chain chain=%s symbol=%s total_pairs=%d",
            chain,
            symbol,
            len(pairs),
        )
        return None

    result = _apply_gates(candidates, symbol=symbol, chain=chain)
    if result is not None:
        _record_metric("resolved_total")
    return result


# =============================================================================
# HTTP fetch
# =============================================================================


async def _fetch_pairs(
    symbol: str,
    *,
    session: Any | None,
    http_timeout_s: float,
) -> list[dict[str, Any]]:
    """Fetch candidate pairs for ``symbol`` from DexScreener.

    Retries once on 429 with exponential backoff. Other HTTP errors raise
    DexScreenerError. Network errors raise DexScreenerError.
    """
    import aiohttp  # local import keeps module import cheap when unused

    url = f"{DEXSCREENER_BASE_URL}{DEXSCREENER_SEARCH_PATH}"
    params = {"q": symbol}
    timeout = aiohttp.ClientTimeout(total=http_timeout_s)

    async def _do_request(s: aiohttp.ClientSession) -> list[dict[str, Any]]:
        for attempt in range(BACKOFF_MAX_RETRIES + 1):
            try:
                async with s.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 429:
                        if attempt < BACKOFF_MAX_RETRIES:
                            sleep_for = BACKOFF_INITIAL_S * (2**attempt)
                            logger.info(
                                "dexscreener_rate_limited symbol=%s attempt=%d sleeping=%.2fs",
                                symbol,
                                attempt,
                                sleep_for,
                            )
                            await asyncio.sleep(sleep_for)
                            continue
                        raise DexScreenerError(
                            f"DexScreener rate-limited after {BACKOFF_MAX_RETRIES + 1} attempts for symbol={symbol}"
                        )
                    if resp.status != 200:
                        raise DexScreenerError(f"HTTP {resp.status} for symbol={symbol}")
                    data = await resp.json(content_type=None)
            except DexScreenerError:
                raise
            except TimeoutError as exc:
                logger.warning("dexscreener_timeout symbol=%s attempt=%d", symbol, attempt)
                if attempt < BACKOFF_MAX_RETRIES:
                    await asyncio.sleep(BACKOFF_INITIAL_S * (2**attempt))
                    continue
                raise DexScreenerError(f"Timed out searching DexScreener for {symbol}") from exc
            except Exception as exc:
                raise DexScreenerError(f"DexScreener fetch failed for {symbol}: {exc}") from exc
            else:
                if not isinstance(data, dict):
                    raise DexScreenerError(f"DexScreener returned non-dict payload: {type(data).__name__}")
                pairs = data.get("pairs") or []
                if not isinstance(pairs, list):
                    raise DexScreenerError("DexScreener payload.pairs is not a list")
                return pairs

        # Loop only exits via return or raise above; this line is unreachable.
        raise DexScreenerError(f"DexScreener retry loop exited without response for symbol={symbol}")

    if session is not None:
        return await _do_request(session)

    async with aiohttp.ClientSession() as owned_session:
        return await _do_request(owned_session)


# =============================================================================
# Candidate extraction
# =============================================================================


def _extract_candidates_on_chain(
    pairs: list[dict[str, Any]],
    *,
    symbol: str,
    chain_slug: str,
) -> list[_Candidate]:
    """Extract candidate (address, liquidity, volume) rows from raw pairs.

    A pair yields a candidate when:
    - ``pair.chainId`` matches the requested chain slug (case-insensitive)
    - one side (base or quote) has a symbol case-insensitively matching ``symbol``
    - that side's address is non-empty
    """
    symbol_upper = symbol.upper()
    chain_slug_lower = chain_slug.lower()
    candidates: list[_Candidate] = []

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        pair_chain = str(pair.get("chainId", "")).lower()
        if pair_chain != chain_slug_lower:
            continue

        liquidity_usd = _safe_float(
            pair.get("liquidity", {}).get("usd") if isinstance(pair.get("liquidity"), dict) else None
        )
        volume_24h_usd = _safe_float(
            pair.get("volume", {}).get("h24") if isinstance(pair.get("volume"), dict) else None
        )
        pair_url = pair.get("url") if isinstance(pair.get("url"), str) else None

        for side in ("baseToken", "quoteToken"):
            token = pair.get(side)
            if not isinstance(token, dict):
                continue
            token_symbol = str(token.get("symbol", "")).strip()
            token_address = str(token.get("address", "")).strip()
            if not token_symbol or not token_address:
                continue
            if token_symbol.upper() != symbol_upper:
                continue
            candidates.append(
                _Candidate(
                    address=token_address,
                    symbol=token_symbol,
                    chain_slug=pair_chain,
                    liquidity_usd=liquidity_usd,
                    volume_24h_usd=volume_24h_usd,
                    pair_url=pair_url,
                )
            )

    return candidates


def _safe_float(value: Any) -> float:
    """Coerce a JSON number to float; unknown/None -> 0.0."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


# =============================================================================
# Gates 1-4
# =============================================================================


def _apply_gates(
    candidates: list[_Candidate],
    *,
    symbol: str,
    chain: str,
) -> DexScreenerResult | None:
    """Apply the four-gate policy and return the accepted candidate, if any.

    Raises ``AmbiguousTokenError`` when the dominance check (gate 4) fails.
    Returns None when all candidates are rejected by gates 1-3.
    """
    # Aggregate by address: a single contract may appear in many pairs; keep
    # its best (max) liquidity and volume across pairs so a liquidity-split
    # token isn't penalised by per-pool numbers.
    aggregated = _aggregate_by_address(candidates)

    passing: list[_Candidate] = []
    for cand in aggregated:
        reason = _gate_reject_reason(cand)
        if reason is None:
            passing.append(cand)
        else:
            _record_metric("rejected_by_gate_total")
            logger.info(
                "dexscreener_gate_rejected symbol=%s chain=%s address=%s liq=%.2f vol=%.2f reason=%s",
                symbol,
                chain,
                cand.address,
                cand.liquidity_usd,
                cand.volume_24h_usd,
                reason,
            )

    if not passing:
        return None

    # Sort by liquidity descending (dominance check uses this ordering)
    passing.sort(key=lambda c: c.liquidity_usd, reverse=True)
    leader = passing[0]

    if len(passing) > 1:
        runner_up = passing[1]
        # runner_up.liquidity_usd is normally >= MIN_LIQUIDITY_USD (gate 1),
        # but an operator can configure MIN_LIQUIDITY_USD=0 via env, and a
        # runner-up at exactly $0 liquidity would otherwise ZeroDivisionError
        # here. Treat zero-liquidity runner-up as effectively absent — the
        # leader is trivially dominant.
        if runner_up.liquidity_usd > 0:
            ratio = leader.liquidity_usd / runner_up.liquidity_usd
        else:
            ratio = float("inf")
        if ratio < DOMINANCE_MULTIPLE:
            addresses = [c.address for c in passing]
            suggestions = [
                f"Candidate {c.address}: liq=${c.liquidity_usd:,.0f}, vol24h=${c.volume_24h_usd:,.0f}" for c in passing
            ]
            suggestions.append("Pass the full contract address instead of the symbol.")
            _record_metric("ambiguous_total")
            logger.info(
                "dexscreener_ambiguous_symbol symbol=%s chain=%s leader=%s runner_up=%s ratio=%.2f threshold=%.2f",
                symbol,
                chain,
                leader.address,
                runner_up.address,
                ratio,
                DOMINANCE_MULTIPLE,
            )
            raise AmbiguousTokenError(
                token=symbol,
                chain=chain,
                reason=(
                    f"DexScreener returned multiple liquid contracts claiming '{symbol}' on {chain} "
                    f"(top two within {DOMINANCE_MULTIPLE:.1f}x liquidity). Disambiguate with an address."
                ),
                matching_addresses=addresses,
                suggestions=suggestions,
            )

    return DexScreenerResult(
        address=leader.address,
        chain=chain,
        symbol=leader.symbol,
        liquidity_usd=leader.liquidity_usd,
        volume_24h_usd=leader.volume_24h_usd,
        pair_url=leader.pair_url,
    )


def _aggregate_by_address(candidates: list[_Candidate]) -> list[_Candidate]:
    """Collapse multiple pairs for the same address into the single best pair.

    When a contract appears in multiple pools on the same chain, we pick the
    ONE pool with the highest liquidity and use *its* liquidity and volume
    together. This protects the turnover gate: previously we took
    ``max(liq)`` and ``max(vol)`` independently across all pools, which could
    combine a dormant deep pool with a shallow active one to synthesise a
    "healthy" candidate that no real pool satisfies (Codex audit P1).

    The practical effect: a liquidity-split token is judged by its best pool,
    not by an unreachable average. That pool must on its own satisfy all
    three gate thresholds (liquidity, volume, turnover).
    """
    best: dict[str, _Candidate] = {}
    for cand in candidates:
        key = cand.address.lower()
        existing = best.get(key)
        if existing is None or cand.liquidity_usd > existing.liquidity_usd:
            best[key] = cand
    return list(best.values())


def _gate_reject_reason(cand: _Candidate) -> str | None:
    """Return a one-word reason the candidate fails gates 1-3, or None if it passes."""
    if cand.liquidity_usd < MIN_LIQUIDITY_USD:
        return f"liquidity<{MIN_LIQUIDITY_USD:.0f}"
    if cand.volume_24h_usd < MIN_VOLUME_USD:
        return f"volume<{MIN_VOLUME_USD:.0f}"
    # Gate 3: turnover ratio. Guard against divide-by-zero (already covered by gate 1).
    if cand.liquidity_usd > 0:
        turnover = cand.volume_24h_usd / cand.liquidity_usd
        if turnover < MIN_TURNOVER_RATIO:
            return f"turnover<{MIN_TURNOVER_RATIO:.2f}"
    return None


# =============================================================================
# Metrics helpers (used by tests + token_service wiring)
# =============================================================================

_METRICS: dict[str, int] = {
    "requests_total": 0,
    "pairs_returned_total": 0,
    "resolved_total": 0,
    "rejected_by_gate_total": 0,
    "ambiguous_total": 0,
}


def get_metrics_snapshot() -> dict[str, int]:
    """Return a shallow copy of lookup metrics (for tests and /metrics)."""
    return dict(_METRICS)


def _record_metric(name: str, delta: int = 1) -> None:
    _METRICS[name] = _METRICS.get(name, 0) + delta


# =============================================================================
# Test-helper: reset module state (not used in production path)
# =============================================================================


def _reset_for_tests() -> None:
    """Reset module-level counters. Tests only."""
    for key in list(_METRICS):
        _METRICS[key] = 0


__all__ = [
    "AmbiguousTokenError",
    "CHAIN_SLUG_MAP",
    "DOMINANCE_MULTIPLE",
    "DexScreenerError",
    "DexScreenerResult",
    "MIN_LIQUIDITY_USD",
    "MIN_TURNOVER_RATIO",
    "MIN_VOLUME_USD",
    "chain_slug_for",
    "find_token_address",
    "get_metrics_snapshot",
]


if __name__ == "__main__":  # pragma: no cover
    # CLI smoke-test: uv run python -m almanak.gateway.services.dexscreener_lookup USDC arbitrum
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    async def _main() -> None:
        if len(sys.argv) < 3:
            print("usage: dexscreener_lookup.py SYMBOL CHAIN")
            sys.exit(1)
        try:
            res = await find_token_address(sys.argv[1], sys.argv[2])
        except AmbiguousTokenError as exc:
            print(f"AMBIGUOUS: {exc}")
            sys.exit(2)
        except DexScreenerError as exc:
            print(f"ERROR: {exc}")
            sys.exit(3)
        if res is None:
            print(f"NOT FOUND: {sys.argv[1]} on {sys.argv[2]}")
            sys.exit(1)
        print(f"OK: {res.address} (liq=${res.liquidity_usd:,.0f}, vol24h=${res.volume_24h_usd:,.0f})")

    asyncio.run(_main())
