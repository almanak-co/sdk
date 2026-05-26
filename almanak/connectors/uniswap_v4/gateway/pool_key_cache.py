"""Uniswap V4 pool_id -> PoolKey derivation cache (VIB-4472 / T03).

In Uniswap V4, every pool is identified on-chain by a 32-byte ``poolId``
computed as ``keccak256(abi.encode(currency0, currency1, fee, tickSpacing,
hooks))``. The hash is irreversible, so resolving a poolId back to the
structured ``PoolKey`` requires an external source.

This module maintains an in-memory derivation cache per chain, populated
from observed ``PoolManager.Initialize`` events. The cache fronts the
gateway ``MarketService.LookupV4PoolKey`` RPC, which is used by the V4
receipt parser to enrich LP / SWAP events whose log payload carries only
the poolId.

Event signature::

    Initialize(
        PoolId  indexed id,            // bytes32
        Currency indexed currency0,    // address
        Currency indexed currency1,    // address
        uint24  fee,
        int24   tickSpacing,
        IHooks  hooks,                 // address
        uint160 sqrtPriceX96,
        int24   tick
    )

Indexed: ``id`` (topics[1]), ``currency0`` (topics[2]), ``currency1``
(topics[3]). Non-indexed payload (5 words = 160 hex chars): ``fee``,
``tickSpacing``, ``hooks``, ``sqrtPriceX96``, ``tick``.

V0 scope: Base only. Other chains can register via ``register_chain``
when their PoolManager addresses are known; lookups on unconfigured
chains return ``None``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from web3 import AsyncHTTPProvider, AsyncWeb3

from almanak.connectors._base.gateway_capabilities import PoolKeyCacheError
from almanak.core.contracts import UNISWAP_V4
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)

# keccak256("Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)")
INITIALIZE_EVENT_TOPIC = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"

# Zero address sentinel for "no hooks" pools.
NO_HOOKS = "0x0000000000000000000000000000000000000000"

# Conservative default scan window per backfill. Large queries can be
# rate-limited or rejected by archive nodes. The gateway will scan the
# last N blocks on a cache miss before giving up.
DEFAULT_BACKFILL_BLOCKS = 50_000

# VIB-4426 — bounded historical-expansion cap. A lookup miss whose target
# pool was initialized > DEFAULT_BACKFILL_BLOCKS ago would otherwise be
# permanently unresolvable (the original implementation only ever scanned
# ``[head - 50k, head]``). To recover, each ``_refresh_chain`` call that
# misses the forward-tail scan ALSO scans one ``HISTORICAL_BACKFILL_WINDOW``
# block window earlier than the previously-earliest-scanned point — up to
# ``MAX_HISTORICAL_BACKFILL_BLOCKS`` total per chain per process lifetime.
# The cap prevents a single rogue lookup against an unknown pool from
# DoS-ing the upstream archive node.
HISTORICAL_BACKFILL_WINDOW = 50_000
MAX_HISTORICAL_BACKFILL_BLOCKS = 500_000


class V4CanonicalSeedCollisionError(RuntimeError):
    """``register_canonical`` was called with a pool_id already present in
    the cache, but the new :class:`CachedPoolKey` differs from the existing
    one. Indicates a programming error in the canonical seed registry — a
    duplicate row that produces the same pool_id hash from different PoolKey
    fields (genuine keccak collisions are vanishingly unlikely; far more
    common is a duplicate symbol pair, a wrong fee tier, or a token-resolver
    address change). The cache refuses to silently overwrite because doing
    so would mask the bug at boot.

    Defined in this module rather than :mod:`canonical_pools` to keep
    the cache's collision contract co-located with the cache itself — the
    cache is the one enforcing the invariant, not the seed module.
    """


# VIB-4818 — refresh-time failures raise the gateway-side base type
# ``PoolKeyCacheError``. The gateway's ``LookupV4PoolKey`` servicer catches
# the base type and discriminates by ``.code`` without importing any V4
# symbol. The original ``V4PoolKeyLookupError`` was a strict subset of the
# base type — same constructor signature, same ``code`` field — so the
# rename is purely cosmetic at the call sites.


@dataclass(frozen=True)
class CachedPoolKey:
    """Decoded PoolKey from an Initialize event.

    Invariants enforced at construction time:
    - ``currency0``, ``currency1``, ``hooks`` are lowercased 0x-prefixed hex.
    - ``int(currency0, 16) < int(currency1, 16)``.
    - ``fee`` fits in uint24.
    - ``tick_spacing`` fits in int24.
    """

    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str

    def __post_init__(self) -> None:
        if int(self.currency0, 16) >= int(self.currency1, 16):
            raise ValueError(f"currency0 must be < currency1 (got {self.currency0} vs {self.currency1})")
        if not 0 <= self.fee < (1 << 24):
            raise ValueError(f"fee out of uint24 range: {self.fee}")
        if not -(1 << 23) <= self.tick_spacing < (1 << 23):
            raise ValueError(f"tick_spacing out of int24 range: {self.tick_spacing}")


def _normalize_pool_id(pool_id: bytes | str) -> str:
    """Return a canonical lowercase 0x-prefixed 32-byte hex string.

    Accepts raw bytes (32 bytes) or hex string (with or without 0x prefix).
    Raises ValueError on invalid input.
    """
    if isinstance(pool_id, bytes):
        if len(pool_id) != 32:
            raise ValueError(f"pool_id must be 32 bytes, got {len(pool_id)}")
        return "0x" + pool_id.hex()
    if isinstance(pool_id, str):
        clean = pool_id.lower()
        if clean.startswith("0x"):
            clean = clean[2:]
        if len(clean) != 64:
            raise ValueError(f"pool_id hex must be 64 chars, got {len(clean)}")
        try:
            int(clean, 16)
        except ValueError as e:
            raise ValueError(f"pool_id is not valid hex: {pool_id!r}") from e
        return "0x" + clean
    raise TypeError(f"pool_id must be bytes or str, got {type(pool_id).__name__}")


def _decode_initialize_log(log: dict) -> tuple[str, CachedPoolKey] | None:
    """Decode a single PoolManager.Initialize log to (pool_id, CachedPoolKey).

    Returns None on any decode failure so the caller can skip and continue.
    """
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None

    def _t(t: object) -> str:
        if isinstance(t, bytes):
            return "0x" + t.hex()
        if isinstance(t, str):
            return t.lower()
        return str(t).lower()

    topic0 = _t(topics[0])
    if topic0 != INITIALIZE_EVENT_TOPIC:
        return None

    try:
        pool_id = _normalize_pool_id(_t(topics[1]))
        # Indexed addresses are right-padded into 32 bytes. Take last 40 hex chars.
        currency0_raw = _t(topics[2])
        currency1_raw = _t(topics[3])
        currency0 = "0x" + currency0_raw[-40:]
        currency1 = "0x" + currency1_raw[-40:]

        # Non-indexed payload: fee (uint24, padded to 32 bytes), tickSpacing
        # (int24, padded), hooks (address, padded), sqrtPriceX96 (uint160,
        # padded), tick (int24, padded). 5 words = 320 hex chars after 0x.
        data = log.get("data") or "0x"
        if isinstance(data, bytes):
            data_hex = data.hex()
        else:
            data_hex = data.lower().removeprefix("0x")
        if len(data_hex) < 5 * 64:
            return None

        fee = int(data_hex[0:64], 16)
        tick_spacing_raw = int(data_hex[64:128], 16)
        # int24 sign extension from 256-bit uint
        if tick_spacing_raw >= (1 << 255):
            tick_spacing = tick_spacing_raw - (1 << 256)
        else:
            tick_spacing = tick_spacing_raw
        hooks = "0x" + data_hex[128:192][-40:]

        key = CachedPoolKey(
            currency0=currency0,
            currency1=currency1,
            fee=fee,
            tick_spacing=tick_spacing,
            hooks=hooks,
        )
        return pool_id, key
    except Exception as exc:  # noqa: BLE001 - swallow per-log decode failures
        logger.debug("V4PoolKeyCache: failed to decode Initialize log: %s", exc)
        return None


class V4PoolKeyCache:
    """In-memory per-chain cache of V4 ``pool_id -> PoolKey``.

    Populated lazily from ``PoolManager.Initialize`` event logs. A miss on
    ``lookup`` triggers a bounded backfill from the chain's PoolManager
    before reporting ``None``.

    Thread-safety: ``lookup`` and ``populate_from_logs`` are coroutine-safe
    via an asyncio lock. Reads after population are dict-O(1).
    """

    def __init__(
        self,
        *,
        rpc_url_resolver=get_rpc_url,
        network: str = "mainnet",
        backfill_blocks: int = DEFAULT_BACKFILL_BLOCKS,
        historical_window: int = HISTORICAL_BACKFILL_WINDOW,
        max_historical_blocks: int = MAX_HISTORICAL_BACKFILL_BLOCKS,
    ) -> None:
        self._rpc_url_resolver = rpc_url_resolver
        self._network = network
        self._backfill_blocks = backfill_blocks
        self._historical_window = historical_window
        self._max_historical_blocks = max_historical_blocks
        # chain -> pool_id -> CachedPoolKey
        self._index: dict[str, dict[str, CachedPoolKey]] = {}
        # chain -> highest block scanned so far (only re-scan above this)
        self._last_scanned_block: dict[str, int] = {}
        # VIB-4426 — chain -> lowest block scanned so far (historical floor).
        # Lookup misses progressively walk this downward by one
        # ``historical_window`` per call until ``max_historical_blocks`` is
        # exhausted, recovering pools initialized before the initial backfill
        # window.
        self._earliest_scanned_block: dict[str, int] = {}
        # VIB-4426 — reuse a single AsyncWeb3 per chain across refreshes.
        # Instantiating AsyncHTTPProvider per call wastes the underlying
        # aiohttp connection pool and re-runs SSL handshake.
        self._web3_clients: dict[str, AsyncWeb3] = {}
        self._lock = asyncio.Lock()

    def known_pool_count(self, chain: str) -> int:
        return len(self._index.get(chain.lower(), {}))

    def register(self, chain: str, pool_id: bytes | str, pool_key: CachedPoolKey) -> None:
        """Insert a PoolKey into the cache without consulting the network.

        Exposed for tests and operator tooling. Production code SHOULD go
        through ``populate_from_logs`` or ``lookup`` so the cache reflects
        on-chain state, not operator assumptions.
        """
        chain_l = chain.lower()
        idx = self._index.setdefault(chain_l, {})
        idx[_normalize_pool_id(pool_id)] = pool_key

    def register_canonical(self, chain: str, pool_id: bytes | str, pool_key: CachedPoolKey) -> str:
        """Insert a deterministically-computed canonical PoolKey.

        Used by :mod:`almanak.connectors.uniswap_v4.gateway.canonical_pools` at gateway
        boot to pre-seed pools whose ``Initialize`` event lives outside the
        bounded backfill window (VIB-4534). Differs from :meth:`register`
        in two ways:

        1. **Idempotent on identical input** — repeated calls with the same
           ``(chain, pool_id, pool_key)`` return ``"already_present"`` and
           do not mutate the cache.
        2. **Collision-detecting on conflicting input** — calling with a
           pool_id already present BUT a different :class:`CachedPoolKey`
           raises :class:`V4CanonicalSeedCollisionError`. This protects
           against duplicate / mis-fielded rows in the seed registry that
           would otherwise silently corrupt the cache.

        Returns:
            ``"registered"`` if the entry was newly inserted, or
            ``"already_present"`` if an identical entry was already cached.

        Raises:
            V4CanonicalSeedCollisionError: pool_id already present with a
                different :class:`CachedPoolKey`.
        """
        chain_l = chain.lower()
        pid = _normalize_pool_id(pool_id)
        idx = self._index.setdefault(chain_l, {})
        existing = idx.get(pid)
        if existing is not None:
            if existing == pool_key:
                return "already_present"
            raise V4CanonicalSeedCollisionError(
                f"V4 canonical seed collision: chain={chain_l} pool_id={pid} "
                f"existing={existing!r} attempted={pool_key!r}"
            )
        idx[pid] = pool_key
        return "registered"

    async def lookup(self, chain: str, pool_id: bytes | str) -> CachedPoolKey | None:
        """Resolve a pool_id to a PoolKey on the given chain.

        On miss, perform a bounded log backfill against the chain's
        PoolManager and re-check. The backfill walks the chain in two
        directions:

        1. Forward tail — scan ``[last_scanned + 1, head]`` to catch newly
           initialized pools.
        2. Historical expansion — if the forward tail did not produce the
           target, scan one ``HISTORICAL_BACKFILL_WINDOW`` block range
           earlier than the previously-earliest-scanned block. Repeated
           lookups walk further back, up to
           ``MAX_HISTORICAL_BACKFILL_BLOCKS`` per chain per process.

        Returns ``None`` if still unknown after both passes.
        """
        chain_l = chain.lower()
        try:
            pid = _normalize_pool_id(pool_id)
        except (TypeError, ValueError) as exc:
            logger.debug("V4PoolKeyCache.lookup: invalid pool_id: %s", exc)
            return None

        hit = self._index.get(chain_l, {}).get(pid)
        if hit is not None:
            return hit

        async with self._lock:
            # Re-check inside lock in case another coroutine populated.
            hit = self._index.get(chain_l, {}).get(pid)
            if hit is not None:
                return hit
            await self._refresh_chain(chain_l, target_pool_id=pid)
            return self._index.get(chain_l, {}).get(pid)

    def _get_or_create_web3(self, chain: str, rpc_url: str) -> AsyncWeb3:
        """Return a chain-scoped AsyncWeb3, instantiating on first use.

        Caching the client preserves the underlying aiohttp connection
        pool and the SSL context across refreshes — instantiating per
        ``_refresh_chain`` call would re-handshake on every cache miss.
        """
        client = self._web3_clients.get(chain)
        if client is None:
            client = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs={"ssl": build_ssl_context()}))
            self._web3_clients[chain] = client
        return client

    async def _refresh_chain(self, chain: str, *, target_pool_id: str | None = None) -> None:
        """Refresh the cache for ``chain`` by scanning Initialize logs.

        Always attempts the forward-tail scan. If ``target_pool_id`` is
        supplied AND not found after the forward tail, runs ONE historical
        backward window (bounded by ``self._max_historical_blocks``).

        Called while holding ``self._lock``.

        Raises:
            PoolKeyCacheError: on configuration / upstream-RPC failure
                so the servicer can map to ``FAILED_PRECONDITION`` /
                ``UNAVAILABLE`` rather than masking as ``NOT_FOUND``
                (Codex P1 #2). Returns normally only when the scan
                completed successfully (the target may or may not be in
                the index).
        """
        chain_addrs = UNISWAP_V4.get(chain)
        if not chain_addrs or "pool_manager" not in chain_addrs:
            raise PoolKeyCacheError(
                f"no V4 PoolManager configured for chain={chain}",
                code="failed_precondition",
            )

        pool_manager = chain_addrs["pool_manager"].lower()
        try:
            rpc_url = self._rpc_url_resolver(chain, network=self._network)
        except Exception as exc:  # noqa: BLE001
            raise PoolKeyCacheError(
                f"RPC URL resolver failed for chain={chain} network={self._network}: {exc}",
                code="failed_precondition",
            ) from exc

        if not rpc_url:
            raise PoolKeyCacheError(
                f"no RPC URL configured for chain={chain} network={self._network}",
                code="failed_precondition",
            )

        w3 = self._get_or_create_web3(chain, rpc_url)
        try:
            head = await w3.eth.block_number
        except Exception as exc:  # noqa: BLE001
            raise PoolKeyCacheError(
                f"eth_blockNumber failed for chain={chain}: {exc}",
                code="unavailable",
            ) from exc

        # --- Pass 1: forward tail ---------------------------------------
        last = self._last_scanned_block.get(chain)
        if last is None:
            from_block = max(0, head - self._backfill_blocks)
        else:
            # Only scan the new tail above what we've already covered.
            from_block = last + 1

        if from_block <= head:
            added = await self.populate_from_logs(
                chain=chain,
                w3=w3,
                pool_manager=pool_manager,
                from_block=from_block,
                to_block=head,
            )
            if added is None:
                # eth_getLogs failed — preserve watermark so the next scan
                # retries this range rather than silently skipping it.
                # VIB-4426 P1 #2 — surface as UNAVAILABLE so the servicer
                # does not mask the upstream failure as NOT_FOUND.
                raise PoolKeyCacheError(
                    f"eth_getLogs failed for chain={chain} range=[{from_block}..{head}]",
                    code="unavailable",
                )
            self._last_scanned_block[chain] = head
            # Initialise the earliest watermark on the first scan so the
            # historical pass below has a valid floor to expand below.
            self._earliest_scanned_block.setdefault(chain, from_block)
            logger.info(
                "V4PoolKeyCache: chain=%s tail-scanned %d..%d, added %d pools (total=%d)",
                chain,
                from_block,
                head,
                added,
                self.known_pool_count(chain),
            )

        # --- Pass 2: historical backward expansion ----------------------
        # Only run when a specific lookup target is still missing AND there
        # is unscanned history left below the configured floor.
        if target_pool_id is None:
            return
        if self._index.get(chain, {}).get(target_pool_id) is not None:
            return

        earliest = self._earliest_scanned_block.get(chain)
        if earliest is None or earliest <= 0:
            return

        # Per-process floor: don't expand below ``head - max_historical_blocks``.
        floor = max(0, head - self._max_historical_blocks)
        if earliest <= floor:
            logger.debug(
                "V4PoolKeyCache: chain=%s historical floor reached (earliest=%d, floor=%d); "
                "pool_id=%s remains unresolved",
                chain,
                earliest,
                floor,
                target_pool_id,
            )
            return

        hist_to = earliest - 1
        hist_from = max(floor, earliest - self._historical_window)
        if hist_from > hist_to:
            return

        added = await self.populate_from_logs(
            chain=chain,
            w3=w3,
            pool_manager=pool_manager,
            from_block=hist_from,
            to_block=hist_to,
        )
        if added is None:
            # RPC failure — preserve earliest watermark so next lookup retries.
            # VIB-4426 P1 #2 — surface as UNAVAILABLE.
            raise PoolKeyCacheError(
                f"eth_getLogs failed for chain={chain} range=[{hist_from}..{hist_to}] (historical)",
                code="unavailable",
            )
        self._earliest_scanned_block[chain] = hist_from
        logger.info(
            "V4PoolKeyCache: chain=%s hist-scanned %d..%d (target=%s), added %d pools (total=%d)",
            chain,
            hist_from,
            hist_to,
            target_pool_id,
            added,
            self.known_pool_count(chain),
        )

    async def populate_from_logs(
        self,
        *,
        chain: str,
        w3: AsyncWeb3,
        pool_manager: str,
        from_block: int,
        to_block: int,
    ) -> int | None:
        """Fetch Initialize logs in [from_block, to_block] and ingest them.

        Self-chunking: providers (Alchemy, Infura, …) cap `eth_getLogs`
        response size before they cap the block range. The single-window
        request can fail with a "response size exceeded" /
        "block range should work" hint even on a 50k-block window
        — observed against Alchemy Base post VIB-4426 #2335 with the
        WETH/USDC v4 PoolManager. We retry by halving the window down to
        a minimum chunk of 1k blocks. Errors that aren't size-limit shaped
        (e.g. transport failures) bubble up after the first attempt.

        Returns:
            int: count of newly inserted entries (duplicates count once); may be 0
                 if the range was empty but the fetch succeeded.
            None: eth_getLogs raised; partial progress is preserved but callers
                  MUST NOT advance the scan watermark over a failed range.
        """
        chain_l = chain.lower()
        raw_logs = await self._get_initialize_logs_chunked(
            chain=chain,
            w3=w3,
            pool_manager=pool_manager,
            from_block=from_block,
            to_block=to_block,
        )
        if raw_logs is None:
            return None

        added = 0
        idx = self._index.setdefault(chain_l, {})
        for raw in raw_logs:
            # web3.py returns AttributeDict-like objects (subclass of dict, but
            # mypy sees the LogReceipt TypedDict). Normalize to a plain dict
            # unconditionally so the decoder's dict[Any, Any] contract holds.
            log: dict[Any, Any] = dict(raw)
            decoded = _decode_initialize_log(log)
            if decoded is None:
                continue
            pid, key = decoded
            if pid not in idx:
                idx[pid] = key
                added += 1
        return added

    async def _get_initialize_logs_chunked(
        self,
        *,
        chain: str,
        w3: AsyncWeb3,
        pool_manager: str,
        from_block: int,
        to_block: int,
        min_chunk_blocks: int = 1_000,
    ) -> list[Any] | None:
        """Fetch Initialize logs across [from_block, to_block], bisecting on
        provider response-size errors.

        Returns the aggregated raw log list on success, or ``None`` if any
        sub-window fails after reaching the minimum chunk size — that's a
        genuine transport / configuration failure, not a size issue, and the
        caller must NOT advance the scan watermark.
        """
        address = AsyncWeb3.to_checksum_address(pool_manager)
        # Stack-based iterative bisection — keeps memory bounded and
        # preserves chronological ordering of accumulated logs (depth-first
        # left-to-right).
        pending: list[tuple[int, int]] = [(from_block, to_block)]
        collected: list[Any] = []
        while pending:
            lo, hi = pending.pop()
            try:
                raw_logs = await w3.eth.get_logs(
                    {
                        "fromBlock": lo,
                        "toBlock": hi,
                        "address": address,
                        "topics": [INITIALIZE_EVENT_TOPIC],
                    }
                )
            except Exception as exc:  # noqa: BLE001
                # Bisect on any failure as long as the window can still be
                # halved above the min chunk. Many providers report
                # response-size issues with a variety of error strings
                # (Alchemy: "Log response size exceeded"; Infura/Quicknode:
                # different wording; some clients raise on `code: -32602`
                # nested inside `-32603 Fork Error: Transport(HttpError)`
                # — observed on Anvil + Alchemy fork). Cheaper to bisect
                # than to enumerate provider error taxonomies.
                span = hi - lo + 1
                if span > min_chunk_blocks:
                    mid = lo + span // 2 - 1
                    # Push right-half first so the left-half is processed
                    # first when popped — preserves chronological order.
                    pending.append((mid + 1, hi))
                    pending.append((lo, mid))
                    logger.info(
                        "V4PoolKeyCache: eth_getLogs window [%d..%d] failed, bisecting to "
                        "[%d..%d] + [%d..%d] (chain=%s, exc=%s)",
                        lo,
                        hi,
                        lo,
                        mid,
                        mid + 1,
                        hi,
                        chain,
                        type(exc).__name__,
                    )
                    continue
                logger.warning(
                    "V4PoolKeyCache: eth_getLogs failed chain=%s pool_manager=%s [%d..%d] "
                    "at min chunk size (span=%d): %s",
                    chain,
                    pool_manager,
                    lo,
                    hi,
                    span,
                    exc,
                )
                return None
            collected.extend(raw_logs)
        return collected


__all__ = [
    "DEFAULT_BACKFILL_BLOCKS",
    "HISTORICAL_BACKFILL_WINDOW",
    "INITIALIZE_EVENT_TOPIC",
    "MAX_HISTORICAL_BACKFILL_BLOCKS",
    "NO_HOOKS",
    "CachedPoolKey",
    "V4CanonicalSeedCollisionError",
    "V4PoolKeyCache",
    "_decode_initialize_log",
    "_normalize_pool_id",
]
