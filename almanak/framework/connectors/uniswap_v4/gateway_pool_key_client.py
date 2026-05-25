"""Framework-side client for ``MarketService.LookupV4PoolKey`` (VIB-4472).

Resolves a V4 ``pool_id`` (32-byte keccak hash) back to its canonical
``PoolKey`` via the gateway. Used by the V4 receipt parser (T07) to
enrich on-chain logs whose payload carries only the bytes32 id.

Strategy-container code MUST NOT bypass this client to read PoolManager
state directly -- the gateway boundary (AGENTS.md §Gateway boundary)
forbids direct RPC from ``almanak/framework/``. The cache that backs the
RPC lives in ``almanak/connectors/uniswap_v4/gateway/pool_key_cache.py``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from collections.abc import Callable
from typing import Any

import grpc

from almanak.framework.connectors.uniswap_v4.sdk import PoolKey
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class V4PoolKeyNotFound(LookupError):
    """The gateway has no cached PoolKey for the given ``(chain, pool_id)``."""

    def __init__(self, pool_id_hex: str, chain: str) -> None:
        self.pool_id_hex = pool_id_hex
        self.chain = chain
        super().__init__(f"V4 pool_id {pool_id_hex} not found on chain {chain}")


def _coerce_pool_id_bytes(pool_id: bytes | str) -> bytes:
    """Accept bytes32 / 0x-prefixed hex / bare hex and return raw 32 bytes."""
    if isinstance(pool_id, bytes):
        if len(pool_id) != 32:
            raise ValueError(f"pool_id must be 32 bytes, got {len(pool_id)}")
        return pool_id
    if isinstance(pool_id, str):
        clean = pool_id.lower().removeprefix("0x")
        if len(clean) != 64:
            raise ValueError(f"pool_id hex must be 64 chars, got {len(clean)}")
        return bytes.fromhex(clean)
    raise TypeError(f"pool_id must be bytes or str, got {type(pool_id).__name__}")


async def lookup_v4_pool_key(
    client: GatewayClient,
    *,
    pool_id: bytes | str,
    chain: str,
    timeout: float = 10.0,
) -> PoolKey:
    """Resolve a V4 pool_id to its canonical PoolKey via the gateway.

    Args:
        client: Connected GatewayClient instance.
        pool_id: 32-byte pool id as raw bytes or hex string (with or without
            0x prefix).
        chain: Chain name (e.g. ``"base"``).
        timeout: gRPC deadline in seconds.

    Returns:
        Canonical ``PoolKey`` with ``currency0 < currency1`` invariant
        preserved by the gateway.

    Raises:
        ValueError: ``pool_id`` is not 32 bytes / 64 hex chars.
        V4PoolKeyNotFound: Gateway returned NOT_FOUND.
        grpc.RpcError: Transport / gateway error other than NOT_FOUND.
    """
    pid_bytes = _coerce_pool_id_bytes(pool_id)
    request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=pid_bytes, chain=chain)

    try:
        response = await asyncio.to_thread(client.market.LookupV4PoolKey, request, timeout=timeout)
    except grpc.RpcError as exc:
        code = getattr(exc, "code", lambda: None)()
        if code == grpc.StatusCode.NOT_FOUND:
            raise V4PoolKeyNotFound("0x" + pid_bytes.hex(), chain) from exc
        logger.debug(
            "LookupV4PoolKey RPC failed for chain=%s pool_id=0x%s: %s",
            chain,
            pid_bytes.hex(),
            exc,
        )
        raise

    pk = response.pool_key
    # The gateway enforces currency0 < currency1; defence-in-depth here so
    # a buggy gateway can't silently feed callers an unsorted pair.
    if int(pk.currency0, 16) >= int(pk.currency1, 16):
        raise ValueError(
            f"gateway returned unsorted PoolKey for pool_id=0x{pid_bytes.hex()}: {pk.currency0} >= {pk.currency1}"
        )

    return PoolKey(
        currency0=pk.currency0,
        currency1=pk.currency1,
        fee=int(pk.fee),
        tick_spacing=int(pk.tick_spacing),
        hooks=pk.hooks,
    )


def _run_coro_blocking(coro_factory: Callable[[], Any], *, timeout: float) -> Any:
    """Run ``coro_factory()`` and block until completion, regardless of caller context.

    VIB-4426 — the sync ``pool_key_lookup`` bridge is invoked from
    ``ResultEnricher.enrich`` which the production runners call from
    ``async def`` coroutines (``strategy_runner._single_chain_handle_success``,
    ``teardown_commit.commit_teardown_intent``, ``inner_runner.execute_intent``).
    Codex P1 #1: ``asyncio.run`` raises ``RuntimeError("cannot be called from
    a running event loop")`` in that case, the parser swallows it as
    ``pool_key_lookup_error``, and V4 LP_CLOSE accounting silently drops.

    Strategy:

    - **No running loop** (sync test harness, pure ``asyncio.run`` entrypoint
      that has already exited): use ``asyncio.run`` directly — the simplest
      path, no thread spawn.
    - **Running loop** (production runners): submit ``asyncio.run(coro)`` to
      a worker thread via ``ThreadPoolExecutor``. The worker thread has no
      event loop, so ``asyncio.run`` succeeds; ``.result(timeout=...)``
      blocks the calling coroutine until the worker returns.

    ``coro_factory`` is a callable that returns a fresh coroutine each call
    (NOT a coroutine object) — coroutines can be awaited only once, and
    constructing two of them for "maybe sync, maybe thread" would leak the
    unused one.

    The timeout argument bounds the blocking wait in the running-loop path;
    ``asyncio.run`` in the sync path is uninterruptable so the wall-clock
    limit there is whatever the gRPC deadline allows.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop in this thread; safe to use asyncio.run directly.
        return asyncio.run(coro_factory())

    # We are inside a running loop. Run on a worker thread which has no loop.
    # ``max_workers=1`` keeps this scoped to a single bounded fork; the
    # context-manager teardown waits for the submitted task to finish.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(asyncio.run, coro_factory())
        return future.result(timeout=timeout)


def make_sync_pool_key_lookup(
    client: GatewayClient,
    *,
    timeout: float = 10.0,
) -> Callable[[str, str], PoolKey | None]:
    """Return a sync ``(pool_id_hex, chain) -> PoolKey | None`` callable.

    VIB-4477 (T08), VIB-4426 P1 #1. The V4 receipt parser is called from
    the sync ``ResultEnricher`` pipeline. The production runners invoke
    that pipeline from ``async def`` coroutines, so the bridge must work
    from both contexts:

    - **No event loop running**: ``asyncio.run`` directly.
    - **Inside a running event loop**: dispatch to a worker thread that
      has its own (empty) loop scope; ``asyncio.run`` succeeds there,
      and the calling coroutine blocks on ``.result()`` until the worker
      returns.

    Returns ``None`` for :class:`V4PoolKeyNotFound` (the parser then logs
    and drops the close event with ``reason=pool_key_not_found``);
    re-raises every other failure so the parser's outer ``except``
    treats it as ``pool_key_lookup_error`` and produces a structured log.

    The bridge is intentionally simple — V0 does not need a high-throughput
    optimisation. Each lookup is ~one RPC round-trip; a future ticket can
    layer a request-coalescing cache on top if profiling shows pressure.

    Args:
        client: Connected :class:`GatewayClient` instance owned by the runner.
        timeout: gRPC deadline in seconds, forwarded to
            :func:`lookup_v4_pool_key`. Also bounds the worker-thread wait
            in the running-loop path.

    Returns:
        A ``PoolKeyLookup``-compatible callable suitable for passing into
        :class:`almanak.framework.connectors.uniswap_v4.receipt_parser.UniswapV4ReceiptParser`.
    """

    def _lookup(pool_id_hex: str, chain: str) -> PoolKey | None:
        # Construct a fresh coroutine per attempt. ``_run_coro_blocking``
        # picks sync-or-thread dispatch based on whether a loop is running.
        try:
            return _run_coro_blocking(
                lambda: lookup_v4_pool_key(
                    client,
                    pool_id=pool_id_hex,
                    chain=chain,
                    timeout=timeout,
                ),
                timeout=timeout,
            )
        except V4PoolKeyNotFound:
            return None

    return _lookup


__all__ = [
    "V4PoolKeyNotFound",
    "lookup_v4_pool_key",
    "make_sync_pool_key_lookup",
]
