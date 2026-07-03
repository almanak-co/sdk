"""Framework-side sync bridge for uncurated Curve pool metadata (VIB-5628).

The Curve receipt parser holds no gateway client of its own; on a static
``CURVE_POOLS`` miss it consults a runner-injected ``pool_meta_lookup``
callable to label an uncurated pool's LP legs (coin addresses / symbols /
pool type). This module builds that callable from the runner's
``GatewayClient``, mirroring the Uniswap V4 ``make_sync_pool_key_lookup``
precedent (``uniswap_v4/gateway_pool_key_client.py``): the receipt parser
receives a sync ``(pool_address, chain) -> metadata | None`` closure, injected
by the enricher only into connectors that declare it in
``receipt_parser_kwargs``.

**No async thread-bridge needed.** Unlike V4's async gRPC ``LookupV4PoolKey``
(which forces the ``_run_coro_blocking`` sync/thread dispatch), the Curve
resolver :func:`~almanak.connectors.curve.pool_resolver.resolve_pool_metadata`
is a plain SYNC function that rides the sync
``almanak.connectors._strategy_base.rpc.eth_call`` seam — the very same seam
the Curve adapter's ``_refresh_pool_info_from_chain`` uses for its refresh
reads. So this bridge is a plain sync closure with no event-loop handling.

The runner ``GatewayClient`` handed to ``ResultEnricher`` is the same object
the Curve adapter threads into ``resolve_pool_metadata`` as its
``gateway_client``; the ``eth_call`` seam gates transport on
``getattr(client, "is_connected", False)``, so it is accepted here without
adaptation.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from almanak.connectors.curve.pool_resolver import CurvePoolMetadata, resolve_pool_metadata

logger = logging.getLogger(__name__)

__all__ = ["make_sync_curve_pool_meta_lookup"]


def make_sync_curve_pool_meta_lookup(
    client: Any,
    *,
    timeout: float = 10.0,
) -> Callable[[str, str], CurvePoolMetadata | None]:
    """Return a sync ``(pool_address, chain) -> CurvePoolMetadata | None`` lookup.

    VIB-5628. Wraps :func:`resolve_pool_metadata` bound to the runner's
    ``GatewayClient`` so the Curve receipt parser can resolve an uncurated
    pool's shape live on a static-registry miss.

    Fail-closed: returns ``None`` (never raises) on any miss / resolver
    failure, mirroring the resolver's own contract — so the parser degrades to
    ``[]`` / ``""`` (Empty != Zero, never fabricates a leg). No async bridge is
    required because :func:`resolve_pool_metadata` is synchronous.

    Args:
        client: Connected ``GatewayClient`` owned by the runner — the same
            object the Curve adapter uses for its on-chain refresh reads.
        timeout: eth_call deadline (seconds) forwarded to the resolver.
    """

    def _lookup(pool_address: str, chain: str) -> CurvePoolMetadata | None:
        try:
            return resolve_pool_metadata(
                chain,
                pool_address,
                gateway_client=client,
                timeout=timeout,
            )
        except Exception:  # noqa: BLE001 — fail closed; parser degrades to legacy
            logger.debug(
                "curve pool_meta_lookup failed for %s on %s",
                pool_address,
                chain,
                exc_info=True,
            )
            return None

    return _lookup
