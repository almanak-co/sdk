"""Pair→pool payload resolution for Aerodrome classic (agent-tool lane, ALM-3365).

Loaded via ``CLASSIC_POOL_READER_SPEC.pair_resolver``. Resolves both pool
flavours through the connector's own factory validator, reads live state
through :class:`SolidlyPoolReader`, and picks the deeper pool when the caller
does not pin the stable flag (``fee_tier`` carries it: 0=volatile, 1=stable).
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.pool_validation_base import reader_rpc_call
from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_pool

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient


def resolve_pair_payload(
    chain: str,
    token_a: str,
    token_b: str,
    *,
    fee_tier: int | None = None,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    usd_price: Callable[[str], Decimal | None] | None = None,
    timeout: float = 10.0,
) -> dict | None:
    from almanak.connectors.aerodrome.pool_reader import CLASSIC_POOL_READER_SPEC
    from almanak.connectors.aerodrome.solidly_reader import SolidlyPoolReader

    if fee_tier is not None and fee_tier not in (0, 1):
        raise ValueError("aerodrome classic fee_tier carries the stable flag: pass 0 (volatile) or 1 (stable)")
    flags = (bool(fee_tier),) if fee_tier is not None else (False, True)
    candidates: list[tuple[bool, str]] = []
    indeterminate = False
    for stable in flags:
        result = validate_aerodrome_pool(chain, token_a, token_b, stable, rpc_url, gateway_client=gateway_client)
        if result.exists and result.pool_address:
            candidates.append((stable, result.pool_address))
        elif result.exists is None:
            indeterminate = True
    if not candidates:
        if indeterminate:
            raise RuntimeError(f"Aerodrome classic pair resolution indeterminate on {chain}: {token_a}/{token_b}")
        return None

    reader = SolidlyPoolReader(
        rpc_call=reader_rpc_call(gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout),
        spec=CLASSIC_POOL_READER_SPEC,
    )
    best: dict | None = None
    best_reserve = -1
    for stable, pool_address in candidates:
        try:
            envelope = reader.read_pool_price(pool_address, chain)
        except Exception:  # noqa: BLE001 — an unreadable candidate is skipped, not fatal
            continue
        state = envelope.value
        # Same pair on both flavours: raw token0 reserve is directly comparable.
        reserve = state.liquidity or 0
        payload = {
            "pool_address": pool_address,
            "stable": stable,
            "current_price": str(state.price),
            "fee_tier": state.fee_tier,
            "fee_tier_source": "explicit" if fee_tier is not None else "sweep",
            "liquidity": str(reserve),
            "token0_decimals": state.token0_decimals,
            "token1_decimals": state.token1_decimals,
            "lp_token": pool_address,
            "resolved_via": "factory_get_pool",
        }
        if reserve > best_reserve:
            best, best_reserve = payload, reserve
    return best
