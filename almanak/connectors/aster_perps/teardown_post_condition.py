"""Teardown closure authority for Aster Pro perp positions.

Aster Pro positions live in the venue's ledger, not on-chain, so closure is
measured from the venue through the gateway: a flat position on the market is a
measured close, a non-zero one is a measured residual (teardown FAILED), and a
read that cannot complete is unmeasured (UNVERIFIED) — never a fabricated close.
The venue-account row (cash, recovered by a withdrawal rather than a close) is
out of scope.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.venue_account_read_base import VENUE_ACCOUNT_VALUATION_SOURCE
from almanak.connectors.aster_perps.compiler import PROTOCOL
from almanak.connectors.aster_perps.gateway_client import AsterGatewayError, GatewayAsterPerpsClient
from almanak.connectors.aster_perps.markets import to_symbol

logger = logging.getLogger(__name__)


def _unverified(position_id: str, error: str) -> ClosureCheckResult:
    return ClosureCheckResult(closed=False, protocol=PROTOCOL, position_id=position_id, error=error, unmeasured=True)


def aster_perps_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,  # noqa: ARG001 — protocol parity; the venue is read through the gateway
    block: int | str | None = None,  # noqa: ARG001 — an off-chain venue has no block to pin
) -> ClosureCheckResult:
    """Verify an Aster Pro perp position is flat at the venue."""
    position_id = str(getattr(position, "position_id", "") or "")
    position_type = str(getattr(position, "position_type", "") or "").upper()
    details = position.details if isinstance(getattr(position, "details", None), dict) else {}
    if not position_type.endswith("PERP") or details.get("valuation_source") == VENUE_ACCOUNT_VALUATION_SOURCE:
        return ClosureCheckResult(
            closed=True,
            protocol=PROTOCOL,
            position_id=position_id,
            not_applicable=True,
            error="aster_perps post-condition verifies perp positions, not the venue account's cash",
        )
    market = str(details.get("market") or "")
    if not market:
        return _unverified(position_id, f"aster_perps closure UNVERIFIED for {position_id}: no details['market']")
    if gateway_client is None or not str(wallet_address or "").strip():
        return _unverified(position_id, "aster_perps post-condition needs a gateway client and the wallet address")
    try:
        symbol = to_symbol(market)
        positions = GatewayAsterPerpsClient(gateway_client).get_positions(wallet_address=wallet_address, symbol=symbol)
    except (AsterGatewayError, ValueError) as exc:
        return _unverified(position_id, f"aster_perps closure UNVERIFIED for {position_id}: {exc}")
    except Exception as exc:  # noqa: BLE001 — the check must never fault the teardown lane
        logger.debug("Aster position read raised for %s", position_id, exc_info=True)
        return _unverified(
            position_id, f"aster_perps closure UNVERIFIED for {position_id}: {type(exc).__name__}: {exc}"
        )
    held = [p for p in positions if p.symbol == symbol and p.position_amt != 0]
    if held:
        return ClosureCheckResult(
            closed=False,
            protocol=PROTOCOL,
            position_id=position_id,
            residual={"symbol": symbol, "position_amt": str(held[0].position_amt)},
        )
    return ClosureCheckResult(closed=True, protocol=PROTOCOL, position_id=position_id, residual={"symbol": symbol})


# The same per-market venue read answers TD-08 Plan-A before and after teardown:
# a non-zero position on the position's own market proves it open, a flat one
# proves it closed, and an unreadable venue stays unmeasured.
aster_perps_teardown_post_condition.supports_open_state_reconciliation = True  # type: ignore[attr-defined]
# ``handles_pending_orders`` is deliberately NOT declared: every Aster order is
# IOC, so no resting order exists for this hook to verify.

__all__ = ["aster_perps_teardown_post_condition"]
