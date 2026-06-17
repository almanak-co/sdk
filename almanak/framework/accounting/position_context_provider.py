"""Durable position context lookups for accounting carry-forward.

The runner's in-memory recent-open cache is an optimization. Accounting
correctness across restarts needs the same OPEN context to be recoverable from
the durable position history used by both the local SDK and hosted gateway.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.framework.observability.pnl_attributor import select_open_for_lp_close

logger = logging.getLogger(__name__)


class PositionContextProvider:
    """Read restart-safe position context from the StateManager surface."""

    def __init__(self, state_manager: Any, *, log: logging.Logger | None = None) -> None:
        self._state_manager = state_manager
        self._log = log or logger

    async def lp_close_open_payload(
        self,
        *,
        deployment_id: str,
        position_id: str,
        close_timestamp: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the matching LP OPEN payload shaped like the recent-open cache.

        ``None`` means no durable context is available. This method never raises:
        CLOSE accounting may degrade attribution, but teardown must keep reducing
        on-chain risk and the iteration lane already handles missing context as
        an unmeasured carry-forward rather than fabricated zero.
        """
        sm = self._state_manager
        if sm is None or not hasattr(sm, "get_position_history"):
            self._log.warning(
                "lp_close_durable_hydration.unavailable deployment_id=%s position_id=%s "
                "reason=state_manager_or_method_absent has_state_manager=%s has_method=%s",
                deployment_id,
                position_id,
                sm is not None,
                hasattr(sm, "get_position_history") if sm is not None else False,
            )
            return None
        try:
            history = await sm.get_position_history(deployment_id, position_id)
        except Exception as exc:  # noqa: BLE001 - context lookup must not halt teardown
            self._log.warning(
                "lp_close_durable_hydration.failed deployment_id=%s position_id=%s err=%s",
                deployment_id,
                position_id,
                exc,
            )
            return None
        if not history:
            return None

        latest_open = select_open_for_lp_close(history, close_timestamp=close_timestamp)
        if latest_open is None:
            return None
        # Empty≠Zero: numeric fields use ``in (None, "")`` coercion so a measured
        # ``Decimal("0")`` / ``0`` survives as ``"0"`` (a falsy ``or ""`` would collapse
        # it into the unmeasured sentinel ``""``). Mirrors the in-memory cache twin.
        raw_value_usd = latest_open.get("value_usd")
        raw_liquidity = latest_open.get("liquidity")
        raw_amount0 = latest_open.get("amount0")
        raw_amount1 = latest_open.get("amount1")
        return {
            "value_usd": "" if raw_value_usd in (None, "") else str(raw_value_usd),
            "ledger_entry_id": str(latest_open.get("ledger_entry_id") or ""),
            "timestamp": str(latest_open.get("timestamp") or ""),
            "tick_lower": latest_open.get("tick_lower"),
            "tick_upper": latest_open.get("tick_upper"),
            "liquidity": "" if raw_liquidity in (None, "") else str(raw_liquidity),
            "token0": str(latest_open.get("token0") or ""),
            "token1": str(latest_open.get("token1") or ""),
            "amount0": "" if raw_amount0 in (None, "") else str(raw_amount0),
            "amount1": "" if raw_amount1 in (None, "") else str(raw_amount1),
        }
