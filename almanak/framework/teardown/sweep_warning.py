"""VIB-4587 / F5 — teardown sweep DX warning.

``asset_policy=target_token`` resolves an ``amount='all'`` SWAP against
the *wallet* balance, not a strategy-scoped balance. A wallet shared
between strategies — or carrying pre-existing balances the strategy
never emitted any events for — will be swept entirely. That's working
as designed, but it's a silent surprise. This helper logs a WARNING
when the from-token has no accounting-event footprint for the
strategy, while leaving the sweep itself unchanged.

Lives under ``almanak/framework/teardown/`` (not the runner) because
both call sites that resolve ``amount='all'`` for teardown SWAPs need
to fire it:

* the inline single-chain fallback in ``runner_teardown.py``
* the manager path in ``teardown_manager.py``

The helper is read-only and best-effort. Any failure (wrong state-
manager flavour, DB locked, payload-JSON garbage) is swallowed — a
DX guard must never block the unwind.

See ``docs/internal/AccountingLiveMay18.md`` §F5 and
``docs/internal/blueprints/14-teardown-system.md`` for the design.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# Payload-JSON keys this helper scans for token symbols when building the
# strategy's emitted-token set. The set is intentionally broad (SWAP,
# SUPPLY/BORROW/REPAY/WITHDRAW, generic asset) so the heuristic is robust
# across primitives without per-primitive special-casing.
_PAYLOAD_TOKEN_KEYS: tuple[str, ...] = (
    "token_in",
    "token_out",
    "token",
    "asset",
    "from_token",
    "to_token",
)


def _extract_intent_type(intent: Any) -> str | None:
    """Return ``intent.intent_type`` as an upper-case string, dispatching on
    whether ``intent`` is a Pydantic object or a dict.

    Resumed manager-path teardown intents arrive as dicts
    (``teardown_manager.py:~829`` round-trips through SQLite), and dict
    serialisations may carry either ``"SWAP"`` or ``"IntentType.SWAP"`` —
    accept both. Returns ``None`` when no usable value is present.
    """
    raw = intent.get("intent_type") if isinstance(intent, dict) else getattr(intent, "intent_type", None)
    value = getattr(raw, "value", raw)
    if not isinstance(value, str):
        return None
    return value.rsplit(".", 1)[-1].upper()


def warn_if_sweep_non_strategy_balance(
    *,
    state_manager: Any,
    deployment_id: str,
    intent: Any,
    balance_token: str,
    balance_value: Any,
) -> None:
    """Log a WARNING when a teardown SWAP would sweep a wallet balance
    the strategy never emitted any accounting events for.

    Arguments are keyword-only because there is no natural call order
    and call sites are intentionally explicit about which state manager
    they're handing in (only the **accounting** ``StateManager`` —
    i.e. ``runner.state_manager`` — exposes ``get_accounting_events_sync``;
    the teardown lifecycle ``TeardownStateManager`` does not).

    Behaviour is unchanged whether or not the warning fires: the
    sweep proceeds, and any internal failure is silently swallowed.

    The intent may be a Pydantic object (``intent.intent_type.value``)
    OR a serialised dict (``intent["intent_type"]``). The teardown
    manager resume path (``teardown_manager.py:~829``) round-trips
    intents through SQLite as dicts, so a SWAP intent surfaces here
    in dict shape on every resumed teardown. Without the dict branch
    those resumed intents would silently bypass the warning — exactly
    the operator-surprise case F5 was built to make visible.
    """
    if _extract_intent_type(intent) != "SWAP":
        return
    if not balance_token or state_manager is None or not deployment_id:
        return
    # Only the accounting StateManager exposes this method. The teardown
    # lifecycle state manager does not; passing it here would AttributeError
    # and the try/except would silently swallow every call. We allow that
    # only as a final safety net — call sites must hand in the right one.
    if not hasattr(state_manager, "get_accounting_events_sync"):
        return
    try:
        events = state_manager.get_accounting_events_sync(deployment_id)
    except Exception:  # noqa: BLE001
        return  # DX guard — never block the unwind.
    if not events:
        # Strategy hasn't emitted anything yet (e.g. teardown fires after
        # a clean iteration with no trades). No baseline to compare;
        # suppress to keep the signal-to-noise ratio sane.
        return
    emitted_tokens: set[str] = set()
    for ev in events:
        if not isinstance(ev, dict):
            continue
        payload_raw = ev.get("payload_json")
        if not payload_raw:
            continue
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        for key in _PAYLOAD_TOKEN_KEYS:
            val = payload.get(key)
            if isinstance(val, str) and val:
                emitted_tokens.add(val.upper())
    if not emitted_tokens:
        return  # Nothing usable in payloads — can't make a confident claim.
    if balance_token.upper() in emitted_tokens:
        return
    logger.warning(
        "🛑 Teardown sweep WARNING: amount='all' for %s would consume the "
        "full wallet balance (%s) but this strategy emitted no accounting "
        "events involving %s. The sweep is wallet-scoped (asset_policy="
        "target_token), so pre-existing balances or balances from other "
        "strategies sharing this wallet are included. "
        "Strategy-emitted tokens: %s. "
        "See docs/internal/AccountingLiveMay18.md §F5 and "
        "docs/internal/blueprints/14-teardown-system.md.",
        balance_token,
        balance_value,
        balance_token,
        sorted(emitted_tokens),
    )


__all__ = ["warn_if_sweep_non_strategy_balance"]
