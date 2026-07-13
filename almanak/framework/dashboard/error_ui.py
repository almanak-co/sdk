"""Shared gateway-error classification + user-facing rendering (VIB-4047).

The dashboard reaches its data exclusively over the gateway gRPC channel. When
that channel fails, the panes must fail **loudly and cleanly**:

* **Never silently empty.** An UNAUTHENTICATED ``GetPositions`` against a
  managed mainnet gateway used to render the same "No positions yet" / zeroed
  tiles as a genuinely-idle strategy — so a dashboard that could not read
  $2.83 of live LP exposure looked identical to one with nothing to show. The
  silent-empty-during-exposure half is the dangerous one: a red banner must
  make the failure impossible to miss.
* **Never a raw stack trace.** A leaked ``_InactiveRpcError`` / ``status = 16``
  gRPC repr in a user-facing pane is noise, not signal. Full detail goes to the
  logs; the pane shows a clean, actionable one-liner (with the raw text behind
  an opt-in debug expander only).

Classification walks the exception's ``__cause__`` / ``__context__`` chain for a
``grpc`` status code (the RPC facades wrap the original ``grpc.RpcError`` via
``raise ... from exc``), falling back to a text match so it stays robust even if
a call site forgot to chain the cause.
"""

from __future__ import annotations

import logging
import traceback
from enum import Enum
from typing import Any

import streamlit as st

from almanak.config.framework import framework_config_from_env

logger = logging.getLogger(__name__)


class GatewayErrorKind(Enum):
    """Coarse gateway-failure class the UI branches on."""

    AUTH = "auth"  # UNAUTHENTICATED / PERMISSION_DENIED — token missing or wrong
    UNAVAILABLE = "unavailable"  # gateway down / not connected / connection refused
    OTHER = "other"  # some other RPC failure — still loud, still clean


def _grpc_status_name(exc: BaseException) -> str | None:
    """Return the gRPC ``StatusCode`` name from ``exc`` or its cause chain.

    The dashboard RPC facades wrap the underlying ``grpc.RpcError`` and re-raise
    a typed error ``from exc``, so the status code lives on ``__cause__`` /
    ``__context__``. Walks that chain (cycle-guarded) and returns the first
    ``code().name`` it finds, or ``None``.
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        code_attr = getattr(cur, "code", None)
        if callable(code_attr):
            try:
                code = code_attr()
            except Exception:  # pragma: no cover - defensive: exotic RpcError fakes
                code = None
            name = getattr(code, "name", None)
            if name:
                return str(name)
        cur = cur.__cause__ or cur.__context__
    return None


def classify_gateway_error(exc: BaseException) -> GatewayErrorKind:
    """Classify a gateway/RPC failure for user-facing rendering."""
    name = _grpc_status_name(exc)
    if name in ("UNAUTHENTICATED", "PERMISSION_DENIED"):
        return GatewayErrorKind.AUTH
    if name in ("UNAVAILABLE", "DEADLINE_EXCEEDED"):
        return GatewayErrorKind.UNAVAILABLE

    # Text fallback — a call site that didn't chain the cause still classifies.
    text = str(exc).lower()
    if "unauthenticated" in text or "authentication token" in text or "permission denied" in text:
        return GatewayErrorKind.AUTH
    if (
        "unavailable" in text
        or "failed to connect" in text
        or "connection refused" in text
        or "not connected" in text
        or "not healthy" in text
    ):
        return GatewayErrorKind.UNAVAILABLE
    return GatewayErrorKind.OTHER


def _clean_message(kind: GatewayErrorKind, *, context: str) -> str:
    """Human, actionable one-liner per failure kind — no raw gRPC text."""
    if kind is GatewayErrorKind.AUTH:
        return (
            f"Cannot authenticate to the gateway — {context} below may be incomplete or missing. "
            "The dashboard could not present a valid session token. If you launched it separately, "
            "start it from the strategy folder (or the same terminal) so it can read the managed "
            "gateway's session token, or export ALMANAK_GATEWAY_AUTH_TOKEN."
        )
    if kind is GatewayErrorKind.UNAVAILABLE:
        return (
            f"Gateway unreachable — {context} cannot be loaded right now. "
            "The strategy's gateway may be starting, stopped, or on a different port."
        )
    return f"Gateway error while loading {context}. See the dashboard logs for detail."


def dashboard_debug_enabled() -> bool:
    """True when the operator opted into raw error detail in the UI.

    Honours the ``show_debug`` Streamlit session flag (the existing app-level
    toggle) and the ``ALMANAK_DASHBOARD_DEBUG`` env var (for the custom /
    hosted-parity subprocess, which may not carry the session flag) — the env
    read is routed through the typed :class:`FrameworkConfig` so it stays behind
    the config-service boundary. Default off: user-facing panes never leak a raw
    traceback unless explicitly asked.
    """
    if framework_config_from_env().dashboard_debug_enabled:
        return True
    try:
        return bool(st.session_state.get("show_debug", False))
    except Exception:  # pragma: no cover - no active streamlit script context
        return False


def render_gateway_error(exc: BaseException, *, context: str, raw: Any = None) -> GatewayErrorKind:
    """Render a LOUD, clean gateway-error banner and return its kind.

    Use anywhere a gateway/RPC read fails while the pane would otherwise show an
    empty / zeroed state. AUTH and UNAVAILABLE render ``st.error`` (red — an
    auth failure during live exposure must never look benign); OTHER renders
    ``st.warning``. The raw exception text is logged and only shown in the UI
    behind the debug expander (:func:`dashboard_debug_enabled`).

    ``raw`` is an optional extra detail string (e.g. the wrapped message) folded
    into the debug expander alongside the traceback.
    """
    kind = classify_gateway_error(exc)
    logger.warning("Gateway error (%s) loading %s: %s", kind.value, context, exc, exc_info=True)

    message = _clean_message(kind, context=context)
    if kind is GatewayErrorKind.OTHER:
        st.warning(message, icon="⚠️")
    else:
        st.error(message, icon="🚫")

    if dashboard_debug_enabled():
        with st.expander("Error detail (debug)"):
            if raw is not None:
                st.code(str(raw))
            st.code("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    return kind
