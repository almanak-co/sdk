"""Shared RPC auth/entitlement error classification (VIB-5736).

A keyed RPC provider (Alchemy / Tenderly / …) that is *reachable* but returns an
HTTP **401/403** for a chain returns the SAME deterministic error on every call.
The classic trigger is an Alchemy app that does not have the chain enabled — first
hit on Robinhood (4663): ``ROBINHOOD_MAINNET is not enabled for this app`` → HTTP
403 on every balance read. Retrying it burns the transient-retry budget on every
runner iteration and never surfaces *why*, producing an opaque crash-loop.

This classifier is the auth/entitlement counterpart to :mod:`indexer_lag`
(which says "retry this — it will self-heal"); this one says **"stop retrying,
this will not self-heal — the operator must fix the config."** Balance-read
retry loops use it to fail FAST with an actionable message instead of retrying a
deterministic 403.

Deliberately narrow — only 401/403 (auth/entitlement). Transient auth-adjacent
statuses are NOT entitlement errors and keep the existing transient-retry path:

* **429** (rate limit) — a keyed provider throttling; retrying with backoff is
  correct.
* **5xx** — provider-side outage; transient.

Status detection is primary (an aiohttp ``ClientResponseError`` from web3's async
HTTPProvider carries ``.status``; a ``requests`` ``HTTPError`` carries
``.response.status_code``); message markers are a secondary fallback for
providers that only put the entitlement text in the error string.
"""

from __future__ import annotations

import re

# Strong, narrow substrings that name an auth/entitlement rejection in the error
# text. Matched case-insensitively. Kept disjoint from transient markers (rate
# limit / 5xx) and from :mod:`indexer_lag` ("unknown block", …) so the two
# classifiers never both fire on one message.
_ENTITLEMENT_MESSAGE_MARKERS: frozenset[str] = frozenset(
    {
        "not enabled for this app",  # Alchemy — chain not enabled on the app (the VIB-5736 trigger)
        "invalid api key",  # keyed-provider bad/rotated key
        "must be authenticated",  # some providers
    }
)

# A bare "forbidden" / "unauthorized" is unsafe: contracts and JSON-RPC
# methods routinely use those words in application-level errors transported via
# HTTP 200. Accept reason phrases only when paired with their HTTP status.
_EXPLICIT_HTTP_AUTH_RE = re.compile(
    r"(?:\b401\b.{0,40}\bunauthori[sz]ed\b"
    r"|\b403\b.{0,40}\bforbidden\b"
    r"|\b(?:http\s+status|status\s+code)\s*[:=]?\s*(?:401|403)\b)",
    re.IGNORECASE,
)

# Transient markers that must WIN over an entitlement marker if both appear —
# a 429 body can mention "forbidden"; rate limits are transient, not entitlement.
_TRANSIENT_MESSAGE_MARKERS: frozenset[str] = frozenset(
    {
        "429",
        "too many requests",
        "rate limit",
        "rate-limit",
    }
)

# Bound the exception-chain walk so a pathological __cause__/__context__ cycle
# can never spin (defensive; real chains are 1–3 deep).
_MAX_CHAIN_DEPTH = 8


def _iter_exception_chain(exc: BaseException | None):
    """Yield ``exc`` and its linked causes/contexts, depth- and cycle-bounded.

    Walks ``__cause__`` and ``__context__`` (the standard exception links) plus
    ``original_error`` (this gateway's own wrapper attribute, e.g. on
    ``RPCError``), so a 403 wrapped several layers deep is still classifiable.
    """
    seen: set[int] = set()
    stack: list[tuple[BaseException, int]] = []
    if exc is not None:
        stack.append((exc, 0))
    while stack:
        current, depth = stack.pop()
        if id(current) in seen or depth > _MAX_CHAIN_DEPTH:
            continue
        seen.add(id(current))
        yield current
        for linked in (
            current.__cause__,
            current.__context__,
            getattr(current, "original_error", None),
        ):
            if isinstance(linked, BaseException):
                stack.append((linked, depth + 1))


def _status_from_exception(exc: BaseException) -> int | None:
    """Best-effort HTTP status from a single exception object (not the chain).

    Handles the two shapes RPC transports raise:
    * ``aiohttp.ClientResponseError`` → ``.status`` (web3 async HTTPProvider)
    * ``requests.HTTPError`` → ``.response.status_code`` (sync HTTPProvider)

    Deliberately does NOT read ``.code``: web3 JSON-RPC errors put the (negative)
    JSON-RPC error code there, which is not an HTTP status.
    """
    response = getattr(exc, "response", None)
    if response is not None:
        code = getattr(response, "status_code", None)
        if isinstance(code, int) and 100 <= code <= 599:
            return code
        code = getattr(response, "status", None)
        if isinstance(code, int) and 100 <= code <= 599:
            return code
    status = getattr(exc, "status", None)
    if isinstance(status, int) and 100 <= status <= 599:
        return status
    return None


def rpc_http_status(exc: BaseException | None) -> int | None:
    """Return the first HTTP status found walking ``exc`` and its causes.

    ``None`` when no HTTP status is discoverable (e.g. a timeout, a wire-level
    connection error, or a JSON-RPC error object with no HTTP envelope).
    """
    for current in _iter_exception_chain(exc):
        status = _status_from_exception(current)
        if status is not None:
            return status
    return None


def is_rpc_entitlement_error(exc: BaseException | None) -> bool:
    """True if ``exc`` is a deterministic auth/entitlement rejection (401/403).

    Fail-fast signal: such an error will return identically on every retry, so
    the caller should stop retrying and surface an actionable message. Returns
    ``False`` for transient auth-adjacent conditions (429 rate limit, 5xx) and
    for everything non-auth (timeouts, reverts, indexer lag, malformed params) —
    those keep failing the normal way.
    """
    if exc is None:
        return False

    status = rpc_http_status(exc)
    if status in (401, 403):
        return True
    if status is not None:
        # A structured transport status is authoritative. Do not reinterpret a
        # 4xx/5xx body containing an auth-like word as a different HTTP status.
        return False

    # No decisive HTTP status: fall back to message markers, but let a transient
    # marker (rate limit) veto so a throttling body that also says "forbidden"
    # stays on the retry path.
    text = " ".join(str(current).lower() for current in _iter_exception_chain(exc))
    if any(marker in text for marker in _TRANSIENT_MESSAGE_MARKERS):
        return False
    return _EXPLICIT_HTTP_AUTH_RE.search(text) is not None or any(
        marker in text for marker in _ENTITLEMENT_MESSAGE_MARKERS
    )


def entitlement_error_message(chain: str, method: str) -> str:
    """Build an actionable operator-facing message for an entitlement failure.

    Names the failing chain + RPC method, explains that retrying will not help,
    and gives the exact fix: enable the chain on the provider, or set the
    ``ALMANAK_{CHAIN}_RPC_URL`` override (with the descriptor's public RPC as a
    ready-to-paste value when one is registered).
    """
    chain_upper = chain.upper()
    # Deployment mode is resolved through the one permitted reader. Import on
    # the exceptional path to keep this low-level classifier import-cheap.
    from almanak.framework.deployment import is_hosted

    if is_hosted():
        return (
            f"RPC provider returned an auth/entitlement error (HTTP 401/403) for chain "
            f"'{chain}' on {method}. The hosted gateway's provisioned provider is reachable "
            f"but not authorized for this chain. This will NOT resolve by retrying. "
            f"Platform operator action required: enable '{chain}' on the provisioned RPC "
            f"application or repair the deployment's approved RPC configuration, then restart "
            f"the gateway. Do not substitute an unapproved public endpoint."
        )

    # Lazy import: rpc_provider builds PUBLIC_RPC_URLS from the ChainRegistry at
    # import time; importing it here (call-time, only on the error path) avoids a
    # module-load cycle and keeps the classifier import-cheap.
    public_hint = ""
    try:
        from almanak.gateway.utils.rpc_provider import PUBLIC_RPC_URLS

        public_rpc = PUBLIC_RPC_URLS.get(chain.lower())
        if public_rpc:
            public_hint = f" (e.g. the chain's public RPC {public_rpc})"
    except Exception:  # pragma: no cover - hint is best-effort, never blocks the error
        public_hint = ""

    return (
        f"RPC provider returned an auth/entitlement error (HTTP 401/403) for chain "
        f"'{chain}' on {method}. The keyed provider is reachable but not authorized "
        f"for this chain — most commonly the Alchemy app does not have '{chain}' "
        f"enabled. This will NOT resolve by retrying. Fix: enable the chain on your "
        f"RPC provider, or set ALMANAK_{chain_upper}_RPC_URL (or {chain_upper}_RPC_URL) "
        f"to a working endpoint{public_hint} and relaunch."
    )


__all__ = [
    "rpc_http_status",
    "is_rpc_entitlement_error",
    "entitlement_error_message",
]
