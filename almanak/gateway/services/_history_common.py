"""Shared validation + normalization helpers for gateway history services.

Consumed by ``PoolHistoryService`` (VIB-4728 / POOL-3 / VIB-4751) and the
upcoming ``RateHistoryService`` (VIB-4747 / RATES-2). Both services need
identical guards because both proxy time-series data through the gateway
boundary: chain-aware EVM normalization, address-format validation,
protocol-aware soft-cap loading.

Pool-specific tables (protocol allowlist, supported ``(chain, protocol)``
pairs, the dispatcher's ``eligible_providers`` map) live in
``pool_history_service.py``. Rates equivalents will land in ``rate_history_service.py``.
The split keeps this module strictly chain-agnostic + protocol-agnostic;
extending it for a new feature should NOT require touching pool/rate-specific
data.

Hard rules enforced here (from the umbrella UAT card
``docs/internal/uat-cards/VIB-4728.md`` and inherited audit rows on PR #2389):

1. Chain-aware normalize (inherited rows #3 + #13): EVM addresses
   strip-then-lowercase; Solana base58 preserves case (lowercasing
   yields a different address on a case-sensitive encoding).
2. Validate address syntax BEFORE any URL / query interpolation
   (inherited row #4): the regex check rejects ``0x`` + path-traversal
   substrings before any URL formatter sees them.
3. Empty / whitespace-only fields are NOT accepted as "use the default"
   — they are ``INVALID_ARGUMENT``. The framework boundary maps this to
   ``DataSourceUnavailable`` per inherited row #10.
4. Soft cap is a HINT, not a hard reject (POOL-6 truncates to it).
   ``get_soft_cap_seconds`` exists for POOL-6 to read; this module
   does NOT itself reject for soft-cap. Hard cap (mechanically
   incoherent on gRPC since ``INVALID_ARGUMENT`` drops the response
   message and ``next_start_ts`` would be lost) is documented in the
   UAT card §"Soft-cap vs hard-cap behavior" and is NOT exposed by
   default in VIB-4728.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import grpc

from almanak.gateway.proto import gateway_pb2

# -----------------------------------------------------------------------------
# Chain-aware address regex
# -----------------------------------------------------------------------------
#
# EVM is case-insensitive hex; Solana is base58 (case-sensitive — lowercasing
# yields a different address). Mirrors the regexes in
# ``pool_analytics_service.py:118-124`` so the analytics service and the
# history services agree on what "valid address" means.

_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

# Solana base58 alphabet excludes 0, O, I, l. Pool / mint addresses are
# 32-44 chars on Solana.
_SOLANA_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


# -----------------------------------------------------------------------------
# Supported chains for NORMALIZATION purposes (broader than provider coverage)
# -----------------------------------------------------------------------------
#
# This set is the union of every chain the gateway might be asked to
# normalize a pool address FOR. It is INTENTIONALLY broader than the
# (chain, protocol) "supported pair" table in ``pool_history_service.py``:
# the validator needs to normalize a Solana address correctly even though
# Solana is not yet wired for pool history, so that the eventual
# "unsupported (chain, protocol) pair" error message can be meaningful
# instead of the validator earlier crashing on ``.lower()`` of the wrong
# string. The (chain, protocol) compatibility check is the gate; this
# set is just "do we know how to normalize this chain's addresses?"

_SOLANA_CHAINS: frozenset[str] = frozenset({"solana"})

SUPPORTED_NORMALIZATION_CHAINS: frozenset[str] = frozenset(
    {
        "ethereum",
        "arbitrum",
        "base",
        "optimism",
        "polygon",
        "avalanche",
        "bsc",
        "sonic",
    }
    | _SOLANA_CHAINS
)


def is_solana_chain(chain: str) -> bool:
    """Return True when ``chain`` is Solana (base58, case-sensitive)."""
    return chain in _SOLANA_CHAINS


def normalize_pool_address(address: str, chain: str) -> str:
    """Chain-aware address normalization (inherited rows #3 + #13).

    EVM addresses are case-insensitive → ``strip().lower()``.
    Solana base58 addresses are case-sensitive → ``strip()`` ONLY.

    Mirrors ``almanak.framework.data.tokens.resolver._normalize_address_for_chain``
    and ``pool_analytics_service._normalize_pool_address``. Centralizing
    here avoids three copies of the same rule drifting.
    """
    address = address.strip()
    if is_solana_chain(chain):
        return address
    return address.lower()


def validate_pool_address_syntax(address: str, chain: str) -> bool:
    """Return True when ``address`` is a syntactically valid pool address
    for ``chain`` (inherited row #4).

    Rejecting malformed input here is a SECURITY guard — the EVM regex
    means a future provider that embeds the address in a URL template
    (e.g. ``GET /api/pools/{address}/ohlcv``) cannot be tricked into
    carrying an attacker-supplied path / query segment. Validate FIRST,
    interpolate SECOND.
    """
    if is_solana_chain(chain):
        return bool(_SOLANA_BASE58_RE.match(address))
    return bool(_EVM_ADDRESS_RE.match(address))


# -----------------------------------------------------------------------------
# Resolution + soft-cap helpers
# -----------------------------------------------------------------------------

_RESOLUTION_SECONDS: dict[int, int] = {
    gateway_pb2.Resolution.RESOLUTION_1H: 3600,
    gateway_pb2.Resolution.RESOLUTION_4H: 14400,
    gateway_pb2.Resolution.RESOLUTION_1D: 86400,
}


def resolution_to_seconds(resolution: int) -> int:
    """Return the bar duration in seconds for a valid ``Resolution`` enum.

    Raises ``ValueError`` for ``RESOLUTION_UNSPECIFIED`` (the validator
    rejects this case earlier; this raise is the defense-in-depth path).
    """
    try:
        return _RESOLUTION_SECONDS[resolution]
    except KeyError as exc:
        raise ValueError(f"unsupported resolution: {resolution}") from exc


# Soft caps live on ``GatewaySettings.pool_history_max_days_{1h,4h,1d}``
# (boundary rule: gateway env reads route through pydantic settings, not
# direct ``os.environ`` calls — see ``scripts/ci/check_config_boundary.py``).
# Defaults: 90d at 1h is the spike R2 sizing for ~432 KB payloads; 180d at
# 4h is symmetric; 730d at 1d covers a 2-year backtest. Settings validate
# non-positive overrides back to the default so a typo can't silently
# disable the cap.

_SOFT_CAP_SETTINGS_ATTRS: dict[int, str] = {
    gateway_pb2.Resolution.RESOLUTION_1H: "pool_history_max_days_1h",
    gateway_pb2.Resolution.RESOLUTION_4H: "pool_history_max_days_4h",
    gateway_pb2.Resolution.RESOLUTION_1D: "pool_history_max_days_1d",
}


def get_soft_cap_seconds(settings: object, resolution: int) -> int:
    """Return the configured soft cap (in seconds) for ``resolution``.

    POOL-3 exposes this; POOL-6 (VIB-4754) reads it to decide truncation.
    A handler that gets a 200d-1h request reads ``get_soft_cap_seconds(
    settings, 1h) == 90 * 86400`` and truncates the response window,
    returning ``CAP_EXCEEDED`` with ``next_start_ts`` so the caller can
    re-chunk.

    The validator does NOT call this function — soft cap is a handler
    concern, not a validation concern (UAT card §"Soft-cap vs hard-cap").

    ``settings`` is a :class:`almanak.gateway.core.settings.GatewaySettings`
    instance (typed at the caller; not imported here to keep the helper
    chain-agnostic). The settings object owns env binding and
    non-positive-fallback semantics via ``_validate_pool_history_max_days``.
    """
    attr = _SOFT_CAP_SETTINGS_ATTRS.get(resolution)
    if attr is None:
        raise ValueError(f"unsupported resolution: {resolution}")
    days = getattr(settings, attr)
    return int(days) * 86400


# -----------------------------------------------------------------------------
# Validation result type
# -----------------------------------------------------------------------------

ValidationFailure = tuple[grpc.StatusCode, str]


def invalid_argument(message: str) -> ValidationFailure:
    """Convenience: build an ``(INVALID_ARGUMENT, message)`` tuple.

    Centralizes the gRPC code choice so a future migration to a different
    code (e.g. ``FAILED_PRECONDITION``) is a one-line change here, not a
    grep-and-replace through every validator.
    """
    return (grpc.StatusCode.INVALID_ARGUMENT, message)


# -----------------------------------------------------------------------------
# Future-time tolerance for end_ts (defense against clock skew)
# -----------------------------------------------------------------------------
# Allow ``end_ts`` to be up to this many seconds in the future without
# rejecting (NTP skew, ``MarketSnapshot.timestamp`` rounding). 5 minutes
# matches VIB-4727's pattern.
END_TS_FUTURE_TOLERANCE_SECONDS: int = 300


# -----------------------------------------------------------------------------
# Truncation + finality (POOL-6 / VIB-4754)
# -----------------------------------------------------------------------------
#
# These pure helpers carry the POOL-6 policy that the handler applies AFTER a
# provider returns a successful (non-empty, ascending) window. Kept here —
# chain- and protocol-agnostic — so ``RateHistoryService`` (VIB-4747) reuses
# them. The wire contract is locked in ``gateway.proto`` (TruncationReason
# enum, ``next_start_ts`` forward cursor) and the umbrella UAT card
# ``docs/internal/uat-cards/VIB-4728.md`` §D3.F7 / §D3.F9.


@dataclass(frozen=True)
class TruncationOutcome:
    """Classification of a successful provider response for truncation.

    * ``kept`` — the ascending slice actually returned to the caller. Equal to
      the input unless the page-cap fired, in which case it is the OLDEST
      ``page_cap_rows`` rows (forward-cursor: serve oldest-first, re-chunk
      forward for the rest).
    * ``reason`` — a ``gateway_pb2.TruncationReason`` enum value.
    * ``next_start_ts`` — the **inclusive** forward re-chunk cursor: ``> 0`` for
      ``CAP_EXCEEDED`` / ``PROVIDER_PAGE_CAP`` (caller re-issues with
      ``start_ts = next_start_ts``, ``end_ts`` unchanged), ``0`` for
      ``PROVIDER_RETENTION`` (do-not-re-chunk sentinel) and
      ``TRUNCATION_REASON_UNSPECIFIED``.
    """

    kept: list[gateway_pb2.PoolSnapshot]
    reason: gateway_pb2.TruncationReason.ValueType
    next_start_ts: int


def classify_truncation(
    *,
    snapshots: list[gateway_pb2.PoolSnapshot],
    eff_start_ts: int,
    eff_end_ts: int,
    clamped: bool,
    resolution_seconds: int,
    page_cap_rows: int,
) -> TruncationOutcome:
    """Classify a successful (non-empty, ascending) provider response.

    ``eff_start_ts`` / ``eff_end_ts`` describe the half-open ``[start, end)``
    window actually dispatched — i.e. AFTER the servicer's soft-cap clamp.
    ``clamped`` is True iff that clamp shortened the caller's original window
    (the soft cap was exceeded). ``page_cap_rows`` is the serving provider's
    configured response row ceiling.

    Precedence (locked in UAT card §D3.F7, design decision D-2): the **binding
    forward limit** wins among the re-chunkable reasons, and the
    do-not-re-chunk ``PROVIDER_RETENTION`` is strictly lowest so it can never
    pre-empt a still-advancing forward walk:

    1. **PROVIDER_PAGE_CAP** — the provider returned MORE rows than its ceiling,
       so the ceiling is the tighter forward boundary. Serve the oldest
       ``page_cap_rows`` rows; ``next_start_ts = kept[-1].timestamp +
       resolution_seconds``. (Takes precedence over CAP_EXCEEDED only when it is
       the tighter limit — otherwise ``n <= page_cap_rows`` and this branch is
       not taken.)
    2. **CAP_EXCEEDED** — the window was soft-cap clamped and the provider filled
       it (no page-cap). ``next_start_ts = eff_end_ts`` (the clamped window's
       exclusive end — window-based, not data-based, so a sparse trailing gap
       doesn't strand the caller before the cap boundary).
    3. **PROVIDER_RETENTION** — neither clamped nor page-capped, but the oldest
       returned (aligned) row is more than one bar after ``eff_start_ts``: the
       provider has no older in-window data and never will. ``next_start_ts =
       0`` (stop). In the forward-cursor model this is informational — its
       sentinel equals UNSPECIFIED's — so a mis-threshold is a mislabel, never a
       control-flow bug.
    4. **TRUNCATION_REASON_UNSPECIFIED** — full requested window served.

    Empty ``snapshots`` (which the handler never passes — empty upstream is a
    not-found failure) is classified UNSPECIFIED with an empty ``kept``.
    """
    tr = gateway_pb2.TruncationReason
    if not snapshots:
        return TruncationOutcome(kept=snapshots, reason=tr.TRUNCATION_REASON_UNSPECIFIED, next_start_ts=0)

    if len(snapshots) > page_cap_rows:
        kept = snapshots[:page_cap_rows]
        next_start_ts = int(kept[-1].timestamp) + resolution_seconds
        return TruncationOutcome(kept=kept, reason=tr.PROVIDER_PAGE_CAP, next_start_ts=next_start_ts)

    if clamped:
        return TruncationOutcome(kept=snapshots, reason=tr.CAP_EXCEEDED, next_start_ts=int(eff_end_ts))

    oldest = int(snapshots[0].timestamp)
    if oldest - int(eff_start_ts) > resolution_seconds:
        return TruncationOutcome(kept=snapshots, reason=tr.PROVIDER_RETENTION, next_start_ts=0)

    return TruncationOutcome(kept=snapshots, reason=tr.TRUNCATION_REASON_UNSPECIFIED, next_start_ts=0)


def compute_finalized_only(*, newest_ts: int, now_seconds: int, cutoff_seconds: int) -> bool:
    """Return True iff every row in the series is finalized.

    A series is finalized iff its NEWEST row is older than the serving
    provider's finality cutoff: ``(now - newest_ts) > cutoff``. The newest row
    is the binding case — if it is finalized, every earlier row is too. A False
    result means the trailing bar is still provisional (within the cutoff) and
    the cache entry must be written under the short-TTL ``provisional`` band so
    a later revision is re-fetched (or re-promoted once it ages past the
    cutoff — see ``_history_cache``).
    """
    return (int(now_seconds) - int(newest_ts)) > int(cutoff_seconds)
