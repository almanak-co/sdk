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
