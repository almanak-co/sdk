"""Shared error-message classification for intent retry semantics.

Single source of truth for the keyword taxonomy used by
``IntentStateMachine._categorize_error`` (VIB-2866 / VIB-1215) and reused by
the teardown error taxonomy (``almanak.framework.teardown.error_taxonomy``) so
the two do not drift into parallel keyword lists.

This module is intentionally dependency-free: it maps an error string to a
category string and nothing else. Behaviour is pinned by
``tests/unit/teardown/test_error_taxonomy.py`` (parity test) plus the existing
state-machine retry tests.
"""

from __future__ import annotations

# VIB-5374: pre-submit feasibility preflight (RC-2) stable error prefixes.
# A connector ``preflight`` hook (BaseProtocolCompiler.preflight) FAILs a
# structurally-doomed intent at compile time with one of these prefixes so the
# state machine routes it to HOLD (fail-fast, no retry storm) rather than paying
# gas on an inevitable on-chain revert. Retrying with the same inputs reproduces
# the same doomed state (expired market, native fee > balance, borrow > LTV
# capacity), so each is terminal.
#
# These are matched BEFORE the generic ``revert`` short-circuit below because the
# human-readable ``reason`` appended to the prefix can legitimately contain the
# word "revert" (e.g. Euler's "the EVC borrow would revert"). The prefix is the
# authoritative classification signal — letting an explanatory "revert" in the
# reason downgrade a permanent INFEASIBLE verdict to a transient REVERT (and back
# into the retry budget) defeats the seam. Checking the prefix first makes the
# classification robust to reason wording, so connector authors need not contort
# their messages to avoid a substring (cf. host-unreachable handling below, which
# uses the same check-before-generic precedent).
_PREFLIGHT_INFEASIBLE_PREFIXES = (
    "pendle_market_expired",
    "gmx_insufficient_native_fee",
    "stargate_insufficient_native_fee",
    "euler_borrow_infeasible",
)


def categorize_error(error_message: str) -> str | None:
    """Categorize an error message into a known error type.

    Args:
        error_message: The error message to categorize.

    Returns:
        Error type string or None if unknown.
    """
    error_lower = error_message.lower()

    # Specific protocol-level capacity revert tokens that always repeat
    # for the same (collateral, borrow_amount). Check BEFORE the generic
    # `revert` keyword so the runner classifies them as
    # COMPILATION_PERMANENT and skips the retry storm.
    #
    # Compound V2 / BENQI: tokens like ``BORROW_LIMIT_REACHED`` and
    # ``INSUFFICIENT_LIQUIDITY`` appear in raw Comptroller revert
    # messages. The bare keyword ``insufficient_liquidity`` is generic
    # enough to surface in non-Compound paths (e.g. router-level swap
    # failures), so we additionally require ``comptroller`` context to
    # avoid false positives.
    #
    # Aave V3: ``COLLATERAL_CANNOT_COVER_NEW_BORROW`` (code 35) is the
    # raw revert when the borrow exceeds available borrows. It's
    # protocol-specific and unambiguous, so it does not need extra
    # context.
    compound_fork_permanent = (
        "borrow_limit_reached",
        "insufficient_liquidity",
    )
    if "comptroller" in error_lower and any(kw in error_lower for kw in compound_fork_permanent):
        return "COMPILATION_PERMANENT"
    if "collateral_cannot_cover_new_borrow" in error_lower:
        return "COMPILATION_PERMANENT"

    # VIB-5374: pre-submit feasibility preflight prefixes. Checked BEFORE the
    # generic ``revert`` short-circuit so an explanatory "revert" in the reason
    # cannot downgrade a permanent INFEASIBLE verdict to a transient REVERT.
    # See _PREFLIGHT_INFEASIBLE_PREFIXES for the full rationale.
    if any(kw in error_lower for kw in _PREFLIGHT_INFEASIBLE_PREFIXES):
        return "COMPILATION_PERMANENT"

    # Common error categories
    if "insufficient" in error_lower and ("funds" in error_lower or "balance" in error_lower):
        return "INSUFFICIENT_FUNDS"
    if "gas" in error_lower and ("limit" in error_lower or "price" in error_lower):
        return "GAS_ERROR"
    if "nonce" in error_lower:
        return "NONCE_ERROR"
    if "timeout" in error_lower or "timed out" in error_lower:
        return "TIMEOUT"
    if "revert" in error_lower:
        return "REVERT"
    if "slippage" in error_lower:
        return "SLIPPAGE"
    if "rate limit" in error_lower or "ratelimit" in error_lower:
        return "RATE_LIMIT"

    # VIB-1215: host-unreachable patterns that always repeat on retry.
    # "Cannot connect to host" / "Connection refused" mean the RPC endpoint
    # is not listening at the configured address — typically a crashed
    # Anvil fork or a misconfigured gateway URL. Burning ~15s of
    # exponential backoff (1+2+4 + initial) before surfacing the failure
    # is pure overhead; the connection cannot succeed without operator
    # intervention. Checked BEFORE the generic ``connection / network``
    # short-circuit so these specific patterns reach COMPILATION_PERMANENT
    # instead of being absorbed as transient NETWORK_ERROR. Conservative
    # by design: ``connection reset`` / ``connection timeout`` stay
    # transient because mid-request resets and overload timeouts can
    # legitimately recover.
    host_unreachable_permanent = (
        "cannot connect to host",
        "connection refused",
    )
    if any(kw in error_lower for kw in host_unreachable_permanent):
        return "COMPILATION_PERMANENT"
    if "connection" in error_lower or "network" in error_lower:
        return "NETWORK_ERROR"

    # Permanent configuration/support errors (non-retriable)
    # These indicate missing protocol support, unsupported chains, missing positions, etc.
    # Placed last so transient errors (timeout, revert, network) are caught first.
    # NOTE: error_lower is lowercased, so keywords here must be lowercase.
    # CLOB 4xx fatal rejections (VIB-3141) are listed below — see VIB-3140 for
    # the upstream dry-run error strings these mirror byte-for-byte (modulo case).
    permanent_keywords = (
        "not supported",
        "unsupported",
        "feature not available",
        "no existing position",
        "no position found",
        "no size specified",
        "unknown router",
        "unknown protocol",
        "unknown market",
        "no router configured",
        "no adapter found",
        "no connector found",
        "protocol not available",
        "missing configuration",
        "not deployed",
        # VIB-3141: CLOB 4xx fatal rejections (Polymarket and similar order books).
        # These are deterministic order validation errors — retrying with the same
        # inputs will fail identically. Transient 5xx errors stay retryable because
        # they never match these substrings.
        "breaks minimum tick size",
        "minimum order value",
        "invalid_order",
        "invalid_tick",
        "order_below_minimum",
        # VIB-3823: LpOpenZeroLiquidityError.ERROR_PREFIX (intent_errors.py)
        # surfaces this exact phrase from both the slot0-driven recompute
        # gate and the post-recompute pre-flight (compiler.py step 4c).
        # Retrying with the same range/amounts always reproduces the M0
        # revert, so classify as terminal to skip the retry storm.
        "mint zero liquidity",
        # VIB-3825: LendingBorrowNotEnabledError.ERROR_PREFIX
        # (intent_errors.py) surfaces from the BORROW compile-time
        # borrowable pre-flight in aave_helpers._check_lending_reserve_borrowable.
        # Retrying with the same asset always reproduces the on-chain
        # Aave V3 code 11 (BORROWING_NOT_ENABLED) revert — strategy must
        # HOLD until governance enables borrowing or pick a different
        # borrow asset.
        "lending borrow not enabled",
        # LendingBorrowExceedsCapacityError.ERROR_PREFIX (intent_errors.py)
        # surfaces from the BORROW compile-time capacity pre-flight in
        # aave_helpers._check_lending_borrow_capacity_{aave_v3,benqi}.
        # Retrying with the same (collateral, borrow_amount) reproduces the
        # on-chain revert (Aave V3 code 35
        # COLLATERAL_CANNOT_COVER_NEW_BORROW; Compound V2 / BENQI
        # Comptroller error code 4 INSUFFICIENT_LIQUIDITY) — strategy must
        # reduce the borrow amount or supply more collateral first.
        # Note: the specific Compound-fork tokens (BORROW_LIMIT_REACHED,
        # INSUFFICIENT_LIQUIDITY) are matched earlier (see
        # ``compound_fork_permanent`` above) so they don't get caught by
        # the generic ``revert`` short-circuit when the pre-flight has
        # failed open and the on-chain revert is what surfaces here.
        "lending borrow exceeds capacity",
        # VIB-3828: EnsoRouterRevertError.ERROR_PREFIX
        # (enso/exceptions.py) surfaces when the Enso router reverts with
        # a known custom-error selector (e.g. 0xef3dcb2f on Base —
        # leverage_loop_cross_chain). Selector-driven reverts repeat
        # deterministically for the same route, so classify as terminal —
        # the strategy must adjust the route, target token, or slippage
        # before retrying.
        "enso router rejected route with selector",
        # VIB-3818: OrcaTickArrayUninitializedError.ERROR_PREFIX
        # (orca/exceptions.py) surfaces from the LP_OPEN compile-time
        # tick-array pre-flight in orca/adapter.py. Retrying with the same
        # tick range always reproduces the on-chain 0xbbf revert
        # (InitializedTickArrayNotFound), so classify as terminal — the
        # strategy must widen the range or pick a different pool.
        "orca tick array(s) not initialized",
        # VIB-3817: Anchor error 101 (InstructionFallbackNotFound) — the
        # on-chain Solana program received an instruction whose 8-byte
        # discriminator doesn't match any handler in its IDL. Always a
        # version-skew issue (program upgraded, vendored discriminator
        # stale); retrying with the same instruction reproduces it. Two
        # variants of the literal show up in different RPC error paths:
        # the JSON-RPC ``custom: 101`` envelope and the program-log
        # ``InstructionFallbackNotFound`` line.
        "instructionfallbacknotfound",
        "drift instruction not recognized by on-chain program",
        # VIB-3817: defence-in-depth for the boot-time discriminator
        # self-check (`verify_drift_discriminators`). The check normally
        # halts strategy startup before any intent dispatch, but if the
        # SDK is ever lazily constructed inside a compile path, the typed
        # error message needs to short-circuit retries here too.
        "drift discriminator mismatch",
        # VIB-2866: deterministic market/pool configuration errors that
        # repeat identically on every retry. Burning ~11s of exponential
        # backoff before surfacing them is pure overhead. The bare token
        # ``no market`` is intentionally excluded because it can appear
        # in transient market-data-feed messages — the longer phrases
        # below are unambiguous.
        "market not found",
        "invalid market",
        "market does not exist",
        "pool not found",
        "invalid pool",
        # VIB-2866: Drift-specific deterministic validation strings.
        # ``DriftAdapter._get_position_size`` raises these when the
        # wallet has no Drift user PDA on-chain (PERP_CLOSE before any
        # PERP_OPEN) or the user PDA exists but has no open position
        # for the target market index. Both repeat identically on
        # every retry — the on-chain state can only change via a
        # successful PERP_OPEN, which the strategy author must
        # initiate explicitly.
        "no drift user account found",
        "no active position found for market index",
        # VIB-5374 pre-submit feasibility preflight prefixes are matched earlier
        # (see _PREFLIGHT_INFEASIBLE_PREFIXES, checked before the generic ``revert``
        # short-circuit) so they are intentionally NOT repeated here. The native-fee
        # prefixes also avoid ``funds``/``balance`` so they never get absorbed by
        # INSUFFICIENT_FUNDS above. The transient counterpart
        # (PreflightOutcome.UNAVAILABLE) carries no prefix, so it never matches and
        # correctly surfaces as an ``is_transient`` FAILED result.
    )
    if any(kw in error_lower for kw in permanent_keywords):
        return "COMPILATION_PERMANENT"

    return None
