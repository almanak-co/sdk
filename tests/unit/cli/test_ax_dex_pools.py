"""`ax dex-pools` outcome contract (VIB-6599).

The contract mirrors `ax perp-market` (ALM-3179) for one reason: an EMPTY venue
list is evidence of absence ONLY when the provider returned a complete view. A
truncated view, or a provider that failed, means "could not tell". Collapsing
those into one exit code is how a planner reports "this token has no tradeable
venue" when it simply could not see one — the same class of confidently-wrong
call that ALM-3179 hit from the other direction.
"""

from __future__ import annotations

import pytest

from almanak.framework.agent_tools.errors import AgentErrorCode
from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus
from almanak.framework.cli.ax import (
    _DEX_POOLS_EXIT_INVALID_INPUT,
    _DEX_POOLS_EXIT_NOT_FOUND,
    _DEX_POOLS_EXIT_OK,
    _DEX_POOLS_EXIT_UNVERIFIED,
    _dex_pools_exit_code,
)


def _ok(pools: list[dict], *, complete: bool, unfiltered_count: int | None = None) -> ToolResponse:
    return ToolResponse(
        status=ToolResponseStatus.SUCCESS,
        data={
            "pools": pools,
            "complete": complete,
            "count": len(pools),
            "unfiltered_count": len(pools) if unfiltered_count is None else unfiltered_count,
        },
    )


def _error(code: AgentErrorCode) -> ToolResponse:
    # Build via the production helper so the payload's category is whatever the
    # real executor would emit — a hand-set category would let this test pass
    # against a shape the CLI never actually receives.
    from almanak.framework.agent_tools.executor import _error_payload

    return ToolResponse(status=ToolResponseStatus.ERROR, error=_error_payload(code, "boom"))


_A_POOL = [{"pool_address": "0xabc", "dex_id": "uniswap_v3", "reserve_usd": "1000"}]


def test_venues_found_exits_ok():
    assert _dex_pools_exit_code(_ok(_A_POOL, complete=True)) == _DEX_POOLS_EXIT_OK


def test_venues_found_on_a_truncated_view_still_exits_ok():
    """`complete=False` bounds "is this the DEEPEST venue", not "does one exist".

    Existence is answered the moment a pool comes back, so a truncated view is
    not an unverified OUTCOME — the depth caveat rides the payload and the
    table instead.
    """
    assert _dex_pools_exit_code(_ok(_A_POOL, complete=False)) == _DEX_POOLS_EXIT_OK


def test_empty_and_complete_is_authoritative_absence():
    assert _dex_pools_exit_code(_ok([], complete=True)) == _DEX_POOLS_EXIT_NOT_FOUND


def test_empty_and_truncated_is_unverified_not_absence():
    """THE case the contract exists for: these two differ by one flag and mean
    opposite things. If this ever returns NOT_FOUND, a caller will report a
    token as untradeable on the strength of a view that never saw the venues."""
    assert _dex_pools_exit_code(_ok([], complete=False)) == _DEX_POOLS_EXIT_UNVERIFIED
    assert _dex_pools_exit_code(_ok([], complete=False)) != _DEX_POOLS_EXIT_NOT_FOUND


def test_a_floor_that_excludes_every_venue_is_not_absence():
    """CodeRabbit finding, made WORSE by the exit-code commit that followed the
    review. `--min-liquidity 1e9` filters every venue out, leaving an empty list
    with complete=True — which classified as exit 1, AUTHORITATIVE ABSENCE. A
    planner would then report a perfectly tradeable token as having no venue
    because the operator set a strict floor. Absence is about the unfiltered
    set; a caller-side filter can never establish it."""
    filtered_out = _ok([], complete=True, unfiltered_count=6)

    assert _dex_pools_exit_code(filtered_out) == _DEX_POOLS_EXIT_OK
    assert _dex_pools_exit_code(filtered_out) != _DEX_POOLS_EXIT_NOT_FOUND


def test_absence_still_reported_when_nothing_was_filtered():
    """The negative control for the test above: with an unfiltered set that is
    genuinely empty, exit 1 must still fire — otherwise the fix above would
    have silently deleted the authoritative-absence signal altogether."""
    assert _dex_pools_exit_code(_ok([], complete=True, unfiltered_count=0)) == _DEX_POOLS_EXIT_NOT_FOUND


def test_validation_error_is_invalid_input():
    assert _dex_pools_exit_code(_error(AgentErrorCode.VALIDATION_ERROR)) == _DEX_POOLS_EXIT_INVALID_INPUT


@pytest.mark.parametrize(
    "code",
    [AgentErrorCode.UPSTREAM_UNAVAILABLE, AgentErrorCode.TIMEOUT, AgentErrorCode.GATEWAY_ERROR],
)
def test_provider_failures_are_unverified_never_absence(code: AgentErrorCode):
    """A dead provider must never be reportable as "no venues exist"."""
    assert _dex_pools_exit_code(_error(code)) == _DEX_POOLS_EXIT_UNVERIFIED


def test_exit_codes_match_the_perp_market_command_contract():
    """Same numbering as `ax perp-market` so the two venue-resolution commands
    can be handled by one caller-side branch rather than two dialects."""
    assert (
        _DEX_POOLS_EXIT_OK,
        _DEX_POOLS_EXIT_NOT_FOUND,
        _DEX_POOLS_EXIT_INVALID_INPUT,
        _DEX_POOLS_EXIT_UNVERIFIED,
    ) == (0, 1, 2, 4)
