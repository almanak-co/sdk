"""Time freshness must not depend on sequencer block production speed."""

from dataclasses import asdict, replace

import pytest

from almanak.connectors.uniswap_v4.freshness import (
    QuoteFreshnessError,
    QuoteFreshnessObservation,
    validate_quote_freshness,
)
from almanak.framework.venues.provider import GatewayBlockIdentity

HASH = "0x" + "a" * 64
OTHER_HASH = "0x" + "b" * 64
NOW = 1_800_000_000


def observation(*, blocks=25, elapsed=2, age=300):
    return QuoteFreshnessObservation(
        quote=GatewayBlockIdentity(100, HASH, NOW - elapsed),
        head=GatewayBlockIdentity(100 + blocks, HASH, NOW),
        expected_quote_hash=HASH,
        observed_at=NOW,
        max_age_seconds=age,
        max_clock_skew_seconds=30,
    )


@pytest.mark.parametrize("blocks", [1, 25, 100, 10_000])
def test_recent_quote_acceptance_is_independent_of_block_count(blocks):
    validate_quote_freshness(observation(blocks=blocks))


@pytest.mark.parametrize("blocks", [1, 3, 25, 10_000])
def test_stale_quote_is_rejected_independently_of_block_count(blocks):
    with pytest.raises(QuoteFreshnessError, match="quote_stale"):
        validate_quote_freshness(observation(blocks=blocks, elapsed=301))


@pytest.mark.parametrize("elapsed,accepted", [(299, True), (300, True), (301, False)])
def test_age_boundary(elapsed, accepted):
    value = observation(elapsed=elapsed)
    if accepted:
        validate_quote_freshness(value)
    else:
        with pytest.raises(QuoteFreshnessError, match="quote_stale"):
            validate_quote_freshness(value)


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ({"expected_quote_hash": OTHER_HASH}, "quote_reorganized"),
        ({"head": GatewayBlockIdentity(99, HASH, NOW)}, "head_behind_quote"),
        ({"head": GatewayBlockIdentity(100, OTHER_HASH, NOW - 2)}, "inconsistent_head"),
        ({"head": GatewayBlockIdentity(125, HASH, NOW - 3)}, "timestamp_inversion"),
        ({"observed_at": NOW + 31}, "head_clock_skew"),
        ({"observed_at": NOW - 31}, "head_clock_skew"),
    ],
)
def test_invalid_chain_observations_preserve_refusal_evidence(mutation, reason):
    value = replace(observation(), **mutation)
    before = asdict(value)
    with pytest.raises(QuoteFreshnessError) as exc:
        validate_quote_freshness(value)
    assert exc.value.reason == reason
    assert exc.value.observation is value
    assert asdict(value) == before
    assert str(value.quote.number) in str(exc.value)
    assert str(value.head.timestamp) in str(exc.value)


def test_wall_clock_prevents_lagging_head_from_extending_quote_lifetime():
    value = replace(observation(elapsed=290), observed_at=NOW + 20)
    with pytest.raises(QuoteFreshnessError, match="quote_stale"):
        validate_quote_freshness(value)


def test_identical_quote_and_head_is_valid_when_recent():
    value = observation()
    validate_quote_freshness(replace(value, head=value.quote))


@pytest.mark.parametrize("field", ["observed_at", "max_age_seconds", "max_clock_skew_seconds"])
@pytest.mark.parametrize("invalid", [True, 0, -1, "45", 45.0, None])
def test_malformed_policy_or_clock_fails_closed(field, invalid):
    with pytest.raises(ValueError, match="positive integer"):
        validate_quote_freshness(replace(observation(), **{field: invalid}))
