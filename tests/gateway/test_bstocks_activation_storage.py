"""Issuer storage proves activation across cold starts without refreshing stock ticks."""

import time
from dataclasses import replace
from decimal import Decimal

import pytest

from almanak.gateway.data.price.scaled_token_reference import AdjustmentCoherence, MultiplierObservation
from almanak.gateway.data.price.scaled_token_storage import verify_activation_storage
from almanak.integrations.bstocks.catalog import GOOGLB

UNIT = 10**18
MAX_UINT = 2**256 - 1


@pytest.mark.parametrize(
    "stored,active,following,effective,expected",
    [
        ((UNIT, UNIT, MAX_UINT), UNIT, UNIT, 0, 0),
        ((UNIT, 2 * UNIT, 90), 2 * UNIT, 2 * UNIT, 0, 90),
        ((UNIT, 2 * UNIT, 100), 2 * UNIT, 2 * UNIT, 0, 100),
        ((UNIT, 2 * UNIT, 101), UNIT, 2 * UNIT, 101, None),
        ((2 * UNIT, UNIT, 99), UNIT, UNIT, 0, 99),
    ],
)
def test_initialization_activation_and_overwrite_are_reconstructed(stored, active, following, effective, expected):
    assert (
        verify_activation_storage(stored, active=active, following=following, effective=effective, block_timestamp=100)
        == expected
    )


@pytest.mark.parametrize(
    "stored,active,following,effective",
    [
        ((UNIT, 2 * UNIT, MAX_UINT), UNIT, UNIT, 0),
        ((2 * UNIT, 2 * UNIT, MAX_UINT), 2 * UNIT, 2 * UNIT, 0),
        ((UNIT, UNIT, 0), UNIT, UNIT, 0),
        ((0, UNIT, 90), UNIT, UNIT, 0),
        ((UNIT, 2 * UNIT, 90), UNIT, UNIT, 0),
        ((UNIT, 2 * UNIT, 90), 2 * UNIT, 2 * UNIT, 90),
        ((UNIT, 2 * UNIT, 101), 2 * UNIT, 2 * UNIT, 101),
        ((UNIT, 2 * UNIT, 101), UNIT, 2 * UNIT, 0),
        ((UNIT, 2 * UNIT, 101), UNIT, 2 * UNIT, 102),
    ],
)
def test_incoherent_getters_and_fabricated_reset_cannot_erase_activation(stored, active, following, effective):
    with pytest.raises(ValueError, match="multiplier_storage_"):
        verify_activation_storage(stored, active=active, following=following, effective=effective, block_timestamp=100)


def observation(**changes):
    now = int(time.time())
    return replace(
        MultiplierObservation(
            multiplier=Decimal("2"),
            block_number=100,
            block_hash="0x" + "ab" * 32,
            block_timestamp=now,
            read_at=now,
            scheduled_multiplier=None,
            effective_at=now - 100,
            beacon=GOOGLB.beacon,
            implementation=GOOGLB.implementation,
            last_activation_at=now - 100,
        ),
        **changes,
    )


def test_cold_and_warm_gateways_accept_the_same_post_activation_quote():
    state = observation()
    quote = state.block_timestamp - 60
    warm = AdjustmentCoherence()
    assert warm.observe(GOOGLB, state, quote) is None
    for coherence in (warm, AdjustmentCoherence(), AdjustmentCoherence()):
        assert coherence.observe(GOOGLB, state, quote) is None


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_source_must_strictly_follow_retained_activation(offset):
    state = observation()
    reason = AdjustmentCoherence().observe(GOOGLB, state, state.last_activation_at + offset)
    assert reason == (None if offset > 0 else "reference_adjustment_alignment_unproven")


def test_late_observation_of_new_multiplier_uses_actual_activation_not_poll_time():
    state = observation()
    coherence = AdjustmentCoherence()
    assert coherence.observe(GOOGLB, state, state.block_timestamp - 50) is None
    changed = replace(
        state,
        block_number=101,
        multiplier=Decimal("3"),
        last_activation_at=state.block_timestamp - 10,
        effective_at=state.block_timestamp - 10,
    )
    assert coherence.observe(GOOGLB, changed, state.block_timestamp - 11) == "reference_adjustment_alignment_unproven"
    assert coherence.observe(GOOGLB, changed, state.block_timestamp - 9) is None
    assert coherence.observe(GOOGLB, state, state.block_timestamp) == "multiplier_observation_out_of_order"


def test_same_block_cannot_replace_retained_activation_evidence():
    state = observation()
    coherence = AdjustmentCoherence()
    assert coherence.observe(GOOGLB, state, state.block_timestamp - 50) is None
    assert (
        coherence.observe(GOOGLB, replace(state, last_activation_at=0), state.block_timestamp)
        == "multiplier_observation_conflict"
    )


def test_reviewed_googlb_implementation_identity_is_pinned():
    assert GOOGLB.implementation == "0xcfed6c4679297ea4889f8183bc057b4a86c64e46"
    assert GOOGLB.implementation_code_sha256 == "814fc45a704716dbda5b91791b69b7780924f66ae604606d670bded136bc339c"
