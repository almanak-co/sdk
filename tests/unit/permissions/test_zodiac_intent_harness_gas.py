"""Regression tests for Safe intent-test outer gas sizing (ALM-3101)."""

from tests.intents._permission_onchain_harness import _inner_gas_hint, _zodiac_outer_gas


def test_action_bundle_gas_estimate_is_preserved_with_wrapper_headroom() -> None:
    """A large inner hint must not become the complete Roles tx limit."""
    inner = _inner_gas_hint({"gas_estimate": 1_500_000})

    assert inner == 1_500_000
    assert _zodiac_outer_gas(inner) == 2_000_000


def test_wrapper_minimum_still_covers_transactions_without_a_hint() -> None:
    assert _inner_gas_hint({"to": "0x0"}) is None
    assert _zodiac_outer_gas(None) == 1_500_000
