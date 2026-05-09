"""BRIDGE branch in `_compute_outbox_position_key` (VIB-4164, T4).

T4 reclassified BRIDGE from `NO_ACCOUNTING` to `AccountingCategory.TRANSFER`
so bridge intents now produce typed `TransferAccountingEvent` rows. The
runner's `_compute_outbox_position_key` is the single source of truth for
the `accounting_outbox.position_key` and `accounting_events.position_key`
columns — without a BRIDGE branch, every bridge transfer row would carry an
empty `position_key`, making source-leg/destination-leg joins impossible.

This test pins the BRIDGE-shaped position_key:

    f"bridge:{from_chain}:{to_chain}:{token}:{wallet_address}"

A future PR that drops the branch or changes the format silently breaks
auditor joins. The test asserts the exact format and the empty-fallback when
required fields are missing.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner


def _call_compute(intent: Any, intent_type: str = "BRIDGE", chain: str = "base", wallet: str = "0xWALLET") -> tuple[str, str]:
    """Call the StrategyRunner method without instantiating the runner.

    `_compute_outbox_position_key` is bound but only reads `self` for
    `logger.debug` (in the `except` branch). We can call it as an unbound
    method with a stand-in `self` that has nothing the happy path touches.
    """
    return StrategyRunner._compute_outbox_position_key(
        SimpleNamespace(),  # type: ignore[arg-type]
        intent,
        intent_type,
        chain,
        wallet,
    )


def test_bridge_position_key_happy_path() -> None:
    """BRIDGE intent with all required fields → bridge:src:dst:token:wallet."""
    intent = SimpleNamespace(
        from_chain="base",
        to_chain="arbitrum",
        token="USDC",
    )
    position_key, market_id = _call_compute(intent)
    assert position_key == "bridge:base:arbitrum:USDC:0xwallet"
    assert market_id == ""


def test_bridge_position_key_normalises_case_and_whitespace() -> None:
    """Token uppercased; chains lowercased + stripped; wallet lowercased."""
    intent = SimpleNamespace(
        from_chain="  Base  ",
        to_chain="ARBITRUM",
        token="usdc",
    )
    position_key, _ = _call_compute(intent, wallet="0xABCDEFAB")
    assert position_key == "bridge:base:arbitrum:USDC:0xabcdefab"


@pytest.mark.parametrize(
    "from_chain,to_chain,token,chain_arg,wallet",
    [
        # When intent.from_chain is empty AND the runner's chain arg is also empty,
        # the helper has no source chain to use → empty position_key.
        ("", "arbitrum", "USDC", "", "0xWALLET"),
        ("base", "", "USDC", "base", "0xWALLET"),  # missing to_chain
        ("base", "arbitrum", "", "base", "0xWALLET"),  # missing token
        ("base", "arbitrum", "USDC", "base", ""),  # missing wallet
    ],
)
def test_bridge_position_key_returns_empty_on_missing_fields(
    from_chain: str, to_chain: str, token: str, chain_arg: str, wallet: str
) -> None:
    """Any missing required field → empty position_key. The handler then
    emits an UNAVAILABLE-confidence event rather than a misleading partial key.
    """
    intent = SimpleNamespace(from_chain=from_chain, to_chain=to_chain, token=token)
    position_key, market_id = _call_compute(intent, chain=chain_arg, wallet=wallet)
    assert position_key == ""
    assert market_id == ""


def test_bridge_position_key_falls_back_to_chain_arg_when_intent_missing_from_chain() -> None:
    """If the intent lacks `from_chain`, fall back to the runner's chain argument.

    The runner's chain argument is the source-side chain at submission time;
    using it as `from_chain` is correct for source-leg writes.
    """
    intent = SimpleNamespace(to_chain="arbitrum", token="USDC")  # no from_chain attr
    position_key, _ = _call_compute(intent, chain="optimism")
    assert position_key == "bridge:optimism:arbitrum:USDC:0xwallet"


def test_bridge_position_key_distinguishes_direction() -> None:
    """Same token, opposite directions → different position_keys.

    Auditors must be able to separate "USDC base→arbitrum" from
    "USDC arbitrum→base" — they're independent transfer flows.
    """
    intent_a = SimpleNamespace(from_chain="base", to_chain="arbitrum", token="USDC")
    intent_b = SimpleNamespace(from_chain="arbitrum", to_chain="base", token="USDC")
    pk_a, _ = _call_compute(intent_a)
    pk_b, _ = _call_compute(intent_b)
    assert pk_a != pk_b
    assert pk_a == "bridge:base:arbitrum:USDC:0xwallet"
    assert pk_b == "bridge:arbitrum:base:USDC:0xwallet"
