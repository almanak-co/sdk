"""Unit tests for explicit bridge cross-chain detection in plan builder."""

from dataclasses import dataclass

from almanak.framework.execution.plan import RemediationAction
from almanak.framework.execution.plan_builder import (
    get_remediation_action,
    is_cross_chain_intent,
)
from almanak.framework.intents import BridgeIntent, Intent


@dataclass
class _LegacyCrossChainIntent:
    chain: str = "arbitrum"
    destination_chain: str = "base"
    intent_type: object = type("LegacyType", (), {"value": "SWAP"})()


def test_bridge_intent_detected_as_cross_chain_when_chains_differ() -> None:
    intent = BridgeIntent(token="USDC", amount=1, from_chain="base", to_chain="arbitrum")
    assert is_cross_chain_intent(intent) is True


def test_bridge_intent_not_cross_chain_when_chains_match() -> None:
    # BridgeIntent model disallows same-chain, so use a lightweight stand-in.
    class _SameChainBridge:
        intent_type = type("BridgeType", (), {"value": "BRIDGE"})()
        from_chain = "base"
        to_chain = "base"

    assert is_cross_chain_intent(_SameChainBridge()) is False


def test_legacy_destination_chain_fallback_still_works() -> None:
    assert is_cross_chain_intent(_LegacyCrossChainIntent()) is True


def test_bridge_remediation_action_is_bridge_back() -> None:
    intent = BridgeIntent(token="USDC", amount=1, from_chain="base", to_chain="arbitrum")
    assert get_remediation_action(intent) == RemediationAction.BRIDGE_BACK


def test_swap_cross_chain_remediation_remains_bridge_back() -> None:
    intent = Intent.swap(
        from_token="USDC",
        to_token="ETH",
        amount=1,
        chain="base",
        destination_chain="arbitrum",
    )
    assert get_remediation_action(intent) == RemediationAction.BRIDGE_BACK
