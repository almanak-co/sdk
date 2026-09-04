"""Zodiac manifest regression for Uniswap V4 on Robinhood Chain (4663).

Robinhood carries a NON-canonical V4 deployment and has no USDC: its liquid
quote asset is USDG, and the framework's default synthetic pair resolves to
Ethena USDe first. Without the connector's pair override the generated
manifest authorises approvals on USDe (never traded) and omits USDG; without
the chain-specific address row the manifest targets the canonical Ethereum
contracts, which have no code on 4663.
"""

from __future__ import annotations

import pytest

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.connector import CONNECTOR
from almanak.connectors.uniswap_v4.sdk import (
    MODIFY_LIQUIDITIES_SELECTOR,
    PERMIT2_ADDRESS,
    PERMIT2_APPROVE_SELECTOR,
    UNIVERSAL_ROUTER_EXECUTE_SELECTOR,
)
from almanak.core.chains.robinhood import DESCRIPTOR as ROBINHOOD
from almanak.framework.intents.compiler_constants import ERC20_APPROVE_SELECTOR
from almanak.framework.permissions.generator import generate_manifest

_CHAIN = ROBINHOOD.name
_USDG = ROBINHOOD.tokens["usdg"].lower()
_WETH = ROBINHOOD.tokens["weth"].lower()
_USDE = ROBINHOOD.tokens["usde"].lower()
_PERMIT2 = PERMIT2_ADDRESS.lower()
_UNIVERSAL_ROUTER = UNISWAP_V4[_CHAIN]["universal_router"].lower()
_POSITION_MANAGER = UNISWAP_V4[_CHAIN]["position_manager"].lower()
_CANONICAL_UNIVERSAL_ROUTER = UNISWAP_V4["ethereum"]["universal_router"].lower()
_CANONICAL_POSITION_MANAGER = UNISWAP_V4["ethereum"]["position_manager"].lower()
_APPROVE = ERC20_APPROVE_SELECTOR.lower()
_PERMIT2_APPROVE = PERMIT2_APPROVE_SELECTOR.lower()
_UR_EXECUTE = UNIVERSAL_ROUTER_EXECUTE_SELECTOR.lower()
_MODIFY_LIQUIDITIES = MODIFY_LIQUIDITIES_SELECTOR.lower()

# Mirror a real WETH/USDG strategy's funding so the config token scan and the
# synthetic discovery both run, as they do for the shipped manifest.
_STRATEGY_CONFIG = {"chain": _CHAIN, "anvil_funding": {_WETH: 1, _USDG: 100}}


def _manifest_pairs(intent_types: list[str]) -> set[tuple[str, str]]:
    manifest = generate_manifest(
        strategy_name="uniswap-v4-robinhood-manifest-regression",
        chain=_CHAIN,
        supported_protocols=["uniswap_v4"],
        intent_types=intent_types,
        config=_STRATEGY_CONFIG,
    )
    return {
        (perm.target.lower(), sel.selector.lower()) for perm in manifest.permissions for sel in perm.function_selectors
    }


def _approve_targets(intent_types: list[str]) -> set[str]:
    return {target for target, sel in _manifest_pairs(intent_types) if sel == _APPROVE}


class TestUniswapV4RobinhoodRegistry:
    def test_connector_declares_every_address_table_chain(self) -> None:
        declared = set(CONNECTOR.supported_chains.chains)
        assert declared == set(UNISWAP_V4), (declared, set(UNISWAP_V4))
        assert _CHAIN in declared

    def test_robinhood_addresses_are_not_the_canonical_deployment(self) -> None:
        canonical = UNISWAP_V4["ethereum"]
        for role, address in UNISWAP_V4[_CHAIN].items():
            assert address.lower() != canonical[role].lower(), (
                f"{role}: Robinhood must carry its own non-canonical V4 address, got the canonical one"
            )


class TestUniswapV4RobinhoodManifest:
    def test_full_manifest_targets_robinhood_position_manager(self) -> None:
        pairs = _manifest_pairs(["SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
        assert (_POSITION_MANAGER, _MODIFY_LIQUIDITIES) in pairs, sorted(pairs)
        assert (_UNIVERSAL_ROUTER, _UR_EXECUTE) in pairs, sorted(pairs)

    def test_swap_targets_robinhood_universal_router_and_usdg(self) -> None:
        pairs = _manifest_pairs(["SWAP"])
        assert (_UNIVERSAL_ROUTER, _UR_EXECUTE) in pairs, sorted(pairs)
        assert (_PERMIT2, _PERMIT2_APPROVE) in pairs, sorted(pairs)
        assert not any(target == _CANONICAL_UNIVERSAL_ROUTER for target, _ in pairs), (
            "SWAP manifest must not target the canonical UniversalRouter, which has no code on 4663"
        )
        approves = _approve_targets(["SWAP"])
        assert _USDG in approves and _WETH in approves, sorted(approves)
        assert _USDE not in approves, "SWAP manifest must not authorise approve on USDe"

    # LP_CLOSE on its own emits no PositionManager grant on any V4 chain (its
    # synthetic vector cannot carry a liquidity figure offline); it is covered by
    # the combined manifest above.
    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_COLLECT_FEES"])
    def test_lp_verbs_target_robinhood_position_manager(self, intent_type: str) -> None:
        pairs = _manifest_pairs([intent_type])
        assert (_POSITION_MANAGER, _MODIFY_LIQUIDITIES) in pairs, (intent_type, sorted(pairs))
        assert not any(target == _CANONICAL_POSITION_MANAGER for target, _ in pairs), (
            f"{intent_type} manifest must not target the canonical PositionManager, which has no code on 4663"
        )

    def test_lp_open_authorises_usdg_and_weth_only(self) -> None:
        approves = _approve_targets(["LP_OPEN"])
        assert _USDG in approves and _WETH in approves, sorted(approves)
        assert _USDE not in approves, "LP_OPEN manifest must not authorise approve on USDe"
