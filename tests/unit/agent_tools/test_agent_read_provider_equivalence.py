"""Per-connector agent-read provider equivalence (VIB-4860 / W8, plan §7.2).

Each ``agent_read_provider`` must return the exact same address / selector
the pre-W8 ``executor.py`` resolved inline. We assert against the
connector's own canonical address tables (the same tables the executor
imported before W8), so a typo in a provider surfaces here rather than as a
silently-wrong RPC target.

These tests also exercise the populated production registry
(``STRATEGY_AGENT_READ_REGISTRY``) end-to-end: ``lookup`` must resolve every
registered protocol to a capability whose descriptors match the tables.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_agent_tool_registry import (
    STRATEGY_AGENT_READ_REGISTRY,
)


def test_uniswap_v3_provider_matches_address_tables() -> None:
    from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
    from almanak.connectors.uniswap_v3.receipt_parser import (
        POSITION_MANAGER_ADDRESSES,
    )

    cap = STRATEGY_AGENT_READ_REGISTRY.lookup("uniswap_v3")
    assert cap is not None
    assert cap.agent_read_keys() == frozenset({"pool_state", "lp_position"})
    # getPool selector is the uint24-fee v3 family selector.
    assert cap.get_pool_selector() == "0x1698ee82"
    # Factory + NPM match the canonical tables on every chain they cover.
    for chain, contracts in UNISWAP_V3.items():
        assert cap.factory_address(chain) == contracts.get("factory")
    for chain, npm in POSITION_MANAGER_ADDRESSES.items():
        assert cap.position_manager_address(chain) == npm
    # Not a lending connector.
    assert cap.lending_pool_address("arbitrum") is None


def test_agni_finance_provider_matches_address_tables() -> None:
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE
    from almanak.connectors.uniswap_v3.receipt_parser import (
        POSITION_MANAGER_ADDRESSES,
    )

    cap = STRATEGY_AGENT_READ_REGISTRY.lookup("agni_finance")
    assert cap is not None
    assert cap.agent_read_keys() == frozenset({"pool_state", "lp_position"})
    assert cap.get_pool_selector() == "0x1698ee82"
    for chain, contracts in AGNI_FINANCE.items():
        assert cap.factory_address(chain) == contracts.get("factory")
    # Pre-W8 the LP handler used POSITION_MANAGER_ADDRESSES (not
    # AGNI_FINANCE["position_manager"]) for the Agni alias — preserve that.
    assert cap.position_manager_address("mantle") == POSITION_MANAGER_ADDRESSES.get("mantle")


def test_pancakeswap_v3_provider_matches_address_tables() -> None:
    """VIB-4902: PancakeSwap's provider resolves factory + its OWN NPM.

    The NPM must be ``PANCAKESWAP_V3[chain]['nft']`` (the connector's own
    table the receipt parser and teardown walker read), NOT the Uniswap V3 NPM.
    """
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.uniswap_v3.receipt_parser import (
        POSITION_MANAGER_ADDRESSES,
    )

    cap = STRATEGY_AGENT_READ_REGISTRY.lookup("pancakeswap_v3")
    assert cap is not None
    assert cap.agent_read_keys() == frozenset({"pool_state", "lp_position"})
    assert cap.get_pool_selector() == "0x1698ee82"
    for chain, contracts in PANCAKESWAP_V3.items():
        assert cap.factory_address(chain) == contracts.get("factory")
        assert cap.position_manager_address(chain) == contracts.get("nft")
        # Regression guard: never the Uniswap NPM (the VIB-4902 fall-through).
        assert cap.position_manager_address(chain) != POSITION_MANAGER_ADDRESSES.get(chain)
    assert cap.lending_pool_address("bsc") is None


def test_sushiswap_v3_provider_matches_address_tables() -> None:
    """VIB-4902: SushiSwap's provider resolves factory + its OWN per-chain NPM.

    The NPM must be ``SUSHISWAP_V3[chain]['position_manager']`` (the connector's
    own table the teardown walker reads), NOT the Uniswap V3 NPM.
    """
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.uniswap_v3.receipt_parser import (
        POSITION_MANAGER_ADDRESSES,
    )

    cap = STRATEGY_AGENT_READ_REGISTRY.lookup("sushiswap_v3")
    assert cap is not None
    assert cap.agent_read_keys() == frozenset({"pool_state", "lp_position"})
    assert cap.get_pool_selector() == "0x1698ee82"
    for chain, contracts in SUSHISWAP_V3.items():
        assert cap.factory_address(chain) == contracts.get("factory")
        assert cap.position_manager_address(chain) == contracts.get("position_manager")
        # Regression guard: never the Uniswap NPM (the VIB-4902 fall-through).
        assert cap.position_manager_address(chain) != POSITION_MANAGER_ADDRESSES.get(chain)
    assert cap.lending_pool_address("arbitrum") is None


def test_aave_v3_provider_matches_pool_table() -> None:
    from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES

    cap = STRATEGY_AGENT_READ_REGISTRY.lookup("aave_v3")
    assert cap is not None
    assert cap.agent_read_keys() == frozenset({"lending_account", "lending_reserves"})  # VIB-4951
    for chain, pool in AAVE_V3_POOL_ADDRESSES.items():
        assert cap.lending_pool_address(chain) == pool
    # Not a CL DEX.
    assert cap.factory_address("arbitrum") is None
    assert cap.position_manager_address("arbitrum") is None


@pytest.mark.parametrize(
    ("protocol", "expected_selector"),
    [
        ("uniswap_v3", "0x1698ee82"),
        ("agni_finance", "0x1698ee82"),
    ],
)
def test_get_pool_selector_per_protocol(protocol: str, expected_selector: str) -> None:
    cap = STRATEGY_AGENT_READ_REGISTRY.lookup(protocol)
    assert cap is not None
    assert cap.get_pool_selector() == expected_selector


def test_registry_has_no_protocol_collisions_at_import() -> None:
    """Force a lookup of every registered protocol — surfaces any
    registration-time collision / missing-capability at import (mirrors the
    W6 ``test_gas_estimate_registry_completeness`` early-lookup pattern)."""
    for proto in STRATEGY_AGENT_READ_REGISTRY.protocols():
        cap = STRATEGY_AGENT_READ_REGISTRY.lookup(str(proto))
        assert cap is not None, f"{proto} registered but not resolvable"
        # Non-empty keyset is a register-time invariant; re-assert here.
        assert cap.agent_read_keys()
