"""Cross-drift guard: agent-read NPM == teardown discovery walker NPM.

VIB-4902 root cause: the PancakeSwap / SushiSwap agent-read providers resolved
the *Uniswap V3* NPM table instead of their own, so the ``get_lp_position`` /
``list_lp_positions`` agent tools queried a different on-chain contract than the
teardown discovery walker — silently returning wrong/empty data for a Pancake or
Sushi position.

Both paths must resolve the SAME NonfungiblePositionManager for every supported
(fork, chain):

* The agent-read provider via ``STRATEGY_AGENT_READ_REGISTRY.lookup(protocol)``
  → ``position_manager_address(chain)``.
* The teardown discovery walker via ``discovery._npms_for_chain(chain)`` and the
  post-condition resolver ``post_conditions._resolve_v3_position_manager``.

All three ultimately read the connector's ``addresses.py`` NPM, so these pass by
construction once VIB-4902 lands — their job is to FAIL the instant a future
change re-introduces a divergent address source (or key) for any path.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_agent_tool_registry import (
    STRATEGY_AGENT_READ_REGISTRY,
)
from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
from almanak.connectors.uniswap_v3.receipt_parser import (
    POSITION_MANAGER_ADDRESSES,
)
from almanak.framework.teardown.discovery import _npms_for_chain
from almanak.framework.teardown.post_conditions import _resolve_v3_position_manager

# The agent-read protocol slug is identical on both sides — that identity is
# part of what this test locks. The connector address table is the single
# source of truth both paths must read.
_FORKS = (
    ("pancakeswap_v3", PANCAKESWAP_V3),
    ("sushiswap_v3", SUSHISWAP_V3),
)


def _fork_chain_params() -> list:
    params: list = []
    for protocol, contracts in _FORKS:
        for chain in sorted(contracts):
            params.append(pytest.param(protocol, chain, id=f"{protocol}-{chain}"))
    return params


@pytest.mark.parametrize(("protocol", "chain"), _fork_chain_params())
def test_agent_read_npm_matches_teardown_walker(protocol: str, chain: str) -> None:
    """Provider NPM == teardown walker NPM for every supported (fork, chain)."""
    cap = STRATEGY_AGENT_READ_REGISTRY.lookup(protocol)
    assert cap is not None, f"no agent-read provider registered for {protocol}"

    provider_npm = cap.position_manager_address(chain)
    assert provider_npm is not None, (
        f"{protocol} agent-read provider returned None for {chain}, but the "
        f"connector address table has an NPM registered for it"
    )

    # 1) The discovery walker (what the teardown manager calls to enumerate
    #    live positions) must surface the same address for this protocol.
    discovered = dict(_npms_for_chain(chain))
    assert discovered.get(protocol) == provider_npm, (
        f"NPM drift for {protocol}/{chain}: agent-read provider resolves "
        f"{provider_npm} but discovery._npms_for_chain resolves "
        f"{discovered.get(protocol)}. Both must read the connector addresses.py."
    )

    # 2) The post-condition resolver (what teardown closure verification calls)
    #    must agree too.
    post_npm = _resolve_v3_position_manager(protocol, chain)
    assert post_npm == provider_npm, (
        f"NPM drift for {protocol}/{chain}: agent-read provider resolves "
        f"{provider_npm} but post_conditions._resolve_v3_position_manager "
        f"resolves {post_npm}."
    )

    # 3) Regression guard for VIB-4902: a fork's NPM must not silently fall
    #    through to the Uniswap V3 NPM table. (Forks share the canonical ABI but
    #    deploy to their own addresses; the Uniswap-table fall-through was the
    #    bug.)
    assert provider_npm != POSITION_MANAGER_ADDRESSES.get(chain), (
        f"{protocol}/{chain} resolved the Uniswap V3 NPM ({provider_npm}) — the VIB-4902 fall-through bug has regressed"
    )
