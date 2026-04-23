"""On-chain permission-authorisation test cases for the Pendle connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.

Pendle swaps require one leg to be a PT (Principal Token). The default
USDC/WETH synthetic pair does not resolve to a Pendle market, so the
connector's ``permission_hints.synthetic_swap_pair`` override pins
arbitrum to ``wstETH -> PT-wstETH``; this case mirrors that pair so the
generated manifest matches the compiled intent.

Note: ``PT-wstETH`` is a Pendle-resolved symbol, not a CHAIN_CONFIGS
entry. The per-chain runner that exercises this case on Anvil (Phase F)
will resolve the PT token via the Pendle adapter's market lookup rather
than the conftest ``tokens`` map; the harness itself may need a small
dispatcher extension to accommodate symbol resolution at that point.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="pendle",
        intent_type="SWAP",
        config={"from_token": "wstETH", "to_token": "PT-wstETH", "amount": "0.05"},
    ),
]
