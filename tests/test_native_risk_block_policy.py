"""Native per-block risk ceilings must cover the block span the rewritten window covers.

A native QA lane rewrites the backtest window to `native_window_seconds` with an
equal cleanup horizon, so `tests/qa/inputs.py::resolved_history` produces a
schedule whose deadline is twice that value after its start. The per-block risk
acquisition in `aave_v3`, `morpho_blue` and `compound_v3` counts every chain
block from the floor of the start to the floor of the deadline, so a ceiling
below that span refuses the lane outright. This is the same rule
`tests/test_compound_v3_qa_policy.py` asserts for the single-connector Comet
references, applied to every reference the policy bounds.
"""

import json
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.configuration.yaml import load
from almanak.connectors.identifiers import ConnectorId

ROOT = Path(__file__).resolve().parents[1]
POLICY = json.loads((ROOT / "tests/qa/policy.json").read_text())

# Fastest block cadence each chain is observed to sustain, in milliseconds, from
# retained evidence under docs/internal/reports/evidence. Integers keep the rule
# exact: binary floats make 1200 // 0.1 one short of the real 12000 intervals.
#
#   eip155:1      2-prepared/cases/588 aave_v4 interest, 100 blocks / 1200 s
#   eip155:8453   dex-finish/base-pancake-composed-prepared aave_v3 risk,
#                 46 blocks / 92 s with no gap
#   eip155:10     shares Base's OP Stack cadence; the accepted policy entries for
#                 compound_v3_optimism_lending and morpho_blue_optimism_lending
#                 already derive 601 from it
#   eip155:137    curve-fork origins 93460480@1788895753 and 93566908@1789055395,
#                 106428 blocks / 159642 s with no missed slot
#   eip155:43114  4-avalanche-v3/cases/221 aave_v3 risk, 92 consecutive blocks one
#                 second apart; its native 1200 s windows hold 1160 and 1162 blocks
#   eip155:56     dex-finish/bnb-v4-trading-prepared pool_state, 1200 blocks / 540 s
#   eip155:42161  the Arbitrum promotion review measured a 4795 raw-block risk span
#                 over the same 1200 s window
#   eip155:4663   curve-fork origins 57446777@1788845924, 58238929@1788925845 and
#                 59523147@1789055394 average 100.88 ms; 100 ms is the fastest
#                 cadence those records admit, and the rule needs the fastest one
#   eip155:999    2026-10-20-hyperevm-morpho/chain-facts.json: spans from blocks
#                 44018431, 48018431, 48818431, 49013431 and 49016382 each hold 1221
#                 blocks in 1200 s, one-second small blocks plus a big block each
#                 minute in one sequence; 983 ms is the largest cadence giving 1221
CHAIN_BLOCK_MILLISECONDS = {
    ChainId("eip155:1"): 12000,
    ChainId("eip155:8453"): 2000,
    ChainId("eip155:10"): 2000,
    ChainId("eip155:137"): 1500,
    ChainId("eip155:43114"): 1000,
    ChainId("eip155:56"): 450,
    ChainId("eip155:42161"): 250,
    ChainId("eip155:4663"): 100,
    ChainId("eip155:999"): 983,
}

# Every entry the QA policy bounds by raw block count, paired with its reference.
LIMIT_MAPS = ("native_collection_limits", "native_component_collection_limits")


def acquisition_milliseconds():
    # resolved_history rewrites the window end to start + native_window_seconds and
    # the cleanup to the same value, and RunSchedule.deadline is end + cleanup.
    return 2 * POLICY["native_window_seconds"] * 1000


def required_blocks(chain):
    if chain not in CHAIN_BLOCK_MILLISECONDS:
        raise AssertionError(f"no retained block cadence for {chain}")
    return acquisition_milliseconds() // CHAIN_BLOCK_MILLISECONDS[chain] + 1


def connector_chains(reference):
    """Map each configured connector to the chains its own markets identify."""
    raw = load(ROOT / "strategies" / reference / "config.yaml").value
    default = raw["strategy"].get("connector")
    chains: dict[ConnectorId, set[ChainId]] = {}
    for market in raw["markets"].values():
        connector = ConnectorId(market.get("connector", default))
        chains.setdefault(connector, set()).add(ChainId(market["chain"]))
    return chains


def bounded_entries():
    """Yield (reference, connector, limits) for every declared raw-block ceiling."""
    for name in LIMIT_MAPS:
        for reference, entry in POLICY[name].items():
            components = (
                entry.items() if name == "native_component_collection_limits" else ((None, entry),)
            )
            for connector, limits in components:
                if "max_risk_blocks" in limits:
                    yield reference, connector, limits


@pytest.mark.parametrize(
    "reference,connector,limits", tuple(bounded_entries()), ids=lambda value: str(value)
)
def test_native_risk_ceilings_cover_the_rewritten_acquisition_window(reference, connector, limits):
    chains = connector_chains(reference)
    selected = (
        chains[ConnectorId(connector)] if connector is not None else set().union(*chains.values())
    )
    assert selected, reference
    required = sum(required_blocks(chain) for chain in selected)
    assert limits["max_risk_blocks"] >= required, (reference, connector, required)
    # One sample is collected per reserve or market, so samples never fall below blocks.
    assert limits["max_risk_samples"] >= limits["max_risk_blocks"]


# The composed candidate references and the per-reserve sample multiple each one
# acquires: aave_v3 samples its collateral and loan reserves separately, while the
# Morpho references borrow inside a single market.
DERIVED = (
    ("avalanche_aave_borrow_and_trade_v3", "aave_v3", "eip155:43114", 2),
    ("bnb_aave_borrow_and_trade_v3", "aave_v3", "eip155:56", 2),
    ("bnb_aave_borrow_and_trade_v4", "aave_v3", "eip155:56", 2),
    ("polygon_aave_borrow_and_trade_v3", "aave_v3", "eip155:137", 2),
    ("polygon_aave_borrow_and_trade_v4", "aave_v3", "eip155:137", 2),
    ("robinhood_morpho_borrow_and_trade_v3", "morpho_blue", "eip155:4663", 1),
    ("robinhood_morpho_borrow_and_trade_v4", "morpho_blue", "eip155:4663", 1),
)


@pytest.mark.parametrize("reference,connector,chain,reserves", DERIVED)
def test_composed_candidate_limits_stay_at_or_above_their_derived_cadence(
    reference, connector, chain, reserves
):
    limits = POLICY["native_component_collection_limits"][reference][connector]
    assert connector_chains(reference)[ConnectorId(connector)] == {ChainId(chain)}
    blocks = required_blocks(ChainId(chain))
    assert limits["max_risk_blocks"] >= blocks
    assert limits["max_risk_samples"] >= reserves * blocks
