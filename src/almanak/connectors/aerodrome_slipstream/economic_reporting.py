"""Slipstream reports sampled dynamic fees and unstaked historical LP semantics."""

from almanak.connectors import v3_economic_reporting as family

ASSUMPTIONS = (
    "swap fees use the latest eligible undiscounted dynamic fee observation",
    "fee discounts, staked NFT positions and gauge rewards are excluded",
    "historical unstaked LP fee growth already includes the protocol staking split",
)


def report(state, model):
    return extended(family.report(state, model))


def valued_report(state, model):
    return extended(family.valued_report(state, model))


def extended(result):
    result["assumptions"] += ASSUMPTIONS
    return result
