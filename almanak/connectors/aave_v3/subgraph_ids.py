"""Canonical Aave V3 The Graph Network deployment IDs.

This lightweight data module is shared by the operator-side historical APY
provider and the gateway connector capability so the two surfaces cannot
drift onto different deployments.
"""

from __future__ import annotations

AAVE_V3_SUBGRAPH_IDS: dict[str, str] = {
    "ethereum": "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g",
    "arbitrum": "DLuE98kEb5pQNXAcKFQGQgfSQ57Xdou4jnVbAEqMfy3B",
    "optimism": "DSfLz8oQBUeU5atALgUFQKMTSYV9mZAVYp4noLSXAfvb",
    "polygon": "Co2URyXjnxaw8WqxKyVHdirq9Ahhm5vcTs4dMedAq211",
    "base": "GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF",
    "avalanche": "2h9woxy8RTjHu1HJsCEnmzpPHFArU33avmUh4f71JpVn",
}

__all__ = ["AAVE_V3_SUBGRAPH_IDS"]
