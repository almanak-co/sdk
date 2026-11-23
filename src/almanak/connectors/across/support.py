"""Across chains with a pinned SpokePool. No run mode is declared yet.

A mode is declared only once its reference lifecycle proof exists, so every chain
listed here names the connector's scope and admits no fork, production or backtest run.
"""

from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.support import ConnectorChainSupport, EconomicAction


def candidate_support():
    return tuple(
        ConnectorChainSupport(
            chain=chain,
            modes=(),
            actions=(EconomicAction.BRIDGE, EconomicAction.REVOKE_BRIDGE),
            account_profiles=("eoa",),
            restrictions=(
                "ERC-20 routes between two bound accounts; no wrapped native asset is sent",
                "pinned SpokePool proxy and implementation",
                "the output amount is the venue's quote under the author's fee ceiling, "
                "a gateway check",
                "arrival and refund are not reconciled yet",
            ),
        )
        for chain in SPOKE_POOLS
    )
