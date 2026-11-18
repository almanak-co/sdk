"""One generated grant keeps lending and V4 swap templates correlated."""

from almanak.connectors.identifiers import ConnectorId
from tests.strategy_fork.aave_v3_authority import assert_permissions as lending_permissions
from tests.strategy_fork.uniswap_v4_optimism_authority import assert_permissions as v4_permissions


def assert_permissions(node, config):
    lending = lending_permissions(node, config)
    exchange = v4_permissions(node, config.component(ConnectorId("uniswap_v4")))
    return {
        "method": "eth_call",
        "allowed_controls": [lending["allowed_control"], exchange["allowed_control"]],
        "rejected": [*lending["rejected"], *exchange["rejected"]],
    }
