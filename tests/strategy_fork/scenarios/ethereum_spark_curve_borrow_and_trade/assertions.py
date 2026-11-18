"""Verify Spark fund and operator balances within the complete Curve composition."""

from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork import curve_authority, pool_lending_authority
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_entry_swap,
    assert_inventory_movements,
    assert_successful_outcomes,
    assert_wallet_balances,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import entered as entered
from tests.strategy_fork.scenarios.sparklend_ethereum_lending.assertions import (
    assert_owned_position,
)

crash_operation = "swap"
teardown_timeout = 420


def assert_permissions(node, config):
    return {
        "sparklend": pool_lending_authority.assert_permissions(
            node, config.component(ConnectorId("sparklend"))
        ),
        "curve": curve_authority.assert_permissions(node, config.component(ConnectorId("curve"))),
    }


def assert_holding(node, state):
    assert (entered(state), state.mode, state.memory.values["traded"]) == (True, "running", True)
    assert_successful_outcomes(state)
    assert min(state.owned[0].supply_scaled, state.owned[1].debt_scaled) > 0
    assert_entry_swap(state)
    assert_inventory_movements(state)
    with EthereumReader(state.config.target("loan").fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in state.owned[:2]:
            assert_owned_position(reader, state, owned, block)
        assert_wallet_balances(reader, state, block)


# A continuous reversal proof includes both pre-crash entry work and swap reconciliation.
entry_timeout = 420
