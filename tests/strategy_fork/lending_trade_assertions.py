"""Shared borrow-and-trade proofs retain protocol-owned debt and one fund cash ledger."""

import pytest

from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.compound_v3 import reader as compound_reader
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue import reader as morpho_reader
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork import compound_authority, curve_authority, morpho_authority
from tests.strategy_fork.aave_v3_authority import wrapped
from tests.strategy_fork.composed_authority import forbidden_swaps
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_entry_swap,
    assert_inventory_movements,
    assert_roundtrip_order,
    assert_wallet_balances,
    entered,
)

crash_operation = "swap"


def identity(config):
    return next(
        i
        for i in config.parameter_components
        if i in (ConnectorId("morpho_blue"), ConnectorId("compound_v3"))
    )


def assert_permissions(node, config):
    native = identity(config)
    probe = morpho_authority if native == ConnectorId("morpho_blue") else compound_authority
    result = probe.assert_permissions(node, config.component(native))
    if ConnectorId("curve") in config.parameter_components:
        result["curve"] = curve_authority.assert_permissions(
            node, config.component(ConnectorId("curve"))
        )
        return result
    profile = config.accounts.binding("fund").profile
    for label, target, inner in forbidden_swaps(config):
        call = wrapped(profile, target, inner)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        result["rejected"].append(dict(case=label, call=call, error_selector="0xd0a9bf58"))
    return result


def measured_position(reader, source, fund, block, native):
    binding = morpho_reader if native == ConnectorId("morpho_blue") else compound_reader
    market = binding.read_market(reader, source, block)
    return binding.read_account(reader, market, fund)


def assert_measured_holding(node, state):
    native = identity(state.config)
    target = state.config.target("loan")
    with EthereumReader(target.fund.chain, node.rpc.endpoint, local_transport=True) as reader:
        block = reader.pin_block("latest")
        account = measured_position(reader, target.reserve.market, target.fund, block, native)
        assert_fund_position(state, account, native)
        profile = state.config.accounts.binding("fund").profile
        if profile.submitter != target.fund:
            operator = measured_position(
                reader, target.reserve.market, profile.submitter, block, native
            )
            assert_operator_empty(operator, native)
        assert_wallet_balances(reader, state, block)


def assert_fund_position(state, account, native):
    held = {p.target.allocation: p for p in state.owned}
    if native == ConnectorId("morpho_blue"):
        measured = (account.position.borrow_shares, account.position.collateral)
        assert measured == (held["loan"].borrow_shares, held["collateral"].collateral)
    else:
        asset = state.config.target("collateral").asset
        measured = (account.principal, collateral_balance(account, asset))
        assert measured == (held["loan"].principal, held["collateral"].collateral)


def collateral_balance(account, asset):
    return next(c.balance for c in account.collaterals if c.asset == asset)


def assert_operator_empty(account, native):
    if native == ConnectorId("morpho_blue"):
        assert (
            account.position.supply_shares
            == account.position.borrow_shares
            == account.position.collateral
            == 0
        )
    else:
        assert account.principal == 0 and all(c.balance == 0 for c in account.collaterals)


def assert_holding(node, state):
    assert entered(state) and state.mode == "running" and state.memory.values["traded"]
    assert_successful_outcomes(state)
    assert not state.owned[0].empty and not state.owned[1].empty
    assert state.owned[0].collateral == state.config.allocation("collateral").capital
    assert_entry_swap(state)
    assert_inventory_movements(state)
    assert_measured_holding(node, state)


def assert_closed(node, state):
    assert state.mode == "closed" and state.pending is None and state.plan is None
    assert state.decision is None and state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert_roundtrip_order(state)
    assert_lending_closed(state)
    exchange = state.closure[-1]
    assert exchange.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)


def assert_lending_closed(state):
    assert all(p.empty for p in state.owned[:2])
    assert not state.memory.values["traded"]
    native = identity(state.config)
    for observation in state.closure[:2]:
        assert observation.allowance == 0
        assert_operator_empty(observation.account, native)
