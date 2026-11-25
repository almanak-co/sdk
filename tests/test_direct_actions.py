"""Direct operator actions as the connectors declare them.

The declarations are static: no chain access, signing or strategy code.
"""

import argparse
import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak import ax_actions
from almanak.caip import ChainId
from almanak.cli_arguments import build_parser, parse_validated
from almanak.configuration.gas import DEFAULT_BASE_FEE_MULTIPLIER_BPS, GasInput
from almanak.connectors import direct
from almanak.connectors.composition import CONNECTOR_IDS, installed_connectors
from almanak.connectors.support import ConnectorChainSupport, EconomicAction, RunMode
from almanak.gateway import production_fees
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.production_config import (
    MAX_BASE_FEE_MULTIPLIER_BPS,
    MIN_BASE_FEE_MULTIPLIER_BPS,
    GasPolicy,
)
from almanak.models import Block, Chain

INSTALLED = {str(key) for key in CONNECTOR_IDS}
ETHEREUM, BASE = ChainId("eip155:1"), ChainId("eip155:8453")


def cell(**changes):
    values = {
        "chain": ETHEREUM,
        "modes": (RunMode.FORK, RunMode.PRODUCTION),
        "actions": (EconomicAction.SUPPLY, EconomicAction.WITHDRAW),
        "account_profiles": ("eoa", "safe_roles"),
        "restrictions": (),
        "direct_actions": (EconomicAction.SUPPLY,),
    }
    return ConnectorChainSupport(**{**values, **changes})


def test_every_installed_connector_declares_its_direct_actions_or_says_why_not():
    declared = set(direct.direct_actions())
    assert declared | set(direct.NOT_DIRECT) == INSTALLED
    assert not declared & set(direct.NOT_DIRECT)
    assert all(reason.strip() for reason in direct.NOT_DIRECT.values())


def test_a_connector_serves_exactly_the_actions_its_chains_declare():
    for name, connector in installed_connectors():
        cells = {str(a) for support in connector.chain_support for a in support.direct_actions}
        found = direct.direct_actions().get(str(name))
        commands = () if found is None else found.actions
        # Every declared kind has a command, and every command a declared kind somewhere.
        assert cells <= {kind for c in commands for kind in direct.COMMAND_INTENTS[c]}, name
        assert all(cells & set(direct.COMMAND_INTENTS[c]) for c in commands), name


def test_every_command_names_the_intents_it_admits():
    assert set(direct.COMMAND_INTENTS) == set(direct.served_actions())
    kinds = {str(action) for action in EconomicAction}
    assert all(set(admitted) <= kinds for admitted in direct.COMMAND_INTENTS.values())


def test_every_served_action_is_a_command_with_its_connectors():
    parser = build_parser()
    ax = next(a for a in parser._actions if getattr(a, "choices", None) and "ax" in a.choices)
    commands = next(a for a in ax.choices["ax"]._actions if getattr(a, "choices", None)).choices
    assert set(direct.served_actions()) == set(ax_actions.ACTIONS)
    for action in direct.served_actions():
        connector = next(a for a in commands[action]._actions if a.dest == "connector")
        assert tuple(connector.choices) == direct.connectors_for(action)


def test_a_direct_action_is_a_supported_production_action_for_a_plain_account():
    assert cell().direct_actions == (EconomicAction.SUPPLY,)
    assert "direct_actions" not in cell(direct_actions=()).model_dump()
    with pytest.raises(ValueError, match="must be a supported action"):
        cell(direct_actions=(EconomicAction.BORROW,))
    with pytest.raises(ValueError, match="must be unique"):
        cell(direct_actions=(EconomicAction.SUPPLY, EconomicAction.SUPPLY))
    with pytest.raises(ValueError, match="production support for EOA"):
        cell(modes=(RunMode.FORK,))
    with pytest.raises(ValueError, match="production support for EOA"):
        cell(account_profiles=("safe_roles",))


def test_a_connector_chain_or_action_that_is_not_declared_is_refused():
    supply, withdraw = direct.ActionRequest("supply", 5), direct.ActionRequest("withdraw")
    direct.require_direct("aave_v4", ETHEREUM, supply)
    direct.require_direct("aave_v4", ETHEREUM, withdraw)
    with pytest.raises(ValueError, match="aave_v4 serves no direct supply on eip155:8453"):
        direct.require_direct("aave_v4", BASE, supply)
    with pytest.raises(ValueError, match="aave_v4 serves no direct collateral"):
        direct.require_direct("aave_v4", ETHEREUM, direct.ActionRequest("collateral", enable=True))
    # A command the connector serves, in a form it does not: Aave V4 withdraws and
    # repays in full.
    with pytest.raises(ValueError, match="aave_v4 serves no direct withdraw --amount on eip155:1"):
        direct.require_direct("aave_v4", ETHEREUM, direct.ActionRequest("withdraw", 5))
    with pytest.raises(ValueError, match="aave_v4 serves no direct repay --amount on eip155:1"):
        direct.require_direct("aave_v4", ETHEREUM, direct.ActionRequest("repay", 5))
    with pytest.raises(ValueError, match="no direct actions for gmx_v2: a GMX request"):
        direct.require_direct("gmx_v2", ChainId("eip155:42161"), supply)
    with pytest.raises(ValueError, match="unsupported as a direct action"):
        cell().require_direct("withdraw")


def action_arguments(*extra):
    return parse_validated(
        build_parser(),
        [
            "ax",
            "supply",
            "--connector",
            "aave_v4",
            "--spoke",
            "0x94e7a5dcbe816e498b89ab752661904e2f56c485",
            "--reserve-id",
            "0",
            "--amount",
            "5",
            "--wallet",
            "wallet",
            "--env-file",
            ".env",
            "--max-gas-price-wei",
            "7",
            "--max-transaction-fee",
            "9",
            *extra,
        ],
    )


def test_the_command_refuses_an_undeclared_chain_before_it_reads_anything(monkeypatch):
    def unreachable(*arguments):
        raise AssertionError("the command read before refusing")

    monkeypatch.setattr(ax_actions, "read_wallet", unreachable)
    monkeypatch.setattr(ax_actions, "transport", unreachable)
    with pytest.raises(ValueError, match="aave_v4 serves no direct supply on eip155:8453"):
        ax_actions.command(action_arguments("--chain", "eip155:8453"))


def test_the_stated_ceilings_reach_the_gas_policy(monkeypatch, capsys):
    seen = {}

    def deployment(connector, request, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=connector, action=action, limits=limits, **options)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(action_arguments("--chain", "eip155:1", "--max-gas-limit", "2000000"))
    assert seen["limits"] == ax_actions.FeeLimits(7, 9, 2_000_000)
    assert (seen["connector"], seen["action"]) == ("aave_v4", direct.ActionRequest("supply", 5))
    assert seen["public"] is False
    assert action_arguments("--chain", "eip155:1").max_gas_limit is None
    with pytest.raises(SystemExit):
        action_arguments("--chain", "eip155:1", "--max-gas-limit", "0")
    assert "positive whole number of gas units" in capsys.readouterr().err


def test_a_configuration_names_its_connector_binding(recorded):
    assert direct.binding_for(recorded.config) is direct.binding("aave_v4")
    named = SimpleNamespace(connector="aave_v4")
    assert direct.binding_for(named) is direct.binding("aave_v4")
    with pytest.raises(ValueError, match="composed configuration serves no direct action"):
        direct.binding_for(SimpleNamespace(connector=None))
    with pytest.raises(ValueError, match="no direct actions for nothing: unknown connector"):
        direct.binding("nothing")


def operator_gateway(recorded, kind, *, operator):
    """The execution adapter's own checks on one operation of a retained decision."""
    operation = recorded.outcomes[0].operation
    gateway = object.__new__(EvmGateway)
    gateway.config, gateway.guard = recorded.config, lambda: None
    gateway.reader = SimpleNamespace(chain=operation.target.fund.chain)
    decision = SimpleNamespace(intent=SimpleNamespace(kind=kind))
    state = SimpleNamespace(operator=operator, decision=decision)
    return lambda: gateway.validate_operation(state, operation, None)


def test_the_gateway_admits_an_operator_request_only_for_a_declared_direct_action(
    recorded, monkeypatch
):
    import dataclasses

    from almanak.connectors import target_routing
    from almanak.connectors.aave_v4.execution import AaveExecution

    # The connector's own validation is proven elsewhere; only the declaration is asked.
    monkeypatch.setattr(AaveExecution, "validate_operation", lambda *arguments: None)
    # A chain that serves fewer actions directly than a strategy may take there.
    connector = target_routing.connector_for_target(recorded.outcomes[0].operation.target)
    cells = tuple(
        cell.model_copy(update={"direct_actions": (EconomicAction.SUPPLY,)})
        for cell in connector.chain_support
    )
    narrowed = dataclasses.replace(connector, chain_support=cells)
    monkeypatch.setattr(target_routing, "connector_for_target", lambda target: narrowed)
    operator_gateway(recorded, "supply", operator=True)()
    # A strategy may borrow on this chain; an operator request may not.
    operator_gateway(recorded, "borrow", operator=False)()
    with pytest.raises(ValueError, match="borrow is unsupported as a direct action"):
        operator_gateway(recorded, "borrow", operator=True)()


@pytest.mark.parametrize(
    ("chain", "basis", "multiplier"),
    [
        ("eip155:1", "next_block", 20000),
        # Avalanche's fee history may hold only the observed block's base fee.
        ("eip155:43114", "observed_block", 20000),
        # Nitro charges the base fee, so a 2x bid only consumes the price ceiling.
        ("eip155:42161", "next_block", 12500),
        ("eip155:4663", "next_block", 12500),
    ],
)
def test_a_step_bids_as_an_author_policy_defaults_on_its_chain(chain, basis, multiplier):
    limits = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
    policy = ax_actions.gas_policy("treasury", ChainId(chain), limits)
    assert (policy["base_fee_basis"], policy["base_fee_multiplier_bps"]) == (basis, multiplier)
    assert (policy["max_gas_price_wei"], policy["max_transaction_fee"]["amount"]) == (7, 9)
    assert policy["max_gas_limit"] == 1_200_000 and policy["accounts"] == ["treasury"]
    assert (
        policy["asset"] == policy["max_transaction_fee"]["asset"] == policy["exit_reserve"]["asset"]
    )


def test_an_operator_may_state_a_gas_limit():
    limits = ax_actions.FeeLimits(
        max_gas_price_wei=7, max_transaction_fee=9, max_gas_limit=2_000_000
    )
    assert ax_actions.gas_policy("treasury", ETHEREUM, limits)["max_gas_limit"] == 2_000_000


POLYGON = ChainId("eip155:137")


def policy_of(chain, limits) -> GasPolicy:
    """The typed gas policy of a direct deployment, validated as its wire form is."""
    document = {"name": "direct", **ax_actions.gas_policy("treasury", chain, limits)}
    return GasPolicy.model_validate_json(json.dumps(document))


def stated_limits(arguments) -> ax_actions.FeeLimits:
    """The fee limits a parsed command hands to its deployment, with nothing read."""
    seen = {}

    def deployment(connector, request, action, wallet, provider, endpoint, limits, **options):
        seen.update(limits=limits)
        raise StopIteration

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ax_actions, "read_wallet", lambda directory: {})
        patch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
        patch.setattr(ax_actions, "deployment", deployment)
        with pytest.raises(StopIteration):
            ax_actions.command(arguments)
    return seen["limits"]


def test_a_stated_base_fee_multiplier_reaches_the_gas_policy():
    stated = action_arguments("--chain", "eip155:1", "--base-fee-multiplier-bps", "10000")
    limits = stated_limits(stated)
    assert limits == ax_actions.FeeLimits(7, 9, None, 10000)
    policy = policy_of(ETHEREUM, limits)
    assert policy.base_fee_multiplier_bps == 10000
    # Only the multiplier differs from the policy of a command without the flag.
    default = ax_actions.gas_policy("treasury", ETHEREUM, ax_actions.FeeLimits(7, 9))
    assert ax_actions.gas_policy("treasury", ETHEREUM, limits) == {
        **default,
        "base_fee_multiplier_bps": 10000,
    }
    assert policy.replacement is None


def test_without_the_flag_the_gas_policy_is_the_chain_default_unchanged():
    assert action_arguments("--chain", "eip155:1").base_fee_multiplier_bps is None
    assert stated_limits(action_arguments("--chain", "eip155:1")) == ax_actions.FeeLimits(7, 9)
    native = "eip155:137/slip44:966"
    limits = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
    assert ax_actions.gas_policy("treasury", POLYGON, limits) == {
        "accounts": ["treasury"],
        "asset": native,
        "source": "network_rpc_fee_history",
        "base_fee_basis": "next_block",
        "reward_percentile": 50,
        "base_fee_multiplier_bps": 20000,
        "max_gas_price_wei": 7,
        "max_gas_limit": 1_200_000,
        "max_transaction_fee": {"asset": native, "amount": 9},
        "exit_reserve": {"asset": native, "amount": 0},
        "gas_limit_buffer_bps": 2500,
    }


@pytest.mark.parametrize("value", ["9999", "50001", "0", "12500.5", "1e4", "-10000", "x", "²"])
def test_a_base_fee_multiplier_outside_the_author_range_is_refused_before_any_read(
    value, monkeypatch, capsys
):
    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    monkeypatch.setattr(ax_actions, "transport", pytest.fail)
    with pytest.raises(SystemExit) as refusal:
        action_arguments("--chain", "eip155:1", f"--base-fee-multiplier-bps={value}")
    assert refusal.value.code == 2
    error = capsys.readouterr().err
    assert "--base-fee-multiplier-bps" in error
    assert "expected a whole number of basis points from 10000 to 50000" in error


def test_the_flag_takes_exactly_the_range_an_author_gas_policy_takes():
    low, high = MIN_BASE_FEE_MULTIPLIER_BPS, MAX_BASE_FEE_MULTIPLIER_BPS
    assert (low, high) == (10000, 50000)
    for value in (low, high):
        assert ax_actions.base_fee_multiplier_bps(str(value)) == value
        limits = ax_actions.FeeLimits(7, 9, None, value)
        policy_of(ETHEREUM, limits)
    author = {
        "accounts": ["treasury"],
        "chain": "eip155:1",
        "max_transaction_fee": "0.01",
        "exit_reserve": "0",
    }

    def authored(value):
        return GasInput.model_validate_json(
            json.dumps({**author, "base_fee_multiplier_bps": value})
        )

    for value in (low, high):
        assert authored(value).base_fee_multiplier_bps == value
    for value in (low - 1, high + 1):
        with pytest.raises(argparse.ArgumentTypeError):
            ax_actions.base_fee_multiplier_bps(str(value))
        with pytest.raises(ValidationError, match="base_fee_multiplier_bps"):
            authored(value)
        with pytest.raises(ValidationError, match="base_fee_multiplier_bps"):
            policy_of(ETHEREUM, ax_actions.FeeLimits(7, 9, None, value))


class FeeHistory:
    """A reader that answers the one-block fee history a quote asks for."""

    def __init__(self, block, base_fees, tip):
        self.block, self.base_fees, self.tip = block, base_fees, tip

    def _rpc(self, method, params):
        assert method == "eth_feeHistory", method
        return {
            "oldestBlock": params[1],
            "baseFeePerGas": [hex(fee) for fee in self.base_fees],
            "reward": [[hex(self.tip)]],
        }

    def pin_block(self, number):
        return self.block


# Polygon at block 97834951: the block's base fee, the next block's, the median tip.
BASE_FEE, NEXT_BASE_FEE, TIP = 248_595_418_034, 248_658_140_102, 78_000_000_000


@pytest.mark.parametrize(
    ("chain", "multiplier", "bid"),
    [
        # Without the flag: twice the next base fee plus the tip.
        ("eip155:137", None, 575_316_280_204),
        ("eip155:137", 20000, 575_316_280_204),
        # The base fee itself plus the tip.
        ("eip155:137", 10000, 326_658_140_102),
        ("eip155:137", 12500, 388_822_675_128),
        ("eip155:137", 50000, 1_321_290_700_510),
        ("eip155:42161", None, 388_822_675_128),
        ("eip155:42161", 10000, 326_658_140_102),
    ],
)
def test_the_bid_is_the_base_fee_times_the_stated_multiplier_plus_the_tip(chain, multiplier, bid):
    limits = ax_actions.FeeLimits(10**13, 10**18, None, multiplier)
    policy = policy_of(ChainId(chain), limits)
    block = Block(
        chain=Chain(chain_id=ChainId(chain), environment="mainnet"),
        number=97834950,
        hash="0x" + "11" * 32,
        timestamp=1795458000,
    )
    reader = FeeHistory(block, (BASE_FEE, NEXT_BASE_FEE), TIP)
    price, quote = production_fees.quoted_price(reader, policy, block)
    assert price == bid
    assert (quote.market_base_fee, quote.priority_fee) == (NEXT_BASE_FEE, TIP)
    if multiplier == 10000:
        assert price == NEXT_BASE_FEE + TIP


def test_a_lower_multiplier_lowers_the_bid_and_leaves_the_ceilings_as_stated():
    block = Block(
        chain=Chain(chain_id=POLYGON, environment="mainnet"),
        number=97834950,
        hash="0x" + "11" * 32,
        timestamp=1795458000,
    )
    reader = FeeHistory(block, (BASE_FEE, NEXT_BASE_FEE), TIP)
    ceiling = 330 * 10**9
    for multiplier, admitted in ((None, False), (10000, True)):
        limits = ax_actions.FeeLimits(ceiling, 10**18, None, multiplier)
        policy = policy_of(POLYGON, limits)
        assert policy.max_gas_price_wei == ceiling
        price, _ = production_fees.quoted_price(reader, policy, block)
        if admitted:
            production_fees.validate_fee_limits(policy, 84_004, price)
            continue
        with pytest.raises(DataError, match="exceeds the configured price ceiling"):
            production_fees.validate_fee_limits(policy, 84_004, price)


def test_the_help_states_the_multiplier_its_range_and_the_stuck_step(capsys):
    with pytest.raises(SystemExit):
        action_arguments("--help")
    text = " ".join(capsys.readouterr().out.split())
    assert "--base-fee-multiplier-bps" in text
    assert f"{MIN_BASE_FEE_MULTIPLIER_BPS} to {MAX_BASE_FEE_MULTIPLIER_BPS}" in text
    assert f"{DEFAULT_BASE_FEE_MULTIPLIER_BPS}, or 12500 on Arbitrum and Robinhood Chain" in text
    assert "the whole quoted price is paid" in text
    assert "The ceilings apply unchanged" in text
    assert "`ax recover` sends the same bytes again at the same price" in text
