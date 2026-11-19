"""Direct operator actions as the connectors declare them.

The declarations are static: no chain access, signing or strategy code.
"""

from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.caip import ChainId
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.composition import CONNECTOR_IDS, installed_connectors
from almanak.connectors.support import ConnectorChainSupport, EconomicAction, RunMode
from almanak.gateway.execution import EvmGateway

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
        assert cells == {kind for c in commands for kind in direct.COMMAND_INTENTS[c]}, name


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
    direct.require_direct("aave_v4", ETHEREUM, "supply")
    with pytest.raises(ValueError, match="aave_v4 serves no direct supply on eip155:8453"):
        direct.require_direct("aave_v4", BASE, "supply")
    with pytest.raises(ValueError, match="aave_v4 serves no direct borrow"):
        direct.require_direct("aave_v4", ETHEREUM, "borrow")
    with pytest.raises(ValueError, match="no direct actions for gmx_v2: a GMX request"):
        direct.require_direct("gmx_v2", ChainId("eip155:42161"), "supply")
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
    from almanak.connectors.aave_v4.execution import AaveExecution

    # The connector's own validation is proven elsewhere; only the declaration is asked.
    monkeypatch.setattr(AaveExecution, "validate_operation", lambda *arguments: None)
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
