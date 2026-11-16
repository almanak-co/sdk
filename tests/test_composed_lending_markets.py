"""Distinct Morpho and Compound ownership participates in the shared native fund ledger."""

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.composed.authority import declared
from almanak.connectors.composed.execution import validate_inventory
from almanak.connectors.composed.fork import ComposedFork
from almanak.connectors.composed.inventory import refresh
from almanak.connectors.composed.lifecycle import ComposedLifecycle
from almanak.connectors.composed.permissions import derive
from almanak.connectors.composed.state import Checkpoint
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.interfaces import Connector
from almanak.connectors.uniswap_v3.operations import Swap
from almanak.effects import AssetDelta, apply_assets
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount, SafeRoles
from almanak.runtime import validate_strategy_config
from almanak.strategy import StrategyDefinition
from tests.composed_public import resolved_parameters
from tests.test_composed_inventory import config as config


@pytest.fixture(params=["morpho_blue", "compound_v3"])
def composed(config, request, monkeypatch):
    protocol = request.param
    candidate = import_module(f"almanak.connectors.{protocol}.support").candidate_support
    native = import_module(f"almanak.connectors.{protocol}.models").Config
    original = Connector.support_for
    monkeypatch.setattr(
        Connector,
        "support_for",
        lambda self, chain: (
            next(r for r in candidate() if r.chain == chain)
            if self.codec.config is native
            else original(self, chain)
        ),
    )
    short = "morpho" if protocol == "morpho_blue" else "compound"
    reference = Path(__file__).parents[1] / f"strategies/base_{short}_borrow_and_trade"
    _, author = resolved_parameters(reference.name)
    prepared = ComposedFork.prepare_chains(author, config.accounts.bindings, config.fork_bindings)
    strategy = StrategyDefinition.read(reference / "strategy.py")
    return SimpleNamespace(protocol=protocol, config=prepared, author=author, strategy=strategy)


def test_native_parameters_ownership_and_author_hook_preserve_protocol_semantics(composed):
    c = composed
    validate_strategy_config(c.strategy, c.author)
    state = ComposedLifecycle.initial(c.config, c.strategy, declared(c.config), None)
    restored = Checkpoint.model_validate_json(state.model_dump_json())
    assert restored == state
    assert all(p.empty for p in state.owned[:2])
    assert {type(p).__module__ for p in state.owned[:2]} == {
        f"almanak.connectors.{c.protocol}.observations"
    }
    identity = ConnectorId(c.protocol)
    assert c.config.component(identity).allocations[0] is c.config.allocations[0]
    assert c.config.target("loan").fund == c.config.target("trade").fund
    assert not hasattr(state.owned[0], "collateral_enabled")


def test_unfunded_trade_is_refused_then_borrowed_cash_can_be_spent_once(composed):
    c = composed
    state = ComposedLifecycle.initial(c.config, c.strategy, declared(c.config), None)
    allocation, target = c.config.allocation("trade"), c.config.target("trade")
    operation = Swap(
        target=target,
        amount=allocation.capital,
        minimum=AssetAmount(asset=target.pool.currency0, amount=1),
        deadline=1788710300,
    )
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(state, operation)
    key = next(b.key for b in state.inventory if b.key.asset == allocation.capital.asset)
    # The loan's amount is the strategy's decision; borrowing exactly the trade's capital
    # funds the trade once.
    borrowed = allocation.capital.amount
    balances = apply_assets(state.inventory, (AssetDelta(key=key, amount=borrowed),))
    funded = state.model_copy(
        update={"inventory": balances, "owned": refresh(state.owned, balances)}
    )
    validate_inventory(funded, operation)
    spent = apply_assets(funded.inventory, (AssetDelta(key=key, amount=-borrowed),))
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(funded.model_copy(update={"inventory": spent}), operation)


def test_generated_permissions_are_scope_only_for_lending_and_exchange(composed):
    c = composed
    fund = c.config.accounts.bindings[0].profile.fund
    profile = SafeRoles(
        kind="safe_roles",
        fund=fund,
        operator=fund.model_copy(update={"address": "0x" + "13" * 20}),
        roles_module="0x" + "34" * 20,
        role_key="0x" + "56" * 32,
    )
    grants = derive(profile, declared(c.config))
    assert grants
    # No grant carries a Roles allowance (operator 28): scope-only authority (blueprint 27).
    assert all(term.operator != 28 for grant in grants for term in grant.conditions)
    lending = c.config.allocation("loan").reserve
    core = lending.market.core if c.protocol == "morpho_blue" else lending.market.comet
    assert any(grant.target == core for grant in grants)
