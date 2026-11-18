"""V3-family and Aerodrome grants carry scope only (blueprint 27 sections 4 and 11).

No grant has a Roles allowance or an amount derived from an author field, so no author
amount can refuse an exit, a revocation or a repeated entry. Approvals pin the exact
spender with any amount, swap inputs and deposits take any uint128 amount (call sanity,
as Uniswap V4 bounds its amounts) and mint ticks are left to the gateway's range and band
checks.
"""

import json
from importlib import import_module

import pytest

from almanak.configuration.yaml import load
from almanak.connectors.v3_pool_grants import tick_conditions
from almanak.gateway.roles_conditions import any_amount, bounded, either, equal, positive
from almanak.models import Account, AccountBinding, Eoa, StrategyAccounts
from scripts.legacy_configuration.author import normalize
from tests.exchange_scope import assert_scope_only, operators
from tests.legacy_references import harness_directory

REFERENCES = (
    ("uniswap_v3", "uniswap_v3_base_trading"),
    ("uniswap_v3", "uniswap_v3_base_lp"),
    ("pancakeswap_v3", "pancakeswap_v3_bnb_trading"),
    ("pancakeswap_v3", "pancakeswap_v3_bnb_lp"),
    ("aerodrome_slipstream", "aerodrome_slipstream_base_trading"),
    ("aerodrome_slipstream", "aerodrome_slipstream_base_lp"),
    ("aerodrome", "aerodrome_base_volatile_trading"),
    ("aerodrome", "aerodrome_base_stable_lp"),
)


def parameters(connector, reference):
    raw = load(harness_directory(reference) / "config.yaml").value
    raw.pop("execution")
    author = normalize(raw)
    models = import_module(f"almanak.connectors.{connector}.models")
    fund = Account(chain=author.allocations[0].chain, address="0x" + "12" * 20)
    return models.Parameters(
        decision_trigger=author.decision_trigger,
        asset_metadata=author.asset_metadata,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=(AccountBinding(name="fund", profile=Eoa(kind="eoa", fund=fund)),)
        ),
    )


def derived(connector, config):
    permissions = import_module(f"almanak.connectors.{connector}.permissions")
    declared = import_module(f"almanak.connectors.{connector}.authority").declared
    return permissions.derive(config.accounts.bindings[0].profile, declared(config))


def with_allocation(config, **changes):
    allocation = config.allocations[0].model_copy(update=changes)
    return type(config).model_validate_json(
        json.dumps(
            {**config.model_dump(mode="json"), "allocations": [allocation.model_dump(mode="json")]}
        )
    )


def branches(grant):
    """A combined grant is an OR over its correlated argument tuples."""
    root = grant.condition
    return root.children if root.operator == 2 else (root,)


def nodes(condition):
    yield condition
    for child in condition.children:
        yield from nodes(child)


MAXIMUM = 2**128 - 1
OPTIONAL = either(equal("uint256", 0), bounded(MAXIMUM))
# Position of amountIn in each connector's swap argument tuple.
AMOUNT_IN = {"uniswap_v3": 4, "pancakeswap_v3": 5, "aerodrome_slipstream": 5, "aerodrome": 0}


def paying(condition, fund):
    """The argument tuple that pins the Safe recipient."""
    recipient = equal("address", fund.address)
    return next(node for node in nodes(condition) if recipient in node.children)


@pytest.mark.parametrize(("connector", "reference"), REFERENCES)
def test_grants_carry_no_allowance_and_no_author_amount(connector, reference):
    config = parameters(connector, reference)
    grants = derived(connector, config)
    assert grants
    assert_scope_only(grants)
    allocation = config.allocations[0]
    # Capital, the entry slice and the policy fields never reach a grant.
    changed = with_allocation(
        config,
        capital=allocation.capital.model_copy(update={"amount": allocation.capital.amount * 7}),
        entry_swap_bps=1 + allocation.entry_swap_bps % 10000,
        slippage_bps=allocation.slippage_bps + 1,
        hold_seconds=allocation.hold_seconds + 1,
    )
    assert derived(connector, changed) == grants


@pytest.mark.parametrize(
    ("connector", "reference"),
    [(c, r) for c, r in REFERENCES if r.endswith("_lp") and c != "aerodrome"],
)
def test_mint_ticks_are_unrestricted_so_a_new_range_needs_no_new_permissions(connector, reference):
    config = parameters(connector, reference)
    grants = derived(connector, config)
    (mint,) = (g for g in grants if g.signature.startswith("mint("))
    assert mint.condition.children[0].children[3:5] == tick_conditions()
    selected = config.allocations[0].range
    spacing = config.allocations[0].pool.tick_spacing
    moved = selected.model_copy(
        update={"lower": selected.lower + spacing, "upper": selected.upper + spacing}
    )
    assert derived(connector, with_allocation(config, range=moved)) == grants


@pytest.mark.parametrize(("connector", "reference"), REFERENCES)
def test_approvals_pin_the_exact_spender_with_any_amount(connector, reference):
    config = parameters(connector, reference)
    target = config.target(config.allocations[0].name)
    spenders = {target.contracts.router, getattr(target.contracts, "position_manager", None)}
    approvals = [g for g in derived(connector, config) if g.signature == "approve(address,uint256)"]
    token_approvals = [g for g in approvals if g.target in {a.reference for a in target.assets}]
    assert {g.target for g in token_approvals} == {a.reference for a in target.assets}
    for grant in token_approvals:
        for branch in branches(grant):
            spender, amount = branch.children
            assert spender.value[-40:] in {s[2:] for s in spenders if s}
            assert amount == any_amount() or (
                connector == "aerodrome" and amount.children[1] == positive()
            )


@pytest.mark.parametrize(("connector", "reference"), REFERENCES)
def test_swap_inputs_take_any_nonzero_uint128_amount(connector, reference):
    config = parameters(connector, reference)
    target = config.target(config.allocations[0].name)
    swaps = [
        g
        for g in derived(connector, config)
        if g.target == target.contracts.router
        and g.signature != "approve(address,uint256)"
        and ("Single" in g.signature or "swap" in g.signature or "multicall" in g.signature)
    ]
    assert swaps
    for grant in swaps:
        for branch in branches(grant):
            assert paying(branch, target.fund).children[AMOUNT_IN[connector]] == bounded(MAXIMUM)
        assert 28 not in set(operators(grant.condition))


@pytest.mark.parametrize(
    ("connector", "reference"), [(c, r) for c, r in REFERENCES if r.endswith("_lp")]
)
def test_deposits_take_any_uint128_amount(connector, reference):
    config = parameters(connector, reference)
    grants = derived(connector, config)
    if connector == "aerodrome":
        (add,) = (g for g in grants if g.signature.startswith("addLiquidity("))
        assert add.condition.children[3:5] == (bounded(MAXIMUM),) * 2
        return
    (mint,) = (g for g in grants if g.signature.startswith("mint("))
    (increase,) = (g for g in grants if g.signature.startswith("increaseLiquidity("))
    # Desired and minimum amounts: zero or uint128.
    assert mint.condition.children[0].children[5:9] == (OPTIONAL,) * 4
    assert increase.condition.children[0].children[1:5] == (OPTIONAL,) * 4
