"""Combined exported predicates retain complete spender/amount branches."""

from almanak.connectors.composed.authority import declared
from almanak.connectors.composed.permissions import derive
from almanak.models import SafeRoles
from tests.test_aave_v3_permissions import value
from tests.test_composed_configuration import parameters


def conditions(node):
    yield node
    for child in node.children:
        yield from conditions(child)


def test_shared_token_approval_keeps_each_spender_with_any_amount_and_no_quota():
    config = parameters()
    capabilities = declared(config)
    fund = config.accounts.bindings[0].profile.fund
    safe = SafeRoles(
        kind="safe_roles",
        fund=fund,
        operator=fund.model_copy(update={"address": "0x" + "13" * 20}),
        roles_module="0x" + "34" * 20,
        role_key="0x" + "56" * 32,
    )
    grants = derive(safe, capabilities)
    loan = config.target("loan")
    trading = config.target("position")
    approval = next(
        g for g in grants if g.target == loan.asset.reference and g.signature.startswith("approve(")
    )
    assert approval.condition.operator == 2
    branches = approval.condition.children
    amounts = {value(branch.children[0], "address"): branch.children[1] for branch in branches}
    assert set(amounts) == {loan.reserve.pool, trading.contracts.router}
    # Scope-only authority: each spender is pinned and approves any amount, or revokes.
    for amount in amounts.values():
        assert amount.operator == 2
        assert {child.operator for child in amount.children} == {16, 17}
    # No grant carries a Roles allowance (operator 28).
    assert all(c.operator != 28 for g in grants if g.condition for c in conditions(g.condition))
