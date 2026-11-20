"""`almanak ax revoke-exchange` on deployed Uniswap V4 pools, one process per command.

Every cell that declares the revocation is rehearsed, and the Ethereum pool of the
native coin beside them: an isolated Anvil fork of the chain stands in for mainnet
behind the production adapter's own planning, signing and reconciliation, and the
command's own resolution builds each deployment. The allowances revoked are the ones
`ax swap` and `ax open-liquidity` leave on the same fork, and unlimited ones given
outside `ax`. It is rehearsal evidence, not a mainnet run.
"""

import pytest

from almanak import ax_actions
from almanak.connectors.composition import connector_from_wire
from tests.fork import test_ax_liquidity as family_liquidity
from tests.fork import test_ax_liquidity_uniswap_v4 as liquidity
from tests.fork import test_ax_swap as family
from tests.fork import test_ax_swap_uniswap_v4 as swaps

pytestmark = pytest.mark.fork
CONNECTOR = "uniswap_v4"
NOTHING_LEFT = "nothing to revoke: no approval of a pool token to Permit2 holds an amount"
SHARED = "every other application that spends these tokens from this wallet"


def declared_chains():
    """Every chain that declares the revocation as a direct action."""
    cells = connector_from_wire({"connector": CONNECTOR}).chain_support
    return [str(c.chain) for c in cells if "revoke_exchange" in map(str, c.direct_actions)]


@pytest.fixture(params=declared_chains(), ids=lambda chain: family.NETWORKS[chain])
def setting(request, tmp_path, monkeypatch):
    cell = liquidity.funded(liquidity.REFERENCES[request.param])
    with swaps.started(request, cell) as fork:
        yield liquidity.prepared(fork, cell, tmp_path, monkeypatch)


@pytest.fixture
def native(request, tmp_path, monkeypatch):
    cell = liquidity.funded(swaps.NATIVE_REFERENCE)
    with swaps.started(request, cell) as fork:
        yield liquidity.prepared(fork, cell, tmp_path, monkeypatch)


def every_allowance(setting):
    """Each token's ERC-20 approval to Permit2, then its Permit2 allowances to the router
    and the position manager, at the fork's head: the order the revocation takes them."""
    with setting.reader() as reader:
        seen = reader.pin_block("latest")
        target = setting.target(setting.around())
        found = [setting.token_allowance(reader, target, asset, seen) for asset in setting.assets]
    return [row for rows in found for row in rows]


def standing(setting):
    """Every allowance the revocation has a step for: one that holds an amount, and a
    Permit2 one that holds none and has not expired."""
    now = setting.wallet.head_time()
    rows = every_allowance(setting)
    return [row for row in rows if int(row["amount"]) or row.get("expiration", 0) > now]


def unexpired(setting):
    """The standing allowances that hold no amount, as a result lists them."""
    return [
        {"asset": row["asset"], "spender": row["spender"], "expiration": row["expiration"]}
        for row in standing(setting)
        if row["amount"] == "0"
    ]


def steps_for(setting, rows):
    permit2 = setting.contracts.permit2
    return ["revoke_erc20" if row["spender"] == permit2 else "revoke_permit2" for row in rows]


def leave_swap_approvals(setting):
    """A swap refused after its approvals leaves both: the sold token's ERC-20 approval
    to Permit2 and its Permit2 allowance to the router."""
    refused = swaps.refused_after_the_approvals(setting.wallet)
    token, amount, _ = setting.wallet.authority()
    assert (refused["refused"]["kind"], token, amount) == (
        "gas_limit",
        setting.cell.amount,
        setting.cell.amount,
    )


def assert_previewed(setting):
    """A preview lists what stands, plans the first revocation, says that the approval
    to Permit2 is shared, and signs nothing."""
    left = setting.allowances()
    preview = setting("revoke-exchange", execute=False)
    step = preview["first_step"]["operation"]
    assert (preview["action"], preview["executed"]) == ("revoke-exchange", False)
    assert (step["kind"], step["amount"]) == (steps_for(setting, standing(setting))[0], 0)
    assert preview["position_before"]["allowances"] == left == setting.allowances()
    assert SHARED in preview["note"] and "outcomes" not in preview


def assert_nothing_stands(setting, revoked):
    """Nothing is left for a revocation, on the chain as in the result, and the wallet
    is free: a second revocation is refused."""
    after = revoked["position_after"]
    assert (after["allowances"], "permit2_unexpired" in after) == ([], False)
    assert (standing(setting), ax_actions.kept_records()) == ([], [])
    with pytest.raises(ValueError, match=NOTHING_LEFT):
        setting("revoke-exchange")


def revoke(setting):
    """`ax revoke-exchange` clears every standing allowance, one step each, and moves
    nothing."""
    left, due, start = setting.allowances(), standing(setting), setting.balances()
    revoked = setting("revoke-exchange")
    family_liquidity.assert_clean(revoked, steps_for(setting, due))
    assert (revoked["position_before"]["allowances"], setting.balances()) == (left, start)
    assert_nothing_stands(setting, revoked)
    return revoked


def assert_position_untouched(setting, revoked, token_id):
    """The revocation neither owns nor touches the NFT, and a close needs no approval."""
    assert "position" not in revoked["position_after"]
    assert setting.nft_owner(token_id) == setting.owner
    liquidity.close_position(setting, token_id)
    assert setting.allowances() == []


def test_what_a_mint_and_a_refused_swap_left_is_revoked_and_the_position_still_closes(setting):
    setting.halve()
    ticks, stated = setting.around(), setting.stated()
    token_id, _ = liquidity.open_position(setting, ticks, stated)
    leave_swap_approvals(setting)
    spenders = {row["spender"] for row in setting.allowances()}
    assert {setting.contracts.permit2, setting.contracts.router} <= spenders
    assert_previewed(setting)
    assert_position_untouched(setting, revoke(setting), token_id)


def assert_spent_allowance_revoked(setting, sold):
    """A completed swap leaves a Permit2 allowance with no amount until its deadline.
    The result lists it apart, and a revocation expires it with one step and no note."""
    left = unexpired(setting)
    assert [row["spender"] for row in left] == [setting.contracts.router]
    assert sold["position_after"]["permit2_unexpired"] == left
    preview = setting("revoke-exchange", execute=False)
    assert (preview["first_step"]["operation"]["kind"], "note" in preview) == (
        "revoke_permit2",
        False,
    )
    assert family.operations(revoke(setting)) == ["revoke_permit2"]


def test_unlimited_approvals_given_outside_are_revoked_and_a_later_swap_approves_again(setting):
    setting.approve_unlimited()
    swaps.permit_unlimited(setting.wallet)
    assert len(setting.allowances()) == 5
    kinds = family.operations(revoke(setting))
    assert (kinds.count("revoke_erc20"), kinds.count("revoke_permit2")) == (2, 3)
    # Nothing is approved now, so a swap approves exactly its amount again and spends it.
    start = setting.wallet.balances()
    sold = setting.wallet(execute=True)
    swaps.assert_swapped(setting.wallet, sold, start, 1)
    assert_spent_allowance_revoked(setting, sold)


def assert_first_step_recovered(setting, recovered, record):
    """The signed revocation is resolved from its receipt; the one not signed is dropped
    and its allowance stands, as the result reports."""
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (family.operations(recovered), "unresolved" in recovered) == (["revoke_erc20"], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)
    left = setting.allowances()
    assert ([row["spender"] for row in left], recovered["position_after"]["allowances"]) == (
        [setting.contracts.router],
        left,
    )


def test_an_interrupted_revocation_is_recovered_and_the_rest_is_revoked_by_the_next(
    setting, monkeypatch, capsys
):
    leave_swap_approvals(setting)
    start = setting.balances()

    def revoking(*, execute):
        return setting("revoke-exchange", execute=execute)

    record = family.stopped_while_waiting_for("revoke_erc20", revoking, monkeypatch, capsys)
    family.assert_blocked(revoking, record, "revoke_erc20")
    assert_first_step_recovered(setting, setting.recover(record), record)
    assert family.operations(revoke(setting)) == ["revoke_permit2"]
    assert setting.balances() == start


def test_a_native_pool_revokes_the_wrapped_tokens_approvals(native):
    coin = native.coin()
    leave_swap_approvals(native)
    revoked = revoke(native)
    assert family.operations(revoked) == ["revoke_erc20", "revoke_permit2"]
    # Only gas left the wallet's native balance.
    assert str(native.pool.currency0) == swaps.NATIVE_COIN and 0 < coin - native.coin() < 5 * 10**16


def test_every_declared_cell_has_a_reference_to_rehearse():
    chains = declared_chains()
    assert len(chains) == 8 and chains == liquidity.declared_chains()
    assert set(chains) == set(liquidity.REFERENCES)
