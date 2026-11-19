"""The `almanak ax` lending commands on deployed Comets, in one process each.

An isolated Anvil fork of each declared chain stands in for it behind the production
adapter's own planning, signing and reconciliation, at the block the chain's reference
fork fixture pins. Every command resolves the market and reads the wallet's legs on the
fork, as the real command does on its endpoint. It is rehearsal evidence, not a mainnet
run.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from eth_abi import encode
from eth_account import Account as Signer
from eth_utils import keccak, to_checksum_address

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.connectors.compound_v3.direct import held_name
from almanak.connectors.direct import ActionRequest
from almanak.connectors.lending_lookup import selection
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number
from almanak.gateway.fork_config import ForkTarget, provider_endpoint
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.safe_funding import fund_usdc_storage
from almanak.models import Chain
from tests.fork.production_gateway_driver import RehearsalGateway

ROOT = Path(__file__).parents[2]
pytestmark = pytest.mark.fork
PASSWORD = "public-test-password"
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=500 * 10**9, max_transaction_fee=10**17)
# Base units: 100 USDC and 1 WETH.
BASE_AMOUNT = 100 * 10**6
COLLATERAL_AMOUNT = 10**18
# Above every reference Comet's minimum borrow, and far inside 1 WETH of collateral.
DEBT_AMOUNT = 200 * 10**6
# A health factor of 1.5 at wad precision, which that debt clears with room.
FLOOR = 15 * 10**17
CHAINS = ("ethereum", "base", "arbitrum", "optimism")


def reference(name):
    """The reference's Comet, base asset and collateral asset, and its fork anchor."""
    author = yaml.safe_load(
        (ROOT / f"strategies/compound_v3_{name}_lending/config.yaml").read_text()
    )
    operator = yaml.safe_load(
        (ROOT / f"examples/operators/compound_v3_{name}_lending.fork.yaml").read_text()
    )
    markets, (anchor,) = author["markets"], operator["fork"]["anchors"]
    return SimpleNamespace(
        chain=Chain(chain_id=ChainId(anchor["chain"]), environment="mainnet"),
        comet=markets["earn"]["comet"],
        base=AssetType(markets["earn"]["asset"]),
        collateral=AssetType(markets["collateral"]["asset"]),
        anchor=anchor,
    )


@pytest.fixture(params=CHAINS)
def venue(request):
    return reference(request.param)


@pytest.fixture
def fork(request, venue):
    env = request.config.getoption("--fork-env").resolve()
    provider = ax_actions.alchemy(venue.chain.chain_id)
    with EthereumReader(venue.chain, provider_endpoint(provider, env)) as reader:
        origin = reader.pin_block(venue.anchor["height"])
    assert origin.hash == venue.anchor["reference"]
    target = ForkTarget(chain=venue.chain, provider=provider, block=origin)
    with Fork(env, origin, target=target) as running:
        yield running


def funded_wallet(fork, venue, directory):
    """A fresh plain account holding the base asset, the collateral asset and gas."""
    token = to_checksum_address(venue.collateral.reference)
    signer = fork.funded_signer(token, 4 * COLLATERAL_AMOUNT, 10 * 10**18)
    fund_usdc_storage(fork, signer.address.lower(), venue.base.reference, 10 * BASE_AMOUNT)
    directory.mkdir(mode=0o700)
    stores = {"treasury": Signer.encrypt(signer.key, PASSWORD, kdf="pbkdf2", iterations=1000)}
    write_private(directory / "keystores.json", json.dumps(stores))
    write_private(directory / "keystore-password", PASSWORD)
    wallet = {
        "account": "treasury",
        "address": signer.address,
        "keystores": str(directory / "keystores.json"),
        "password_file": str(directory / "keystore-password"),
    }
    write_private(directory / "wallet.json", json.dumps(wallet))
    return ax_actions.read_wallet(directory), signer


def as_origin_chain(fork, chain):
    """Present the fork under the chain's own number.

    The fork keeps the pinned block's time: the adapter's clock is the fork's own, and a
    jump to the present would age every price feed.
    """
    fork.rpc("anvil_setChainId", [chain_number(chain.chain_id)])
    fork.rpc.signing_chain = chain.chain_id
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


def backend(fork, config, wallet, identity):
    return RehearsalGateway(
        config,
        fork.rpc.endpoint,
        identity,
        Path(wallet["keystores"]),
        Path(wallet["password_file"]),
        None,
        fixture_clock=True,
    )


class Desk:
    """One wallet's commands against one Comet on the fork."""

    def __init__(self, fork, venue, wallet, signer, identity):
        self.fork, self.venue, self.wallet, self.signer = fork, venue, wallet, signer
        self.identity = identity

    def config(self, asset, request):
        """The deployment the command builds: market resolution and the wallet's legs."""
        venue = self.venue
        market = {"chain": str(venue.chain.chain_id), "comet": venue.comet, "asset": str(asset)}
        return ax_actions.deployment(
            "compound_v3",
            selection("compound_v3", market),
            request,
            self.wallet,
            ax_actions.alchemy(venue.chain.chain_id),
            self.fork.rpc.endpoint,
            LIMITS,
        )

    def __call__(self, action, asset, amount=None, *, execute, floor=None):
        request = ActionRequest(action, amount, min_health_factor=floor)
        config = self.config(asset, request)
        gateway = backend(self.fork, config, self.wallet, self.identity)
        try:
            result = ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()
        result["allocations"] = [item.name for item in config.allocations]
        return result

    def recover(self, record):
        return ax_actions.recover(
            record, lambda stored: backend(self.fork, stored, self.wallet, self.identity)
        )

    def approve(self, asset, amount):
        """Let the Comet spend an asset outside the SDK, as the wallet's owner could."""
        selector = keccak(text="approve(address,uint256)")[:4]
        data = selector + encode(("address", "uint256"), (self.venue.comet, amount))
        token = to_checksum_address(asset.reference)
        self.fork.rpc.send(self.signer, token, "0x" + data.hex())


@pytest.fixture
def desk(fork, venue, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    wallet, signer = funded_wallet(fork, venue, tmp_path / "wallet")
    identity = as_origin_chain(fork, venue.chain)
    return Desk(fork, venue, wallet, signer, identity)


SUPPLIED = (["approve", "supply"], True)
WITHDRAWN = (["withdraw"], True)


def steps(result):
    """The steps a command sent, and whether its request completed."""
    return [item["operation"] for item in result["outcomes"]], result["completed"]


def owned(result, moment="position_after"):
    """The wallet's signed base principal, and the requested asset's collateral units."""
    position = result[moment]
    return int(position["base_principal"]), int(position["collateral"])


def assert_preview(preview, venue):
    assert (preview["executed"], "outcomes" in preview) == (False, False)
    assert preview["first_step"]["operation"]["kind"] == "approve"
    assert preview["position_before"] == {
        "asset": str(venue.base),
        "base_principal": "0",
        "collateral": "0",
    }


def assert_supplied(result, before):
    assert steps(result) == SUPPLIED
    assert owned(result, "position_before")[0] == before < owned(result)[0]


def test_supply_then_withdraw_each_as_one_command(desk, venue):
    assert_preview(desk("supply", venue.base, BASE_AMOUNT, execute=False), venue)
    first = desk("supply", venue.base, BASE_AMOUNT, execute=True)
    assert_supplied(first, 0)
    # A later command adopts the position the account already holds and adds to it.
    assert_supplied(desk("supply", venue.base, BASE_AMOUNT, execute=True), owned(first)[0])
    assert_part_withdrawn(desk, venue)
    withdrawn = desk("withdraw", venue.base, execute=True)
    assert (steps(withdrawn), owned(withdrawn)) == (WITHDRAWN, (0, 0))
    with pytest.raises(DataError, match="nothing to withdraw"):
        desk("withdraw", venue.base, execute=True)


def assert_part_withdrawn(desk, venue):
    """An amount of the base supply leaves; an amount above it would borrow and is refused."""
    part = desk("withdraw", venue.base, BASE_AMOUNT, execute=True)
    assert steps(part) == WITHDRAWN
    assert 0 < owned(part)[0] < owned(part, "position_before")[0]
    with pytest.raises(DataError, match="would borrow, which `ax borrow` does"):
        desk("withdraw", venue.base, 2 * BASE_AMOUNT, execute=True)


def interrupted(command, kind, monkeypatch, capsys):
    """Run a command that is interrupted while its step of `kind` waits for a receipt."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None and state.pending.operation.kind == kind:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            command()
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def test_an_interrupted_supply_is_recovered_and_the_wallet_is_freed(
    desk, venue, monkeypatch, capsys
):
    record = interrupted(
        lambda: desk("supply", venue.base, BASE_AMOUNT, execute=True),
        "approve",
        monkeypatch,
        capsys,
    )
    assert_blocked(desk, venue, record)
    assert_recovered(desk.recover(record), record)
    # The approval stands. A supply of another amount cannot use it and says so; the
    # same supply needs only its own step.
    with pytest.raises(DataError, match="State exactly that amount"):
        desk("supply", venue.base, BASE_AMOUNT + 1, execute=True)
    assert steps(desk("supply", venue.base, BASE_AMOUNT, execute=True)) == (["supply"], True)


def assert_blocked(desk, venue, record):
    (listed,) = ax_actions.kept_records()
    assert (listed["path"], listed["unresolved"]["operation"]) == (str(record), "approve")
    assert listed["chain"] == str(venue.chain.chain_id)
    # The kept record blocks the wallet until its signed step is resolved.
    with pytest.raises(RuntimeError, match="already claimed"):
        desk("supply", venue.base, BASE_AMOUNT, execute=False)


def assert_recovered(recovered, record):
    assert (recovered["resolved"], recovered["discarded_request"]) == (True, True)
    assert (steps_sent(recovered), "unresolved" in recovered) == (["approve"], False)
    assert (ax_actions.kept_records(), record.exists()) == ([], False)


def steps_sent(recovered):
    return [item["operation"] for item in recovered["outcomes"]]


def test_a_wallet_holding_another_leg_keeps_it_through_each_command(desk, venue):
    held = desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    assert (steps(held), owned(held)) == (SUPPLIED, (0, COLLATERAL_AMOUNT))
    # The base command's deployment covers the collateral the wallet already holds.
    supplied = desk("supply", venue.base, BASE_AMOUNT, execute=True)
    assert supplied["allocations"] == ["position", held_name(venue.collateral)]
    assert_supplied(supplied, 0)
    # A collateral command reports the base principal the wallet holds beside it.
    more = desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    assert more["allocations"] == ["position", held_name(venue.base)]
    assert owned(more) == (owned(supplied)[0], 2 * COLLATERAL_AMOUNT)
    assert_each_leg_withdrawn(desk, venue)


def assert_each_leg_withdrawn(desk, venue):
    assert steps(desk("withdraw", venue.base, execute=True)) == WITHDRAWN
    # With the base leg gone, the collateral is the wallet's only leg again.
    released = desk("withdraw", venue.collateral, execute=True)
    assert released["allocations"] == ["position"]
    assert (steps(released), owned(released)) == (WITHDRAWN, (0, 0))


def test_a_withdrawal_clears_an_allowance_left_for_the_comet(desk, venue):
    assert_supplied(desk("supply", venue.base, BASE_AMOUNT, execute=True), 0)
    # An unlimited allowance, as another interface may have asked the owner for.
    desk.approve(venue.base, 2**256 - 1)
    with pytest.raises(DataError, match="State exactly that amount"):
        desk("supply", venue.base, BASE_AMOUNT, execute=True)
    withdrawn = desk("withdraw", venue.base, execute=True)
    assert (steps(withdrawn), owned(withdrawn)) == ((["withdraw", "revoke"], True), (0, 0))
    assert_supplied(desk("supply", venue.base, BASE_AMOUNT, execute=True), 0)


def test_a_debt_is_opened_and_closed_by_separate_commands(desk, venue):
    """Borrow the base asset against supplied collateral, then every way out, each one
    command that adopts what the earlier ones left."""
    held = desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    assert (steps(held), owned(held)) == (SUPPLIED, (0, COLLATERAL_AMOUNT))
    assert_borrowed(desk, venue)
    assert_collateral_moves_beside_the_debt(desk, venue)
    assert_repaid(desk, venue)
    # Without debt the collateral leaves in full, and no floor is asked for.
    released = desk("withdraw", venue.collateral, execute=True)
    assert (released["allocations"], steps(released)) == (["position"], WITHDRAWN)
    assert (owned(released), ax_actions.kept_records()) == ((0, 0), [])


def assert_borrowed(desk, venue):
    # With no base supply a base withdrawal has nothing to take: it never borrows.
    with pytest.raises(DataError, match="nothing to withdraw"):
        desk("withdraw", venue.base, DEBT_AMOUNT, execute=False)
    # A floor no position with debt reaches.
    with pytest.raises(DataError, match="below the configured minimum"):
        desk("borrow", venue.base, DEBT_AMOUNT, execute=False, floor=10**60)
    opened = desk("borrow", venue.base, DEBT_AMOUNT, execute=True, floor=FLOOR)
    assert opened["allocations"] == ["position", held_name(venue.collateral)]
    assert steps(opened) == (["borrow"], True)
    assert owned(opened, "position_before")[0] == 0 > owned(opened)[0]
    # In debt a base supply would repay, which is the repay command's to do.
    with pytest.raises(DataError, match="would repay that debt; use `ax repay`"):
        desk("supply", venue.base, BASE_AMOUNT, execute=False)


def assert_collateral_moves_beside_the_debt(desk, venue):
    part = COLLATERAL_AMOUNT // 10
    # A collateral withdrawal is risk-checked while debt is open, so it states a floor.
    with pytest.raises(ValueError, match="state --min-health-factor"):
        desk("withdraw", venue.collateral, part, execute=False)
    taken = desk("withdraw", venue.collateral, part, execute=True, floor=FLOOR)
    assert steps(taken) == WITHDRAWN
    # The command for the collateral asset reports the debt as a negative principal.
    assert owned(taken)[0] < 0 and owned(taken)[1] == COLLATERAL_AMOUNT - part
    # Adding collateral is not risk-checked and needs no floor.
    added = desk("supply", venue.collateral, part, execute=True)
    assert (steps(added), owned(added)[1]) == (SUPPLIED, COLLATERAL_AMOUNT)


def assert_repaid(desk, venue):
    some = desk("repay", venue.base, DEBT_AMOUNT // 2, execute=True)
    assert steps(some) == (["approve", "repay"], True)
    assert owned(some, "position_before")[0] < owned(some)[0] < 0
    # The whole debt: an approval of the held balance, the repayment, then the
    # revocation of what the approval left.
    closed = desk("repay", venue.base, execute=True)
    assert steps(closed) == (["approve", "repay", "revoke"], True)
    assert owned(closed) == (0, 0)
    with pytest.raises(DataError, match="nothing to repay"):
        desk("repay", venue.base, execute=False)


@pytest.mark.parametrize("venue", ["ethereum"], indirect=True)
def test_a_full_repayment_stopped_before_its_revocation_is_finished_by_repaying_again(
    desk, venue, monkeypatch, capsys
):
    desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    desk("borrow", venue.base, DEBT_AMOUNT, execute=True, floor=FLOOR)
    # The repayment is mined, and the command stops before it signs the revocation.
    record = interrupted(
        lambda: desk("repay", venue.base, execute=True), "repay", monkeypatch, capsys
    )
    recovered = desk.recover(record)
    assert (recovered["resolved"], steps_sent(recovered)) == (True, ["approve", "repay"])
    # No debt is left, and the approval of the held balance stands: the same command
    # plans the revocation alone.
    cleared = desk("repay", venue.base, execute=True)
    assert (steps(cleared), owned(cleared)) == ((["revoke"], True), (0, 0))
    with pytest.raises(DataError, match="nothing to repay") as nothing:
        desk("repay", venue.base, execute=False)
    assert "allowance" not in str(nothing.value)
