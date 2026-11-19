"""`almanak ax supply` and `ax withdraw` on deployed Comets, in one process each.

An isolated Anvil fork of each declared chain stands in for it behind the production
adapter's own planning, signing and reconciliation. Every command resolves the market
and reads the wallet's legs on the fork, as the real command does on its endpoint. It
is rehearsal evidence, not a mainnet run.
"""

import json
import time
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
# Base units: 100 USDC and 1 WETH.
BASE_AMOUNT = 100 * 10**6
COLLATERAL_AMOUNT = 10**18
# Above every reference Comet's minimum borrow, and far inside 1 WETH of collateral.
DEBT_AMOUNT = 200 * 10**6
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
    """Present the fork under the chain's own number, with a head as recent as a live one."""
    fork.rpc("anvil_setChainId", [chain_number(chain.chain_id)])
    fork.rpc.signing_chain = chain.chain_id
    fork.rpc("evm_setNextBlockTimestamp", [int(time.time())])
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

    def config(self, asset, action):
        """The deployment the command builds: market resolution and the wallet's legs."""
        venue = self.venue
        request = selection(
            "compound_v3",
            {"chain": str(venue.chain.chain_id), "comet": venue.comet, "asset": str(asset)},
        )
        args = SimpleNamespace(
            connector="compound_v3",
            public_rpc=None,
            max_gas_price_wei=500 * 10**9,
            max_transaction_fee=10**17,
            max_gas_limit=None,
        )
        provider = ax_actions.alchemy(venue.chain.chain_id)
        return ax_actions.action_config(
            args, request, action, self.wallet, provider, self.fork.rpc.endpoint
        )

    def __call__(self, action, asset, amount=None, *, execute):
        request = ActionRequest(action, amount)
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

    def send(self, to, signature, *arguments):
        """One transaction from the wallet outside the SDK, as its owner could send anywhere."""
        types = signature[signature.index("(") + 1 : -1].split(",")
        data = keccak(text=signature)[:4] + encode(types, arguments)
        self.fork.rpc.send(self.signer, to_checksum_address(to), "0x" + data.hex())

    def borrow(self, amount):
        self.send(self.venue.comet, "withdraw(address,uint256)", self.venue.base.reference, amount)

    def approve(self, asset, amount):
        self.send(asset.reference, "approve(address,uint256)", self.venue.comet, amount)


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
    """Signed base principal and collateral base units of the requested position."""
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
    withdrawn = desk("withdraw", venue.base, execute=True)
    assert (steps(withdrawn), owned(withdrawn)) == (WITHDRAWN, (0, 0))
    with pytest.raises(DataError, match="nothing to withdraw"):
        desk("withdraw", venue.base, execute=True)


def stopped_after_the_first_send(desk, venue, monkeypatch, capsys):
    """Run a supply that is interrupted while its approval waits for a receipt."""
    from almanak.operator_runtime import OperatorRuntime

    def stop(self, state):
        if state.pending is not None:
            raise KeyboardInterrupt

    with monkeypatch.context() as patched:
        patched.setattr(OperatorRuntime, "wait", stop)
        with pytest.raises(KeyboardInterrupt):
            desk("supply", venue.base, BASE_AMOUNT, execute=True)
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if "event" in line]
    (kept,) = [event for event in events if event.get("event") == "record_kept"]
    return Path(kept["path"])


def test_an_interrupted_supply_is_recovered_and_the_wallet_is_freed(
    desk, venue, monkeypatch, capsys
):
    record = stopped_after_the_first_send(desk, venue, monkeypatch, capsys)
    assert_blocked(desk, venue, record)
    assert_recovered(desk.recover(record), record)
    # The approval stands. A supply of another amount cannot use it and says so; the
    # same supply needs only its own step.
    with pytest.raises(DataError, match="Supply exactly that amount"):
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
    more = desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    assert more["allocations"] == ["position", held_name(venue.base)]
    assert owned(more) == (0, 2 * COLLATERAL_AMOUNT)
    assert_each_leg_withdrawn(desk, venue)


def assert_each_leg_withdrawn(desk, venue):
    assert steps(desk("withdraw", venue.base, execute=True)) == WITHDRAWN
    # With the base leg gone, the collateral is the wallet's only leg again.
    released = desk("withdraw", venue.collateral, execute=True)
    assert released["allocations"] == ["position"]
    assert (steps(released), owned(released)) == (WITHDRAWN, (0, 0))


def test_a_wallet_in_debt_may_add_collateral_but_not_repay_or_release_it(desk, venue):
    desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    desk.borrow(DEBT_AMOUNT)
    with pytest.raises(DataError, match="would repay that debt"):
        desk("supply", venue.base, BASE_AMOUNT, execute=True)
    with pytest.raises(DataError, match="nothing to withdraw.*the wallet owes"):
        desk("withdraw", venue.base, execute=True)
    with pytest.raises(DataError, match="needs a health floor"):
        desk("withdraw", venue.collateral, execute=True)
    added = desk("supply", venue.collateral, COLLATERAL_AMOUNT, execute=True)
    assert (added["allocations"], steps(added)) == (["position", held_name(venue.base)], SUPPLIED)
    assert owned(added) == (0, 2 * COLLATERAL_AMOUNT)
    assert ax_actions.kept_records() == []


def test_a_withdrawal_clears_an_allowance_left_for_the_comet(desk, venue):
    assert_supplied(desk("supply", venue.base, BASE_AMOUNT, execute=True), 0)
    # An unlimited allowance, as another interface may have asked the owner for.
    desk.approve(venue.base, 2**256 - 1)
    with pytest.raises(DataError, match="Supply exactly that amount"):
        desk("supply", venue.base, BASE_AMOUNT, execute=True)
    withdrawn = desk("withdraw", venue.base, execute=True)
    assert (steps(withdrawn), owned(withdrawn)) == ((["withdraw", "revoke"], True), (0, 0))
    assert_supplied(desk("supply", venue.base, BASE_AMOUNT, execute=True), 0)
