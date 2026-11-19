"""`almanak ax borrow` and `ax repay` on deployed Aave V4 Spokes, one command each.

One isolated Anvil fork per chain stands in for mainnet behind the production adapter's
own planning, signing and reconciliation, at the block the borrowing reference's fork
fixture pins. The market is resolved and the deployment built by the command's own code.
It is rehearsal evidence, not a mainnet run.
"""

import json
from pathlib import Path
from typing import NamedTuple

import pytest
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.configuration.yaml import load
from almanak.connectors.direct import ActionRequest
from almanak.connectors.lending_lookup import selection
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import fund_erc20
from almanak.gateway.read_endpoints import read_endpoint
from almanak.models import Chain
from tests.fork.production_gateway_driver import RehearsalGateway

pytestmark = pytest.mark.fork
ROOT = Path(__file__).parents[2]
REFERENCE = "aave_v4_multichain_borrowing"
# Base units of the wrapped native asset supplied as collateral, and of the borrowed
# asset, which has six decimals on both chains.
COLLATERAL, BORROWED = 10**18, 10**6
FLOOR = 15 * 10**17
# A floor no position meets.
UNREACHABLE = 10**6 * 10**18
PASSWORD = "public-test-password"
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=5000 * 10**9, max_transaction_fee=10**19)
# Per chain of the reference: a second reserve of its Spoke with a collateral factor and
# one without, at the pinned block.
CELLS = {"ethereum": (8, 9), "avalanche": (3, 5)}


class Cell(NamedTuple):
    chain: ChainId
    anchor: dict
    spoke: str
    # Reserve IDs: the reference's collateral and debt, then the two named in CELLS.
    collateral: int
    debt: int
    second: int
    inert: int
    borrowed: AssetType


def fixture_of(name):
    """One chain's pinned block, Spoke, reserve IDs and borrowed asset in the reference."""
    fork = load(ROOT / f"examples/operators/{REFERENCE}.fork.yaml").value["fork"]
    markets = load(ROOT / f"strategies/{REFERENCE}/config.yaml").value["markets"]
    collateral, debt = markets[f"{name}_collateral_market"], markets[f"{name}_debt_market"]
    chain = ChainId(collateral["chain"])
    (anchor,) = [item for item in fork["anchors"] if item["chain"] == str(chain)]
    wrapped = evm_network(chain).wrapped_native
    funded = [AssetType(a["asset"]) for fund in fork["funds"] for a in fund["assets"]]
    (borrowed,) = [asset for asset in funded if asset.chain == chain and asset != wrapped]
    assert collateral["spoke"] == debt["spoke"]
    reserves = (collateral["reserve_id"], debt["reserve_id"], *CELLS[name])
    return Cell(chain, anchor, collateral["spoke"], *reserves, borrowed)


def origin_block(env, chain, anchor):
    with EthereumReader(
        Chain(chain_id=chain, environment="mainnet"), read_endpoint(chain, env)
    ) as r:
        block = r.pin_block(anchor["height"])
    assert block.hash == anchor["reference"]
    return block


def funded_wallet(fork, token, directory):
    signer = fork.funded_signer(to_checksum_address(token), 4 * COLLATERAL, 100 * 10**18)
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
    return ax_actions.read_wallet(directory)


def as_mainnet(fork, chain):
    fork.rpc("anvil_setChainId", [chain_number(chain)])
    # The fork keeps the pinned block's time: the adapter's clock is the fork's own, and
    # a jump to the present would age every price feed past its staleness bound.
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


class Desk:
    """One wallet on one fork; every action is its own deployment, as each command's is."""

    def __init__(self, fork, cell, tmp_path):
        self.fork, self.cell = fork, cell
        self.collateral, self.debt = cell.collateral, cell.debt
        wrapped = erc20_address(evm_network(cell.chain).wrapped_native, cell.chain)
        self.wallet = funded_wallet(fork, wrapped, tmp_path / "wallet")
        self.identity = as_mainnet(fork, cell.chain)

    def backend(self, config):
        return RehearsalGateway(
            config,
            self.fork.rpc.endpoint,
            self.identity,
            Path(self.wallet["keystores"]),
            Path(self.wallet["password_file"]),
            None,
            fixture_clock=True,
        )

    def deployment(self, request, reserve_id):
        cell = self.cell
        market = {"chain": str(cell.chain), "spoke": cell.spoke, "reserve_id": reserve_id}
        return ax_actions.deployment(
            "aave_v4",
            selection("aave_v4", market),
            request,
            self.wallet,
            ax_actions.alchemy(cell.chain),
            self.fork.rpc.endpoint,
            LIMITS,
        )

    def __call__(self, action, amount=None, *, execute=True, reserve=None, floor=None):
        """Run one command on the collateral reserve, or on `reserve`."""
        request = ActionRequest(action, amount, min_health_factor=floor)
        config = self.deployment(request, self.collateral if reserve is None else reserve)
        gateway = self.backend(config)
        try:
            return ax_actions.run(config, gateway, request, execute=execute)
        finally:
            gateway.close()

    def fund(self, reserve_id, amount):
        """Fund the wallet with a reserve's asset, as the command's own resolution names it."""
        config = self.deployment(ActionRequest("supply", amount), reserve_id)
        asset = config.allocations[0].reserve.asset
        fund_erc20(self.fork, self.wallet["address"], asset, amount)


@pytest.fixture(params=CELLS)
def desk(request, tmp_path, monkeypatch):
    # Records and claims stay out of the operator's own state: the first action sweeps
    # every record it finds there.
    monkeypatch.setattr("almanak.state.claims_root", lambda: tmp_path / "state/claims")
    cell = fixture_of(request.param)
    env = request.config.getoption("--fork-env").resolve()
    origin = origin_block(env, cell.chain, cell.anchor)
    target = ForkTarget(chain=origin.chain, provider=ax_actions.alchemy(cell.chain), block=origin)
    with Fork(env, origin, target=target) as fork:
        desk = Desk(fork, cell, tmp_path)
        # The wallet holds the borrowed asset already, so a full repayment covers interest.
        fund_erc20(fork, desk.wallet["address"], cell.borrowed, 10 * BORROWED)
        yield desk


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def shares(result, moment, kind):
    return int(result[moment][f"{kind}_shares"])


def supplied(desk, amount=COLLATERAL, reserve=None):
    result = desk("supply", amount, reserve=reserve)
    assert operations(result) == ["approve", "supply"] and result["completed"]
    return result


def withdrawn(desk, reserve=None):
    result = desk("withdraw", reserve=reserve)
    assert operations(result) == ["withdraw"] and result["completed"]
    assert shares(result, "position_after", "supplied") == 0


def test_a_debt_is_opened_and_closed_by_separate_commands(desk):
    """Borrow against supplied collateral, repay in full, then withdraw the collateral,
    each one command that adopts what the earlier ones left."""
    with pytest.raises(ValueError, match="has supplied no collateral on this Spoke"):
        desk("borrow", BORROWED, execute=False, reserve=desk.debt, floor=FLOOR)
    supplied(desk)

    preview = desk("borrow", BORROWED, execute=False, reserve=desk.debt, floor=FLOOR)
    # A supply is not collateral on a Spoke until the borrow enables it.
    assert preview["first_step"]["operation"]["kind"] == "collateral"
    opened = desk("borrow", BORROWED, reserve=desk.debt, floor=FLOOR)
    assert operations(opened) == ["collateral", "borrow"] and opened["completed"]
    assert shares(opened, "position_before", "debt") == 0 < shares(opened, "position_after", "debt")

    # The collateral is enabled now, so a borrow the floor refuses signs nothing.
    with pytest.raises(DataError, match="projected account health is below") as refused:
        desk("borrow", BORROWED, reserve=desk.debt, floor=UNREACHABLE)
    assert refused.value.kind == "risk_limit"
    assert_held_while_indebted(desk)

    closed = desk("repay", reserve=desk.debt)
    assert operations(closed) == ["approve", "repay", "revoke", "collateral"]
    assert closed["completed"] and shares(closed, "position_after", "debt") == 0
    # The repayment released the collateral, and nothing is left for another one.
    with pytest.raises(ValueError, match="no remaining debt, allowance or collateral"):
        desk("repay", execute=False, reserve=desk.debt)
    withdrawn(desk)


def assert_held_while_indebted(desk):
    """What the other commands do while the debt is open."""
    with pytest.raises(ValueError, match="is collateral for the wallet's debt in reserve"):
        desk("withdraw", execute=False)
    # More collateral is always admitted: a supply never lowers the account's health.
    more = supplied(desk)
    assert shares(more, "position_before", "supplied") < shares(more, "position_after", "supplied")
    with pytest.raises(ValueError, match="serves one debt reserve per wallet and Spoke"):
        desk("borrow", BORROWED, execute=False, reserve=desk.cell.second, floor=FLOOR)
    # The connector binds one position per reserve, so the owed reserve takes no supply.
    with pytest.raises(DataError, match="observed exposure differs from owned supply") as owed:
        desk("supply", BORROWED, execute=False, reserve=desk.debt)
    assert owed.value.kind == "ownership_mismatch"


def test_a_first_borrow_the_floor_refuses_leaves_collateral_that_still_withdraws(desk):
    """The collateral is enabled before the borrow is projected against the floor."""
    supplied(desk)
    refused = desk("borrow", BORROWED, reserve=desk.debt, floor=UNREACHABLE)
    assert operations(refused) == ["collateral"] and not refused["completed"]
    assert refused["refused"]["kind"] == "risk_limit"
    assert shares(refused, "position_after", "debt") == 0
    # A repayment with no debt releases the collateral the refused borrow enabled.
    released = desk("repay", reserve=desk.debt)
    assert operations(released) == ["collateral"] and released["completed"]

    again = desk("borrow", BORROWED, reserve=desk.debt, floor=UNREACHABLE)
    assert operations(again) == ["collateral"] and again["refused"]["kind"] == "risk_limit"
    # With no debt on the Spoke, enabled collateral withdraws as any supply does.
    withdrawn(desk)


def test_a_borrow_enables_every_supply_that_can_back_it(desk):
    """Every reserve the wallet supplies is part of the debt's position; the one without
    a collateral factor backs nothing and is left as it is."""
    supplied(desk)
    for reserve in (desk.cell.second, desk.cell.inert):
        desk.fund(reserve, BORROWED)
        supplied(desk, BORROWED, reserve)
    opened = desk("borrow", BORROWED, reserve=desk.debt, floor=FLOOR)
    assert operations(opened) == ["collateral", "collateral", "borrow"] and opened["completed"]
    # A supply that is not collateral withdraws while the debt is open.
    withdrawn(desk, desk.cell.inert)
    with pytest.raises(ValueError, match="is collateral for the wallet's debt in reserve"):
        desk("withdraw", execute=False, reserve=desk.cell.second)
    closed = desk("repay", reserve=desk.debt)
    assert operations(closed) == ["approve", "repay", "revoke", "collateral", "collateral"]
    assert closed["completed"]
    withdrawn(desk, desk.cell.second)
    withdrawn(desk)
