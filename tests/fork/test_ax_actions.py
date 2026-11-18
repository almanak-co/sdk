"""`almanak ax supply` and `ax withdraw` on deployed Aave V4 contracts, in one process each.

An isolated Anvil fork stands in for mainnet behind the production adapter's own
planning, signing and reconciliation. It is rehearsal evidence, not a mainnet run.
"""

import json
import time

import pytest
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak import ax_actions
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.credentials import write_private
from almanak.gateway.evm import erc20_address
from almanak.gateway.fork_fixture import Fork
from tests.fork.production_gateway_driver import RehearsalGateway
from tests.fork.test_lifecycle import FORK_CONFIG

pytestmark = pytest.mark.fork
AMOUNT = 10**16
PASSWORD = "public-test-password"


@pytest.fixture
def fork(request):
    config = ForkConfig.model_validate_json(FORK_CONFIG.read_text())
    with Fork(request.config.getoption("--fork-env").resolve(), config.origin) as running:
        running.fixture = config
        yield running


def funded_wallet(fork, reserve, directory):
    token = to_checksum_address(erc20_address(reserve.asset, reserve.chain.chain_id))
    signer = fork.funded_signer(token, 4 * AMOUNT, 5 * 10**18)
    directory.mkdir(mode=0o700)
    # Any account name: the action binds the wallet under its keystore entry's name.
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


def as_mainnet(fork):
    fork.rpc("anvil_setChainId", [1])
    fork.rpc("evm_setNextBlockTimestamp", [int(time.time())])
    fork.rpc("evm_mine")
    return fork.rpc("anvil_metadata")


def backend(fork, config, wallet, identity):
    return RehearsalGateway(
        config,
        fork.rpc.endpoint,
        identity,
        ax_actions.Path(wallet["keystores"]),
        ax_actions.Path(wallet["password_file"]),
        None,
        fixture_clock=True,
    )


def act(fork, config, wallet, identity, action, amount=None, *, execute):
    gateway = backend(fork, config, wallet, identity)
    try:
        return ax_actions.run(config, gateway, action, amount, execute=execute)
    finally:
        gateway.close()


@pytest.fixture
def setting(fork, tmp_path):
    origin = fork.fixture.origin.chain
    reserve = AaveReserve.model_validate(
        {**fork.fixture.allocations[0].reserve.model_dump(), "chain": origin}
    )
    wallet = funded_wallet(fork, reserve, tmp_path / "wallet")
    identity = as_mainnet(fork)
    config = ax_actions.lending_config(
        reserve,
        wallet,
        ax_actions.alchemy(origin.chain_id),
        max_gas_price_wei=500 * 10**9,
        max_transaction_fee=10**17,
    )
    return lambda action, amount=None, *, execute: act(
        fork, config, wallet, identity, action, amount, execute=execute
    )


def operations(result):
    return [item["operation"] for item in result["outcomes"]]


def shares(result, moment):
    return int(result[moment]["supplied_shares"])


def assert_preview(preview):
    assert preview["executed"] is False and "outcomes" not in preview
    assert preview["first_step"]["operation"]["kind"] == "approve"
    assert shares(preview, "position_before") == 0


def assert_supplied(result, before):
    assert operations(result) == ["approve", "supply"] and result["completed"]
    assert shares(result, "position_before") == before
    assert shares(result, "position_after") > before


def test_supply_then_withdraw_each_as_one_command(setting):
    assert_preview(setting("supply", AMOUNT, execute=False))
    first = setting("supply", AMOUNT, execute=True)
    assert_supplied(first, 0)
    # A later command adopts the position the account already holds and adds to it.
    assert_supplied(setting("supply", AMOUNT, execute=True), shares(first, "position_after"))
    withdrawn = setting("withdraw", execute=True)
    assert operations(withdrawn) == ["withdraw"] and withdrawn["completed"]
    assert shares(withdrawn, "position_after") == 0
