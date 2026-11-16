"""Plain EOA admission refuses native-receive delegation before spending or funding."""

from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.gateway.accounts import require_plain_eoa
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.fork_fixture import Fork
from almanak.models import Account, AccountBinding, Chain, Eoa, StrategyAccounts

ADDRESS = "0x" + "12" * 20
DELEGATED = "0xef0100" + "34" * 20


@pytest.mark.parametrize("code", [DELEGATED, "0x6000", "0x0", None])
def test_nonempty_or_missing_code_is_not_plain_eoa_authority(code):
    with pytest.raises(DataError, match="EOA fund must have empty code"):
        require_plain_eoa(lambda method, params: code, ADDRESS)


def test_gateway_rechecks_fund_code_after_prior_authority_check():
    reads = []
    codes = iter(("0x", DELEGATED))

    def rpc(method, params):
        reads.append((method, params))
        return next(codes)

    accounts = StrategyAccounts(
        bindings=(
            AccountBinding(
                name="fund",
                profile=Eoa(
                    kind="eoa",
                    fund=Account(
                        chain=Chain(chain_id=ChainId("eip155:1"), environment="mainnet"),
                        address=ADDRESS,
                    ),
                ),
            ),
        )
    )
    backend = SimpleNamespace(
        config=SimpleNamespace(accounts=accounts), reader=SimpleNamespace(_rpc=rpc)
    )
    EvmGateway.verify_authority(backend, None, "fund")
    with pytest.raises(DataError, match="delegated accounts are unsupported"):
        EvmGateway.verify_authority(backend, None, "fund")
    assert reads == [("eth_getCode", [ADDRESS, "latest"])] * 2


@pytest.mark.parametrize("code", ["0x", DELEGATED])
def test_fork_checks_plain_fund_before_native_balance_mutation(monkeypatch, code):
    signer = SimpleNamespace(address=ADDRESS)
    monkeypatch.setattr(
        "almanak.gateway.fork_fixture.Signer", SimpleNamespace(create=lambda: signer)
    )
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        if method == "eth_getCode":
            return code
        if method == "eth_getBalance":
            return "0x64"
        assert method == "anvil_setBalance"
        return True

    node = object.__new__(Fork)
    node.rpc = rpc
    if code == "0x":
        assert node.funded_signer("", 0, 100, plain_account=True) is signer
        assert [method for method, _ in calls] == [
            "eth_getCode",
            "anvil_setBalance",
            "eth_getBalance",
        ]
    else:
        with pytest.raises(DataError, match="EOA fund must have empty code"):
            node.funded_signer("", 0, 100, plain_account=True)
        assert calls == [("eth_getCode", [ADDRESS, "latest"])]
