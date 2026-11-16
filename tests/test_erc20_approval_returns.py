"""ERC-20 approve returns follow SafeERC20: empty or exactly ABI true; nothing else."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v3 import execution as aave_v3
from almanak.connectors.aave_v3.operations import ApproveOperation as AaveApprove
from almanak.connectors.aave_v3.operations import RevokeOperation as AaveRevoke
from almanak.connectors.aave_v4 import execution as aave_v4
from almanak.connectors.aave_v4.operations import ApproveOperation as AaveV4Approve
from almanak.connectors.aave_v4.operations import RevokeOperation as AaveV4Revoke
from almanak.connectors.aerodrome import simulation as aerodrome
from almanak.connectors.aerodrome.operations import Approval as AerodromeApproval
from almanak.connectors.compound_v3 import execution as compound
from almanak.connectors.compound_v3.operations import ApproveOperation as CometApprove
from almanak.connectors.compound_v3.operations import RevokeOperation as CometRevoke
from almanak.connectors.compound_v3.operations import SupplyOperation as CometSupply
from almanak.connectors.curve import simulation as curve
from almanak.connectors.curve.operations import Approval as CurveApproval
from almanak.connectors.curve.operations import Swap as CurveSwap
from almanak.connectors.erc20_approval import ABI_TRUE, approval_succeeded
from almanak.connectors.gmx_v2 import abi as gmx
from almanak.connectors.gmx_v2 import exchange_abi as gmx_exchange
from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval
from almanak.connectors.gmx_v2.operations import Approval as GmxApproval
from almanak.connectors.morpho_blue import execution as morpho
from almanak.connectors.morpho_blue.operations import ApproveOperation as MorphoApprove
from almanak.connectors.morpho_blue.operations import RepayOperation as MorphoRepay
from almanak.connectors.morpho_blue.operations import RevokeOperation as MorphoRevoke
from almanak.connectors.morpho_blue.operations import SupplyOperation as MorphoSupply
from almanak.connectors.morpho_blue.operations import WithdrawOperation as MorphoWithdraw
from almanak.connectors.sparklend import execution as spark
from almanak.connectors.sparklend.operations import ApproveOperation as SparkApprove
from almanak.connectors.sparklend.operations import RevokeOperation as SparkRevoke
from almanak.connectors.uniswap_v3.operations import Approval as V3Approval
from almanak.connectors.uniswap_v3.simulation import validate_simulation as uniswap_v3
from almanak.connectors.uniswap_v4.deployments import NETWORK_ROLES
from almanak.connectors.uniswap_v4.domain import UniswapPool
from almanak.connectors.uniswap_v4.execution import UniswapExecution
from almanak.connectors.uniswap_v4.models import Deployment, Target
from almanak.connectors.uniswap_v4.observation_models import Allowance
from almanak.connectors.uniswap_v4.planning import approval
from almanak.connectors.uniswap_v4.state import Approval as V4Approval
from almanak.connectors.uniswap_v4.state import Swap as V4Swap
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Account, Chain

USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"


def usdc_usdt_target():
    """The hookless fee-10 Ethereum USDC/USDT pool a fork run round-tripped."""
    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    roles = NETWORK_ROLES["eip155:1"]
    pool = UniswapPool(
        kind="uniswap_v4",
        chain=chain,
        pool_manager=roles["pool_manager"],
        state_view=roles["state_view"],
        currency0=AssetType(f"eip155:1/erc20:{USDC}"),
        currency1=AssetType(f"eip155:1/erc20:{USDT}"),
        fee=10,
        tick_spacing=1,
        hooks=ZERO_ADDRESS,
    )
    return Target(
        allocation="trade",
        account="fund",
        fund=Account(chain=chain, address="0x" + "ab" * 20),
        pool=pool,
        contracts=Deployment(
            router=roles["router"],
            position_manager=roles["position_manager"],
            quoter=roles["quoter"],
            permit2=roles["permit2"],
        ),
        range=None,
        funding_asset=None,
    )


FALSE = encode(("bool",), (False,))
MALFORMED = {
    "short": ABI_TRUE[:31],
    "long": ABI_TRUE + bytes(32),
    "noncanonical_true": (2).to_bytes(32, "big"),
    "single_byte": b"\x01",
}


def hexed(raw):
    return "0x" + raw.hex()


def v4_erc20(amount):
    return V4Approval.model_construct(
        kind="approve_erc20" if amount else "revoke_erc20", amount=amount, expiration=None
    )


# (validator, approval operation) for every connector's ERC-20 approve/revoke step.
APPROVALS = {
    "uniswap_v4_approve": (UniswapExecution.validate_simulation, v4_erc20(10)),
    "uniswap_v4_revoke": (UniswapExecution.validate_simulation, v4_erc20(0)),
    "uniswap_v3_family": (uniswap_v3, V3Approval.model_construct(amount=10)),
    "curve": (curve.validate_simulation, CurveApproval.model_construct(amount=10)),
    "aerodrome": (aerodrome.validate_simulation, AerodromeApproval.model_construct(amount=10)),
    "aave_v3_approve": (aave_v3.validate_simulation, AaveApprove.model_construct()),
    "aave_v3_revoke": (aave_v3.validate_simulation, AaveRevoke.model_construct()),
    "sparklend_approve": (spark.validate_simulation, SparkApprove.model_construct()),
    "sparklend_revoke": (spark.validate_simulation, SparkRevoke.model_construct()),
    "morpho_approve": (morpho.validate_simulation, MorphoApprove.model_construct()),
    "morpho_revoke": (morpho.validate_simulation, MorphoRevoke.model_construct()),
    "compound_approve": (compound.validate_simulation, CometApprove.model_construct()),
    "compound_revoke": (compound.validate_simulation, CometRevoke.model_construct()),
    "aave_v4_approve": (
        lambda op, result, before: aave_v4.validate_simulation(op, result, 0, 0),
        AaveV4Approve.model_construct(),
    ),
    "aave_v4_revoke": (
        lambda op, result, before: aave_v4.validate_simulation(op, result, 0, 0),
        AaveV4Revoke.model_construct(),
    ),
    "gmx": (gmx.validate_simulation, GmxApproval.model_construct()),
    "gmx_exchange": (
        lambda op, result, before: gmx_exchange.validate_simulation(op, result),
        ExchangeApproval.model_construct(),
    ),
}


def test_helper_accepts_only_empty_or_canonical_true():
    assert approval_succeeded(b"")
    assert approval_succeeded(ABI_TRUE)
    assert not approval_succeeded(FALSE)
    for raw in MALFORMED.values():
        assert not approval_succeeded(raw)


@pytest.mark.parametrize("name", sorted(APPROVALS))
def test_every_connector_accepts_empty_and_true_approve_returns(name):
    validate, operation = APPROVALS[name]
    validate(operation, "0x", None)
    validate(operation, hexed(ABI_TRUE), None)


@pytest.mark.parametrize("name", sorted(APPROVALS))
@pytest.mark.parametrize("raw", [FALSE, *MALFORMED.values()], ids=["false", *MALFORMED])
def test_every_connector_refuses_false_or_malformed_approve_returns(name, raw):
    validate, operation = APPROVALS[name]
    with pytest.raises((DataError, ValueError)):
        validate(operation, hexed(raw), None)


def test_non_approve_calls_keep_their_return_validation():
    with pytest.raises(DataError, match="malformed"):
        curve.validate_simulation(
            CurveSwap.model_construct(minimum=SimpleNamespace(amount=1)), "0x"
        )
    loan = MorphoSupply.model_construct(
        target=SimpleNamespace(reserve=SimpleNamespace(is_loan=True))
    )
    with pytest.raises(DataError, match="assets and shares"):
        morpho.validate_simulation(loan, "0x", None)
    with pytest.raises(DataError, match="unexpected"):
        compound.validate_simulation(CometSupply.model_construct(), hexed(ABI_TRUE), None)


def test_permit2_approve_and_v4_swaps_still_require_empty_returns():
    permit = V4Approval.model_construct(kind="approve_permit2", amount=10, expiration=5)
    UniswapExecution.validate_simulation(permit, "0x", None)
    for operation in (permit, V4Swap.model_construct()):
        with pytest.raises(DataError, match="unexpected simulation bytes"):
            UniswapExecution.validate_simulation(operation, hexed(ABI_TRUE), None)


def snapshot(token, permit, expiration=10**10, reference=USDC):
    selected = usdc_usdt_target()
    usdc = AssetType(f"eip155:1/erc20:{reference}")
    return SimpleNamespace(
        target=selected,
        block=SimpleNamespace(timestamp=100),
        allowances=(
            Allowance(asset=usdc, spender=selected.contracts.permit2, amount=token),
            Allowance(
                asset=usdc, spender=selected.contracts.router, amount=permit, expiration=expiration
            ),
        ),
    ), usdc


def test_v4_clears_a_residual_usdt_allowance_before_a_larger_approve():
    before, usdt = snapshot(token=7, permit=0, reference=USDT)
    step = approval(before, usdt, 100, before.target.contracts.router, 500)
    assert (step.kind, step.amount, step.spender) == (
        "revoke_erc20",
        0,
        before.target.contracts.permit2,
    )


def test_v4_approves_other_tokens_over_a_residual_directly():
    before, usdc = snapshot(token=7, permit=0)
    step = approval(before, usdc, 100, before.target.contracts.router, 500)
    assert (step.kind, step.amount) == ("approve_erc20", 100)
    before, usdc = snapshot(token=0, permit=0)
    step = approval(before, usdc, 100, before.target.contracts.router, 500)
    assert (step.kind, step.amount, step.expiration) == ("approve_erc20", 100, None)


def test_v4_permit2_allowance_is_overwritten_directly_and_sufficient_authority_is_reused():
    before, usdc = snapshot(token=100, permit=7)
    step = approval(before, usdc, 100, before.target.contracts.router, 500)
    assert (step.kind, step.amount, step.expiration) == ("approve_permit2", 100, 500)
    before, usdc = snapshot(token=100, permit=100, expiration=50)
    step = approval(before, usdc, 100, before.target.contracts.router, 500)
    assert step.kind == "approve_permit2"
    before, usdc = snapshot(token=100, permit=100)
    assert approval(before, usdc, 100, before.target.contracts.router, 500) is None


def morpho_operation(kind, is_loan=True, amount=5, shares=0):
    return kind.model_construct(
        target=SimpleNamespace(reserve=SimpleNamespace(is_loan=is_loan)),
        amount=SimpleNamespace(amount=amount),
        shares=shares,
    )


def words(*values):
    return hexed(encode(("uint256",) * len(values), values))


@pytest.mark.parametrize(
    ("operation", "result"),
    [
        (morpho_operation(MorphoSupply, is_loan=False), "0x"),
        (morpho_operation(MorphoSupply), words(5, 3)),
        (morpho_operation(MorphoRepay, shares=3), words(4, 3)),
        (morpho_operation(MorphoWithdraw, shares=3), words(9, 3)),
    ],
)
def test_morpho_protocol_returns_that_match_their_operation_pass(operation, result):
    morpho.validate_simulation(operation, result, None)


@pytest.mark.parametrize(
    ("operation", "result", "message"),
    [
        (morpho_operation(MorphoSupply, is_loan=False), hexed(ABI_TRUE), "unexpected data"),
        (morpho_operation(MorphoSupply), hexed(ABI_TRUE), "assets and shares"),
        (morpho_operation(MorphoSupply), words(4, 3), "changed its quantity"),
        (morpho_operation(MorphoSupply), words(5, 0), "no share effect"),
        (morpho_operation(MorphoRepay, shares=3), words(4, 2), "changed its shares"),
        (morpho_operation(MorphoRepay, shares=3), words(6, 3), "exceeded its bound"),
    ],
)
def test_morpho_protocol_returns_that_differ_are_refused(operation, result, message):
    with pytest.raises(DataError, match=message):
        morpho.validate_simulation(operation, result, None)
