"""Fixed Permit2 allowances and bounded approval changes on both money paths."""

from copy import deepcopy
from decimal import Decimal
from unittest.mock import patch

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.approvals import approval_amounts, observe_permit2_allowance
from almanak.connectors.uniswap_v4.operation import transaction_digest, validate_execution
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.sdk import PERMIT2_ADDRESS, SwapQuote
from almanak.framework.execution.interfaces import ConnectorValidationError
from almanak.framework.intents.vocabulary import SwapIntent
from tests.unit.connectors.uniswap_v4.test_lp_lifecycle_identity import lp_open
from tests.unit.connectors.uniswap_v4.test_position_observation import TOKEN, WALLET, ZERO, PositionGateway

ALLOWANCE = keccak(text="allowance(address,address)")[:4]


class AllowanceGateway(PositionGateway):
    allowance = 2**256 - 1
    malformed = None

    def __init__(self):
        super().__init__()
        self.allowance_reads = []

    def read(self, *, payload, **kwargs):
        if payload[:4] == ALLOWANCE:
            self.allowance_reads.append({"payload": payload, **kwargs})
            return self.malformed if self.malformed is not None else encode(["uint256"], [self.allowance])
        return super().read(payload=payload, **kwargs)


def compile_operation(gateway, mode):
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET),
        venue_verification_gateway_factory=lambda: gateway,
    )
    key = PoolKey(ZERO, TOKEN, 1000, 10)
    if mode == "lp":
        return lp_open(adapter, key)
    intent = SwapIntent(
        from_token=TOKEN,
        to_token=ZERO,
        amount=Decimal("0.000001"),
        protocol="uniswap_v4",
        max_price_impact=Decimal("0.10"),
        swap_params={"pool_key": key.to_wire()},
    )
    quote = SwapQuote(10**12, 9 * 10**11, key.fee, TOKEN, ZERO, pool_key=key)
    with (
        patch.object(adapter, "_resolve_token", side_effect=lambda token, **kw: (token, 18)),
        patch.object(adapter, "_quote_for_swap", return_value=(quote, "onchain_quoter")),
    ):
        bundle = adapter.compile_swap_intent(intent, {ZERO: Decimal(1), TOKEN: Decimal(1)})
    bundle.metadata["protocol"] = "uniswap_v4"
    return bundle


@pytest.mark.parametrize("mode", ["swap", "lp"])
@pytest.mark.parametrize("current,count", [(2**256 - 1, 2), (0, 3), (1, 4)])
def test_sufficient_allowance_skips_erc20_while_permit2_stays_bounded(mode, current, count):
    gateway = AllowanceGateway()
    gateway.allowance = current
    bundle = compile_operation(gateway, mode)
    assert len(bundle.transactions) == count, bundle.metadata
    artifact = bundle.metadata["v4_operation"]
    amount = int(artifact["amount_in"] if mode == "swap" else bundle.metadata["amount1_desired"])
    permit = bundle.transactions[-2]
    assert permit["to"].lower() == PERMIT2_ADDRESS.lower()
    token, spender, authorized, _ = decode(
        ["address", "address", "uint160", "uint48"], bytes.fromhex(permit["data"][10:])
    )
    assert token == TOKEN and authorized == amount
    assert spender == adapter_spender(mode).lower()
    erc20 = [decode(["address", "uint256"], bytes.fromhex(tx["data"][10:]))[1] for tx in bundle.transactions[:-2]]
    assert erc20 == ([] if current >= amount else [0, amount] if current else [amount])
    validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)
    for read in gateway.allowance_reads:
        assert read["chain"] == "base" and read["block_number"] == gateway.head
        assert read["target"].reference == TOKEN
        assert decode(["address", "address"], read["payload"][4:]) == (WALLET, PERMIT2_ADDRESS.lower())


def adapter_spender(mode):
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    return UNISWAP_V4["base"]["universal_router" if mode == "swap" else "position_manager"]


@pytest.mark.parametrize("mode", ["swap", "lp"])
@pytest.mark.parametrize("current", [0, 1])
def test_revoked_allowance_after_compile_refuses_before_signing(mode, current):
    gateway = AllowanceGateway()
    bundle = compile_operation(gateway, mode)
    original = deepcopy(bundle)
    gateway.allowance = current
    with pytest.raises(ConnectorValidationError, match="allowance decreased") as refused:
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)
    assert bundle == original
    assert refused.value.code == "INSUFFICIENT_ERC20_ALLOWANCE"
    check = refused.value.evidence["approval_checks"][-1]
    assert check["measured"] is True
    assert check["current_raw"] == str(current)
    assert check["return_data"] == "0x" + encode(["uint256"], [current]).hex()
    assert check["block_number"] == gateway.head
    assert check["token"] == TOKEN
    assert check["owner"] == WALLET
    assert check["spender"] == PERMIT2_ADDRESS.lower()
    assert int(check["required_raw"]) > current
    assert check["approval_amounts_raw"] == []
    assert check["decision"] == "refuse_insufficient"


@pytest.mark.parametrize("mode", ["swap", "lp"])
@pytest.mark.parametrize("mutation", ["overspend", "wrong_spender", "extra_transaction", "missing_permit"])
def test_approval_variations_do_not_relax_transaction_authorization(mode, mutation):
    gateway = AllowanceGateway()
    gateway.allowance = 1
    bundle = compile_operation(gateway, mode)
    if mutation == "overspend":
        tx = bundle.transactions[1]
        tx["data"] = tx["data"][:-64] + f"{2**256 - 1:064x}"
    elif mutation == "wrong_spender":
        tx = bundle.transactions[1]
        tx["data"] = tx["data"][:10] + WALLET[2:].rjust(64, "0") + tx["data"][-64:]
    elif mutation == "extra_transaction":
        bundle.transactions.insert(-1, deepcopy(bundle.transactions[0]))
    else:
        bundle.transactions.pop(-2)
    bundle.metadata["v4_operation"]["transaction_digest"] = transaction_digest(bundle.transactions)
    with pytest.raises(ValueError, match="approval|Permit2"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("raw", [b"", b"\x00" * 31, b"\x00" * 64, "0x00"])
def test_unmeasured_or_malformed_allowance_is_never_assumed_zero(raw):
    gateway = AllowanceGateway()
    gateway.malformed = raw
    with pytest.raises(ValueError, match="canonical uint256"):
        observe_permit2_allowance(gateway, chain="base", token=TOKEN, wallet=WALLET, block_number=10)


def test_allowance_rpc_failure_propagates():
    gateway = AllowanceGateway()
    with patch.object(gateway, "read", side_effect=TimeoutError("unmeasured")), pytest.raises(TimeoutError):
        observe_permit2_allowance(gateway, chain="base", token=TOKEN, wallet=WALLET, block_number=10)


@pytest.mark.parametrize("current,required", [(-1, 1), (True, 1), (0, -1), (0, True), (0, 2**160)])
def test_invalid_approval_budgets_and_observations_are_rejected(current, required):
    with pytest.raises(ValueError):
        approval_amounts(current, required)


def test_exact_allowance_and_zero_budget_need_no_erc20_write():
    assert approval_amounts(12, 12) == ()
    assert approval_amounts(0, 0) == ()
    assert approval_amounts(None, 12) == (12,)


@pytest.mark.parametrize("mode", ["swap", "lp"])
@pytest.mark.parametrize("current", [0, 1, 2**256 - 1])
def test_actual_allowance_observation_survives_compile_and_execution(mode, current):
    gateway = AllowanceGateway()
    gateway.allowance = current
    bundle = compile_operation(gateway, mode)
    checks = bundle.metadata["v4_approval_checks"]
    assert len(checks) == 1
    observation = checks[0]
    assert observation["measured"] is True
    assert observation["current_raw"] == str(current)
    assert observation["return_data"] == "0x" + encode(["uint256"], [current]).hex()
    assert observation["block_number"] == gateway.head
    assert observation["chain"] == "base"
    assert observation["token"] == TOKEN
    assert observation["owner"] == WALLET
    assert observation["spender"] == PERMIT2_ADDRESS.lower()
    assert (
        observation["call_data"] == "0x" + (ALLOWANCE + encode(["address", "address"], [WALLET, PERMIT2_ADDRESS])).hex()
    )
    amount = int(observation["required_raw"])
    expected = [] if current >= amount else [0, amount] if current else [amount]
    assert observation["approval_amounts_raw"] == [str(value) for value in expected]
    original = deepcopy(bundle)
    execution = validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)
    assert bundle == original
    executed = execution["approval_checks"][0]
    assert executed["permit2_spender"] == adapter_spender(mode).lower()
    assert executed["permit2_call_data"] == bundle.transactions[-2]["data"]
    assert executed["approval_amounts_raw"] == observation["approval_amounts_raw"]
    assert executed["measured"] is (not expected)
    assert executed["current_raw"] == (str(current) if not expected else None)


def test_offline_approval_plan_does_not_claim_a_measured_zero():
    adapter = UniswapV4Adapter(config=UniswapV4Config(chain="base", wallet_address=WALLET))
    checks = []
    adapter._erc20_permit2_approvals(TOKEN, 123, block_number=None, evidence=checks)
    assert checks[0]["current_raw"] is None
    assert checks[0]["measured"] is False
    assert checks[0]["return_data"] is None
    assert checks[0]["block_number"] is None
    assert checks[0]["approval_amounts_raw"] == ["123"]


def test_compile_attempts_do_not_share_allowance_evidence():
    gateway = AllowanceGateway()
    first = compile_operation(gateway, "swap")
    prior = deepcopy(first.metadata["v4_approval_checks"])
    gateway.allowance = 0
    second = compile_operation(gateway, "swap")
    assert first.metadata["v4_approval_checks"] == prior
    assert second.metadata["v4_approval_checks"][0]["current_raw"] == "0"
