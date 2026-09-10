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
        from_token=TOKEN, to_token=ZERO, amount=Decimal("0.000001"),
        protocol="uniswap_v4", max_price_impact=Decimal("0.10"),
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
    token, spender, authorized, _ = decode(["address", "address", "uint160", "uint48"], bytes.fromhex(permit["data"][10:]))
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
def test_revoked_allowance_after_compile_refuses_before_signing(mode):
    gateway = AllowanceGateway()
    bundle = compile_operation(gateway, mode)
    gateway.allowance = 0
    with pytest.raises(ValueError, match="allowance decreased"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("mode", ["swap", "lp"])
@pytest.mark.parametrize("mutation", ["overspend", "wrong_spender", "extra_transaction", "missing_permit"])
def test_approval_variations_do_not_relax_transaction_authorization(mode, mutation):
    gateway = AllowanceGateway()
    gateway.allowance = 1
    bundle = compile_operation(gateway, mode)
    if mutation == "overspend":
        tx = bundle.transactions[1]
        tx["data"] = tx["data"][:-64] + f'{2**256-1:064x}'
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
