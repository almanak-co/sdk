"""Adversarial operation continuity, reorg, hook dispatch and venue tests."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.behavior import admit_hook
from almanak.connectors.uniswap_v4.operation import transaction_digest, validate_execution
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.sdk import SwapQuote
from almanak.connectors.uniswap_v4.venue_verifier import V4VenueVerifier, verification_request
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.venues import VenueBindingFailure

ZERO = "0x" + "0" * 40
TOKEN = "0x" + "1" * 40
WALLET = "0x" + "2" * 40
HOOK = "0x" + "3" * 36 + "1000"


class Gateway:
    head = 10
    hash = "0x" + "a" * 64
    price = 2**96
    stored_fee = 1000
    runtime = b"\x60\x00"

    def block_number(self, **kwargs):
        return self.head

    def block_hash(self, **kwargs):
        return self.hash

    def code(self, **kwargs):
        return self.runtime

    def read(self, *, payload, **kwargs):
        if payload == keccak(text="poolManager()")[:4]:
            return encode(["address"], [UNISWAP_V4["base"]["pool_manager"]])
        return encode(["uint160", "int24", "uint24", "uint24"], [self.price, 0, 0, self.stored_fee])


@pytest.fixture
def gateway():
    return Gateway()


@pytest.fixture
def bundle(gateway):
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, HOOK)
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET),
        venue_verification_gateway_factory=lambda: gateway,
    )
    intent = SwapIntent(
        from_token=ZERO,
        to_token=TOKEN,
        amount=Decimal("0.000001"),
        protocol="uniswap_v4",
        swap_params={"pool_key": key.to_wire(), "hook_data": "0x"},
    )
    quote = SwapQuote(1000000000000, 900000000000, key.fee, ZERO, TOKEN, pool_key=key)
    with (
        patch.object(adapter, "_resolve_token", side_effect=lambda token, **kw: (token, 18)),
        patch.object(adapter, "_quote_for_swap", return_value=(quote, "onchain_quoter")),
    ):
        value = adapter.compile_swap_intent(intent, {})
    value.metadata["protocol"] = "uniswap_v4"
    return value


def test_dynamic_fee_updates_do_not_change_venue_identity(gateway):
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, HOOK)
    before = V4VenueVerifier().verify_venue(verification_request("base", key), gateway)
    gateway.stored_fee = 500000
    after = V4VenueVerifier().verify_venue(verification_request("base", key), gateway)
    assert before.binding.binding_hash == after.binding.binding_hash
    assert before.evidence != after.evidence


def test_uninitialized_or_wrong_manager_refused(gateway):
    key = PoolKey(ZERO, TOKEN, 1000, 17)
    gateway.price = 0
    assert isinstance(V4VenueVerifier().verify_venue(verification_request("base", key), gateway), VenueBindingFailure)
    request = verification_request("base", key)
    changed = tuple(replace(c, value=WALLET) if c.name == "pool_manager" else c for c in request.binding_components)
    assert isinstance(
        V4VenueVerifier().verify_venue(replace(request, binding_components=changed), gateway), VenueBindingFailure
    )


def test_bound_operation_accepts_mutable_stored_fee(gateway, bundle):
    gateway.stored_fee = 500000
    validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("mutation", ["calldata", "recipient", "pool", "chain", "expiry", "reorg", "code", "age"])
def test_operation_rejects_changed_context_before_submission(gateway, bundle, mutation):
    if mutation == "calldata":
        bundle.transactions[-1]["data"] += "00"
    elif mutation == "recipient":
        bundle.metadata["v4_operation"]["wallet"] = TOKEN
    elif mutation == "pool":
        bundle.metadata["pool_key"]["tick_spacing"] += 1
    elif mutation == "chain":
        bundle.metadata["v4_operation"]["chain"] = "arbitrum"
    elif mutation == "expiry":
        bundle.metadata["v4_operation"]["expires_at"] = 1
    elif mutation == "reorg":
        gateway.hash = "0x" + "b" * 64
    elif mutation == "code":
        gateway.runtime = b"\x60\x01"
    else:
        gateway.head += 151
    with pytest.raises(ValueError):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


def test_rehashing_tampered_router_actions_does_not_make_them_valid(gateway, bundle):
    bundle.transactions[-1]["data"] += "00"
    bundle.metadata["v4_operation"]["transaction_digest"] = transaction_digest(bundle.transactions)
    with pytest.raises(ValueError, match="nested router"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


def test_safe_inner_amount_quote_cannot_be_relabelled_as_safe_qualification(gateway, bundle):
    with pytest.raises(ValueError, match="outer-route"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=True, gateway=gateway)


@pytest.mark.parametrize("hook_bits", [0x0080, 0x0040, 0x0088, 0x0044])
def test_callback_free_profile_rejects_any_swap_callback(gateway, hook_bits):
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, "0x" + "3" * 36 + f"{hook_bits:04x}")
    with pytest.raises(ValueError, match="No reviewed"):
        admit_hook(
            chain="base",
            key=key,
            operation="swap_exact_in",
            route="universal_router_eoa",
            hook_data=b"",
            gateway=gateway,
            block_number=10,
        )


def test_same_hook_can_be_eligible_for_swap_but_not_lp(gateway):
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, "0x" + "3" * 36 + "0800")
    assert admit_hook(
        chain="base",
        key=key,
        operation="swap_exact_in",
        route="universal_router_eoa",
        hook_data=b"",
        gateway=gateway,
        block_number=10,
    )
    with pytest.raises(ValueError, match="No reviewed"):
        admit_hook(
            chain="base",
            key=key,
            operation="lp_open",
            route="position_manager_eoa",
            hook_data=b"",
            gateway=gateway,
            block_number=10,
        )


def test_rehashed_auxiliary_transfer_cannot_be_smuggled_into_native_swap(gateway, bundle):
    bundle.transactions.insert(0, {"to": TOKEN, "value": 1000, "data": "0x"})
    bundle.metadata["v4_operation"]["transaction_digest"] = transaction_digest(bundle.transactions)
    with pytest.raises(ValueError, match="auxiliary"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("mutation", ["remove_protocol", "rename_protocol", "intent_type", "remove_all_metadata"])
def test_transaction_targets_cannot_bypass_protocol_validation(gateway, bundle, mutation):
    from almanak.framework.execution.connector_validation import validate_connector_execution

    if mutation == "remove_protocol":
        bundle.metadata.pop("protocol")
    elif mutation == "rename_protocol":
        bundle.metadata["protocol"] = "uniswap_v3"
    elif mutation == "intent_type":
        bundle.intent_type = "TRANSFER"
    else:
        bundle.metadata.clear()
    with pytest.raises(ValueError, match="bound protocol"):
        validate_connector_execution(
            bundle, chain="base", wallet=WALLET, is_safe=False, observer_factory=lambda: gateway
        )


def test_claimed_minimum_must_equal_actual_encoded_floor(gateway, bundle):
    bundle.metadata["v4_operation"]["minimum_out"] = "999999999999999"
    bundle.metadata["amount_out_minimum"] = "999999999999999"
    with pytest.raises(ValueError, match="minimum output"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)
