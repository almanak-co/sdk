"""Adversarial operation continuity, reorg, hook dispatch and venue tests."""

import json
import time
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.behavior import admit_hook
from almanak.connectors.uniswap_v4.freshness import QuoteFreshnessError
from almanak.connectors.uniswap_v4.operation import transaction_digest, validate_execution
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.router_deployments import ROUTER_DEPLOYMENTS, RouterABI
from almanak.connectors.uniswap_v4.sdk import SwapQuote
from almanak.connectors.uniswap_v4.venue_verifier import V4VenueVerifier, verification_request
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.venues import VenueBindingFailure
from almanak.framework.venues.provider import GatewayBlockIdentity

ZERO = "0x" + "0" * 40
TOKEN = "0x" + "1" * 40
WALLET = "0x" + "2" * 40
HOOK = "0x" + "3" * 36 + "1000"


class Gateway:
    chain = "base"
    head = 10
    hash = "0x" + "a" * 64
    price = 2**96
    stored_fee = 1000
    runtime = b"\x60\x00"
    elapsed = 0

    def __init__(self):
        self.quote_time = int(time.time())

    def block_number(self, **kwargs):
        return self.head

    def block_hash(self, **kwargs):
        return self.hash

    def block_identity(self, *, block_number, **kwargs):
        return GatewayBlockIdentity(
            block_number,
            self.block_hash(block_number=block_number, **kwargs),
            self.quote_time + (self.elapsed if block_number > 10 else 0),
        )

    def code(self, **kwargs):
        return self.runtime

    def read(self, *, payload, **kwargs):
        if payload[:4] == keccak(text="allowance(address,address)")[:4]:
            return encode(["uint256"], [0])
        if payload == keccak(text="poolManager()")[:4]:
            return encode(["address"], [UNISWAP_V4[self.chain]["pool_manager"]])
        return encode(["uint160", "int24", "uint24", "uint24"], [self.price, 0, 0, self.stored_fee])


@pytest.fixture
def gateway(monkeypatch):
    # Bind the synthetic deployment to the synthetic gateway runtime.
    profile = ROUTER_DEPLOYMENTS["robinhood"]
    monkeypatch.setitem(
        ROUTER_DEPLOYMENTS, "robinhood", replace(profile, runtime_hash="0x" + keccak(Gateway.runtime).hex())
    )
    return Gateway()


@pytest.fixture
def bundle(gateway):
    return _compile_bundle(gateway)


def _compile_bundle(gateway):
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, HOOK)
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain=gateway.chain, wallet_address=WALLET),
        venue_verification_gateway_factory=lambda: gateway,
    )
    intent = SwapIntent(
        from_token=ZERO,
        to_token=TOKEN,
        amount=Decimal("0.000001"),
        max_price_impact=Decimal("0.10"),
        protocol="uniswap_v4",
        swap_params={"pool_key": key.to_wire(), "hook_data": "0x"},
    )
    quote = SwapQuote(1000000000000, 900000000000, key.fee, ZERO, TOKEN, pool_key=key)
    with (
        patch.object(adapter, "_resolve_token", side_effect=lambda token, **kw: (token, 18)),
        patch.object(adapter, "_quote_for_swap", return_value=(quote, "onchain_quoter")),
    ):
        value = adapter.compile_swap_intent(intent, {ZERO: Decimal("1"), TOKEN: Decimal("1")})
    value.metadata["protocol"] = "uniswap_v4"
    return value


def test_compiled_impact_evidence_is_bound_to_swap_inputs(bundle):
    evidence = bundle.metadata["price_impact_check"]
    assert evidence["status"] == "passed"
    assert evidence["chain"] == "base"
    assert evidence["pool_id"] == bundle.metadata["pool_id"]
    assert evidence["token_in"] == ZERO
    assert evidence["token_out"] == TOKEN
    assert evidence["amount_in_raw"] == "1000000000000"
    assert evidence["quote_amount_raw"] == "900000000000"
    assert evidence["oracle_estimate_raw"] == "1000000000000"
    assert evidence["quote_block"] == 10
    assert Decimal(evidence["price_impact"]) == Decimal("0.1")
    assert Decimal(evidence["max_price_impact"]) == Decimal("0.10")


def test_price_impact_log_binds_intent_and_matches_bundle(gateway, caplog):
    caplog.set_level("INFO", logger="almanak.connectors.uniswap_v4.adapter")
    bundle = _compile_bundle(gateway)
    records = [record.getMessage() for record in caplog.records if "v4_price_impact_check" in record.getMessage()]
    assert len(records) == 1
    prefix, _, payload = records[0].partition(" evidence=")
    assert prefix == f"v4_price_impact_check intent_id={bundle.metadata['intent_id']}"
    assert json.loads(payload) == bundle.metadata["price_impact_check"]


@pytest.mark.parametrize("chain", sorted(UNISWAP_V4))
@pytest.mark.parametrize("blocks,elapsed", [(1, 299), (100, 299), (10_000, 299), (1, 300), (3, 301)])
def test_quote_age_uses_elapsed_time_on_every_supported_chain(gateway, chain, blocks, elapsed):
    gateway.chain = chain
    gateway.quote_time -= elapsed
    value = _compile_bundle(gateway)
    gateway.head += blocks
    gateway.elapsed = elapsed
    arguments = {
        "chain": chain,
        "wallet": WALLET,
        "is_safe": False,
        "gateway": gateway,
        "now": gateway.quote_time + elapsed,
    }
    if elapsed > 300:
        with pytest.raises(QuoteFreshnessError, match="quote_stale"):
            validate_execution(value, **arguments)
    else:
        validate_execution(value, **arguments)


@pytest.mark.parametrize("timestamp", [None, True, "1", 1])
def test_version_two_quote_timestamp_must_match_measured_header(gateway, bundle, timestamp):
    bundle.metadata["v4_operation"]["quote_block_timestamp"] = timestamp
    with pytest.raises(ValueError, match="timestamp differs"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


def test_version_two_requires_quote_timestamp(gateway, bundle):
    bundle.metadata["v4_operation"].pop("quote_block_timestamp")
    with pytest.raises(ValueError, match="timestamp differs"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("elapsed", [299, 301])
def test_version_one_recovers_canonical_timestamp_without_mutating_artifact(gateway, elapsed):
    gateway.quote_time -= elapsed
    value = _compile_bundle(gateway)
    artifact = value.metadata["v4_operation"]
    artifact["schema_version"] = 1
    artifact.pop("quote_block_timestamp")
    before = deepcopy(artifact)
    gateway.head += 10_000
    gateway.elapsed = elapsed
    arguments = {
        "chain": "base",
        "wallet": WALLET,
        "is_safe": False,
        "gateway": gateway,
        "now": gateway.quote_time + elapsed,
    }
    if elapsed > 300:
        with pytest.raises(QuoteFreshnessError, match="quote_stale"):
            validate_execution(value, **arguments)
    else:
        validate_execution(value, **arguments)
    assert artifact == before


def test_version_one_cannot_ignore_a_supplied_inconsistent_timestamp(gateway, bundle):
    bundle.metadata["v4_operation"]["schema_version"] = 1
    bundle.metadata["v4_operation"]["quote_block_timestamp"] = 1
    with pytest.raises(ValueError, match="timestamp differs"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("managed_fork", [True, False, None, "true", 1])
def test_historical_clock_requires_positive_runtime_fork_declaration(gateway, managed_fork):
    gateway.quote_time -= 30 * 86400
    value = _compile_bundle(gateway)
    value.metadata["managed_fork"] = True
    arguments = {"chain": "base", "wallet": WALLET, "is_safe": False, "gateway": gateway, "managed_fork": managed_fork}
    if managed_fork is True:
        validate_execution(value, **arguments)
    else:
        with pytest.raises(QuoteFreshnessError, match="head_clock_skew"):
            validate_execution(value, **arguments)


def test_historical_fork_still_refuses_elapsed_chain_time(gateway):
    gateway.quote_time -= 30 * 86400
    value = _compile_bundle(gateway)
    gateway.head += 1
    gateway.elapsed = 301
    with pytest.raises(QuoteFreshnessError, match="quote_stale"):
        validate_execution(value, chain="base", wallet=WALLET, is_safe=False, gateway=gateway, managed_fork=True)


def test_historical_fork_still_refuses_expired_host_deadline(gateway):
    gateway.quote_time -= 30 * 86400
    value = _compile_bundle(gateway)
    deadline = value.metadata["v4_operation"]["expires_at"]
    with pytest.raises(ValueError, match="operation expired"):
        validate_execution(
            value, chain="base", wallet=WALLET, is_safe=False, gateway=gateway, managed_fork=True, now=deadline
        )


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
        gateway.head += 3
        gateway.elapsed = 120
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


@pytest.mark.parametrize("chain", ["base", "robinhood"])
def test_execution_refuses_router_layout_tampering(gateway, chain):
    gateway.chain = chain
    value = _compile_bundle(gateway)
    value.metadata["v4_operation"]["router_abi"] = "unqualified"
    with pytest.raises(ValueError, match="router ABI"):
        validate_execution(value, chain=chain, wallet=WALLET, is_safe=False, gateway=gateway)


def test_new_layout_requires_explicit_artifact_identity(gateway):
    gateway.chain = "robinhood"
    value = _compile_bundle(gateway)
    assert value.metadata["v4_operation"]["router_abi"] == RouterABI.V4_HOP_PRICE.value
    value.metadata["v4_operation"].pop("router_abi")
    with pytest.raises(ValueError, match="router ABI"):
        validate_execution(value, chain="robinhood", wallet=WALLET, is_safe=False, gateway=gateway)


def test_legacy_layout_artifact_remains_executable_without_new_field(gateway, bundle):
    bundle.metadata["v4_operation"].pop("router_abi")
    validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


def test_new_router_runtime_requires_qualification_before_quote(gateway):
    gateway.chain = "robinhood"
    gateway.runtime = b"different runtime"
    key = PoolKey(ZERO, TOKEN, 31100, 17)
    result = V4VenueVerifier().verify_venue(verification_request("robinhood", key), gateway)
    assert isinstance(result, VenueBindingFailure)
    assert "runtime differs" in result.detail
