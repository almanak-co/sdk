"""Administrative continuity is exact while ordinary hook-fee decay remains live."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.doppler_dependencies import observe_doppler_swap_dependencies
from almanak.connectors.uniswap_v4.pool_key import PoolKey

FIXTURES = Path(__file__).parents[3] / "fixtures/uniswap_v4"
OBSERVATIONS = json.loads((FIXTURES / "doppler_dependencies/observations.json").read_text())
KEY = PoolKey.from_wire(OBSERVATIONS["key"])
NESTED = OBSERVATIONS["evidence"]["nested_hook"]
VIEW_QUOTER = "0x3881e5246e81e1bf731a9fc1856268d381bb9bd7"


class RecordedGateway:
    def __init__(self):
        self.rows = deepcopy(OBSERVATIONS["reads"])
        self.codes = {
            KEY.hooks: (FIXTURES / "doppler_liquidity/runtime.hex").read_text(),
            NESTED: (FIXTURES / "doppler_dependencies/nested-runtime.hex").read_text(),
            VIEW_QUOTER: (FIXTURES / "doppler_dependencies/internal_quoter-runtime.hex").read_text(),
        }
        self.blocks = []

    def read(self, *, chain, target, payload, block_number):
        assert chain == "robinhood"
        self.blocks.append(block_number)
        for row in self.rows:
            if row["target"] == target.reference and row["payload"] == "0x" + payload.hex():
                return bytes.fromhex(row["result"][2:])
        raise AssertionError("Unexpected read")

    def code(self, *, chain, target, block_number):
        assert chain == "robinhood"
        self.blocks.append(block_number)
        return bytes.fromhex(self.codes.get(target.reference, "0x").strip()[2:])

    def block_hash(self, *, chain, block_number):
        assert chain == "robinhood"
        self.blocks.append(block_number)
        return OBSERVATIONS["evidence"]["block_hash"]

    def change(self, target, signature, outputs, change):
        selector = "0x" + keccak(text=signature)[:4].hex()
        row = next(r for r in self.rows if r["target"] == target and r["payload"].startswith(selector))
        values = list(decode(outputs, bytes.fromhex(row["result"][2:])))
        change(values)
        row["result"] = "0x" + encode(outputs, values).hex()


def observe(gateway, **changes):
    return observe_doppler_swap_dependencies(
        **({"chain": "robinhood", "key": KEY, "gateway": gateway, "block_number": 100} | changes)
    )


def test_live_recorded_dependencies_are_block_pinned_and_reproducible():
    gateway = RecordedGateway()
    evidence = observe(gateway)
    assert evidence.digest == OBSERVATIONS["evidence"]["digest"]
    assert evidence.nested_hook == NESTED
    assert evidence.fee_schedule == (1788997134, 800000, 2340, 10)
    assert set(gateway.blocks) == {100}
    assert observe(gateway, block_number=101).digest == evidence.digest


@pytest.mark.parametrize("target", [KEY.hooks, NESTED, VIEW_QUOTER])
def test_unknown_dependency_runtime_is_refused(target):
    gateway = RecordedGateway()
    gateway.codes[target] = "0x6000"
    with pytest.raises(ValueError, match="runtime is not qualified"):
        observe(gateway)


@pytest.mark.parametrize(
    "target,signature",
    [
        (KEY.hooks, "poolManager()"),
        (NESTED, "poolManager()"),
        (NESTED, "INITIALIZER()"),
        (VIEW_QUOTER, "poolManager()"),
    ],
)
def test_semantic_dependency_identity_is_checked(target, signature):
    gateway = RecordedGateway()
    gateway.change(target, signature, ["address"], lambda v: v.__setitem__(0, KEY.currency0))
    with pytest.raises(ValueError, match="different"):
        observe(gateway)


@pytest.mark.parametrize("flags", [0, 1, 8, 10])
def test_disabled_or_unknown_swap_flags_are_refused(flags):
    gateway = RecordedGateway()
    gateway.change(KEY.hooks, "isDopplerHookEnabled(address)", ["uint256"], lambda v: v.__setitem__(0, flags))
    with pytest.raises(ValueError, match="flags"):
        observe(gateway)


SCHEDULE_TYPES = ["uint32", "uint24", "uint24", "uint24", "uint32"]


def test_normal_last_fee_decay_does_not_invalidate_administrative_digest():
    gateway = RecordedGateway()
    original = observe(gateway)
    for last_fee in (800000, 400000, 2340):
        gateway.change(NESTED, "getFeeSchedule(bytes32)", SCHEDULE_TYPES, lambda v, fee=last_fee: v.__setitem__(3, fee))
        assert observe(gateway).digest == original.digest


@pytest.mark.parametrize("index,new_value", [(0, 1788997135), (1, 799999), (2, 2339), (4, 11)])
def test_administrative_fee_schedule_changes_invalidate_digest(index, new_value):
    gateway = RecordedGateway()
    original = observe(gateway)
    gateway.change(NESTED, "getFeeSchedule(bytes32)", SCHEDULE_TYPES, lambda v: v.__setitem__(3, 2340))
    gateway.change(NESTED, "getFeeSchedule(bytes32)", SCHEDULE_TYPES, lambda v: v.__setitem__(index, new_value))
    assert observe(gateway).digest != original.digest


@pytest.mark.parametrize("index,new_value", [(1, 800001), (2, 800001), (3, 1), (4, 0)])
def test_invalid_fee_schedule_is_refused(index, new_value):
    gateway = RecordedGateway()
    gateway.change(NESTED, "getFeeSchedule(bytes32)", SCHEDULE_TYPES, lambda v: v.__setitem__(index, new_value))
    with pytest.raises(ValueError, match="schedule"):
        observe(gateway)


def test_beneficiary_and_routing_mode_are_bound():
    gateway = RecordedGateway()
    original = observe(gateway)
    gateway.change(NESTED, "getPoolInfo(bytes32)", ["address"] * 3, lambda v: v.__setitem__(2, KEY.currency0))
    changed = observe(gateway)
    assert changed.digest != original.digest
    gateway.change(NESTED, "getFeeRoutingMode(bytes32)", ["uint8"], lambda v: v.__setitem__(0, 1))
    assert observe(gateway).digest != changed.digest


def test_invalid_distribution_and_malformed_abi_fail_closed():
    gateway = RecordedGateway()
    gateway.change(NESTED, "getFeeDistributionInfo(bytes32)", ["uint256"] * 8, lambda v: v.__setitem__(0, 10**18 + 1))
    with pytest.raises(ValueError, match="distribution"):
        observe(gateway)
    gateway = RecordedGateway()
    gateway.rows[0]["result"] += "00" * 32
    with pytest.raises(ValueError, match="noncanonical"):
        observe(gateway)


def test_wrong_chain_and_static_pool_are_not_qualified():
    gateway = RecordedGateway()
    with pytest.raises(ValueError, match="outside"):
        observe(gateway, chain="base")
    with pytest.raises(ValueError, match="outside"):
        observe(gateway, key=PoolKey(KEY.currency0, KEY.currency1, 31100, KEY.tick_spacing, KEY.hooks))
    assert not gateway.blocks


def test_wrong_full_key_and_ambiguous_asset_mapping_are_refused():
    gateway = RecordedGateway()
    wrong_key = PoolKey(KEY.currency0, KEY.currency1, KEY.fee, KEY.tick_spacing + 1, KEY.hooks)
    with pytest.raises(ValueError, match="uniquely bind"):
        observe(gateway, key=wrong_key)
    selector = "0x" + keccak(text="getState(address)")[:4].hex()
    states = [r for r in gateway.rows if r["payload"].startswith(selector)]
    assert len(states) == 2
    states[1]["result"] = states[0]["result"]
    with pytest.raises(ValueError, match="uniquely bind"):
        observe(gateway)


def test_live_configuration_change_is_bound_without_freezing_inventory():
    gateway = RecordedGateway()
    original = observe(gateway)
    state_types = ["address", "uint256", "address", "bytes", "uint8", "(address,address,uint24,int24,address)", "int24"]
    gateway.change(KEY.hooks, "getState(address)", state_types, lambda v: v.__setitem__(1, v[1] + 1))
    assert observe(gateway).digest == original.digest
    gateway.change(KEY.hooks, "getState(address)", state_types, lambda v: v.__setitem__(6, v[6] - 1))
    assert observe(gateway).digest != original.digest
    gateway.change(KEY.hooks, "getState(address)", state_types, lambda v: v.__setitem__(4, 3))
    with pytest.raises(ValueError, match="initialized trading pair"):
        observe(gateway)
