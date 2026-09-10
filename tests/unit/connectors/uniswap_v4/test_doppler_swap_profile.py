"""Reproduced periphery and mutable hook dependencies bind swap admission."""

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.doppler_swap import DopplerRehypeSwapProfile
from tests.unit.connectors.uniswap_v4.test_doppler_dependencies import (
    FIXTURES,
    KEY,
    NESTED,
    SCHEDULE_TYPES,
    RecordedGateway,
)

ADDRESSES = UNISWAP_V4["robinhood"]


@pytest.fixture
def gateway():
    g = RecordedGateway()
    for name in ("quoter", "universal_router"):
        g.codes[ADDRESSES[name].lower()] = (FIXTURES / f"doppler_dependencies/{name}-runtime.hex").read_text()
    g.rows.append(
        {
            "target": ADDRESSES["quoter"].lower(),
            "payload": "0x" + keccak(text="poolManager()")[:4].hex(),
            "result": "0x" + encode(["address"], [ADDRESSES["pool_manager"]]).hex(),
        }
    )
    return g


def verify(gateway, **changes):
    return DopplerRehypeSwapProfile().verify(
        **(
            {
                "chain": "robinhood",
                "key": KEY,
                "operation": "swap_exact_in",
                "route": "universal_router_eoa",
                "hook_data": b"",
                "gateway": gateway,
                "block_number": 100,
            }
            | changes
        )
    )


def test_exact_reproduced_family_and_periphery_are_pinned(gateway):
    evidence = verify(gateway)
    assert evidence.profile == "doppler_rehype_exact_input"
    assert evidence.operation == "swap_exact_in"
    assert evidence.amount_equivalent_to_quoter is True
    assert set(gateway.blocks) == {100}
    assert verify(gateway, block_number=101).dependency_digest == evidence.dependency_digest


@pytest.mark.parametrize("name", ["quoter", "universal_router"])
def test_unqualified_periphery_is_refused(gateway, name):
    gateway.codes[ADDRESSES[name].lower()] = "0x6000"
    with pytest.raises(ValueError, match="runtime is not qualified"):
        verify(gateway)


def test_quoter_manager_is_bound(gateway):
    gateway.change(ADDRESSES["quoter"].lower(), "poolManager()", ["address"], lambda v: v.__setitem__(0, KEY.currency0))
    with pytest.raises(ValueError, match="different PoolManager"):
        verify(gateway)


@pytest.mark.parametrize(
    "changes",
    [
        {"chain": "base"},
        {"route": "universal_router_safe"},
        {"operation": "swap_exact_out"},
        {"operation": "lp_open"},
    ],
)
def test_unqualified_operation_and_route_do_not_read_state(gateway, changes):
    assert verify(gateway, **changes) is None
    assert not gateway.blocks


def test_arbitrary_hook_data_is_not_admitted(gateway):
    with pytest.raises(ValueError, match="empty hook_data"):
        verify(gateway, hook_data=b"\x01")
    assert not gateway.blocks


def test_fee_policy_change_invalidates_execution_evidence(gateway):
    quoted = verify(gateway)
    gateway.change(NESTED, "getFeeSchedule(bytes32)", SCHEDULE_TYPES, lambda v: v.__setitem__(0, 1788997135))
    assert verify(gateway).dependency_digest != quoted.dependency_digest


def test_registered_admission_uses_reproduced_swap_family(gateway):
    from almanak.connectors.uniswap_v4.behavior import admit_hook

    evidence = admit_hook(
        chain="robinhood",
        key=KEY,
        operation="swap_exact_in",
        route="universal_router_eoa",
        hook_data=b"",
        gateway=gateway,
        block_number=100,
    )
    assert evidence == verify(gateway)


def test_active_native_route_waits_for_native_settlement_qualification(gateway):
    from almanak.connectors.uniswap_v4.pool_key import PoolKey

    native_key = PoolKey("0x" + "0" * 40, KEY.currency1, KEY.fee, KEY.tick_spacing, KEY.hooks)
    assert verify(gateway, key=native_key) is None
    assert not gateway.blocks
