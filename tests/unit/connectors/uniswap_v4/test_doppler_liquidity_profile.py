"""Runtime-bound LP admission and rejection of uncovered hook operations."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.behavior import admit_hook
from almanak.connectors.uniswap_v4.doppler import DopplerLiquidityProfile
from almanak.connectors.uniswap_v4.pool_key import PoolKey

HOOK = "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544"
KEY = PoolKey("0x" + "1" * 40, "0x" + "2" * 40, 8388608, 60, HOOK)
RUNTIME = bytes.fromhex(
    (Path(__file__).parents[3] / "fixtures/uniswap_v4/doppler_liquidity/runtime.hex").read_text().strip()[2:]
)


@pytest.fixture
def gateway():
    value = MagicMock()
    value.code.return_value = RUNTIME
    value.read.return_value = encode(["address"], [UNISWAP_V4["robinhood"]["pool_manager"]])
    value.code.side_effect = lambda *, target, **kwargs: value.code.return_value if target.reference == HOOK else b""
    value.read.side_effect = lambda *, target, **kwargs: value.read.return_value if target.reference == HOOK else b""
    value.block_hash.return_value = "0x" + "a" * 64
    return value


def verify(gateway, **changes):
    args = {
        "chain": "robinhood",
        "key": KEY,
        "operation": "lp_open",
        "route": "position_manager_eoa",
        "hook_data": b"",
        "gateway": gateway,
        "block_number": 100,
    }
    return DopplerLiquidityProfile().verify(**(args | changes))


def test_registered_lp_admission_requires_no_nested_swap_reads(gateway):
    args = {"chain": "robinhood", "key": KEY, "hook_data": b"", "gateway": gateway, "block_number": 100}
    evidence = admit_hook(**args, operation="lp_open", route="position_manager_eoa")
    assert evidence.profile == DopplerLiquidityProfile.name
    assert gateway.read.call_count == 1


@pytest.mark.parametrize("operation", ["lp_open", "lp_close", "lp_collect_fees"])
def test_reproduced_runtime_and_pinned_manager_qualify_only_liquidity(gateway, operation):
    evidence = verify(gateway, operation=operation)
    assert evidence.operation == operation
    assert evidence.hook == HOOK and evidence.block_number == 100
    assert evidence.block_hash == gateway.block_hash.return_value
    assert evidence.amount_equivalent_to_quoter
    assert len(RUNTIME) == 25533
    assert gateway.read.call_args.kwargs["payload"] == keccak(text="poolManager()")[:4]
    for call in (gateway.code.call_args, gateway.read.call_args):
        assert call.kwargs["target"].reference == HOOK
    for call in (gateway.code.call_args, gateway.read.call_args, gateway.block_hash.call_args):
        assert call.kwargs["chain"] == "robinhood" and call.kwargs["block_number"] == 100


@pytest.mark.parametrize(
    "changes",
    [
        {"operation": "swap_exact_in"},
        {"route": "universal_router_eoa"},
        {"route": "position_manager_safe"},
        {"chain": "base"},
        {"key": PoolKey(KEY.currency0, KEY.currency1, KEY.fee, KEY.tick_spacing, "0x" + "3" * 36 + "2546")},
    ],
)
def test_other_operation_routes_and_return_delta_flags_are_not_qualified(gateway, changes):
    assert verify(gateway, **changes) is None
    gateway.code.assert_not_called()


@pytest.mark.parametrize(
    "runtime", [b"", b"\x60\x00", RUNTIME[:-1] + bytes([RUNTIME[-1] ^ 1])], ids=["empty", "unknown", "mutated"]
)
def test_unknown_or_mutated_runtime_is_rejected(gateway, runtime):
    gateway.code.return_value = runtime
    assert verify(gateway) is None
    gateway.read.assert_not_called()


def test_wrong_manager_is_rejected(gateway):
    gateway.read.return_value = encode(["address"], [KEY.currency0])
    with pytest.raises(ValueError, match="different PoolManager"):
        verify(gateway)


def test_nonempty_hook_data_is_rejected(gateway):
    with pytest.raises(ValueError, match="empty hook_data"):
        verify(gateway, hook_data=b"\x01")


def test_unmeasured_manager_is_not_admitted(gateway):
    gateway.read.side_effect = TimeoutError("RPC timeout")
    with pytest.raises(TimeoutError):
        verify(gateway)


def test_runtime_continuity_digest_survives_new_block_but_binds_instance(gateway):
    original = verify(gateway)
    assert verify(gateway, block_number=101).dependency_digest == original.dependency_digest
    replica = PoolKey(KEY.currency0, KEY.currency1, KEY.fee, KEY.tick_spacing, "0x" + "3" * 36 + "2544")
    gateway.code.side_effect = lambda *, target, **kwargs: RUNTIME if target.reference in (HOOK, replica.hooks) else b""
    manager = encode(["address"], [UNISWAP_V4["robinhood"]["pool_manager"]])
    gateway.read.side_effect = lambda *, target, **kwargs: manager if target.reference in (HOOK, replica.hooks) else b""
    assert verify(gateway, key=replica).dependency_digest != original.dependency_digest


def test_lp_compilation_and_execution_revalidate_both_callback_admissions():
    from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
    from almanak.connectors.uniswap_v4.operation import validate_execution
    from tests.unit.connectors.uniswap_v4.test_lp_lifecycle_identity import lp_open
    from tests.unit.connectors.uniswap_v4.test_position_observation import WALLET, PositionGateway

    class DopplerGateway(PositionGateway):
        chain = "robinhood"
        hook_runtime = RUNTIME

        def code(self, *, target, **kwargs):
            if target.reference.lower() == HOOK:
                return self.hook_runtime
            return super().code(target=target, **kwargs)

    gateway = DopplerGateway()
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain="robinhood", wallet_address=WALLET),
        venue_verification_gateway_factory=lambda: gateway,
    )
    bundle = lp_open(adapter, KEY)
    assert bundle.transactions, bundle.metadata
    artifact = bundle.metadata["v4_operation"]
    assert artifact["hook_evidence"]["profile"] == DopplerLiquidityProfile.name
    assert artifact["exit_hook_evidence"]["profile"] == DopplerLiquidityProfile.name
    validate_execution(bundle, chain="robinhood", wallet=WALLET, is_safe=False, gateway=gateway)
    gateway.hook_runtime = RUNTIME[:-1] + bytes([RUNTIME[-1] ^ 1])
    with pytest.raises(ValueError, match="No reviewed hook"):
        validate_execution(bundle, chain="robinhood", wallet=WALLET, is_safe=False, gateway=gateway)
