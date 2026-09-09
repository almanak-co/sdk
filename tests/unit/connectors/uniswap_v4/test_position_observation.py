"""NFT ownership, subscriber dispatch and principal floor safety contracts."""

from dataclasses import replace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.position import observe_position, withdrawal_minima
from tests.unit.connectors.uniswap_v4.test_operation_contract import TOKEN, WALLET, ZERO, Gateway

KEY = PoolKey(ZERO, TOKEN, 777, 10)


class PositionGateway(Gateway):
    owner = WALLET
    subscriber = 0
    liquidity = 1000000000000000000
    key = KEY
    wrong_id = False

    def read(self, *, payload, **kwargs):
        selector = payload[:4]
        if selector == keccak(text="ownerOf(uint256)")[:4]:
            return encode(["address"], [self.owner])
        if selector == keccak(text="getPositionLiquidity(uint256)")[:4]:
            return encode(["uint128"], [self.liquidity])
        if selector == keccak(text="getPoolAndPositionInfo(uint256)")[:4]:
            identifier = 0 if self.wrong_id else (int(self.key.pool_id, 16) >> 56) << 56
            info = identifier | (100 << 32) | (((1 << 24) - 100) << 8) | self.subscriber
            return encode(
                ["address", "address", "uint24", "int24", "address", "uint256"],
                [self.key.currency0, self.key.currency1, self.key.fee, self.key.tick_spacing, self.key.hooks, info],
            )
        return super().read(payload=payload, **kwargs)


def test_owned_position_has_full_key_and_signed_ticks():
    position = observe_position(PositionGateway(), chain="base", token_id=42, wallet=WALLET)
    assert position.key == KEY and (position.tick_lower, position.tick_upper) == (-100, 100)
    assert position.liquidity == 10**18


@pytest.mark.parametrize("mutation", ["owner", "subscriber", "wrong_id", "uninitialized"])
def test_position_refuses_unqualified_or_mismatched_state(mutation):
    gateway = PositionGateway()
    if mutation == "owner":
        gateway.owner = TOKEN
    elif mutation == "subscriber":
        gateway.subscriber = 1
    elif mutation == "wrong_id":
        gateway.wrong_id = True
    else:
        gateway.price = 0
    with pytest.raises(ValueError):
        observe_position(gateway, chain="base", token_id=42, wallet=WALLET)


@pytest.mark.parametrize("liquidity,bps", [(0, 50), (-1, 50), (10**18 + 1, 50), (True, 50), (1, 10000), (1, -1)])
def test_invalid_withdrawal_never_becomes_an_unbounded_close(liquidity, bps):
    position = observe_position(PositionGateway(), chain="base", token_id=42, wallet=WALLET)
    with pytest.raises(ValueError):
        withdrawal_minima(position, liquidity, bps)


def test_principal_floors_are_monotonic_and_partial_liquidity_is_bounded():
    position = observe_position(PositionGateway(), chain="base", token_id=42, wallet=WALLET)
    tight = withdrawal_minima(position, position.liquidity, 50)
    loose = withdrawal_minima(position, position.liquidity, 1000)
    half = withdrawal_minima(position, position.liquidity // 2, 50)
    for index in (0, 1):
        assert 0 < half[index] <= loose[index] < tight[index]
        assert abs(2 * half[index] - tight[index]) <= 2


def test_one_sided_position_has_only_the_measured_leg_floor():
    position = observe_position(PositionGateway(), chain="base", token_id=42, wallet=WALLET)
    below = replace(position, sqrt_price_x96=1)
    above = replace(position, sqrt_price_x96=2**150)
    assert withdrawal_minima(below, below.liquidity, 50)[1] == 0
    assert withdrawal_minima(above, above.liquidity, 50)[0] == 0


def test_rounding_both_legs_to_zero_requires_explicit_policy():
    position = observe_position(PositionGateway(), chain="base", token_id=42, wallet=WALLET)
    with pytest.raises(ValueError, match="round to zero"):
        withdrawal_minima(position, 1, 9900)


@pytest.mark.parametrize("increase", [False, True])
def test_full_close_requires_recompile_if_owned_liquidity_changes(increase):
    from almanak.connectors.uniswap_v4.operation import _validate_position_continuity
    from almanak.framework.models.reproduction_bundle import ActionBundle

    gateway = PositionGateway()
    quoted_liquidity = gateway.liquidity
    bundle = ActionBundle(
        intent_type="LP_CLOSE",
        transactions=[],
        metadata={
            "position_id": "42",
            "liquidity_removed": quoted_liquidity,
            "close_all": True,
        },
    )
    _validate_position_continuity(bundle, KEY, gateway, "base", WALLET)
    gateway.liquidity += 1 if increase else -1
    with pytest.raises(ValueError, match="liquidity"):
        _validate_position_continuity(bundle, KEY, gateway, "base", WALLET)
