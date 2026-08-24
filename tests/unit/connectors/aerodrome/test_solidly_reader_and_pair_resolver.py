"""Unit tests for the Solidly (Aerodrome classic) reader + pair resolver (ALM-3365).

Scripted-RPC tests: getReserves/token0/token1/decimals/stable on the pool,
getFee on the factory, getPool on the factory for pair resolution. Values
mirror the on-chain Base AERO/USDC volatile pool verified 2026-08-18
(getFee=30 → fee_tier 3000 in v3 units).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.aerodrome.pair_resolver import resolve_pair_payload
from almanak.connectors.aerodrome.pool_reader import CLASSIC_POOL_READER_SPEC, SLIPSTREAM_POOL_READER_SPEC
from almanak.connectors.aerodrome.solidly_reader import (
    DECIMALS_SELECTOR,
    GET_FEE_SELECTOR,
    GET_RESERVES_SELECTOR,
    STABLE_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    SolidlyPoolReader,
)
from almanak.framework.data.exceptions import DataUnavailableError

USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
AERO = "0x940181a94a35a4569e4529a3cdfb74e38fd98631"
POOL = "0x6cdcb1c4a4d1c3c6d054b27ac5b77e89eafb971d"
STABLE_POOL = "0x1111111111111111111111111111111111111111"
FACTORY = CLASSIC_POOL_READER_SPEC.factory_addresses["base"].lower()


def _word(value: int) -> bytes:
    return value.to_bytes(32, "big")


def _addr(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address.removeprefix("0x"))


def _scripted_rpc(script: dict[tuple[str, str], bytes]):
    def _call(chain: str, to: str, data: str) -> bytes:
        key = (to.lower(), data)
        if key not in script:
            raise ValueError(f"unscripted call to={to} data={data[:10]}")
        return script[key]

    return _call


def _pool_script(pool: str = POOL, *, stable: int = 0, reserve0: int = 14_581_689_696_646) -> dict:
    return {
        (pool, GET_RESERVES_SELECTOR): _word(reserve0) + _word(29_565_000_000_000_000_000_000) + _word(0),
        (pool, TOKEN0_SELECTOR): _addr(USDC),
        (pool, TOKEN1_SELECTOR): _addr(AERO),
        (pool, STABLE_SELECTOR): _word(stable),
        (USDC, DECIMALS_SELECTOR): _word(6),
        (AERO, DECIMALS_SELECTOR): _word(18),
        (
            FACTORY,
            GET_FEE_SELECTOR + pool.removeprefix("0x").zfill(64) + str(stable).zfill(64),
        ): _word(30),
    }


def test_read_pool_price_reserve_ratio_and_fee():
    reader = SolidlyPoolReader(rpc_call=_scripted_rpc(_pool_script()), spec=CLASSIC_POOL_READER_SPEC)
    envelope = reader.read_pool_price(POOL, "base")
    state = envelope.value
    # price of token0 (USDC) in token1 (AERO): (29565e18/1e18) / (14581689.696646e6/1e6)
    expected = (Decimal(29_565_000_000_000_000_000_000) / Decimal(10) ** 18) / (
        Decimal(14_581_689_696_646) / Decimal(10) ** 6
    )
    assert state.price == expected
    assert state.fee_tier == 3000
    assert state.tick is None
    assert state.liquidity == 14_581_689_696_646
    assert state.token0_decimals == 6
    assert state.token1_decimals == 18


def test_read_pool_price_stable_uses_samm_marginal_not_reserve_ratio():
    # Imbalanced sAMM: reserve ratio would be 0.5; the x³y+xy³ marginal is
    # (3x²y+y³)/(x³+3xy²). Verified on-chain 2026-08-24 against the Base
    # sAMM DAI/USDC pool's own getAmountOut quote (79 ppm apart at 15%
    # imbalance, where the reserve ratio was off by 14.8%).
    reserve0, reserve1 = 2_000_000_000_000, 1_000_000 * 10**18  # 2M USDC vs 1M AERO
    script = _pool_script(stable=1, reserve0=reserve0)
    script[(POOL, GET_RESERVES_SELECTOR)] = _word(reserve0) + _word(reserve1) + _word(0)
    reader = SolidlyPoolReader(rpc_call=_scripted_rpc(script), spec=CLASSIC_POOL_READER_SPEC)
    state = reader.read_pool_price(POOL, "base").value
    x = Decimal(reserve0) / Decimal(10) ** 6
    y = Decimal(reserve1) / Decimal(10) ** 18
    assert state.price == (3 * x * x * y + y**3) / (x**3 + 3 * x * y * y)
    assert state.price != y / x


def test_read_pool_price_fails_closed_on_empty_pool():
    script = _pool_script(reserve0=0)
    reader = SolidlyPoolReader(rpc_call=_scripted_rpc(script), spec=CLASSIC_POOL_READER_SPEC)
    with pytest.raises(DataUnavailableError):
        reader.read_pool_price(POOL, "base")


def test_read_pool_price_fails_closed_on_zero_reserve1():
    # A zero reserve1 must not surface as an EXECUTION_GRADE price of 0.
    script = _pool_script()
    script[(POOL, GET_RESERVES_SELECTOR)] = _word(14_581_689_696_646) + _word(0) + _word(0)
    reader = SolidlyPoolReader(rpc_call=_scripted_rpc(script), spec=CLASSIC_POOL_READER_SPEC)
    with pytest.raises(DataUnavailableError):
        reader.read_pool_price(POOL, "base")


def test_reader_requires_spec():
    with pytest.raises(ValueError, match="PoolReaderSpec"):
        SolidlyPoolReader(rpc_call=_scripted_rpc({}))


def test_pair_resolver_picks_deeper_flavour():
    from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

    def fake_validate(chain, token_a, token_b, stable, rpc_url, gateway_client=None):
        pool = STABLE_POOL if stable else POOL
        return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool)

    script = _pool_script() | _pool_script(STABLE_POOL, stable=1, reserve0=1_000_000)
    with (
        patch("almanak.connectors.aerodrome.pair_resolver.validate_aerodrome_pool", side_effect=fake_validate),
        patch(
            "almanak.connectors._strategy_base.pool_validation_base.eth_call",
            side_effect=lambda rpc_url, to, data, timeout=10.0, *, chain=None, gateway_client=None, **kw: _scripted_rpc(
                script
            )(chain, to, data),
        ),
    ):
        payload = resolve_pair_payload("base", AERO, USDC, rpc_url="http://x")
    assert payload is not None
    assert payload["pool_address"] == POOL
    assert payload["stable"] is False
    assert payload["fee_tier"] == 3000
    assert payload["fee_tier_source"] == "sweep"
    assert payload["lp_token"] == POOL


def _resolve_script(*, volatile_pool: str | None = POOL, stable_pool: str | None = STABLE_POOL) -> dict:
    from almanak.connectors._strategy_base.v3_pool_abi import encode_get_pool

    script = _pool_script() | _pool_script(STABLE_POOL, stable=1, reserve0=1_000_000)
    for flag, pool in ((0, volatile_pool), (1, stable_pool)):
        calldata = encode_get_pool("0x79bc57d5", USDC, AERO, flag)
        script[(FACTORY, calldata)] = _addr(pool) if pool else bytes(32)
    return script


def test_resolve_pool_address_pins_stable_flag():
    reader = SolidlyPoolReader(rpc_call=_scripted_rpc(_resolve_script()), spec=CLASSIC_POOL_READER_SPEC)
    assert reader.resolve_pool_address(USDC, AERO, "base", fee_tier=1) == STABLE_POOL
    assert reader.resolve_pool_address(USDC, AERO, "base", fee_tier=0) == POOL


def test_resolve_pool_address_translates_generic_fee_tier_to_deeper_flavour():
    # Generic v3 sweeps (LWAP default tiers, pool_price_by_pair's 3000) must
    # not be ABI-encoded into the factory's bool argument.
    reader = SolidlyPoolReader(rpc_call=_scripted_rpc(_resolve_script()), spec=CLASSIC_POOL_READER_SPEC)
    assert reader.resolve_pool_address(USDC, AERO, "base", fee_tier=3000) == POOL
    reader_stable_only = SolidlyPoolReader(
        rpc_call=_scripted_rpc(_resolve_script(volatile_pool=None)), spec=CLASSIC_POOL_READER_SPEC
    )
    assert reader_stable_only.resolve_pool_address(USDC, AERO, "base", fee_tier=3000) == STABLE_POOL


def test_pair_resolver_rejects_non_flag_fee_tier():
    with pytest.raises(ValueError, match="stable flag"):
        resolve_pair_payload("base", AERO, USDC, fee_tier=3000, rpc_url="http://x")


def test_pair_resolver_honest_miss():
    from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

    miss = PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND)
    with patch("almanak.connectors.aerodrome.pair_resolver.validate_aerodrome_pool", return_value=miss):
        assert resolve_pair_payload("base", AERO, USDC, rpc_url="http://x") is None


def test_pair_resolver_indeterminate_transport_raises_not_miss():
    from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult

    unknown = PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down")
    with patch("almanak.connectors.aerodrome.pair_resolver.validate_aerodrome_pool", return_value=unknown):
        with pytest.raises(RuntimeError, match="indeterminate"):
            resolve_pair_payload("base", AERO, USDC, rpc_url="http://x")


def test_slipstream_known_pool_ts200_is_the_cl_pool():
    # Regression: this slot previously pointed at the classic AERO/USDC vAMM
    # pool. Verified on-chain 2026-08-18: cl_factory.getPool(WETH, USDC, 200).
    known = SLIPSTREAM_POOL_READER_SPEC.known_pools["base"]
    entry = known[
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            200,
        )
    ]
    assert entry.lower() == "0x148bc43946a902258916e580b0e6d92aaa74746f"
    assert entry.lower() != POOL


def test_specs_declare_pair_resolvers():
    from almanak.connectors.curve.pool_reader import POOL_READER_SPEC as CURVE_SPEC
    from almanak.connectors.uniswap_v4.pool_reader import POOL_READER_SPEC as V4_SPEC

    for spec in (CLASSIC_POOL_READER_SPEC, CURVE_SPEC, V4_SPEC):
        assert spec.pair_resolver is not None
        assert callable(spec.pair_resolver.load())
