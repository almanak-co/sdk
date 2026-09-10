"""Block-scoped NFT identity and integer principal bounds for V4 withdrawals."""

from dataclasses import dataclass

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors._strategy_base.concentrated_liquidity_math import tick_to_sqrt_price_x96
from almanak.connectors._strategy_base.slippage import compute_min_amount_out_from_bps
from almanak.connectors._strategy_base.v4_pool_abi import encode_get_slot0
from almanak.framework.venues import VenueTargetRole, VenueVerificationGateway

from .addresses import UNISWAP_V4
from .pool_key import PoolKey
from .venue_verifier import address_ref


@dataclass(frozen=True, slots=True)
class PositionObservation:
    key: PoolKey
    token_id: int
    owner: str
    liquidity: int
    tick_lower: int
    tick_upper: int
    sqrt_price_x96: int
    block_number: int
    block_hash: str


def observe_position(
    gateway: VenueVerificationGateway,
    *,
    chain: str,
    token_id: int,
    wallet: str,
) -> PositionObservation:
    if type(token_id) is not int or token_id < 0:
        raise ValueError("V4 token_id must be a nonnegative integer")
    addresses = UNISWAP_V4[chain]
    target = address_ref(VenueTargetRole.POSITION_MANAGER, addresses["position_manager"])
    block = gateway.block_number(chain=chain)
    block_hash = gateway.block_hash(chain=chain, block_number=block)

    def read(signature: str) -> bytes:
        return gateway.read(
            chain=chain,
            target=target,
            payload=keccak(text=signature)[:4] + encode(["uint256"], [token_id]),
            block_number=block,
        )

    owner = decode(["address"], read("ownerOf(uint256)"))[0].lower()
    if owner != wallet.lower():
        raise ValueError("V4 NFT owner does not match the executing wallet")
    currency0, currency1, fee, spacing, hooks, info = decode(
        ["address", "address", "uint24", "int24", "address", "uint256"],
        read("getPoolAndPositionInfo(uint256)"),
    )
    key = PoolKey.from_wire(
        {"currency0": currency0, "currency1": currency1, "fee": fee, "tick_spacing": spacing, "hooks": hooks}
    )
    if info & 0xFF:
        raise ValueError("Subscribed V4 positions require a separately reviewed subscriber behavior profile")
    if info >> 56 != int(key.pool_id, 16) >> 56:
        raise ValueError("V4 position info does not match the full PoolKey")

    def tick(shift: int) -> int:
        value = (info >> shift) & 0xFFFFFF
        return value - (1 << 24) if value & (1 << 23) else value

    lower, upper = tick(8), tick(32)
    if not -887272 <= lower < upper <= 887272 or lower % spacing or upper % spacing:
        raise ValueError("V4 position ticks do not match the pool spacing")
    liquidity = decode(["uint128"], read("getPositionLiquidity(uint256)"))[0]
    slot = gateway.read(
        chain=chain,
        target=address_ref(VenueTargetRole.PERMISSION_TARGET, addresses["state_view"]),
        payload=bytes.fromhex(encode_get_slot0(key.pool_id)[2:]),
        block_number=block,
    )
    price = decode(["uint160", "int24", "uint24", "uint24"], slot)[0]
    if not price or gateway.block_hash(chain=chain, block_number=block) != block_hash:
        raise ValueError("V4 position state is unavailable or reorganized")
    return PositionObservation(key, token_id, owner, liquidity, lower, upper, price, block, block_hash)


def withdrawal_minima(position: PositionObservation, liquidity: int, slippage_bps: int) -> tuple[int, int]:
    """Per-leg principal minima at the requested tolerance, excluding unmeasured fees."""
    if type(liquidity) is not int or not 0 < liquidity <= position.liquidity:
        raise ValueError("Withdrawal liquidity must be positive and bounded by the measured NFT liquidity")
    if type(slippage_bps) is not int or not 0 <= slippage_bps < 10000:
        raise ValueError("Default withdrawal bounds require slippage below 100%")
    lower = tick_to_sqrt_price_x96(position.tick_lower)
    upper = tick_to_sqrt_price_x96(position.tick_upper)
    price = min(upper, max(lower, position.sqrt_price_x96))
    amount0 = liquidity * (upper - price) * (1 << 96) // (price * upper)
    amount1 = liquidity * (price - lower) // (1 << 96)
    minima = (
        compute_min_amount_out_from_bps(amount0, slippage_bps),
        compute_min_amount_out_from_bps(amount1, slippage_bps),
    )
    if minima == (0, 0):
        raise ValueError("Both measured withdrawal floors round to zero; provide explicit author minima")
    return minima
