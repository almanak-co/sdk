from eth_abi import encode
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.connectors.uniswap_v4.domain import PoolObservation, UniswapPool
from almanak.connectors.uniswap_v4.hooks import DYNAMIC_FEE
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import currency_address
from almanak.models import Block, address


def pool_id(source: UniswapPool) -> str:
    key = (source.currency0, source.currency1, source.fee, source.tick_spacing, source.hooks)
    return key_id(source.chain.chain_id, key)


def key_id(chain: ChainId, key) -> str:
    """The PoolKey hash, from the five key members alone; admission is not consulted."""
    currency0, currency1, fee, tick_spacing, hooks = key
    encoded = encode(
        ["address", "address", "uint24", "int24", "address"],
        [
            currency_address(currency0, chain),
            currency_address(currency1, chain),
            fee,
            tick_spacing,
            hooks,
        ],
    )
    return "0x" + keccak(encoded).hex()


def read_pool(reader: EthereumReader, source: UniswapPool, block: Block) -> PoolObservation:
    if source.chain != reader.chain or block.chain != source.chain:
        raise DataError("chain_mismatch", "pool source has a different chain/environment")
    identifier = bytes.fromhex(pool_id(source)[2:])
    (manager,) = reader.call(source.state_view, "poolManager()", block, outputs=("address",))
    if address(manager) != source.pool_manager:
        raise DataError("source_mismatch", "StateView is bound to a different PoolManager")
    sqrt_price, tick, protocol_fee, lp_fee = reader.call(
        source.state_view,
        "getSlot0(bytes32)",
        block,
        outputs=("uint160", "int24", "uint24", "uint24"),
        inputs=("bytes32",),
        args=(identifier,),
    )
    if sqrt_price == 0:
        raise DataError("unavailable", "requested pool is not initialized at this block")
    (liquidity,) = reader.call(
        source.state_view,
        "getLiquidity(bytes32)",
        block,
        outputs=("uint128",),
        inputs=("bytes32",),
        args=(identifier,),
    )
    # A dynamic-fee pool stores the hook's last fee update; its reviewed hook
    # overrides the fee per swap, so slot0 cannot be compared with the key.
    if source.fee != DYNAMIC_FEE and lp_fee != source.fee:
        raise DataError("source_mismatch", "observed LP fee differs from the static PoolKey fee")
    return PoolObservation(
        source=source,
        block=block,
        currency0=reader.asset(source.currency0, block),
        currency1=reader.asset(source.currency1, block),
        sqrt_price_x96=sqrt_price,
        tick=tick,
        liquidity=liquidity,
        protocol_fee=protocol_fee,
        lp_fee=lp_fee,
    )
