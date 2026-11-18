"""A synthetic chain for pool price series: block times, pool prices and Swap logs.

It answers the raw JSON-RPC reads the SDK makes (headers, the pool's `slot0`, Swap
logs) and the two historical gateway operations, and counts every read.
"""

import json
from collections import Counter
from decimal import Decimal

from almanak.backtest.history_logs import MAX_LOG_BLOCKS
from almanak.backtest.pool_prices import PricePool
from almanak.backtest.pool_reads import V3_SWAP, fetch_swaps
from almanak.caip import AssetType, ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Asset, Block, Chain

HOUR = 3600
CHAIN = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
CBBTC = AssetType("eip155:8453/erc20:0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf")
POOL = "0xd0b53d9277642d899df5c87a3966a349a798f224"
GENESIS = HOUR  # the first block's timestamp


def sqrt_price(usdc_per_weth: Decimal) -> int:
    """The sqrtPriceX96 of a WETH (18 decimals) / USDC (6 decimals) pool at a price."""
    raw = usdc_per_weth * Decimal(10) ** -12
    return int((raw.sqrt() * Decimal(2) ** 96).to_integral_value())


PRICE_POOL = PricePool(
    connector="uniswap_v3",
    pool=POOL,
    block=Block(chain=CHAIN, number=1, hash="0x" + f"{1:064x}", timestamp=GENESIS + 2),
    assets=(
        Asset(chain=CHAIN, identifier=WETH, decimals=18),
        Asset(chain=CHAIN, identifier=USDC, decimals=6),
    ),
    pricing="sqrt_price",
    state=POOL,
    emitter=POOL,
    swap_topics=(V3_SWAP,),
)


def word(value: int) -> str:
    return (value % 2**256).to_bytes(32, "big").hex()


class FakeChain(EthereumReader):
    """Blocks every `spacing` seconds (or at `timestamps`); `price(n)` is the pool's
    sqrtPriceX96 after block n; `swaps` are (block, amount0, amount1, sqrtPriceX96);
    the finalized block trails the head by `finality_lag` blocks.
    Like Alchemy, it refuses a log range longer than `MAX_LOG_BLOCKS` holding more than
    `log_cap` logs (HTTP 400, JSON-RPC -32602)."""

    def __init__(
        self,
        blocks=20 * 1800,
        spacing=2,
        timestamps=None,
        price=None,
        swaps=(),
        log_cap=10_000,
        finality_lag=0,
    ):
        super().__init__(CHAIN, "https://fake-chain.invalid")
        self.timestamps = timestamps or [GENESIS + spacing * n for n in range(blocks)]
        self.price = price or (lambda n: sqrt_price(Decimal(2000 + n // 1800)))
        self.swaps = list(swaps)
        self.reads: Counter = Counter()
        self.removed: set[int] = set()
        self.log_cap = log_cap
        self.log_ranges: list[tuple[int, int]] = []
        self.finality_lag = finality_lag

    def block(self, number: int) -> dict:
        return {
            "number": hex(number),
            "hash": "0x" + f"{number:064x}",
            "timestamp": hex(self.timestamps[number]),
        }

    def header(self, number: int) -> Block:
        return Block(
            chain=CHAIN,
            number=number,
            hash="0x" + f"{number:064x}",
            timestamp=self.timestamps[number],
        )

    def floor(self, at: int) -> int:
        return max(n for n, ts in enumerate(self.timestamps) if ts <= at)

    def _rpc(self, method, params, *, allow_none=False):
        self.reads[method] += 1
        if method == "eth_chainId":
            return hex(8453)
        if method == "eth_getBlockByNumber":
            tag = params[0]
            head = len(self.timestamps) - 1
            if tag == "latest":
                number = head
            elif tag == "finalized":
                number = head - self.finality_lag
            else:
                number = int(tag, 16)
            return self.block(number)
        if method == "eth_call":
            call, anchor = params
            assert call["to"] == POOL and call["data"] == "0x3850c7bd"
            number = int(anchor["blockHash"], 16)
            return "0x" + word(self.price(number)) + word(0) * 6
        if method == "eth_getLogs":
            (query,) = params
            first, last = int(query["fromBlock"], 16), int(query["toBlock"], 16)
            self.log_ranges.append((first, last))
            logs = [
                self.log(index, swap)
                for index, swap in enumerate(self.swaps)
                if first <= swap[0] <= last and query["address"] == POOL
            ]
            if last - first + 1 > MAX_LOG_BLOCKS and len(logs) > self.log_cap:
                raise DataError("http", "eth_getLogs: HTTP 400", code=-32602)
            return logs
        raise AssertionError(f"unexpected read {method}")

    def log(self, index, swap) -> dict:
        number, amount0, amount1, price = swap
        return {
            "removed": index in self.removed,
            "address": POOL,
            "blockNumber": hex(number),
            "blockHash": "0x" + f"{number:064x}",
            "transactionHash": "0x" + f"{index:064x}",
            "transactionIndex": hex(0),
            "logIndex": hex(index),
            "topics": [V3_SWAP, "0x" + "00" * 32, "0x" + "00" * 32],
            "data": "0x" + word(amount0) + word(amount1) + word(price) + word(1) + word(0),
        }

    def send(self, operation, request):
        """The historical gateway's price pool operations, as a backtest reaches them."""
        self.reads[operation] += 1
        if operation == "price_pool":
            return json.loads(PRICE_POOL.model_dump_json())
        assert operation == "swap_read"
        return json.loads(fetch_swaps(self, request).model_dump_json())
