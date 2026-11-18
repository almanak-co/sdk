"""HyperEVM fee quotes price the small-block market, from captured Alchemy answers."""

import json
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.production_config import GasPolicy
from almanak.gateway.production_fees import fee_history_block, pinned_gas_price, quoted_price
from almanak.models import Block, Chain

ROOT = Path(__file__).parent
CAPTURE = json.loads((ROOT / "fixtures/hyperevm-large-block-fees.json").read_text())
POLICY = json.loads((ROOT / "strategy_fork/rehearsal-policy.json").read_text())["eip155:999"]
HYPEREVM = Chain(chain_id=ChainId("eip155:999"), environment="mainnet")
LARGE = CAPTURE["pin"]


class Reader:
    """Answers the captured headers and one-block fee histories by their block number."""

    def __init__(self, block, headers=None):
        self.block = block
        self.headers = headers or CAPTURE["headers"]
        self.calls = []

    def pin_block(self, selector):
        self.calls.append(("pin_block", selector))
        return self.block

    def _rpc(self, method, params, **kwargs):
        if method == "eth_getBlockByNumber":
            self.calls.append((method, int(params[0], 16)))
            return self.headers[str(int(params[0], 16))]
        assert method == "eth_feeHistory"
        number, percentiles = int(params[1], 16), params[2]
        self.calls.append((method, number))
        return CAPTURE["fee_history"][f"{number}:{percentiles[0] if percentiles else 'none'}"]


def pinned(number, chain=HYPEREVM):
    header = CAPTURE["headers"][str(number)]
    return Block(
        chain=chain, number=number, hash=header["hash"], timestamp=int(header["timestamp"], 16)
    )


def base_fee(number):
    return int(CAPTURE["headers"][str(number)]["baseFeePerGas"], 16)


def policy():
    return GasPolicy.model_validate_json(json.dumps({**POLICY, "accounts": ["lender"]}))


def test_a_large_block_pin_quotes_the_small_block_a_transaction_lands_in():
    block = pinned(LARGE)
    reader = Reader(block)
    price, quote = quoted_price(reader, policy(), block)
    # The large block's own history predicts the large-block market, about 87x below
    # the next small block; its small parent predicts that block exactly.
    large_next = int(CAPTURE["fee_history"][f"{LARGE}:50"]["baseFeePerGas"][1], 16)
    assert large_next * 80 < base_fee(LARGE + 1)
    assert quote.block == block
    assert quote.next_base_fee == base_fee(LARGE + 1)
    assert price >= base_fee(LARGE + 1)
    assert ("eth_feeHistory", LARGE - 1) in reader.calls
    # Re-preparation at the same pin reads the same history.
    assert quoted_price(Reader(block), policy(), block) == (price, quote)


@pytest.mark.parametrize("number", [LARGE - 1, LARGE + 1])
def test_a_small_block_pin_quotes_its_own_history(number):
    block = pinned(number)
    reader = Reader(block)
    assert fee_history_block(reader, block) == number
    _, quote = quoted_price(reader, policy(), block)
    expected = int(CAPTURE["fee_history"][f"{number}:50"]["baseFeePerGas"][1], 16)
    assert quote.next_base_fee == expected


def test_pinned_gas_price_reads_the_small_parent_of_a_large_block():
    block = pinned(LARGE)
    assert pinned_gas_price(Reader(block), block) >= base_fee(LARGE + 1)


def test_other_chains_read_no_headers():
    ethereum = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    block = pinned(LARGE, ethereum)
    reader = Reader(block)
    assert fee_history_block(reader, block) == LARGE
    assert reader.calls == []


def test_block_zero_has_no_parent_to_read():
    block = Block(chain=HYPEREVM, number=0, hash="0x" + "11" * 32, timestamp=0)
    assert fee_history_block(Reader(block), block) == 0


def test_a_parent_that_is_not_the_pins_parent_is_a_reorganization():
    block = pinned(LARGE)
    headers = {**CAPTURE["headers"]}
    headers[str(LARGE - 1)] = {**headers[str(LARGE - 1)], "hash": "0x" + "22" * 32}
    with pytest.raises(DataError, match="changed during preparation"):
        fee_history_block(Reader(block, headers), block)


def test_a_pinned_header_with_another_hash_is_a_reorganization():
    block = pinned(LARGE).model_copy(update={"hash": "0x" + "33" * 32})
    with pytest.raises(DataError, match="changed during preparation"):
        fee_history_block(Reader(block), block)


def test_an_incomplete_header_is_malformed():
    block = pinned(LARGE)
    headers = {**CAPTURE["headers"]}
    headers[str(LARGE)] = {k: v for k, v in headers[str(LARGE)].items() if k != "gasLimit"}
    with pytest.raises(DataError, match="incomplete"):
        fee_history_block(Reader(block, headers), block)


def test_the_rehearsal_ceiling_admits_the_corrected_large_block_quote():
    # 50 gwei at the 2M gas limit is the 0.1 HYPE transaction fee ceiling.
    assert (
        POLICY["max_gas_price_wei"] * POLICY["max_gas_limit"]
        == (POLICY["max_transaction_fee"]["amount"])
    )
    price, _ = quoted_price(Reader(pinned(LARGE)), policy(), pinned(LARGE))
    assert price < POLICY["max_gas_price_wei"]
