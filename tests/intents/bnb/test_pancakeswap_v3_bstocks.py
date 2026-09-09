"""Exact GOOGLB fork transfer proof; no equity-reference or issuer-eligibility certification."""

import json
from decimal import Decimal, localcontext

import pytest
from eth_abi import decode
from web3 import Web3

from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser
from almanak.framework.data.tokens.resolver import TokenResolver
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import get_token_balance, get_token_decimals

CHAIN = "bsc"
GOOGLB = "0x3f53de71c126bdabae20f9cd64848d317f6c3238"
BSC_USD = "0x55d398326f99059ff775485246999027b3197955"
POOL = "0x89001d846f7ca36ee089f73eefc25657e1798144"
FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"


def _pool_read(web3, signature, output_types, block):
    data = web3.eth.call(
        {"to": Web3.to_checksum_address(POOL), "data": Web3.keccak(text=signature)[:4]},
        block_identifier=block,
    )
    return decode(output_types, data)


@pytest.mark.bsc
@pytest.mark.swap
@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
async def test_googlb_exact_pool_entry_and_close_raw_token_round_trip(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    anvil_eth_call_adapter,
    tmp_path,
):
    """A virtual quote-unit buys real fork tokens, then sells exactly those raw units."""
    assert web3.client_version.lower().startswith("anvil"), "Fork-only proof requires Anvil"
    assert web3.eth.chain_id == 56
    base = Web3.to_checksum_address(GOOGLB)
    quote = Web3.to_checksum_address(BSC_USD)
    pool = Web3.to_checksum_address(POOL)
    block = web3.eth.block_number
    block_hash = web3.eth.get_block(block)["hash"].hex()
    assert _pool_read(web3, "factory()", ["address"], block)[0].lower() == FACTORY
    assert _pool_read(web3, "token0()", ["address"], block)[0].lower() == GOOGLB
    assert _pool_read(web3, "token1()", ["address"], block)[0].lower() == BSC_USD
    assert _pool_read(web3, "fee()", ["uint24"], block)[0] == 2500
    assert _pool_read(web3, "tickSpacing()", ["int24"], block)[0] == 50
    assert (get_token_decimals(web3, base), get_token_decimals(web3, quote)) == (18, 18)
    sqrt_price = _pool_read(
        web3, "slot0()", ["uint160", "int24", "uint16", "uint16", "uint16", "uint32", "bool"], block
    )[0]
    assert sqrt_price > 0
    with localcontext() as ctx:
        ctx.prec = 80
        base_in_quote = Decimal(sqrt_price) ** 2 / Decimal(2**192)

    # Pool-derived compiler valuation isolates execution. It is neither an
    # independent fair-value oracle nor a USD/reference freshness assertion.
    resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"))
    resolver.register_token(symbol="GOOGLB", chain=CHAIN, address=base, decimals=18, name="Alphabet bStock")
    prices = {
        "GOOGLB": base_in_quote,
        base.lower(): base_in_quote,
        f"bsc:{base.lower()}": base_in_quote,
        "USDT": Decimal("1"),
        quote.lower(): Decimal("1"),
        f"bsc:{quote.lower()}": Decimal("1"),
    }
    compiler = IntentCompiler(
        chain=CHAIN,
        wallet_address=funded_wallet,
        price_oracle=prices,
        rpc_url=orchestrator.rpc_url,
        token_resolver=resolver,
        venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
    )
    parser = PancakeSwapV3ReceiptParser(chain=CHAIN)
    base_before = get_token_balance(web3, base, funded_wallet)
    quote_before = get_token_balance(web3, quote, funded_wallet)
    entry_raw = 10**18
    entry = SwapIntent(
        from_token=quote,
        to_token=base,
        amount=Decimal("1"),
        max_slippage=Decimal("0.01"),
        max_price_impact=Decimal("0.01"),
        protocol="pancakeswap_v3",
        chain=CHAIN,
        swap_params={"pool": pool, "fee_tier": 2500},
    )
    entry_compilation = compiler.compile(entry)
    assert entry_compilation.status.value == "SUCCESS", entry_compilation.error
    assert entry_compilation.action_bundle is not None
    entry_execution = await orchestrator.execute(entry_compilation.action_bundle)
    assert entry_execution.success, entry_execution.error
    entry_swaps = []
    tx_hashes = []
    for result in entry_execution.transaction_results:
        assert result.receipt is not None
        assert result.receipt.status == 1
        receipt = result.receipt.to_dict()
        parsed = parser.parse_receipt(receipt)
        assert parsed.success, parsed.error
        entry_swaps.extend(parsed.swaps)
        tx_hashes.append(result.receipt.tx_hash)
    assert len(entry_swaps) == 1
    assert entry_swaps[0].pool.lower() == POOL
    assert entry_swaps[0].recipient.lower() == funded_wallet.lower()
    assert entry_swaps[0].amount0 < 0 and entry_swaps[0].amount1 == entry_raw
    base_after_entry = get_token_balance(web3, base, funded_wallet)
    quote_after_entry = get_token_balance(web3, quote, funded_wallet)
    received = base_after_entry - base_before
    assert received > 0
    assert received == -entry_swaps[0].amount0
    assert quote_before - quote_after_entry == entry_raw

    with localcontext() as ctx:
        ctx.prec = 80
        close_amount = Decimal(received) / Decimal(10**18)
    close = SwapIntent(
        from_token=base,
        to_token=quote,
        amount=close_amount,
        max_slippage=Decimal("0.01"),
        max_price_impact=Decimal("0.01"),
        protocol="pancakeswap_v3",
        chain=CHAIN,
        swap_params={"pool": pool, "fee_tier": 2500},
    )
    close_compilation = compiler.compile(close)
    assert close_compilation.status.value == "SUCCESS", close_compilation.error
    assert close_compilation.action_bundle is not None
    close_execution = await orchestrator.execute(close_compilation.action_bundle)
    assert close_execution.success, close_execution.error
    close_swaps = []
    for result in close_execution.transaction_results:
        assert result.receipt is not None
        assert result.receipt.status == 1
        receipt = result.receipt.to_dict()
        parsed = parser.parse_receipt(receipt)
        assert parsed.success, parsed.error
        close_swaps.extend(parsed.swaps)
        tx_hashes.append(result.receipt.tx_hash)
    assert len(close_swaps) == 1
    assert close_swaps[0].pool.lower() == POOL
    assert close_swaps[0].recipient.lower() == funded_wallet.lower()
    assert close_swaps[0].amount0 == received and close_swaps[0].amount1 < 0
    base_after_close = get_token_balance(web3, base, funded_wallet)
    quote_after_close = get_token_balance(web3, quote, funded_wallet)
    assert base_after_entry - base_after_close == received
    assert base_after_close == base_before
    assert quote_after_close - quote_after_entry == -close_swaps[0].amount1 > 0
    print(
        json.dumps(
            {
                "proof": "fork_raw_token_round_trip",
                "valuation": "pool_derived_quote_units_not_equity_reference",
                "block": block,
                "block_hash": block_hash,
                "wallet": funded_wallet,
                "pool": POOL,
                "entry_quote_raw": entry_raw,
                "base_received_raw": received,
                "close_quote_received_raw": quote_after_close - quote_after_entry,
                "residual_base_raw": base_after_close - base_before,
                "transaction_hashes": tx_hashes,
            }
        )
    )
