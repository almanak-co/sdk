"""Exact GOOGLB fork transfer proof; no equity-reference or issuer-eligibility certification."""

import json
from decimal import Decimal, localcontext

import pytest
from eth_abi import decode, encode
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


@pytest.mark.bsc
@pytest.mark.lp
@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_googlb_quote_only_out_of_range_lp_round_trip(
    web3: Web3, funded_wallet: str, orchestrator: ExecutionOrchestrator, anvil_eth_call_adapter, tmp_path
):
    """A range below spot holds only quote; its close must measure zero base, not invent missing proceeds."""
    from almanak.framework.intents import LP_POSITION_MANAGERS, LPCloseIntent, LPOpenIntent

    assert web3.client_version.lower().startswith("anvil"), "Fork-only proof requires Anvil"
    assert web3.eth.chain_id == 56
    block = web3.eth.block_number
    assert _pool_read(web3, "factory()", ["address"], block)[0].lower() == FACTORY
    assert _pool_read(web3, "token0()", ["address"], block)[0].lower() == GOOGLB
    assert _pool_read(web3, "token1()", ["address"], block)[0].lower() == BSC_USD
    assert _pool_read(web3, "fee()", ["uint24"], block)[0] == 2500
    sqrt_price = _pool_read(
        web3, "slot0()", ["uint160", "int24", "uint16", "uint16", "uint16", "uint32", "bool"], block
    )[0]
    with localcontext() as ctx:
        ctx.prec = 80
        spot = Decimal(sqrt_price) ** 2 / Decimal(2**192)
    resolver = TokenResolver(cache_file=str(tmp_path / "lp-tokens.json"))
    resolver.register_token(symbol="GOOGLB", chain=CHAIN, address=GOOGLB, decimals=18, name="Alphabet bStock")
    compiler = IntentCompiler(
        chain=CHAIN,
        wallet_address=funded_wallet,
        price_oracle={"GOOGLB": spot, "USDT": Decimal("1"), GOOGLB: spot, BSC_USD: Decimal("1")},
        rpc_url=orchestrator.rpc_url,
        token_resolver=resolver,
        gateway_client=anvil_eth_call_adapter,
        venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
    )
    parser = PancakeSwapV3ReceiptParser(chain=CHAIN)
    manager = Web3.to_checksum_address(LP_POSITION_MANAGERS[CHAIN]["pancakeswap_v3"])
    before = tuple(get_token_balance(web3, token, funded_wallet) for token in (GOOGLB, BSC_USD))
    intent = LPOpenIntent(
        pool=POOL,
        amount0=Decimal("0"),
        amount1=Decimal("1"),
        range_lower=spot * Decimal("0.70"),
        range_upper=spot * Decimal("0.80"),
        protocol="pancakeswap_v3",
        chain=CHAIN,
        max_slippage=Decimal("0.01"),
    )
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", compilation.error
    assert compilation.action_bundle is not None
    assert compilation.action_bundle.metadata["pool"].lower() == POOL
    execution = await orchestrator.execute(compilation.action_bundle)
    assert execution.success is True, execution.error
    opened = []
    hashes = []
    for tx in execution.transaction_results:
        assert tx.receipt is not None and tx.receipt.status == 1
        receipt = tx.receipt.to_dict()
        parsed = parser.parse_receipt(receipt)
        assert parsed.success, parsed.error
        data = parser.extract_lp_open_data(receipt)
        if data is not None:
            opened.append(data)
        hashes.append(tx.receipt.tx_hash)
    assert len(opened) == 1
    opened_data = opened[0]
    position_id = int(opened_data.position_id)
    assert opened_data.pool_address.lower() == POOL
    after_open = tuple(get_token_balance(web3, token, funded_wallet) for token in (GOOGLB, BSC_USD))
    assert before[0] - after_open[0] == opened_data.amount0 == 0
    assert before[1] - after_open[1] == opened_data.amount1 > 0
    assert before[1] - after_open[1] <= 10**18
    position_data = web3.eth.call({"to": manager, "data": "0x99fbab88" + hex(position_id)[2:].zfill(64)})
    assert int.from_bytes(position_data[7 * 32 : 8 * 32], "big") == opened_data.liquidity > 0
    mismatch_address = decode(
        ["address"],
        web3.eth.call(
            {
                "to": Web3.to_checksum_address(FACTORY),
                "data": Web3.keccak(text="getPool(address,address,uint24)")[:4]
                + encode(
                    ["address", "address", "uint24"],
                    ["0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", BSC_USD, 500],
                ),
            }
        ),
    )[0]
    assert int(mismatch_address, 16) != 0 and mismatch_address.lower() != POOL
    mismatch = compiler.compile(
        LPCloseIntent(
            position_id=str(position_id),
            pool=mismatch_address,
            protocol="pancakeswap_v3",
            chain=CHAIN,
        )
    )
    assert mismatch.status.value != "SUCCESS"
    assert not mismatch.transactions
    assert tuple(get_token_balance(web3, token, funded_wallet) for token in (GOOGLB, BSC_USD)) == after_open
    close = LPCloseIntent(position_id=str(position_id), pool=POOL, protocol="pancakeswap_v3", chain=CHAIN)
    compilation = compiler.compile(close)
    assert compilation.status.value == "SUCCESS", compilation.error
    assert compilation.action_bundle is not None
    assert compilation.action_bundle.metadata["pool_address"].lower() == POOL
    decrease = next(tx for tx in compilation.transactions if tx.tx_type == "lp_decrease_liquidity")
    token_id, liquidity, minimum0, minimum1, _deadline = decode(
        ["uint256", "uint128", "uint256", "uint256", "uint256"], bytes.fromhex(decrease.data[2:])[4:]
    )
    assert token_id == position_id and liquidity == opened_data.liquidity
    assert minimum0 == 0 and minimum1 > 0
    execution = await orchestrator.execute(compilation.action_bundle)
    assert execution.success is True, execution.error
    collected0 = collected1 = 0
    measured = False
    for tx in execution.transaction_results:
        assert tx.receipt is not None and tx.receipt.status == 1
        receipt = tx.receipt.to_dict()
        parsed = parser.parse_receipt(receipt)
        assert parsed.success, parsed.error
        data = parser.extract_lp_close_data(receipt)
        if data is not None and data.source == "collect":
            assert data.amount0_collected is not None and data.amount1_collected is not None
            collected0 += data.amount0_collected
            collected1 += data.amount1_collected
            measured = True
        hashes.append(tx.receipt.tx_hash)
    after_close = tuple(get_token_balance(web3, token, funded_wallet) for token in (GOOGLB, BSC_USD))
    assert measured
    assert after_close[0] - after_open[0] == collected0 == 0
    assert after_close[1] - after_open[1] == collected1 > 0
    # This isolated inactive position earns no fees: no pool swaps occur between mint and close.
    assert (
        _pool_read(
            web3, "slot0()", ["uint160", "int24", "uint16", "uint16", "uint16", "uint32", "bool"], web3.eth.block_number
        )[0]
        == sqrt_price
    )
    assert abs(before[1] - after_close[1]) <= 2
    with pytest.raises(Exception, match="Invalid token ID|execution reverted"):
        web3.eth.call({"to": manager, "data": "0x99fbab88" + hex(position_id)[2:].zfill(64)})
    print(
        json.dumps(
            {
                "proof": "quote_only_out_of_range_lp",
                "pool": POOL,
                "position_id": position_id,
                "quote_in_raw": before[1] - after_open[1],
                "quote_out_raw": collected1,
                "base_out_raw": collected0,
                "minimum0_raw": minimum0,
                "minimum1_raw": minimum1,
                "mismatched_pool": mismatch_address,
                "mismatch_compilation_error": mismatch.error,
                "mint_liquidity": opened_data.liquidity,
                "nft_burn_verified": True,
                "transaction_hashes": hashes,
            }
        )
    )
