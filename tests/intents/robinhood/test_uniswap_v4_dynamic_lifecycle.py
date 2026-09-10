"""Real managed-fork lifecycles for a catalogue-external, active dynamic hook."""

from copy import deepcopy
from decimal import Decimal

import pytest
from eth_abi import decode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.operation import transaction_digest
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.position import observe_position
from almanak.connectors.uniswap_v4.receipt_parser import MODIFY_LIQUIDITY_TOPIC, UniswapV4ReceiptParser
from almanak.framework.data.tokens import TokenResolver
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from tests.conftest_gateway import _create_anvil_fixture
from tests.intents.conftest import get_token_balance, get_token_decimals

pytestmark = [
    pytest.mark.robinhood,
    pytest.mark.no_zodiac(reason="Reviewed active-hook operation profile admits ERC20 EOA routes only"),
]

# This pool was created after the chain-wide intent fixture's calibrated block.
anvil_robinhood = _create_anvil_fixture(
    "robinhood", public_rpc_fallback="https://rpc.mainnet.chain.robinhood.com", fork_block_number=59_190_000
)
KEY = PoolKey(
    "0x29cdaa3a468682573f405f070884eb933cde1e18",
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168",
    8388608,
    8,
    "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544",
)
PARAMS = {"pool_key": KEY.to_wire(), "pool_id": KEY.pool_id, "hook_data": "0x"}


@pytest.fixture
def dynamic_compiler(web3, funded_wallet, anvil_eth_call_adapter, tmp_path):
    resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"))
    for token, symbol, decimals in ((KEY.currency0, "BELIKEBOB", 18), (KEY.currency1, "USDG", 6)):
        assert get_token_decimals(web3, token) == decimals
        symbol_data = anvil_eth_call_adapter.eth_call("robinhood", token, "0x95d89b41")
        assert decode(["string"], bytes.fromhex(symbol_data[2:]))[0] == symbol
        resolver.register_token(symbol=symbol, chain="robinhood", address=token, decimals=decimals)
    response = anvil_eth_call_adapter.eth_call(
        "robinhood",
        UNISWAP_V4["robinhood"]["state_view"],
        "0x" + keccak(text="getSlot0(bytes32)")[:4].hex() + KEY.pool_id[2:],
    )
    sqrt_price, _, _, _ = decode(["uint160", "int24", "uint24", "uint24"], bytes.fromhex(response[2:]))
    assert sqrt_price > 0
    # The fork's measured spot supplies the relative price; USDG is the numeraire.
    ratio = (Decimal(sqrt_price) / Decimal(2**96)) ** 2 * Decimal(10**12)
    return IntentCompiler(
        chain="robinhood",
        wallet_address=funded_wallet,
        token_resolver=resolver,
        gateway_client=anvil_eth_call_adapter,
        venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
        price_oracle={KEY.currency0: ratio, KEY.currency1: Decimal(1)},
    )


def balances(web3, wallet):
    return tuple(get_token_balance(web3, token, wallet) for token in (KEY.currency0, KEY.currency1))


def assert_execution(result, bundle):
    assert result.success, result.error
    evidence = result.extracted_data["execution_evidence"]
    assert evidence["simulation"]["simulated"] is True
    assert evidence["simulation"]["success"] is True
    assert all(item["status"] == "accepted" for item in evidence["connector_validation"])
    artifact = bundle.metadata["v4_operation"]
    assert artifact["pool_key"] == KEY.to_wire()
    assert artifact["transaction_digest"] == transaction_digest(bundle.transactions)
    assert artifact["hook_evidence"]["hook"] == KEY.hooks
    for validation in evidence["connector_validation"]:
        for observation in validation["observations"]:
            freshness = observation["freshness"]
            assert freshness["managed_fork"] is True
            assert freshness["quote"]["number"] == artifact["quote_block"]
            assert freshness["expected_quote_hash"] == artifact["quote_block_hash"]
    assert bundle.metadata["pool_id"] == KEY.pool_id
    receipts = [tx.receipt.to_dict() for tx in result.transaction_results if tx.receipt is not None]
    assert len(receipts) == len(bundle.transactions)
    assert all(int(receipt["status"]) == 1 for receipt in receipts)
    return receipts


def assert_lp_receipt(receipt):
    assert receipt["to_address"].lower() == UNISWAP_V4["robinhood"]["position_manager"].lower()
    assert any(
        log["address"].lower() == UNISWAP_V4["robinhood"]["pool_manager"].lower()
        and log["topics"][0].lower() == MODIFY_LIQUIDITY_TOPIC
        and log["topics"][1].lower() == KEY.pool_id
        for log in receipt["logs"]
    )


async def swap(compiler, orchestrator, web3, wallet, source, amount):
    tokens = (KEY.currency0, KEY.currency1)
    before = balances(web3, wallet)
    intent = SwapIntent(
        from_token=tokens[source],
        to_token=tokens[1 - source],
        amount=Decimal(amount) / Decimal(10 ** (18 if source == 0 else 6)),
        protocol="uniswap_v4",
        chain="robinhood",
        swap_params=deepcopy(PARAMS),
        max_slippage=Decimal("0.005"),
    )
    intent = type(intent).deserialize(intent.serialize())
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", compilation.error
    bundle = compilation.action_bundle
    result = await orchestrator.execute(bundle)
    receipts = assert_execution(result, bundle)
    parser = UniswapV4ReceiptParser(chain="robinhood")
    extract_kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=bundle.metadata)
    parsed = [
        parser.parse_receipt(
            receipt,
            **extract_kwargs,
        )
        for receipt in receipts
    ]
    settlement = next(item for item in parsed if item.swap_result is not None)
    assert settlement.error is None
    assert any(
        event.pool_id.lower() == KEY.pool_id
        and event.sender.lower() == UNISWAP_V4["robinhood"]["universal_router"].lower()
        for event in settlement.swap_events
    )
    after = balances(web3, wallet)
    assert before[source] - after[source] == amount == settlement.swap_result.amount_in
    received = after[1 - source] - before[1 - source]
    assert received > 0 and received == settlement.swap_result.amount_out
    extracted = next(
        value
        for receipt in receipts
        if (
            value := parser.extract_swap_amounts(
                receipt,
                **extract_kwargs,
            )
        )
        is not None
    )
    serialized = extracted.to_dict()
    assert serialized["token_in_address"] == tokens[source]
    assert serialized["token_out_address"] == tokens[1 - source]
    assert extracted.amount_in == amount and extracted.amount_out == received
    return received


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
async def test_dynamic_hook_swap_roundtrip(dynamic_compiler, orchestrator, web3, funded_wallet):
    """Both legs delegate all four layers to swap(), including exact bilateral settlement.

    # noqa: layers — the shared helper executes and verifies each real swap.
    """
    before = balances(web3, funded_wallet)
    bought = await swap(dynamic_compiler, orchestrator, web3, funded_wallet, 1, 1_000_000)
    await swap(dynamic_compiler, orchestrator, web3, funded_wallet, 0, bought)
    after = balances(web3, funded_wallet)
    assert after[0] == before[0]
    assert 0 < before[1] - after[1] < 100_000


@pytest.mark.intent(IntentType.LP_OPEN)
@pytest.mark.intent(IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_dynamic_hook_lp_lifecycle(dynamic_compiler, orchestrator, web3, funded_wallet, anvil_eth_call_adapter):
    compiler = dynamic_compiler
    before = balances(web3, funded_wallet)
    bought = await swap(compiler, orchestrator, web3, funded_wallet, 1, 1_000_000)
    ratio = compiler.price_oracle[KEY.currency0]
    amounts = (Decimal(bought) / Decimal(10**18) * Decimal("0.99"), Decimal("0.99"))
    intent = LPOpenIntent(
        pool=KEY.pool_id,
        amount0=amounts[0],
        amount1=amounts[1],
        range_lower=ratio * Decimal("0.9"),
        range_upper=ratio * Decimal("1.1"),
        protocol="uniswap_v4",
        chain="robinhood",
        protocol_params={**PARAMS, "allow_estimated_price": False},
    )
    intent = type(intent).deserialize(intent.serialize())
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", compilation.error
    deposited_before = tuple(get_token_balance(web3, token, funded_wallet) for token in (KEY.currency0, KEY.currency1))
    result = await orchestrator.execute(compilation.action_bundle)
    receipts = assert_execution(result, compilation.action_bundle)
    parser = UniswapV4ReceiptParser(chain="robinhood")
    parsed = [parser.parse_receipt(receipt) for receipt in receipts]
    opened = next(item for item in parsed if item.modify_liquidity_events)
    assert opened.error is None
    assert any(
        event.pool_id.lower() == KEY.pool_id
        and event.sender.lower() == UNISWAP_V4["robinhood"]["position_manager"].lower()
        for event in opened.modify_liquidity_events
    )
    position_id = next(value for receipt in receipts if (value := parser.extract_position_id(receipt)) is not None)
    liquidity = sum(parser.extract_liquidity(receipt) or 0 for receipt in receipts)
    assert liquidity > 0
    observed_position = observe_position(
        anvil_eth_call_adapter, chain="robinhood", token_id=position_id, wallet=funded_wallet
    )
    assert observed_position.key == KEY and observed_position.liquidity == liquidity

    def lookup_pool_key(pool_id, chain):
        assert chain == "robinhood" and pool_id == observed_position.key.pool_id
        return observed_position.key

    parser = UniswapV4ReceiptParser(chain="robinhood", pool_key_lookup=lookup_pool_key)
    open_kwargs = parser.build_extract_kwargs(field="lp_open_data", bundle_metadata=compilation.action_bundle.metadata)
    mint_receipt, open_data = next(
        (receipt, value)
        for receipt in receipts
        if (value := parser.extract_lp_open_data(receipt, **open_kwargs)) is not None
    )
    assert_lp_receipt(mint_receipt)
    assert open_data.position_id == position_id and open_data.liquidity == liquidity
    assert open_data.pool_address == KEY.pool_id
    assert (open_data.currency0, open_data.currency1) == (KEY.currency0, KEY.currency1)
    after_open = tuple(get_token_balance(web3, token, funded_wallet) for token in (KEY.currency0, KEY.currency1))
    assert all(0 < deposited_before[i] - after_open[i] <= int(amounts[i] * 10 ** (18 if i == 0 else 6)) for i in (0, 1))
    assert (open_data.amount0, open_data.amount1) == tuple(deposited_before[i] - after_open[i] for i in (0, 1))
    close = LPCloseIntent(
        position_id=str(position_id), pool=KEY.pool_id, protocol="uniswap_v4", chain="robinhood", protocol_params=PARAMS
    )
    compilation = compiler.compile(close)
    assert compilation.status.value == "SUCCESS", compilation.error
    result = await orchestrator.execute(compilation.action_bundle)
    receipts = assert_execution(result, compilation.action_bundle)
    parsed = [parser.parse_receipt(receipt) for receipt in receipts]
    assert any(item.error is None and item.modify_liquidity_events for item in parsed)
    assert sum(event.liquidity_delta for item in parsed for event in item.modify_liquidity_events) == -liquidity
    raw_liquidity = web3.eth.call(
        {
            "to": UNISWAP_V4["robinhood"]["position_manager"],
            "data": "0x"
            + keccak(text="getPositionLiquidity(uint256)")[:4].hex()
            + int(position_id).to_bytes(32, "big").hex(),
        }
    )
    assert decode(["uint128"], raw_liquidity)[0] == 0
    close_kwargs = parser.build_extract_kwargs(
        field="lp_close_data", bundle_metadata=compilation.action_bundle.metadata
    )
    burn_receipt, close_data = next(
        (receipt, value)
        for receipt in receipts
        if (value := parser.extract_lp_close_data(receipt, **close_kwargs)) is not None
    )
    assert_lp_receipt(burn_receipt)
    assert close_data.position_id == str(position_id) and close_data.liquidity_removed == liquidity
    assert close_data.pool_address == KEY.pool_id
    assert (close_data.currency0, close_data.currency1) == (KEY.currency0, KEY.currency1)
    assert close_data.position_hash == open_data.position_hash
    after_close = balances(web3, funded_wallet)
    assert (close_data.amount0_collected, close_data.amount1_collected) == tuple(
        after_close[i] - after_open[i] for i in (0, 1)
    )
    assert all(after_close[i] > after_open[i] for i in (0, 1))
    await swap(compiler, orchestrator, web3, funded_wallet, 0, after_close[0] - before[0])
    assert balances(web3, funded_wallet)[0] == before[0]


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.asyncio
async def test_dynamic_hook_artifact_mismatch_refuses_before_submission(
    dynamic_compiler, orchestrator, web3, funded_wallet
):
    intent = SwapIntent(
        from_token=KEY.currency1,
        to_token=KEY.currency0,
        amount=Decimal("1"),
        protocol="uniswap_v4",
        chain="robinhood",
        swap_params=deepcopy(PARAMS),
        max_slippage=Decimal("0.005"),
    )
    compilation = dynamic_compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", compilation.error
    bundle = compilation.action_bundle
    original = deepcopy(bundle.to_dict())
    bundle.metadata["v4_operation"]["pool_key"]["tick_spacing"] = 16
    attempted = deepcopy(bundle.to_dict())
    before = tuple(get_token_balance(web3, token, funded_wallet) for token in (KEY.currency0, KEY.currency1))
    native_before = web3.eth.get_balance(funded_wallet)
    nonce_before = web3.eth.get_transaction_count(funded_wallet)
    result = await orchestrator.execute(bundle)
    assert not result.success
    assert "Connector operation refused" in result.error
    assert result.transaction_results == []
    assert tuple(get_token_balance(web3, token, funded_wallet) for token in (KEY.currency0, KEY.currency1)) == before
    assert web3.eth.get_balance(funded_wallet) == native_before
    assert web3.eth.get_transaction_count(funded_wallet) == nonce_before
    assert bundle.to_dict() == attempted
    assert original["metadata"]["v4_operation"]["pool_key"] == KEY.to_wire()
