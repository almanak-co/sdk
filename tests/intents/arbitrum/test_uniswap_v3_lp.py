"""Production-grade LP Intent tests for Uniswap V3 on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states

LP Close test cases:
  #1: Position has liquidity + fees (normal close)
  #2: Position has no liquidity and no fees (already decreased + collected)
  #3: Position has no liquidity but owed tokens (decreased but not collected)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_uniswap_v3_lp.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3
from web3.exceptions import ContractLogicError

from almanak.connectors.uniswap_v3.adapter import UniswapV3Adapter, UniswapV3Config
from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
from almanak.connectors.uniswap_v3.receipt_parser import EVENT_TOPICS, UniswapV3ReceiptParser
from almanak.connectors.uniswap_v3.sdk import compute_pool_address
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator, ExecutionResult
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import (
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle
from tests.intents._lp_setup_helpers import (
    collect_all_tokens,
    decrease_all_liquidity,
    query_position_liquidity,
)
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.intent_evidence import decode_explorer_view

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
MAX_UINT128 = 2**128 - 1

# Pool: WETH/USDC 0.3% fee tier
# After sorting by address on Arbitrum: token0=WETH (0x82aF...), token1=USDC (0xaf88...)
# So amount0=WETH, amount1=USDC, range is in USDC-per-WETH terms
POOL = "WETH/USDC/3000"
LP_AMOUNT_WETH = Decimal("0.2")  # amount0 (WETH after sorting on Arbitrum)
LP_AMOUNT_USDC = Decimal("500")  # amount1 (USDC after sorting on Arbitrum)

# Wide price range in USDC-per-WETH terms to ensure both tokens are deposited
# range_lower=200   -> ETH at $200
# range_upper=20000 -> ETH at $20,000
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")


# =============================================================================
# Helpers
# =============================================================================
#
# ``query_position_liquidity``, ``decrease_all_liquidity``, ``collect_all_tokens``
# live in ``tests/intents/_lp_setup_helpers.py`` so the no-liquidity edge-case
# tests (which manipulate position state outside the intent system) route their
# setup tx through whatever orchestrator the test holds — EOA-signed under
# ``ExecutionOrchestrator``, ``execTransactionWithRole``-wrapped under
# ``ZodiacOrchestrator``. Local copies in every chain's LP test file would
# silently drift out of sync with the Zodiac wiring.


async def _open_position_with_result(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> tuple[int, LPOpenIntent, ExecutionResult, dict[str, Any]]:
    """Open a setup position and retain its real result for Layer-5 seeding."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WETH,  # WETH is token0 on Arbitrum
        amount1=LP_AMOUNT_USDC,  # USDC is token1 on Arbitrum
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="uniswap_v3",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", f"LP Open compilation failed: {compilation_result.error}"
    assert compilation_result.action_bundle is not None

    execution_result = await orchestrator.execute(compilation_result.action_bundle)
    assert execution_result.success, f"LP Open execution failed: {execution_result.error}"

    # Extract position ID from mint receipt
    parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
            if pos_id is not None:
                return pos_id, intent, execution_result, compilation_result.action_bundle.metadata

    raise AssertionError("Failed to extract position ID from LP Open receipt")


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> int:
    """Open an LP position via LPOpenIntent and return only its token ID."""
    position_id, _, _, _ = await _open_position_with_result(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
    return position_id


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-uniswap-v3-lp-arbitrum",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol="uniswap_v3",
    )


def _enrich_for_accounting(
    execution_result: ExecutionResult,
    intent: Any,
    wallet: str,
    bundle_metadata: dict[str, Any],
) -> ExecutionResult:
    return enrich_result(
        execution_result,
        intent,
        _execution_context(wallet),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def _single_event_tx(execution_result: ExecutionResult, topic: str, label: str):
    """Return the only receipt carrying ``topic``; approvals are not evidence."""
    matches = []
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt is None:
            continue
        logs = tx_result.receipt.to_dict().get("logs", [])
        if any(log.get("topics") and str(log["topics"][0]).lower() == topic.lower() for log in logs):
            matches.append(tx_result)
    assert len(matches) == 1, f"Expected exactly one {label} receipt, got {len(matches)}"
    return matches[0]


def _position_owner(web3: Web3, token_id: int) -> str | None:
    """Read ERC-721 ownerOf; a revert means the position NFT was burned."""
    data = "0x6352211e" + hex(token_id)[2:].zfill(64)
    try:
        raw = web3.eth.call({"to": Web3.to_checksum_address(POSITION_MANAGER), "data": data})
    except ContractLogicError:
        return None
    if len(raw) != 32:
        raise AssertionError(f"ownerOf({token_id}) returned {len(raw)} bytes, expected 32")
    return Web3.to_checksum_address("0x" + raw[-20:].hex())


def _position_state(web3: Web3, token_id: int) -> dict[str, Any] | None:
    """Read the canonical NPM position identity and lifecycle fields."""
    data = "0x99fbab88" + hex(token_id)[2:].zfill(64)
    try:
        raw = web3.eth.call({"to": Web3.to_checksum_address(POSITION_MANAGER), "data": data})
    except ContractLogicError:
        return None
    if len(raw) < 12 * 32:
        raise AssertionError(f"positions({token_id}) returned {len(raw)} bytes, expected >= 384")

    def word(index: int) -> bytes:
        return raw[index * 32 : (index + 1) * 32]

    def signed_word(index: int) -> int:
        value = int.from_bytes(word(index), "big")
        return value - (1 << 256) if value >= (1 << 255) else value

    return {
        "token0": Web3.to_checksum_address("0x" + word(2)[-20:].hex()),
        "token1": Web3.to_checksum_address("0x" + word(3)[-20:].hex()),
        "fee": int.from_bytes(word(4), "big"),
        "tick_lower": signed_word(5),
        "tick_upper": signed_word(6),
        "liquidity": int.from_bytes(word(7), "big"),
        "tokens_owed0": int.from_bytes(word(10), "big"),
        "tokens_owed1": int.from_bytes(word(11), "big"),
    }


def _wallet_transfers(receipt: dict[str, Any], wallet: str) -> list[dict[str, Any]]:
    wallet = wallet.lower()
    return [
        log
        for log in decode_explorer_view(receipt)["logs"]
        if log.get("name") == "Transfer"
        and wallet
        in {
            str((log.get("args") or {}).get("from", "")).lower(),
            str((log.get("args") or {}).get("to", "")).lower(),
        }
    ]


async def _execute_same_pool_fee_accrual_swap(
    *,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    """Accrue fees through the exact WETH/USDC/3000 pool on the Anvil fork.

    A normal SwapIntent is compiled first so the Safe manifest is derived from
    production intent facts. The test-only bundle then pins fee=3000; it is
    setup evidence only and never paints the SWAP coverage cell.
    """
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    usdc_addr = tokens["USDC"]
    weth_addr = tokens["WETH"]
    amount_in = int(Decimal("1000") * Decimal(10 ** get_token_decimals(web3, usdc_addr)))
    manifest_intent = SwapIntent(
        from_token="USDC",  # noqa: S106 - public test token symbol, not a credential
        to_token="WETH",  # noqa: S106 - public test token symbol, not a credential
        amount=Decimal("1000"),
        max_slippage=Decimal("0.05"),
        protocol="uniswap_v3",
        chain=CHAIN_NAME,
    )
    manifest_compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    manifest_result = manifest_compiler.compile(manifest_intent)
    assert manifest_result.status.value == "SUCCESS", f"Fee-accrual manifest seed must compile: {manifest_result.error}"

    adapter = UniswapV3Adapter(
        UniswapV3Config(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_provider=price_oracle,
        )
    )
    router = adapter.addresses["swap_router"]
    approve_tx = adapter._build_approve_tx(usdc_addr, router, amount_in)
    swap_tx = adapter._build_exact_input_single_tx(
        token_in=usdc_addr,
        token_out=weth_addr,
        fee=3000,
        recipient=funded_wallet,
        amount_in=amount_in,
        amount_out_minimum=0,
    )
    bundle = ActionBundle(
        intent_type=IntentType.SWAP.value,
        transactions=[tx.to_dict() for tx in (approve_tx, swap_tx) if tx is not None],
        metadata={"protocol": "uniswap_v3", "chain": CHAIN_NAME, "selected_fee_tier": 3000},
    )
    result = await orchestrator.execute(bundle)
    assert result.success, f"Exact-pool fee-accrual swap failed: {result.error}"


# =============================================================================
# Fixtures
# =============================================================================


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestUniswapV3LPOpenIntent:
    """Test Uniswap V3 LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool, amounts, and price range
    - IntentCompiler generates correct NonfungiblePositionManager mint TX
    - Transactions execute successfully on-chain
    - Position NFT is minted and has liquidity
    - Balance changes are correct
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        intent_evidence,
    ):
        """Run exact LP_OPEN evidence through Safe + Zodiac."""
        await self._run_lp_open(
            web3,
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
            layer5_accounting_harness,
            intent_evidence,
        )

    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_OPEN receipt contract through EOA")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_weth_usdc_eoa(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        intent_evidence,
    ):
        """Run exact LP_OPEN evidence through the EOA path."""
        await self._run_lp_open(
            web3,
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
            layer5_accounting_harness,
            intent_evidence,
        )

    async def _run_lp_open(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        intent_evidence,
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        intent_evidence.bind(intent)
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        mint_tx = _single_event_tx(
            execution_result, EVENT_TOPICS["IncreaseLiquidity"], "IncreaseLiquidity-emitting LP_OPEN"
        )
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        lp_open_data = intent_evidence.capture_parse(
            intent=intent,
            transaction_result=mint_tx,
            parser=lambda receipt: parser.extract_lp_open_data(receipt),
            parser_method="extract_lp_open_data",
        )
        assert lp_open_data is not None, "Parser must extract LP_OPEN data"
        position_id = lp_open_data.position_id
        assert position_id is not None
        state = _position_state(web3, position_id)
        owner = _position_owner(web3, position_id)
        assert state is not None and state["liquidity"] > 0
        assert owner is not None and owner.lower() == funded_wallet.lower()
        assert state["fee"] == 3000
        assert {state["token0"].lower(), state["token1"].lower()} == {
            weth_addr.lower(),
            usdc_addr.lower(),
        }
        assert lp_open_data.liquidity == state["liquidity"]
        assert lp_open_data.tick_lower == state["tick_lower"]
        assert lp_open_data.tick_upper == state["tick_upper"]

        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        weth_spent = weth_before - weth_after
        assert usdc_spent > 0 or weth_spent > 0, "Must deposit at least one token into LP"
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        assert usdc_spent <= expected_usdc_max, f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"
        assert weth_spent <= expected_weth_max, f"WETH spent ({weth_spent}) exceeds desired ({expected_weth_max})"
        assert lp_open_data.amount0 == weth_spent
        assert lp_open_data.amount1 == usdc_spent
        assert lp_open_data.currency0.lower() == weth_addr.lower()
        assert lp_open_data.currency1.lower() == usdc_addr.lower()

        receipt = mint_tx.receipt.to_dict()
        transfers = _wallet_transfers(receipt, funded_wallet)
        nft_mints = [
            log
            for log in transfers
            if str(log.get("address", "")).lower() == POSITION_MANAGER.lower()
            and str((log.get("args") or {}).get("from", "")).lower() == "0x0000000000000000000000000000000000000000"
            and str((log.get("args") or {}).get("to", "")).lower() == funded_wallet.lower()
            and int((log.get("args") or {}).get("value", -1)) == position_id
        ]
        token_outflows = {
            str(log.get("address", "")).lower(): int((log.get("args") or {}).get("value", 0))
            for log in transfers
            if str((log.get("args") or {}).get("from", "")).lower() == funded_wallet.lower()
            and str(log.get("address", "")).lower() in {weth_addr.lower(), usdc_addr.lower()}
        }
        hard = (
            len(nft_mints) == 1
            and token_outflows.get(weth_addr.lower()) == weth_spent
            and token_outflows.get(usdc_addr.lower()) == usdc_spent
        )

        accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=_enrich_for_accounting(
                execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
            ),
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            resolved_pool=compilation_result.action_bundle.metadata.get("pool_name"),
        )
        accounting_payload = json.loads(accounting_row["payload_json"])
        assert accounting_payload["position_key"] == accounting_row["position_key"]
        assert accounting_payload["pool_address"].startswith("0x")
        assert accounting_payload["tick_lower"] == state["tick_lower"]
        assert accounting_payload["tick_upper"] == state["tick_upper"]

        intent_evidence.record_fidelity(
            hard=hard,
            flags={
                "parser_data_present": lp_open_data is not None,
                "position_owner_match": owner.lower() == funded_wallet.lower(),
                "position_liquidity_match": lp_open_data.liquidity == state["liquidity"],
                "position_ticks_match": (
                    lp_open_data.tick_lower == state["tick_lower"] and lp_open_data.tick_upper == state["tick_upper"]
                ),
                "currency0_match": lp_open_data.currency0.lower() == weth_addr.lower(),
                "currency1_match": lp_open_data.currency1.lower() == usdc_addr.lower(),
                "amount0_eq_wallet_delta": lp_open_data.amount0 == weth_spent,
                "amount1_eq_wallet_delta": lp_open_data.amount1 == usdc_spent,
                "single_nft_mint": len(nft_mints) == 1,
                "erc20_transfers_match": hard,
                "accounting_position_key_present": bool(accounting_payload["position_key"]),
            },
            witnesses=[
                {"kind": "position_state", "owner": owner, **state},
                {"kind": "independent_transfer_logs", "matches": transfers},
                {
                    "kind": "accounting_event",
                    "event_type": accounting_row["event_type"],
                    "tx_hash": accounting_row["tx_hash"],
                    "position_key": accounting_payload["position_key"],
                    "pool_address": accounting_payload["pool_address"],
                },
            ],
            notes=[] if hard else ["NFT/ERC-20 transfer witnesses were not unique; keep receipt SOFT."],
        )
        intent_evidence.record_balance_deltas(
            checks={"wallet_deltas_eq_independent_position_and_transfer_witnesses": hard},
            token0={
                "address": weth_addr,
                "symbol": "WETH",
                "before": weth_before,
                "after": weth_after,
                "delta": -weth_spent,
            },
            token1={
                "address": usdc_addr,
                "symbol": "USDC",
                "before": usdc_before,
                "after": usdc_after,
                "delta": -usdc_spent,
            },
        )
        receipt_block = int(receipt["blockNumber"] if "blockNumber" in receipt else receipt["block_number"])
        position_call = "0x99fbab88" + hex(position_id)[2:].zfill(64)
        owner_call = "0x6352211e" + hex(position_id)[2:].zfill(64)
        position_state_raw = web3.eth.call(
            {"to": Web3.to_checksum_address(POSITION_MANAGER), "data": position_call},
            block_identifier=receipt_block,
        )
        owner_state_raw = web3.eth.call(
            {"to": Web3.to_checksum_address(POSITION_MANAGER), "data": owner_call},
            block_identifier=receipt_block,
        )
        block_hash = web3.eth.get_block(receipt_block)["hash"]
        token0 = state["token0"]
        token1 = state["token1"]
        factory = UNISWAP_V3[CHAIN_NAME]["factory"]
        pool_address = compute_pool_address(factory, token0, token1, state["fee"])
        intent_evidence.record_semantic_contract(
            schema_version=1,
            profile="v3_lp.v1",
            intent="LP_OPEN",
            account=funded_wallet,
            pool_reference=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            resource_address=POSITION_MANAGER,
            factory_address=factory,
            pool_address=pool_address,
            token0=token0,
            token1=token1,
            fee_tier=state["fee"],
            position_id=position_id,
            tick_lower=state["tick_lower"],
            tick_upper=state["tick_upper"],
            liquidity=state["liquidity"],
            max_amount0_raw=expected_weth_max,
            max_amount1_raw=expected_usdc_max,
            actual_amount0_raw=weth_spent,
            actual_amount1_raw=usdc_spent,
            parser_position_id=lp_open_data.position_id,
            parser_liquidity=lp_open_data.liquidity,
            parser_amount0_raw=lp_open_data.amount0,
            parser_amount1_raw=lp_open_data.amount1,
            position_state_raw="0x" + position_state_raw.hex(),
            owner_state_raw="0x" + owner_state_raw.hex(),
            position_state_block=receipt_block,
            position_state_block_hash=block_hash.hex(),
        )


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestUniswapV3LPCloseIntent:
    """Test Uniswap V3 LP Close using LPCloseIntent.

    Test cases:
    #1: Position has liquidity (normal LP close)
    #2: Position has no liquidity and no owed tokens (already decreased + collected)
    #3: Position has no liquidity but has owed tokens (decreased but not collected)
    """

    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_with_liquidity(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        intent_evidence,
    ):
        """Run exact LP_CLOSE evidence through Safe + Zodiac."""
        await self._run_lp_close(
            web3,
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
            layer5_accounting_harness,
            intent_evidence,
        )

    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_CLOSE receipt contract through EOA")
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_with_liquidity_eoa(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        intent_evidence,
    ):
        """Run exact LP_CLOSE evidence through the EOA path."""
        await self._run_lp_close(
            web3,
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
            layer5_accounting_harness,
            intent_evidence,
        )

    async def _run_lp_close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        intent_evidence,
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        position_id, open_intent, open_result, open_metadata = await _open_position_with_result(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )
        state_before = _position_state(web3, position_id)
        owner_before = _position_owner(web3, position_id)
        assert state_before is not None and state_before["liquidity"] > 0
        assert owner_before is not None and owner_before.lower() == funded_wallet.lower()

        open_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=open_intent,
            result=_enrich_for_accounting(open_result, open_intent, funded_wallet, open_metadata),
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            resolved_pool=open_metadata.get("pool_name"),
        )

        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        intent_evidence.bind(close_intent)
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(close_intent)
        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"LP Close execution failed: {execution_result.error}"

        decrease_tx = _single_event_tx(
            execution_result, EVENT_TOPICS["DecreaseLiquidity"], "DecreaseLiquidity-emitting LP_CLOSE"
        )
        collect_tx = _single_event_tx(execution_result, EVENT_TOPICS["Collect"], "Collect-emitting LP_CLOSE")
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        decrease_data = intent_evidence.capture_parse(
            intent=close_intent,
            transaction_result=decrease_tx,
            parser=lambda receipt: parser.extract_lp_close_data(receipt),
            parser_method="extract_lp_close_data:decrease",
        )
        collect_data = intent_evidence.capture_parse(
            intent=close_intent,
            transaction_result=collect_tx,
            parser=lambda receipt: parser.extract_lp_close_data(receipt),
            parser_method="extract_lp_close_data:collect",
        )
        assert decrease_data is not None and decrease_data.source == "decrease_liquidity"
        assert decrease_data.liquidity_removed == state_before["liquidity"]
        assert collect_data is not None and collect_data.source == "collect"

        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_returned = usdc_after_close - usdc_before_close
        weth_returned = weth_after_close - weth_before_close
        assert usdc_returned > 0 or weth_returned > 0, (
            f"Must receive tokens back when closing: USDC={usdc_returned}, WETH={weth_returned}"
        )
        assert collect_data.amount0_collected == weth_returned
        assert collect_data.amount1_collected == usdc_returned
        assert collect_data.currency0.lower() == weth_addr.lower()
        assert collect_data.currency1.lower() == usdc_addr.lower()

        owner_after = _position_owner(web3, position_id)
        state_after = _position_state(web3, position_id)
        assert owner_after is None, "LP_CLOSE must burn the position NFT"
        assert state_after is None, "Burned LP position must no longer be queryable"

        all_logs = [
            log
            for tx_result in execution_result.transaction_results
            if tx_result.receipt is not None
            for log in decode_explorer_view(tx_result.receipt.to_dict())["logs"]
        ]
        nft_burns = [
            log
            for log in all_logs
            if log.get("name") == "Transfer"
            and str(log.get("address", "")).lower() == POSITION_MANAGER.lower()
            and str((log.get("args") or {}).get("from", "")).lower() == funded_wallet.lower()
            and str((log.get("args") or {}).get("to", "")).lower() == "0x0000000000000000000000000000000000000000"
            and int((log.get("args") or {}).get("value", -1)) == position_id
        ]
        wallet_inflows = {
            str(log.get("address", "")).lower(): int((log.get("args") or {}).get("value", 0))
            for log in all_logs
            if log.get("name") == "Transfer"
            and str((log.get("args") or {}).get("to", "")).lower() == funded_wallet.lower()
            and str(log.get("address", "")).lower() in {weth_addr.lower(), usdc_addr.lower()}
        }
        hard = (
            len(nft_burns) == 1
            and wallet_inflows.get(weth_addr.lower()) == weth_returned
            and wallet_inflows.get(usdc_addr.lower()) == usdc_returned
        )

        close_enriched = _enrich_for_accounting(
            execution_result, close_intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        close_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=close_intent,
            result=close_enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_CLOSE",
            price_oracle=price_oracle,
            resolved_pool=compilation_result.action_bundle.metadata.get("pool_name"),
        )
        open_payload = json.loads(open_accounting_row["payload_json"])
        close_payload = json.loads(close_accounting_row["payload_json"])
        assert close_payload["position_key"] == open_payload["position_key"]

        intent_evidence.record_fidelity(
            hard=hard,
            flags={
                "decrease_parser_present": decrease_data is not None,
                "collect_parser_present": collect_data is not None,
                "liquidity_removed_eq_pre_state": decrease_data.liquidity_removed == state_before["liquidity"],
                "amount0_eq_wallet_delta": collect_data.amount0_collected == weth_returned,
                "amount1_eq_wallet_delta": collect_data.amount1_collected == usdc_returned,
                "currency0_match": collect_data.currency0.lower() == weth_addr.lower(),
                "currency1_match": collect_data.currency1.lower() == usdc_addr.lower(),
                "nft_burned": owner_after is None and state_after is None,
                "single_nft_burn": len(nft_burns) == 1,
                "erc20_transfers_match": hard,
                "accounting_position_key_match": close_payload["position_key"] == open_payload["position_key"],
            },
            witnesses=[
                {"kind": "position_state_before", "owner": owner_before, **state_before},
                {"kind": "position_state_after", "owner": owner_after, "position": state_after},
                {"kind": "independent_transfer_logs", "nft_burns": nft_burns, "wallet_inflows": wallet_inflows},
                {
                    "kind": "accounting_lifecycle",
                    "open_position_key": open_payload["position_key"],
                    "close_position_key": close_payload["position_key"],
                    "close_tx_hash": close_accounting_row["tx_hash"],
                },
            ],
            notes=[] if hard else ["NFT/ERC-20 close witnesses were not unique; keep receipts SOFT."],
        )
        intent_evidence.record_balance_deltas(
            checks={"wallet_deltas_eq_independent_position_and_transfer_witnesses": hard},
            token0={
                "address": weth_addr,
                "symbol": "WETH",
                "before": weth_before_close,
                "after": weth_after_close,
                "delta": weth_returned,
            },
            token1={
                "address": usdc_addr,
                "symbol": "USDC",
                "before": usdc_before_close,
                "after": usdc_after_close,
                "delta": usdc_returned,
            },
        )

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_no_fees(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #2: Close position with no liquidity and no owed tokens.

        This tests the edge case where a position has already had its liquidity
        removed and tokens collected externally (e.g., via direct contract calls).
        The LPCloseIntent should handle this gracefully.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Decrease all liquidity via direct contract call
        3. Collect all owed tokens via direct contract call
        4. Verify position has 0 liquidity
        5. Record balances BEFORE close
        6. Close via LPCloseIntent
        7. Verify ERC-20 balances unchanged (nothing to collect)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Liquidity, No Owed Tokens")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Decrease all liquidity directly. Routes through the orchestrator
        # so that under default-on Zodiac the call is wrapped in
        # ``execTransactionWithRole`` — the position is owned by the Safe, an
        # EOA-signed call would revert. The helper seeds an LPCloseIntent into
        # the recorder so the late-binding manifest covers the selector.
        await decrease_all_liquidity(
            web3,
            orchestrator,
            chain=CHAIN_NAME,
            protocol="uniswap_v3",
            position_manager=POSITION_MANAGER,
            token_id=position_id,
        )
        print("Decreased all liquidity via direct call")

        # 3. Collect all owed tokens directly (same orchestrator-routed reason).
        await collect_all_tokens(
            web3,
            orchestrator,
            chain=CHAIN_NAME,
            protocol="uniswap_v3",
            position_manager=POSITION_MANAGER,
            token_id=position_id,
            recipient=funded_wallet,
        )
        print("Collected all owed tokens via direct call")

        # 4. Verify 0 liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 5. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        # 6. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling LPCloseIntent for empty position...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print("Executing LP Close on empty position...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, "LP Close on empty position is a no-op success (VIB-3644)"
        assert compilation_result.action_bundle.metadata.get("no_op") is True, (
            "Empty LP_CLOSE must carry no_op metadata"
        )
        assert compilation_result.action_bundle.transactions == [], "No-op bundle must have 0 transactions"
        assert len(execution_result.transaction_results) == 0, "No-op execution must produce 0 executed transactions"

        # 7. Verify ERC-20 balances unchanged (nothing to collect)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_delta = usdc_after_close - usdc_before_close
        weth_delta = weth_after_close - weth_before_close

        assert usdc_delta == 0, f"USDC balance should be unchanged for empty position, got delta: {usdc_delta}"
        assert weth_delta == 0, f"WETH balance should be unchanged for empty position, got delta: {weth_delta}"

        print(f"USDC delta: {usdc_delta}")
        print(f"WETH delta: {weth_delta}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_but_owed_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #3: Close position with no liquidity but uncollected owed tokens.

        After decreaseLiquidity, the principal tokens (and any accrued fees)
        become "owed" to the position but are not yet transferred. The collect
        step in LPCloseIntent should retrieve these owed tokens.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Execute a swap to generate trading fees for the position
        3. Decrease all liquidity via direct contract call (tokens become owed)
        4. Do NOT collect - tokens remain owed
        5. Verify position has 0 liquidity
        6. Record balances BEFORE close
        7. Close via LPCloseIntent (should collect owed tokens and burn)
        8. Verify tokens were collected
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test #3: LP Close - No Liquidity, But Owed Tokens")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Execute a swap through the pool to generate fees
        swap_intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.05"),
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        swap_compilation = compiler.compile(swap_intent)
        if swap_compilation.status.value == "SUCCESS" and swap_compilation.action_bundle:
            swap_result = await orchestrator.execute(swap_compilation.action_bundle)
            if swap_result.success:
                print("Executed swap to generate LP fees")
            else:
                print(f"Swap failed (non-critical for this test): {swap_result.error}")

        # 3. Decrease all liquidity (tokens become owed but not collected).
        # Routes via the orchestrator — see the no_fees test above for why.
        await decrease_all_liquidity(
            web3,
            orchestrator,
            chain=CHAIN_NAME,
            protocol="uniswap_v3",
            position_manager=POSITION_MANAGER,
            token_id=position_id,
        )
        print("Decreased all liquidity via direct call (tokens now owed)")

        # 4. Do NOT collect - leave tokens owed

        # 5. Verify 0 liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 6. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        # 7. Close via LPCloseIntent (should collect owed tokens)
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        print("\nCompiling LPCloseIntent for position with owed tokens...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print("Executing LP Close...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, (
            f"LP Close should succeed for position with owed tokens. Error: {execution_result.error}"
        )

        # Parse receipts
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                lp_close_data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if lp_close_data:
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        # 8. Verify tokens were collected (owed tokens from decrease + any fees)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_collected = usdc_after_close - usdc_before_close
        weth_collected = weth_after_close - weth_before_close

        print(f"\nUSDC collected: {format_token_amount(usdc_collected, usdc_decimals)}")
        print(f"WETH collected: {format_token_amount(weth_collected, weth_decimals)}")

        # At least one token must be collected (there were owed tokens from the decrease)
        assert usdc_collected > 0 or weth_collected > 0, (
            f"Must collect owed tokens from decreased position. "
            f"USDC collected: {usdc_collected}, WETH collected: {weth_collected}"
        )

        print("\nALL CHECKS PASSED")


# =============================================================================
# CollectFeesIntent Tests (LP_COLLECT_FEES) — VIB-4307
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestUniswapV3CollectFeesIntent:
    """Test Uniswap V3 LP_COLLECT_FEES using CollectFeesIntent on Arbitrum.

    Flow (4 layers):
      1. Open an in-range LP position via LPOpenIntent.
      2. Execute a swap through the same pool to accrue fees on the position.
      3. Issue CollectFeesIntent(protocol="uniswap_v3", protocol_params={"position_id": ...}).
      4. Verify wallet balances increased (fees were transferred to the wallet).
    """

    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        intent_evidence,
    ):
        """Run exact LP_COLLECT_FEES evidence through Safe + Zodiac."""
        await self._run_collect_fees(
            web3,
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
            intent_evidence,
        )

    @pytest.mark.no_zodiac(
        reason="Exact-axis QA parity: exercise the same LP_COLLECT_FEES receipt contract through EOA"
    )
    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdc_eoa(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        intent_evidence,
    ):
        """Run exact LP_COLLECT_FEES evidence through the EOA path."""
        await self._run_collect_fees(
            web3,
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
            intent_evidence,
        )

    async def _run_collect_fees(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        intent_evidence,
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        position_id, _, _, _ = await _open_position_with_result(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )
        state_before = _position_state(web3, position_id)
        owner_before = _position_owner(web3, position_id)
        assert state_before is not None and state_before["liquidity"] > 0
        assert owner_before is not None and owner_before.lower() == funded_wallet.lower()
        await _execute_same_pool_fee_accrual_swap(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            anvil_rpc_url=anvil_rpc_url,
        )
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
            protocol_params={"position_id": position_id},
        )
        intent_evidence.bind(collect_intent)
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(collect_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"CollectFees compilation must succeed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"CollectFees execution failed: {execution_result.error}"

        collect_tx = _single_event_tx(execution_result, EVENT_TOPICS["Collect"], "Collect-emitting LP_COLLECT_FEES")
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        collect_data = intent_evidence.capture_parse(
            intent=collect_intent,
            transaction_result=collect_tx,
            parser=lambda receipt: parser.extract_lp_close_data(receipt),
            parser_method="extract_lp_close_data:collect_fees",
        )
        assert collect_data is not None and collect_data.source == "collect"
        assert collect_data.fees0 == collect_data.amount0_collected
        assert collect_data.fees1 == collect_data.amount1_collected

        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_received = usdc_after - usdc_before
        weth_received = weth_after - weth_before
        assert usdc_received > 0 or weth_received > 0, (
            f"Same-pool swap must accrue collectible fees: USDC={usdc_received}, WETH={weth_received}"
        )
        assert collect_data.amount0_collected == weth_received
        assert collect_data.amount1_collected == usdc_received
        currency0_match = collect_data.amount0_collected == 0 or (
            collect_data.currency0 is not None and collect_data.currency0.lower() == weth_addr.lower()
        )
        currency1_match = collect_data.amount1_collected == 0 or (
            collect_data.currency1 is not None and collect_data.currency1.lower() == usdc_addr.lower()
        )
        assert currency0_match
        assert currency1_match

        state_after = _position_state(web3, position_id)
        owner_after = _position_owner(web3, position_id)
        assert state_after is not None
        assert state_after["liquidity"] == state_before["liquidity"]
        assert owner_after is not None and owner_after.lower() == funded_wallet.lower()

        transfers = _wallet_transfers(collect_tx.receipt.to_dict(), funded_wallet)
        wallet_inflows = {
            str(log.get("address", "")).lower(): int((log.get("args") or {}).get("value", 0))
            for log in transfers
            if str((log.get("args") or {}).get("to", "")).lower() == funded_wallet.lower()
            and str(log.get("address", "")).lower() in {weth_addr.lower(), usdc_addr.lower()}
        }
        hard = (
            wallet_inflows.get(weth_addr.lower(), 0) == weth_received
            and wallet_inflows.get(usdc_addr.lower(), 0) == usdc_received
            and (weth_received > 0 or usdc_received > 0)
            and currency0_match
            and currency1_match
        )

        # VIB-4344: the production Uniswap V3 enricher does not yet derive
        # lp_close_data for LP_COLLECT_FEES. This four-layer proof therefore
        # makes no accounting-persistence claim from a test-injected object.

        intent_evidence.record_fidelity(
            hard=hard,
            flags={
                "collect_parser_present": collect_data is not None,
                "positive_fees": weth_received > 0 or usdc_received > 0,
                "amount0_eq_wallet_delta": collect_data.amount0_collected == weth_received,
                "amount1_eq_wallet_delta": collect_data.amount1_collected == usdc_received,
                "currency0_match_or_zero": currency0_match,
                "currency1_match_or_zero": currency1_match,
                "position_owner_preserved": owner_after.lower() == funded_wallet.lower(),
                "liquidity_unchanged": state_after["liquidity"] == state_before["liquidity"],
                "erc20_transfers_match": hard,
            },
            witnesses=[
                {"kind": "position_state_before", "owner": owner_before, **state_before},
                {"kind": "position_state_after", "owner": owner_after, **state_after},
                {"kind": "independent_transfer_logs", "matches": transfers},
            ],
            notes=[] if hard else ["Fee transfer witnesses did not exactly match wallet deltas; keep receipt SOFT."],
        )
        intent_evidence.record_balance_deltas(
            checks={"wallet_deltas_eq_independent_fee_transfer_witnesses": hard},
            token0={
                "address": weth_addr,
                "symbol": "WETH",
                "before": weth_before,
                "after": weth_after,
                "delta": weth_received,
            },
            token1={
                "address": usdc_addr,
                "symbol": "USDC",
                "before": usdc_before,
                "after": usdc_after,
                "delta": usdc_received,
            },
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
