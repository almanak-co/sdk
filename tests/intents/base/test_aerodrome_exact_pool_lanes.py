"""Four-layer exact-address Aerodrome lanes on Base Anvil (PR #3828 follow-on to ALM-3462).

Covers the lanes that share the Slipstream LP_OPEN fix's contract:

- Classic (Solidly) ``LP_OPEN`` by bare pool address — reversed through the
  pool's ``metadata()`` and authenticated against the registered factory.
- ``SWAP`` with ``swap_params={"pool": "0x..."}`` honoured as an exact pin for
  a Classic pool and for a Slipstream CL pool on the router-bound factory, and
  refused for a CL pool on a factory the registered swap router cannot reach.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.aerodrome.addresses import AERODROME
from almanak.connectors._strategy_base.teardown_post_condition import get_teardown_post_condition
from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown.models import PositionInfo, PositionType
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance, get_token_decimals

CHAIN_NAME = "base"
# Real Base pools, read on-chain 2026-09-02:
#   factory.getPool(WETH, USDC, volatile) -> CLASSIC_POOL (token0=WETH, token1=USDC)
#   legacy CL factory (the one the registered Slipstream swap router is bound to), WETH/USDC ts=100
#   current CL factory (NOT reachable by the swap router), WETH/USDC ts=50
CLASSIC_POOL = "0xcDAC0d6c6C59727a65F871236188350531885C43"
ROUTER_CL_POOL = "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59"
UNREACHABLE_CL_POOL = "0x3FE04A59Ebd38cF06080a6F60a98D124eb59392A"
ROUTER_FACTORY = AERODROME["base"]["cl_factory"]
# A real, well-formed contract that is not an Aerodrome pool (the WETH token).
NOT_A_POOL = "0x4200000000000000000000000000000000000006"

LP_AMOUNT_WETH = Decimal("0.005")  # amount0 (WETH is token0: 0x4200… < 0x8335…)
LP_AMOUNT_USDC = Decimal("10")  # amount1
SWAP_AMOUNT_USDC = Decimal("100")


def _compiler(funded_wallet: str, price_oracle: dict, anvil_rpc_url: str, gateway=None) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
        gateway_client=gateway,
    )


def _swap_intent(pool: str) -> SwapIntent:
    return SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=SWAP_AMOUNT_USDC,
        max_slippage=Decimal("0.20"),
        protocol="aerodrome",
        chain=CHAIN_NAME,
        swap_params={"pool": pool},
    )


async def _run_pinned_swap(
    *,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    compiler: IntentCompiler,
    pool: str,
    expected_routing: str,
    expected_tick_spacing: int | None,
) -> None:
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    usdc, weth = tokens["USDC"], tokens["WETH"]
    usdc_decimals = get_token_decimals(web3, usdc)
    usdc_before = get_token_balance(web3, usdc, funded_wallet)
    weth_before = get_token_balance(web3, weth, funded_wallet)

    # Layer 1 — the pin is honoured, not auto-routed.
    compilation = compiler.compile(_swap_intent(pool))
    assert compilation.status.value == "SUCCESS", f"Pinned swap compilation failed: {compilation.error}"
    assert compilation.action_bundle is not None
    meta = compilation.action_bundle.metadata
    assert meta["pinned_pool"].lower() == pool.lower()
    assert meta["routing"] == expected_routing
    assert meta["routing_fallback"] is False
    if expected_tick_spacing is None:
        assert "tick_spacing" not in meta
    else:
        assert meta["tick_spacing"] == expected_tick_spacing

    # Layer 2 — execute.
    execution = await orchestrator.execute(compilation.action_bundle)
    assert execution.success, f"Pinned swap execution failed: {execution.error}"

    # Layer 3 — the production parser recovers the swap.
    token0, token1 = sorted([usdc.lower(), weth.lower()])
    parser = AerodromeReceiptParser(chain=CHAIN_NAME, token0_address=token0, token1_address=token1)
    parsed_swaps = 0
    for tx_result in execution.transaction_results:
        if tx_result.receipt is None:
            continue
        parsed = parser.parse_receipt(tx_result.receipt.to_dict())
        if parsed.success and parsed.swap_result:
            parsed_swaps += 1
            assert parsed.swap_result.amount_in_decimal > 0
            assert parsed.swap_result.amount_out_decimal > 0
    assert parsed_swaps >= 1, "must parse at least one swap event"

    # Layer 4 — bilateral balance deltas.
    usdc_spent = usdc_before - get_token_balance(web3, usdc, funded_wallet)
    weth_received = get_token_balance(web3, weth, funded_wallet) - weth_before
    assert usdc_spent == int(SWAP_AMOUNT_USDC * Decimal(10**usdc_decimals))
    assert weth_received > 0


@pytest.mark.base
@pytest.mark.swap
class TestAerodromePinnedSwap:
    @pytest.mark.no_zodiac(
        reason="VIB-5548: a Classic-router swap is not authorized by the default-on Zodiac manifest "
        "(synthetic-intent CL route only) — same gap as test_swap_classic_override_using_intent. "
        "Pin resolution + execution correctness are under test here; Classic-route Zodiac authz is "
        "the separate permission-discovery follow-up. As of 2026-09-02."
    )
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_pinned_to_exact_classic_pool(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        await _run_pinned_swap(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            compiler=_compiler(funded_wallet, price_oracle, anvil_rpc_url),
            pool=CLASSIC_POOL,
            expected_routing="classic",
            expected_tick_spacing=None,
        )

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_pinned_to_exact_cl_pool_on_router_factory(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        await _run_pinned_swap(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            compiler=_compiler(funded_wallet, price_oracle, anvil_rpc_url),
            pool=ROUTER_CL_POOL,
            expected_routing="cl",
            expected_tick_spacing=100,
        )

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    # Compile-time refusal: no bundle exists to execute, so Layers 2-4 cannot
    # apply; bilateral balance conservation is asserted instead.
    async def test_swap_pinned_to_cl_pool_the_router_cannot_reach_is_refused(  # noqa: layers
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """A current-generation CL pool is a real pool, but the registered swap router is bound to the legacy factory."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_before = get_token_balance(web3, tokens["USDC"], funded_wallet)
        weth_before = get_token_balance(web3, tokens["WETH"], funded_wallet)

        compilation = _compiler(funded_wallet, price_oracle, anvil_rpc_url).compile(_swap_intent(UNREACHABLE_CL_POOL))
        assert compilation.status.value == "FAILED"
        assert compilation.action_bundle is None
        assert f"swap router on {CHAIN_NAME} routes through factory {ROUTER_FACTORY}" in (compilation.error or "")
        assert "0xf8f2eb4940cfe7d13603dddd87f123820fc061ef" in (compilation.error or "")

        assert get_token_balance(web3, tokens["USDC"], funded_wallet) == usdc_before
        assert get_token_balance(web3, tokens["WETH"], funded_wallet) == weth_before


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeClassicExactPoolLP:
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_classic_lp_open_by_exact_pool_address(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc, weth = tokens["USDC"], tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc)
        weth_decimals = get_token_decimals(web3, weth)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)
        # The Classic LP token IS the pool contract.
        lp_before = get_token_balance(web3, CLASSIC_POOL, funded_wallet)

        intent = LPOpenIntent(
            pool=CLASSIC_POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=Decimal("0.5"),  # full-range Classic ignores the band; the intent still requires one
            range_upper=Decimal("2"),
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )
        # The exact lane reads the pool through the gateway boundary.
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url, gateway=anvil_eth_call_adapter)

        # Layer 1 — the bare address clears the format gate and is factory-authenticated.
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", f"Exact Classic LP_OPEN compilation failed: {compilation.error}"
        assert compilation.action_bundle is not None
        meta = compilation.action_bundle.metadata
        assert meta["pool"].lower() == CLASSIC_POOL.lower()
        assert meta["stable"] is False, "pool type must come from the contract"
        assert meta["token0"]["address"].lower() == weth.lower()
        assert meta["token1"]["address"].lower() == usdc.lower()

        # Layer 2 — execute.
        execution = await orchestrator.execute(compilation.action_bundle)
        assert execution.success, f"Exact Classic LP_OPEN execution failed: {execution.error}"
        for tx_result in execution.transaction_results:
            assert tx_result.receipt is not None and tx_result.receipt.status == 1

        # Layer 3 — the production parser recovers the mint ON THIS pool with real amounts.
        parser = AerodromeReceiptParser(chain=CHAIN_NAME, token0_address=weth, token1_address=usdc)
        mint_results = []
        for tx_result in execution.transaction_results:
            if tx_result.receipt is None:
                continue
            parsed = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parsed.success, f"LP_OPEN receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            if parsed.liquidity_result is not None:
                mint_results.append(parsed.liquidity_result)
        assert len(mint_results) == 1, "exactly one add-liquidity receipt expected"
        (mint,) = mint_results
        assert mint.operation == "add"
        assert mint.pool_address.lower() == CLASSIC_POOL.lower()
        assert mint.amount0 > 0 and mint.amount1 > 0

        # Layer 4 — deposits leave the wallet within the requested amounts and LP tokens arrive.
        usdc_spent = usdc_before - get_token_balance(web3, usdc, funded_wallet)
        weth_spent = weth_before - get_token_balance(web3, weth, funded_wallet)
        lp_received = get_token_balance(web3, CLASSIC_POOL, funded_wallet) - lp_before
        assert usdc_spent > 0 and weth_spent > 0
        assert usdc_spent <= int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        assert weth_spent <= int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        assert lp_received > 0
        usdc_after_open = get_token_balance(web3, usdc, funded_wallet)
        weth_after_open = get_token_balance(web3, weth, funded_wallet)

        # Teardown post-condition (TD-14) while OPEN: the wallet holds LP tokens
        # and a Classic close is clamped, so the hook must report unmeasured —
        # never a fabricated "closed" and never a residual FAILED.
        teardown_hook = get_teardown_post_condition("aerodrome")
        assert teardown_hook is not None, "aerodrome must have a registered teardown post-condition"
        position = PositionInfo(
            position_type=PositionType.LP,
            position_id=CLASSIC_POOL,
            chain=CHAIN_NAME,
            protocol="aerodrome",
            value_usd=Decimal("0"),
            details={"pool": CLASSIC_POOL, "pool_address": CLASSIC_POOL.lower(), "lp_token": CLASSIC_POOL},
        )
        open_check = teardown_hook(position, funded_wallet, gateway_client=anvil_eth_call_adapter)
        assert open_check.closed is False and open_check.unmeasured is True, open_check

        # Close by the same exact address (Classic LP_CLOSE keys on the pool address).
        close_compilation = compiler.compile(
            LPCloseIntent(position_id=CLASSIC_POOL, pool=CLASSIC_POOL, protocol="aerodrome", chain=CHAIN_NAME)
        )
        assert close_compilation.status.value == "SUCCESS", (
            f"Exact Classic LP_CLOSE compilation failed: {close_compilation.error}"
        )
        assert close_compilation.action_bundle is not None
        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, f"Exact Classic LP_CLOSE execution failed: {close_execution.error}"
        # The production close extraction (the same path the result enricher and
        # accounting use) must recover both collected amounts from the remove receipt.
        close_data = []
        for tx_result in close_execution.transaction_results:
            assert tx_result.receipt is not None and tx_result.receipt.status == 1
            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)
            assert parsed.success, f"LP_CLOSE receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
            extracted = parser.extract_lp_close_data(receipt_dict)
            if extracted is not None:
                close_data.append(extracted)
        assert len(close_data) == 1, "exactly one remove-liquidity receipt expected"
        (burn,) = close_data
        assert burn.amount0_collected is not None and burn.amount0_collected > 0
        assert burn.amount1_collected is not None and burn.amount1_collected > 0
        assert get_token_balance(web3, CLASSIC_POOL, funded_wallet) == 0
        assert get_token_balance(web3, usdc, funded_wallet) > usdc_after_open
        assert get_token_balance(web3, weth, funded_wallet) > weth_after_open

        # Teardown post-condition AFTER the close, pinned to the close block:
        # LP-token balance is zero — a MEASURED closure.
        close_block = max(tx.receipt.block_number for tx in close_execution.transaction_results if tx.receipt)
        closed_check = teardown_hook(position, funded_wallet, gateway_client=anvil_eth_call_adapter, block=close_block)
        assert closed_check.closed is True and not closed_check.unmeasured, closed_check

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    # Compile-time refusal: no bundle exists to execute, so Layers 2-4 cannot
    # apply; bilateral balance conservation is asserted instead.
    async def test_classic_lp_open_by_non_pool_address_is_refused(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        """A well-formed contract that is not an Aerodrome pool must fail before any approval."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_before = get_token_balance(web3, tokens["USDC"], funded_wallet)
        weth_before = get_token_balance(web3, tokens["WETH"], funded_wallet)

        compilation = _compiler(funded_wallet, price_oracle, anvil_rpc_url, gateway=anvil_eth_call_adapter).compile(
            LPOpenIntent(
                pool=NOT_A_POOL,
                amount0=LP_AMOUNT_WETH,
                amount1=LP_AMOUNT_USDC,
                range_lower=Decimal("0.5"),
                range_upper=Decimal("2"),
                protocol="aerodrome",
                chain=CHAIN_NAME,
            )
        )
        assert compilation.status.value == "FAILED"
        assert compilation.action_bundle is None
        assert "Could not resolve Aerodrome pool metadata" in (compilation.error or ""), compilation.error
        assert get_token_balance(web3, tokens["USDC"], funded_wallet) == usdc_before
        assert get_token_balance(web3, tokens["WETH"], funded_wallet) == weth_before
