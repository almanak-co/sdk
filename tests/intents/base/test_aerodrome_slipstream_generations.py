"""Aerodrome Slipstream: the pool decides its factory generation, in every lane (Base Anvil).

Base has two reviewed Slipstream generations, each with its own factory,
NonfungiblePositionManager, SwapRouter and Quoter. These four-layer cells prove
that no lane picks a generation from a constant:

- a symbolic key owned by exactly one generation mints on THAT generation's NPM
  (``WETH/USDC/100`` -> legacy; ``USDC/cbBTC/10`` -> current), closes through
  the same NPM, and the teardown post-condition certifies the closure;
- a symbolic key both generations answer (``WETH/USDC/50``) is refused with the
  pool address requested, before any approval is built;
- a pool on a factory no reviewed generation owns (a real Aerodrome pool on the
  unregistered "Gauge Caps" factory, and a Uniswap V3 pool that answers the same
  ABI) is refused by the LP lane and by the swap pin.

The address-form LP lane and the swap pin per generation are certified by
``test_aerodrome_slipstream_exact_pool_lp.py`` and
``test_aerodrome_exact_pool_lanes.py``.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors._strategy_base.teardown_post_condition import get_teardown_post_condition
from almanak.connectors.aerodrome.addresses import (
    SlipstreamDeployment,
    slipstream_deployment_for_factory,
    slipstream_lp_deployments,
)
from almanak.connectors.aerodrome.pool_validation import encode_aerodrome_cl_get_pool
from almanak.connectors.aerodrome.receipt_parser import AerodromeSlipstreamReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import Intent, IntentCompiler, LPCloseIntent
from almanak.framework.intents.vocabulary import IntentType, PriceBand
from almanak.framework.teardown.models import PositionInfo, PositionType
from tests.intents._lp_setup_helpers import query_position_liquidity
from tests.intents.conftest import CHAIN_CONFIGS, fund_erc20_token, get_token_balance, get_token_decimals

CHAIN_NAME = "base"
PROTOCOL = "aerodrome_slipstream"
LEGACY_FACTORY = "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A"
CURRENT_FACTORY = "0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef"
# Real Base pools, read on-chain: each symbolic key below is owned by exactly
# the generation named, or by both.
LEGACY_ONLY_KEY = "WETH/USDC/100"
LEGACY_ONLY_POOL = "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59"
CURRENT_ONLY_KEY = "USDC/cbBTC/10"
CURRENT_ONLY_POOL = "0x3F53aFD15909bF5B1c5963b5C0D28123668ce174"
AMBIGUOUS_KEY = "WETH/USDC/50"
AMBIGUOUS_POOLS = {
    "legacy": "0xAaD23a67F2AC693ABBe543489aeB3F24F561D517",
    "current": "0x3FE04A59Ebd38cF06080a6F60a98D124eb59392A",
}
UNREGISTERED_AERODROME_POOL = "0xc758d81B9b81A6FCDAd075bD471874A2c46B54e0"
UNREGISTERED_AERODROME_FACTORY = "0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a"
UNISWAP_V3_POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
UNISWAP_V3_FACTORY = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
CBBTC = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
CBBTC_BALANCE_SLOT = 9
_FACTORY_SELECTOR = "0xc45a0155"
_SLOT0_SELECTOR = "0x3850c7bd"
_TOKEN0_SELECTOR = "0x0dfe1681"
_TOKEN1_SELECTOR = "0xd21220a7"
_NPM_MINT_SELECTOR = "0xb5007d1f"
PRICE_ORACLE = {"USDC": Decimal("1"), "WETH": Decimal("3000")}


def _call(web3: Web3, to: str, data: str) -> bytes:
    return bytes(web3.eth.call({"to": Web3.to_checksum_address(to), "data": data}))


def _word_address(word: bytes) -> str:
    return "0x" + word[-20:].hex()


def _pool_factory(web3: Web3, pool: str) -> str:
    return _word_address(_call(web3, pool, _FACTORY_SELECTOR))


def _factory_answer(web3: Web3, factory: str, token_a: str, token_b: str, tick_spacing: int) -> str:
    return _word_address(_call(web3, factory, encode_aerodrome_cl_get_pool(token_a, token_b, tick_spacing)))


def _generation(factory: str) -> SlipstreamDeployment:
    deployment = slipstream_deployment_for_factory(CHAIN_NAME, factory)
    assert deployment is not None, factory
    return deployment


def _compiler(funded_wallet: str, anvil_rpc_url: str, adapter, price_oracle: dict[str, Decimal]) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
        gateway_client=adapter,
        venue_verification_gateway_factory=lambda: adapter,
    )


def _teardown_position(token_id: int, deployment: SlipstreamDeployment, pool: str) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=str(token_id),
        chain=CHAIN_NAME,
        protocol=PROTOCOL,
        value_usd=Decimal("0"),
        details={"nft_manager": deployment.position_manager, "pool_address": pool.lower()},
    )


def _price_band_around_spot(web3: Web3, pool: str, token0_decimals: int, token1_decimals: int) -> PriceBand:
    """A band that straddles the pool's live price (token1 per token0), in human units."""
    slot0 = _call(web3, pool, _SLOT0_SELECTOR)
    sqrt_price = Decimal(int.from_bytes(slot0[:32], "big")) / Decimal(2**96)
    spot = sqrt_price * sqrt_price * Decimal(10 ** (token0_decimals - token1_decimals))
    return PriceBand(lower=spot * Decimal("0.5"), upper=spot * Decimal("2"))


async def _open_close_certify(
    *,
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    anvil_rpc_url: str,
    adapter,
    pool_key: str,
    expected_pool: str,
    expected_factory: str,
    token0: str,
    token1: str,
    amount0: Decimal,
    amount1: Decimal,
    price_oracle: dict[str, Decimal],
    pool_ref: str | None = None,
) -> None:
    """Open by symbolic key or exact address, prove its generation, close, and certify."""
    deployment = _generation(expected_factory)
    other = next(d for d in slipstream_lp_deployments(CHAIN_NAME) if d != deployment)
    intent_pool = pool_ref or pool_key
    token0_decimals = get_token_decimals(web3, token0)
    token1_decimals = get_token_decimals(web3, token1)

    # Chain truth independent of the compiler: exactly one reviewed factory owns the key.
    tick_spacing = int(pool_key.split("/")[-1])
    assert _pool_factory(web3, expected_pool) == expected_factory.lower()
    assert _factory_answer(web3, deployment.factory, token0, token1, tick_spacing) == expected_pool.lower()
    if pool_ref is None:
        assert _factory_answer(web3, other.factory, token0, token1, tick_spacing) == "0x" + "0" * 40
    else:
        assert _factory_answer(web3, other.factory, token0, token1, tick_spacing) != expected_pool.lower()

    token0_before = get_token_balance(web3, token0, funded_wallet)
    token1_before = get_token_balance(web3, token1, funded_wallet)
    assert token0_before > 0 and token1_before > 0, "wallet funding failed"

    compiler = _compiler(funded_wallet, anvil_rpc_url, adapter, price_oracle)
    intent = Intent.lp_open(
        pool=intent_pool,
        amount0=amount0,
        amount1=amount1,
        range_spec=_price_band_around_spot(web3, expected_pool, token0_decimals, token1_decimals),
        protocol=PROTOCOL,
        chain=CHAIN_NAME,
    )

    # Layer 1 — the supplied pool reference resolves to the owning generation, not a default.
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", f"LP_OPEN {intent_pool} compilation failed: {compilation.error}"
    assert compilation.action_bundle is not None
    metadata = compilation.action_bundle.metadata
    assert metadata["slipstream_deployment"] == deployment.generation
    assert metadata["nft_manager"].lower() == deployment.position_manager.lower()
    assert metadata["tick_spacing"] == tick_spacing
    mint_tx = compilation.action_bundle.transactions[-1]
    mint_target = mint_tx["to"] if isinstance(mint_tx, dict) else mint_tx.to
    mint_data = mint_tx["data"] if isinstance(mint_tx, dict) else mint_tx.data
    assert mint_target.lower() == deployment.position_manager.lower()
    assert mint_target.lower() != other.position_manager.lower()
    assert mint_data.lower().startswith(_NPM_MINT_SELECTOR)
    operational = {ref["reference"].lower() for ref in metadata["venue_operational_refs"]}
    assert deployment.position_manager.lower() in operational
    assert other.position_manager.lower() not in operational

    # Layer 2 — execute on the managed fork.
    execution = await orchestrator.execute(compilation.action_bundle)
    assert execution.success is True, f"LP_OPEN {intent_pool} execution failed: {execution.error}"
    fork_block = int(os.environ.get("ANVIL_FORK_BLOCK_BASE") or 1)
    for tx_result in execution.transaction_results:
        assert tx_result.receipt is not None and tx_result.receipt.status == 1
        assert tx_result.receipt.block_number >= fork_block

    # Layer 3 — the parser recovers the position and the NFT lives on the owning NPM only.
    parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
    position_id = None
    open_data = None
    for tx_result in execution.transaction_results:
        if tx_result.receipt is None:
            continue
        receipt_dict = tx_result.receipt.to_dict()
        parsed = parser.parse_receipt(receipt_dict)
        assert parsed.success, f"LP_OPEN receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
        position_id = parser.extract_position_id(receipt_dict) or position_id
        open_data = parser.extract_lp_open_data(receipt_dict) or open_data
    assert position_id is not None
    assert open_data is not None
    assert open_data.amount0 > 0 and open_data.amount1 > 0
    assert query_position_liquidity(web3, deployment.position_manager, int(position_id)) > 0

    # Layer 4 — both legs deposited, neither over the request.
    token0_spent = token0_before - get_token_balance(web3, token0, funded_wallet)
    token1_spent = token1_before - get_token_balance(web3, token1, funded_wallet)
    assert 0 < token0_spent <= int(amount0 * Decimal(10**token0_decimals))
    assert 0 < token1_spent <= int(amount1 * Decimal(10**token1_decimals))
    assert (open_data.amount0, open_data.amount1) == (token0_spent, token1_spent)
    token0_after_open = get_token_balance(web3, token0, funded_wallet)
    token1_after_open = get_token_balance(web3, token1, funded_wallet)

    teardown_hook = get_teardown_post_condition(PROTOCOL)
    assert teardown_hook is not None
    open_check = teardown_hook(
        _teardown_position(int(position_id), deployment, expected_pool), funded_wallet, gateway_client=adapter
    )
    assert open_check.closed is False and not open_check.unmeasured, open_check
    assert open_check.residual["position_manager"].lower() == deployment.position_manager.lower()

    # Close through the generation that owns the NFT.
    close_compilation = compiler.compile(
        LPCloseIntent(
            position_id=str(position_id), pool=intent_pool, collect_fees=True, protocol=PROTOCOL, chain=CHAIN_NAME
        )
    )
    assert close_compilation.status.value == "SUCCESS", f"LP_CLOSE compilation failed: {close_compilation.error}"
    assert close_compilation.action_bundle is not None
    assert close_compilation.action_bundle.metadata["slipstream_deployment"] == deployment.generation
    assert close_compilation.action_bundle.metadata["nft_manager"].lower() == deployment.position_manager.lower()
    close_execution = await orchestrator.execute(close_compilation.action_bundle)
    assert close_execution.success is True, f"LP_CLOSE {intent_pool} execution failed: {close_execution.error}"
    close_data = None
    for tx_result in close_execution.transaction_results:
        assert tx_result.receipt is not None and tx_result.receipt.status == 1
        receipt_dict = tx_result.receipt.to_dict()
        parsed = parser.parse_receipt(receipt_dict)
        assert parsed.success, f"LP_CLOSE receipt parsing failed for {tx_result.tx_hash}: {parsed.error}"
        close_data = parser.extract_lp_close_data(receipt_dict) or close_data
    assert close_data is not None
    assert query_position_liquidity(web3, deployment.position_manager, int(position_id)) == 0
    token0_returned = get_token_balance(web3, token0, funded_wallet) - token0_after_open
    token1_returned = get_token_balance(web3, token1, funded_wallet) - token1_after_open
    assert token0_returned > 0 and token1_returned > 0
    assert (close_data.amount0_collected, close_data.amount1_collected) == (token0_returned, token1_returned)

    close_block = max(tx.receipt.block_number for tx in close_execution.transaction_results if tx.receipt)
    closed_check = teardown_hook(
        _teardown_position(int(position_id), deployment, expected_pool),
        funded_wallet,
        gateway_client=adapter,
        block=close_block,
    )
    assert closed_check.closed is True and not closed_check.unmeasured, closed_check


@pytest.mark.base
@pytest.mark.lp
class TestSlipstreamGenerationFromExactPool:
    @pytest.mark.parametrize(
        ("factory", "pool"),
        [
            (LEGACY_FACTORY, AMBIGUOUS_POOLS["legacy"]),
            (CURRENT_FACTORY, AMBIGUOUS_POOLS["current"]),
        ],
        ids=["legacy", "current"],
    )
    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_exact_pool_address_opens_and_closes_on_owning_generation(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
        factory: str,
        pool: str,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        await _open_close_certify(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            anvil_rpc_url=anvil_rpc_url,
            adapter=anvil_eth_call_adapter,
            pool_key=AMBIGUOUS_KEY,
            pool_ref=pool,
            expected_pool=pool,
            expected_factory=factory,
            token0=tokens["WETH"],
            token1=tokens["USDC"],
            amount0=Decimal("0.05"),
            amount1=Decimal("150"),
            price_oracle=PRICE_ORACLE,
        )


@pytest.mark.base
@pytest.mark.lp
class TestSlipstreamGenerationFromSymbolicKey:
    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_legacy_only_key_mints_on_the_legacy_npm(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        await _open_close_certify(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            anvil_rpc_url=anvil_rpc_url,
            adapter=anvil_eth_call_adapter,
            pool_key=LEGACY_ONLY_KEY,
            expected_pool=LEGACY_ONLY_POOL,
            expected_factory=LEGACY_FACTORY,
            token0=tokens["WETH"],
            token1=tokens["USDC"],
            amount0=Decimal("0.05"),
            amount1=Decimal("150"),
            price_oracle=PRICE_ORACLE,
        )

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_current_only_key_mints_on_the_current_npm(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        # cbBTC is not part of the shared funding set; seed it here (FiatToken
        # balance slot 9, verified on chain) so the position is two-sided.
        fund_erc20_token(funded_wallet, CBBTC, 10**8, CBBTC_BALANCE_SLOT, anvil_rpc_url)
        assert _word_address(_call(web3, CURRENT_ONLY_POOL, _TOKEN0_SELECTOR)) == tokens["USDC"].lower()
        assert _word_address(_call(web3, CURRENT_ONLY_POOL, _TOKEN1_SELECTOR)) == CBBTC.lower()
        # The pool quotes cbBTC per USDC; the oracle wants USD per cbBTC.
        band = _price_band_around_spot(web3, CURRENT_ONLY_POOL, 6, 8)
        cbbtc_usd = Decimal("1") / (band.lower * Decimal("2"))
        await _open_close_certify(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            anvil_rpc_url=anvil_rpc_url,
            adapter=anvil_eth_call_adapter,
            pool_key=CURRENT_ONLY_KEY,
            expected_pool=CURRENT_ONLY_POOL,
            expected_factory=CURRENT_FACTORY,
            token0=tokens["USDC"],
            token1=CBBTC,
            amount0=Decimal("200"),
            amount1=Decimal("0.002"),
            price_oracle={**PRICE_ORACLE, "cbBTC": cbbtc_usd},
        )

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    # Compile-time refusal: no bundle exists to execute, so Layers 2-4 cannot
    # apply; bilateral balance conservation is asserted instead.
    async def test_key_owned_by_both_generations_is_refused(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth, usdc = tokens["WETH"], tokens["USDC"]
        for generation, pool in AMBIGUOUS_POOLS.items():
            deployment = next(d for d in slipstream_lp_deployments(CHAIN_NAME) if d.generation == generation)
            assert _factory_answer(web3, deployment.factory, weth, usdc, 50) == pool.lower()
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        compilation = _compiler(funded_wallet, anvil_rpc_url, anvil_eth_call_adapter, PRICE_ORACLE).compile(
            Intent.lp_open(
                pool=AMBIGUOUS_KEY,
                amount0=Decimal("0.05"),
                amount1=Decimal("150"),
                range_spec=PriceBand(lower=Decimal("1000"), upper=Decimal("12000")),
                protocol=PROTOCOL,
                chain=CHAIN_NAME,
            )
        )
        assert compilation.status.value == "FAILED"
        assert compilation.action_bundle is None
        error = compilation.error or ""
        assert "Ambiguous" in error and "Name the pool address" in error, error
        for pool in AMBIGUOUS_POOLS.values():
            assert pool.lower() in error.lower()
        assert get_token_balance(web3, usdc, funded_wallet) == usdc_before
        assert get_token_balance(web3, weth, funded_wallet) == weth_before

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    # Compile-time refusal: no bundle exists to execute, so Layers 2-4 cannot
    # apply; bilateral balance conservation is asserted instead.
    async def test_pools_on_unreviewed_factories_are_refused_by_the_lp_lane(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url: str,
        anvil_eth_call_adapter,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth, usdc = tokens["WETH"], tokens["USDC"]
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)
        compiler = _compiler(funded_wallet, anvil_rpc_url, anvil_eth_call_adapter, PRICE_ORACLE)

        for pool, factory in (
            (UNREGISTERED_AERODROME_POOL, UNREGISTERED_AERODROME_FACTORY),
            (UNISWAP_V3_POOL, UNISWAP_V3_FACTORY),
        ):
            assert _pool_factory(web3, pool) == factory.lower()
            assert slipstream_deployment_for_factory(CHAIN_NAME, factory) is None
            compilation = compiler.compile(
                Intent.lp_open(
                    pool=pool,
                    amount0=Decimal("0.05"),
                    amount1=Decimal("150"),
                    range_spec=PriceBand(lower=Decimal("1000"), upper=Decimal("12000")),
                    protocol=PROTOCOL,
                    chain=CHAIN_NAME,
                )
            )
            assert compilation.status.value == "FAILED", pool
            assert compilation.action_bundle is None
            assert "reports unreviewed factory" in (compilation.error or ""), compilation.error
            assert factory.lower() in (compilation.error or "").lower()
        assert get_token_balance(web3, usdc, funded_wallet) == usdc_before
        assert get_token_balance(web3, weth, funded_wallet) == weth_before
