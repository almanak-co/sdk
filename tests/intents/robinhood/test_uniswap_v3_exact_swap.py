"""Atomic, sealable Uniswap V3 SWAP proofs on Robinhood Chain (4663).

USDG (Global Dollar, Paxos, 6 dec) is the settlement asset: Robinhood has no
Circle USDC with real liquidity, and WETH/USDG fee-500 is the chain's only V3
pool with meaningful depth.
"""

from decimal import Decimal, localcontext

import pytest
from eth_abi import decode
from web3 import Web3

from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
from almanak.connectors.uniswap_v3.sdk import compute_pool_address
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType
from qa_lab.operator_gateway import OperatorGatewayClient
from tests.intents._uniswap_v3_exact_proofs import (
    execute_uniswap_v3_exact_reverse_cleanup,
    run_uniswap_v3_swap_exact_proof,
)
from tests.intents.conftest import CHAIN_CONFIGS, get_token_decimals

CHAIN = "robinhood"
STABLE = "USDG"


def _fork_pool_relative_prices(web3, gateway):
    """Use USDG quote units at the fork block, without asserting external USD fair value."""
    tokens = CHAIN_CONFIGS[CHAIN]["tokens"]
    weth, usdg = tokens["WETH"], tokens[STABLE]
    pool = Web3.to_checksum_address(compute_pool_address(UNISWAP_V3[CHAIN]["factory"], weth, usdg, 500))
    block = web3.eth.block_number
    raw = gateway.eth_call(CHAIN, pool, "0x3850c7bd", block=block, raise_on_error=True)
    sqrt_price = decode(["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"], bytes.fromhex(raw[2:]))[0]
    assert sqrt_price > 0, "Execution valuation requires initialized fork pool state"
    with localcontext() as context:
        context.prec = 80
        token0, token1 = sorted((weth.lower(), usdg.lower()))
        quote_ratio = Decimal(sqrt_price) ** 2 / Decimal(2**192)
        quote_ratio *= Decimal(10) ** (get_token_decimals(web3, token0) - get_token_decimals(web3, token1))
        weth_in_usdg = quote_ratio if token0 == weth.lower() else Decimal(1) / quote_ratio
    print(
        f"Execution-only pool-relative valuation: pool={pool}, block={block}, USDG quote unit=1; no external feed claim"
    )
    return {"WETH": weth_in_usdg, weth.lower(): weth_in_usdg, STABLE: Decimal(1), usdg.lower(): Decimal(1)}


@pytest.mark.robinhood
@pytest.mark.swap
class TestUniswapV3ExactSwapProof:
    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="swap.v1")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_safe(
        self, web3, funded_wallet, orchestrator, price_oracle, intent_evidence, anvil_eth_call_adapter
    ):
        await run_uniswap_v3_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            gateway_client=anvil_eth_call_adapter,
            from_symbol=STABLE,
            to_symbol="WETH",
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="swap.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_eoa(
        self, web3, anvil_rpc_url, funded_wallet, orchestrator, price_oracle, intent_evidence
    ):
        gateway = OperatorGatewayClient(web3, CHAIN)
        price_oracle = _fork_pool_relative_prices(web3, gateway)
        target = await run_uniswap_v3_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            compiler_config=IntentCompilerConfig(managed_fork=False),
            rpc_url=anvil_rpc_url,
            gateway_client=gateway,
            max_price_impact=Decimal("0.02"),
            from_symbol=STABLE,
            to_symbol="WETH",
        )
        await execute_uniswap_v3_exact_reverse_cleanup(
            chain=CHAIN,
            web3=web3,
            wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            execution_context=None,
            compiler_config=IntentCompilerConfig(managed_fork=False),
            rpc_url=anvil_rpc_url,
            gateway_client=gateway,
            amount_in_raw=target.amount_out_raw,
            max_price_impact=Decimal("0.02"),
            from_symbol="WETH",
            to_symbol=STABLE,
        )
