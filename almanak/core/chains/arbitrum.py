"""Arbitrum One (chain_id 42161) — L2 (Optimistic rollup)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, Explorer, GasProfile, NativeToken, RpcProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.ARBITRUM,
        name="arbitrum",
        chain_id=42161,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=0.01,
            fallback_base_fee_gwei=0.1,
            fallback_priority_fee_gwei=0.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://arbitrum-one-rpc.publicnode.com",
            alchemy_prefix="arb",
            tenderly_subdomain="arbitrum",
            anvil_port=8545,
            block_time_seconds=0.25,
        ),
        explorer=Explorer(
            api_url="https://api.arbiscan.io/api",
            api_key_env="ARBISCAN_API_KEY",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS in
        # ``framework/intents/compiler_constants.py``.
        tokens={
            "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # Native USDC
            "usdc_bridged": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e
            "usdt": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "weth": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "wbtc": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        },
        aliases=("arb",),
    )
)
