"""Avalanche C-Chain (chain_id 43114)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    ChainDescriptor,
    Explorer,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.AVALANCHE,
        name="avalanche",
        chain_id=43114,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="AVAX",
            name="Avalanche",
            decimals=18,
            wrapped_address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=100,
            cost_cap_native=1.0,
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES. Avalanche native
            # USDC is a proxy and needs ~150k+ for swap calldata.
            operation_overrides={
                "swap_simple": 180000,
            },
            fallback_base_fee_gwei=25.0,
            fallback_priority_fee_gwei=1.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
            # VIB-4857: Avalanche Anvil forks are slower than L2s. Mirrors
            # the legacy CHAIN_RECEIPT_TIMEOUTS entry in chain_executor.py.
            receipt_polling=180,
        ),
        rpc=RpcProfile(
            public_rpc="https://avalanche-c-chain-rpc.publicnode.com",
            alchemy_prefix="avax",
            tenderly_subdomain="avalanche",
            anvil_port=8547,
            poa=True,
            block_time_seconds=2.0,
        ),
        explorer=Explorer(
            api_url="https://api.snowtrace.io/api",
            api_key_env="SNOWTRACE_API_KEY",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "usdt": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
            "wavax": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        aliases=("avax",),
    )
)
