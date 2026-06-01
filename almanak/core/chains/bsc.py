"""BNB Smart Chain (chain_id 56).

The legacy ``CHAIN_TX_TIMEOUTS`` had no entry for BSC (it fell back to the
framework default 120s). The descriptor captures that by leaving
``tx_confirmation`` ``None`` — the orchestrator's ``.get(chain, DEFAULT)``
still picks up the framework default.
"""

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
        enum=Chain.BSC,
        name="bsc",
        chain_id=56,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="BNB",
            name="BNB",
            decimals=18,
            wrapped_address="0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        ),
        gas=GasProfile(
            buffer=1.2,
            simulation_buffer=0.1,
            price_cap_gwei=20,
            cost_cap_native=0.05,
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES. BNB Uniswap V3
            # uses more gas for LP ops.
            operation_overrides={
                "lp_decrease_liquidity": 400000,
                "lp_collect": 300000,
                "lp_burn": 150000,
            },
            fallback_base_fee_gwei=3.0,
            fallback_priority_fee_gwei=0.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=300,
            # VIB-4857: BSC Anvil forks are slow — quoter ~60-80s, gas
            # estimate ~155s. Mirrors the legacy CHAIN_RECEIPT_TIMEOUTS
            # entry in chain_executor.py.
            receipt_polling=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://bsc-rpc.publicnode.com",
            alchemy_prefix="bnb",
            anvil_port=8546,
            poa=True,
        ),
        explorer=Explorer(
            api_url="https://api.bscscan.com/api",
            api_key_env="BSCSCAN_API_KEY",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            "usdt": "0x55d398326f99059fF775485246999027B3197955",
            "wbnb": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "weth": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        aliases=("bnb", "binance"),
    )
)
