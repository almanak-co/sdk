"""Linea (chain_id 59144) — zkEVM L2."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    ChainDescriptor,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.LINEA,
        name="linea",
        chain_id=59144,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.3,
            price_cap_gwei=10,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=None,  # legacy: not in CHAIN_GRPC_EXECUTE_TIMEOUTS
        ),
        rpc=RpcProfile(
            public_rpc="https://linea-rpc.publicnode.com",
            alchemy_prefix="linea",
            anvil_port=8552,
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
            "usdt": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
            "weth": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "linea",
            "dexscreener": "linea",
        },
        aliases=(),
    )
)
