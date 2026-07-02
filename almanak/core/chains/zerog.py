"""0G Chain (chain_id 16661) — AI L1 (preview support)."""

from almanak.core.enums import ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, RpcProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        name="zerog",
        chain_id=16661,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="A0GI",
            name="0G",
            decimals=18,
            wrapped_address="0x1Cd0690fF9a693f5EF2dD976660a8dAFc81A109c",
            # W0G has its own CG listing
            coingecko_id="zero-gravity",
            wrapped_symbol="W0G",
            wrapped_coingecko_id="wrapped-0g",
            # No verified 0G / A0GI SLIP-44 entry; native CAIP-19 stays fail-loud.
            slip44=None,
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=50,
            cost_cap_native=10.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            # zerog has no Alchemy / Tenderly route — public RPC + anvil only.
            public_rpc="https://rpc.ankr.com/0g_mainnet_evm",
            anvil_port=8558,
            fork_requires_archive=True,
        ),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "zerog",
            "dexscreener": "zerog",
        },
        aliases=("0g",),
    )
)
