"""Monad (chain_id 143) — high-throughput EVM-compatible L1."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    AnvilProfile,
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
        enum=Chain.MONAD,
        name="monad",
        chain_id=143,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="MON",
            name="Monad",
            decimals=18,
            wrapped_address="0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
            coingecko_id="monad",
            wrapped_symbol="WMON",
            wrapped_coingecko_id="monad",
            slip44=268435779,  # SLIP-44 "Monad" — MON (CAIP-19)
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=50,
            cost_cap_native=10.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=60,
            grpc_execute=240,
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.monad.xyz",
            alchemy_prefix="monad",
            anvil_port=8555,
        ),
        explorer=Explorer(browse_url="https://explorer.monad.xyz"),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
            "weth": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
            "wmon": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
            "wbtc": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "monad",
            "dexscreener": "monad",
        },
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
                "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
                "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
                "USDT0": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
                "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
            },
        ),
        aliases=(),
    )
)
