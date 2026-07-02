"""Plasma (chain_id 9745) — EVM L1."""

from almanak.core.enums import ChainFamily

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
        name="plasma",
        chain_id=9745,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="XPL",
            name="Plasma",
            decimals=18,
            wrapped_address="0x6100E367285b01F48D07953803A2d8dCA5D19873",
            # wrapper aliases native
            coingecko_id="plasma",
            wrapped_symbol="WXPL",
            wrapped_coingecko_id="plasma",
            # No verified Plasma SLIP-44 entry; native CAIP-19 stays fail-loud.
            slip44=None,
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=50,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.plasma.to",
            alchemy_prefix="plasma",
            tenderly_subdomain="plasma",
            anvil_port=8554,
            rate_limit_rpm=300,
        ),
        explorer=Explorer(browse_url="https://plasmascan.io"),
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "plasma",
            "dexscreener": "plasma",
            "zerion": "plasma",
        },
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WXPL": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
                "USDT0": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
                "FUSDT0": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",
                "PENDLE": "0x17Bac5F906c9A0282aC06a59958D85796c831f24",
            },
        ),
        aliases=(),
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("XPL", "WXPL", "USDC", "USDT", "WETH", "PENDLE"),
    )
)
