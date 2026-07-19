"""Linea (chain_id 59144) — zkEVM L2."""

from almanak.core.enums import ChainFamily

from ._contracts import safe_stack_contracts
from ._descriptor import (
    AnvilProfile,
    ChainDescriptor,
    ChainlinkFeeds,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
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
            slip44=60,  # SLIP-44 coin type for Ether (CAIP-19 native)
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
            fork_requires_archive=True,
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
            "usdt": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
            "weth": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        },
        # Safe v1.4.1 + Zodiac Roles stack (canonical CREATE2 addresses, verified
        # deployed on Linea mainnet). Declaring it enables Safe-wallet execution
        # and the Zodiac Roles permission path for Linea strategies (VIB-5916) —
        # without it MULTISEND_ADDRESSES has no Linea entry and every
        # execTransactionWithRole batch fails. Matches the fragment every other
        # Safe-enabled chain uses.
        contracts=safe_stack_contracts(),
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "linea",
            "dexscreener": "linea",
        },
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "ETH/USD": "0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA",
                "BTC/USD": "0x7A99092816C8BD5ec8ba229e3a6E6Da1E628E1F9",
                "USDC/USD": "0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5",
                "USDT/USD": "0xefCA2bbe0EdD0E22b2e0d2F8248E99F4bEf4A7dB",
                "DAI/USD": "0x5133D67c38AFbdd02997c14Abd8d83676B4e309A",
            },
        ),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
                "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
                "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
            },
            balance_slots={
                "USDC": 9,
                "WETH": 3,
                "USDT": 51,
            },
            wrapped_native_deposit=True,
        ),
        aliases=(),
        color="#61dfff",  # Plan 027: Linea cyan (from legacy CHAIN_COLORS)
    )
)
