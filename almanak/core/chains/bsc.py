"""BNB Smart Chain (chain_id 56).

The legacy ``CHAIN_TX_TIMEOUTS`` had no entry for BSC (it fell back to the
framework default 120s). The descriptor captures that by leaving
``tx_confirmation`` ``None`` — the orchestrator's ``.get(chain, DEFAULT)``
still picks up the framework default.
"""

from almanak.core.enums import Chain, ChainFamily

from ._contracts import safe_stack_contracts
from ._descriptor import (
    AnvilProfile,
    ChainDescriptor,
    ChainlinkFeeds,
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
            coingecko_id="binancecoin",
            wrapped_symbol="WBNB",
            wrapped_coingecko_id="binancecoin",
            # SLIP-44 "Binance Smart Chain" (9006) — our Chain.BSC. NOT 714,
            # which is "Binance Chain" (BEP2), a different network we don't model.
            slip44=9006,
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
            rate_limit_rpm=300,
        ),
        explorer=Explorer(
            api_url="https://api.bscscan.com/api",
            api_key_env="BSCSCAN_API_KEY",
            browse_url="https://bscscan.com",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            "usdt": "0x55d398326f99059fF775485246999027B3197955",
            "wbnb": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "weth": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "tenderly": "bsc",
            "coingecko": "binance-smart-chain",
            "dexscreener": "bsc",
            "geckoterminal": "bsc",
            "defillama": "bsc",
            "defillama_display": "BSC",
            "zerion": "binance-smart-chain",
            "moralis": "bsc",
            "okx": "56",
        },
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "BNB/USD": "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE",
                "BTC/USD": "0x264990fbd0A4796A3E3d8E37C4d5F87a3aCa5Ebf",
                "ETH/USD": "0x9ef1B8c0E4F7dc8bF5719Ea496883DC6401d5b2e",
                "USDC/USD": "0x51597f405303c4377E36123CbF172bc359765377",
                "USDT/USD": "0xB97Ad0E74fa7d920791E90258A6E2085088b4320",
                "DAI/USD": "0x132d3C0B1D2cEa0BC552588063bdBb210FDeecfA",
                "LINK/USD": "0xca236E327F629f9Fc2c30A4E95775EbF0B89fac8",
                "CAKE/USD": "0xb6064eD41d4F67e353768AA239CA98F9c422E159",
                "AAVE/USD": "0xA8357BF572460fC40f4B0aCacbB2a6A61c89f475",
            },
        ),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_stack_contracts(enso_delegate_primary=True),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
                "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
                "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
                "USDT": "0x55d398326f99059fF775485246999027B3197955",
            },
            balance_slots={
                "USDC": 1,
                "WBNB": 3,
                "USDT": 1,
                "BUSD": 0,
            },
            wrapped_native_deposit=True,
        ),
        aliases=("bnb", "binance"),
        color="#f0b90b",  # Plan 027: BSC yellow (from legacy CHAIN_COLORS)
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("BNB", "WBNB", "USDC", "USDT", "WETH", "BTCB", "DAI"),
    )
)
