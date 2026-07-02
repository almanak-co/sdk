"""X Layer (chain_id 196) — zkEVM L2 (Polygon CDK, OKX)."""

from almanak.core.enums import ChainFamily

from ._contracts import safe_multisend_contracts
from ._descriptor import AnvilProfile, ChainDescriptor, GasProfile, NativeToken, RpcProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        name="xlayer",
        chain_id=196,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="OKB",
            name="OKB",
            decimals=18,
            wrapped_address="0xe538905cf8410324e03A5A23C1c177a474D59b2b",
            coingecko_id="okb",
            wrapped_symbol="WOKB",
            wrapped_coingecko_id="okb",
            # No verified X Layer / OKB-native SLIP-44 entry. Registry 996 is
            # OKExChain, a different OKX chain, so native CAIP-19 stays fail-loud.
            slip44=None,
        ),
        gas=GasProfile(
            buffer=1.3,
            simulation_buffer=0.3,
            price_cap_gwei=10,
            cost_cap_native=1.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.xlayer.tech",
            alchemy_prefix="xlayer",
            anvil_port=8557,
            fork_requires_archive=True,
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
            "usdt": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USD₮0 (Aave V3.6 reserve)
            "weth": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
            "wokb": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
            "xeth": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
            "xbtc": "0xb7C00000bcDEeF966b20B3D884B98E64d2b06b4f",
            "usdg": "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8",
            # Intentional alias to the same USD₮0 address — both keys are
            # surfaced for callers that pass the explicit USDT0 / LayerZero
            # designation. Mirrors the legacy CHAIN_TOKENS shape.
            "usdt0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
        },
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "xlayer",
            "dexscreener": "xlayer",
        },
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_multisend_contracts(),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WOKB": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
                "WETH": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
                "xETH": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
                "USDC": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
                "USDT": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
                "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
                "WBTC": "0xEA034fb02eB1808C2cc3adbC15f447B93CbE08e1",
            },
            balance_slots={
                "USDT0": 51,
            },
        ),
        aliases=(),
    )
)
