"""X Layer (chain_id 196) — zkEVM L2 (Polygon CDK, OKX)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, RpcProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.XLAYER,
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
        contracts={"safe_multisend": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"},
        aliases=(),
    )
)
