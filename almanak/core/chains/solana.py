"""Solana — non-EVM chain.

The legacy ``CHAIN_IDS`` mapped Solana to ``0`` (sentinel for "non-EVM");
we keep that contract. EVM gas / timeout knobs do not apply, so the
``GasProfile`` is populated with no-op fallback values (None caps,
``buffer`` carrying the framework default 1.0 — Solana uses
compute-unit + priority-fee accounting, not gas multipliers).
"""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, RpcProfile, Timeouts
from ._registry import register_chain

# Solana cluster URLs. Solana names networks by *cluster* (mainnet-beta,
# devnet, testnet) instead of by EIP-155 chain id, so this lives alongside
# the descriptor rather than inside ``RpcProfile``. Consumed by
# ``almanak.gateway.utils.rpc_provider.SOLANA_CLUSTER_URLS``.
SOLANA_CLUSTERS: dict[str, str] = {
    "mainnet-beta": "https://api.mainnet-beta.solana.com",
    "devnet": "https://api.devnet.solana.com",
    "testnet": "https://api.testnet.solana.com",
}

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.SOLANA,
        name="solana",
        chain_id=0,  # Non-EVM sentinel; matches legacy CHAIN_IDS
        family=ChainFamily.SOLANA,
        native=NativeToken(
            symbol="SOL",
            name="Solana",
            decimals=9,
            # Wrapped SOL mint (SPL token, base58)
            wrapped_address="So11111111111111111111111111111111111111112",
        ),
        gas=GasProfile(
            buffer=None,
            simulation_buffer=None,
            price_cap_gwei=None,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,
            grpc_execute=None,
        ),
        rpc=RpcProfile(
            # ``public_rpc`` mirrors ``SOLANA_CLUSTERS["mainnet-beta"]``
            # so the generic ``PUBLIC_RPC_URLS`` lookup works for Solana too.
            public_rpc="https://api.mainnet-beta.solana.com",
            alchemy_prefix="solana",
            anvil_port=8899,
            rate_limit_rpm=300,
        ),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "dexscreener": "solana",
            "geckoterminal": "solana",
            "defillama_display": "Solana",
            "zerion": "solana",
            "okx": "501",
        },
        aliases=("sol",),
    )
)
