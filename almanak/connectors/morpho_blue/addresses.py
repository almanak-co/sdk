"""Morpho Blue contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``MorphoBlueGatewayConnector``;
strategy-side connector code reads the dicts directly.

Three surfaces live here:

* ``MORPHO_BLUE_ADDRESS`` — the universal Morpho Blue deployment address
  used on every chain that deployed via the vanity-address factory.
  Always look up the per-chain address in ``MORPHO_BLUE[chain]["morpho"]``
  rather than relying on this constant directly — Arbitrum / Polygon /
  Monad each have chain-specific deployer pattern addresses.
* ``MORPHO_BLUE`` — per-chain Morpho + Bundler addresses.
* ``MORPHO_BLUE_TOKENS`` — the canonical underlying-token address
  catalogue used by the strategy-side adapter.

The contract-kind vocabulary (``morpho`` / ``bundler``) is connector-
private — callers outside this folder should consume the gateway
registry, not guess key names.
"""

from __future__ import annotations

# Default Morpho Blue deployment address, used on chains where Morpho deployed via the
# vanity-address factory. Not universal: Monad / Arbitrum / Polygon each deployed at
# a distinct address. Always look the per-chain address up in MORPHO_BLUE[chain]["morpho"]
# rather than relying on this constant directly.
MORPHO_BLUE_ADDRESS = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

MORPHO_BLUE: dict[str, dict[str, str]] = {
    "ethereum": {
        "morpho": MORPHO_BLUE_ADDRESS,
        "bundler": "0x4095F064B8d3c3548A3bebfd0Bbfd04750E30077",
    },
    "base": {
        "morpho": MORPHO_BLUE_ADDRESS,
        "bundler": "0x23055618898e202386e6c13955a58D3C68200BFB",
    },
    "arbitrum": {
        # Arbitrum deployed via a distinct factory — the universal
        # 0xBBBB...FFCb address has 0 bytes of code here. Registry fixed 2026-04-17
        # after on-chain verification (prior value was the non-deployed vanity address
        # and caused every Morpho Blue compile call to fail with "Unknown market").
        # Source: Morpho's GraphQL API (blue-api.morpho.org) + on-chain eth_getCode.
        "morpho": "0x6c247b1F6182318877311737BaC0844bAa518F5e",
        # Bundler address listed for future multicall integration. Not used by any
        # current supply/borrow/repay/withdraw path — verify live selectors before
        # wiring it into execution (tracked as a follow-up in VIB-2967 epic).
        "bundler": "0x1FA4431bC113D308beE1d46B0e98Cb805FB48C13",
    },
    "polygon": {
        # Polygon uses a chain-specific deployment; the universal 0xBBBB...FFCb
        # vanity address has 0 bytes of code here (verified 2026-04-17 via Morpho
        # GraphQL blue-api.morpho.org and on-chain eth_getCode). Contract creation
        # block is 66,931,042. Same Arbitrum-style pattern: the dispatch address
        # reported by Morpho's own API is the one the factory actually deployed to.
        "morpho": "0x1bF0c2541F820E775182832f06c0B7Fc27A25f67",
        # Bundler3 (Morpho multicall router on Polygon). Sourced from Morpho's
        # official blue-sdk addresses.ts and verified on-chain (1,547 bytes).
        # Listed for future multicall integration — not used by any current
        # supply/borrow/repay/withdraw path.
        "bundler": "0x2d9C3A9E67c966C711208cc78b34fB9E9f8db589",
    },
    "monad": {
        # Monad uses a distinct deployment (chain-specific deployer pattern, block 31,907,457).
        # Bundler3 variant — equivalent to the universal Bundler on other chains.
        "morpho": "0xD5D960E8C380B724a48AC59E2DfF1b2CB4a1eAee",
        "bundler": "0x82b684483e844422FD339df0b67b3B111F02c66E",
    },
}

MORPHO_BLUE_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "cbETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "sDAI": "0x83F20F44975D03b1b09e64809B757c47f942BEeA",
        "MORPHO": "0x9994E35Db50125E0DF82e4c2dde62496CE330999",
        "USDe": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
        "sUSDe": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
        "weETH": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
        "ezETH": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    },
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "weETH": "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe",
    },
    "polygon": {
        # Tokens used in live Polygon Morpho Blue markets. USDC here is the
        # native Circle USDC (NOT the bridged USDC.e at 0x2791...) — markets
        # created after Circle's native launch quote prices against this one.
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "WBTC": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
        "WPOL": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "wstETH": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
    },
    "monad": {
        # Tokens used in live Monad Morpho Blue markets (sourced from
        # morpho-org/morpho-blue-api-metadata `tokens.json` and verified on-chain).
        "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
        "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "USDT0": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
        "cbBTC": "0xd18B7EC58Cdf4876f6AFebd3Ed1730e4Ce10414b",
        "wstETH": "0x10Aeaf63194db8d453d4D85a06E5eFE1dd0b5417",
        "weETH": "0xA3D68b74bF0528fdD07263c60d6488749044914b",
        "AUSD": "0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a",
    },
}


__all__ = ["MORPHO_BLUE_ADDRESS", "MORPHO_BLUE", "MORPHO_BLUE_TOKENS"]
