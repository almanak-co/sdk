"""Morpho Blue contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``MorphoBlueGatewayConnector``;
strategy-side connector code reads the dicts directly.

Four surfaces live here:

* ``MORPHO_BLUE_ADDRESS`` — the universal Morpho Blue deployment address
  used on every chain that deployed via the vanity-address factory.
  Always look up the per-chain address in ``MORPHO_BLUE[chain]["morpho"]``
  rather than relying on this constant directly — Arbitrum / Polygon /
  Monad each have chain-specific deployer pattern addresses.
* ``MORPHO_BLUE`` — per-chain Morpho + Bundler addresses.
* ``MORPHO_BLUE_TOKENS`` — the canonical underlying-token address
  catalogue used by the strategy-side adapter.
* ``MORPHO_MARKETS`` — per-chain ``market_id -> market params`` catalogue
  (loan/collateral tokens, oracle, irm, lltv). Re-exported by ``adapter.py``
  for backward compatibility, and read by the account-state spec's per-market
  accessor (VIB-4929 PR-3a). Owning it here keeps every Morpho address /
  market literal in one connector-private module.

The contract-kind vocabulary (``morpho`` / ``bundler``) is connector-
private — callers outside this folder should consume the gateway
registry, not guess key names.
"""

from __future__ import annotations

from typing import Any

# Default Morpho Blue deployment address, used on chains where Morpho deployed via the
# vanity-address factory. Not universal: Monad / Arbitrum / Polygon / Robinhood each
# deployed at a distinct address. Always look the per-chain address up in
# MORPHO_BLUE[chain]["morpho"] rather than relying on this constant directly.
#
# Per-chain NON-vanity singletons (the vanity 0xBBBB…FFCb has ZERO code on these chains —
# using it makes every compile fail "Unknown market"):
#   - arbitrum: 0x6c247b1F6182318877311737BaC0844bAa518F5e
#   - polygon:  0x1bF0c2541F820E775182832f06c0B7Fc27A25f67
#   - monad:    0xD5D960E8C380B724a48AC59E2DfF1b2CB4a1eAee
#   - robinhood:0x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010 (verified via eth_getCode 2026-07-09)
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
    "robinhood": {
        # Robinhood Chain (4663, Arbitrum Orbit L2). NON-vanity singleton: the
        # universal 0xBBBB…FFCb vanity address has ZERO bytes of code here
        # (verified via eth_getCode 2026-07-09) — using it makes every compile
        # fail "Unknown market". Source: docs.morpho.org + on-chain verification.
        # Morpho is ~73% of Robinhood TVL (powers the Earn product).
        "morpho": "0x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010",
        # Bundler3 is not published on Morpho's Robinhood address page and is not
        # used by any supply/borrow/repay/withdraw path, so it is deliberately
        # omitted rather than invented (MORPHO_BUNDLER_ADDRESSES tolerates absence).
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
    "robinhood": {
        # All verified on-chain at block 5_610_000 (2026-07-09). USDG (Global Dollar,
        # Paxos, 6 dec) is the loan asset of EVERY Robinhood Morpho market; USDe
        # (Ethena, 18 dec) is the deepest collateral. syrupUSDG backs the secondary
        # market. No real Circle-USDC / Tether-USDT exists on 4663 (the 6-dec ones are
        # dead, ~11 holders) — deliberately omitted, not invented.
        "WETH": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
        "USDG": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
        "USDe": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
        "syrupUSDG": "0x40858070814a57FdF33a613ae84fE0a8b4a874f7",
    },
}

# Pre-configured Morpho Blue markets (market_id -> market info)
# Market ID is keccak256(abi.encode(loanToken, collateralToken, oracle, irm, lltv))
MORPHO_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        # wstETH/USDC market (86% LLTV)
        "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc": {
            "name": "wstETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            "oracle": "0x48F7E36EB6B826B2dF4B2E630B62Cd25e89E40e2",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # wstETH/WETH market (94.5% LLTV)
        "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41": {
            "name": "wstETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            "oracle": "0x2a01EB9496094dA03c4E364Def50f5aD1280AD72",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 945000000000000000,  # 94.5%
        },
        # WBTC/USDC market (86% LLTV)
        "0x3a85e619751152991742810df6ec69ce473daef99e28a64ab2340d7b7ccfee49": {
            "name": "WBTC/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "WBTC",
            "collateral_token_address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            "oracle": "0xDddd770BADd886dF3864029e4B377B5F6a2B6b83",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # sUSDe/DAI market (86% LLTV) - Ethena synthetic dollar
        "0x39d11026eae1c6ec02aa4c0910778664089cdd97c3fd23f68f7cd05e2e95af48": {
            "name": "sUSDe/DAI",
            "loan_token": "DAI",
            "loan_token_address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            "collateral_token": "sUSDe",
            "collateral_token_address": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
            "oracle": "0x5D916980D5Ae1737a8330Bf24dF812b2911Aae25",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # sUSDe/USDC market (91.5% LLTV) - Ethena synthetic dollar
        "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab": {
            "name": "sUSDe/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "sUSDe",
            "collateral_token_address": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
            "oracle": "0x873CD44b860DEDFe139f93e12A4AcCa0926Ffb87",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
        },
        # weETH/WETH market (90% LLTV) - ether.fi wrapped ETH
        "0x698fe98247a40c5771537b5786b2f3f9d78eb487b4ce4d75533cd0e94d88a115": {
            "name": "weETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collateral_token": "weETH",
            "collateral_token_address": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
            "oracle": "0x3fa58b74e9a8eA8768eb33c8453e9C2Ed089A40a",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 900000000000000000,  # 90%
        },
        # ezETH/WETH market (86% LLTV) - Renzo restaked ETH
        "0x49bb2d114be9041a787432952927f6f144f05ad3e83196a7d062f374ee11d0ee": {
            "name": "ezETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collateral_token": "ezETH",
            "collateral_token_address": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",
            "oracle": "0x61025e2B0122ac8bE4e37365A4003d87ad888Cc3",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
        },
        # =====================================================================
        # Pendle PT Collateral Markets
        # =====================================================================
        # PT-sUSDe-5FEB2026/USDC market (91.5% LLTV) - Pendle PT as collateral (expired, verified on-chain)
        "0xd174bb7b8dd6ef16b116753b56679932ee13382b94f81bf66a2b37962cb41f56": {
            "name": "PT-sUSDe-5FEB2026/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "PT-sUSDe-5FEB2026",
            "collateral_token_address": "0xE8483517077afa11A9B07f849cee2552f040d7b2",
            "oracle": "0xFAfb71F2fe9a4330c34a192812F36D8d6f07f095",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
            "is_pt_market": True,
        },
        # PT-sUSDe-27MAR2025/USDC market (91.5% LLTV) - expired but verified on-chain
        "0x346afa2b6d528222a2f9721ded6e7e2c40ac94877a598f5dae5013c651d2a462": {
            "name": "PT-sUSDe-27MAR2025/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "PT-sUSDe-27MAR2025",
            "collateral_token_address": "0xE00bd3Df25fb187d6ABBB620b3dfd19839947b81",
            "oracle": "0x9c0174fE7748F318dcB7300b93B170b6026280B0",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
            "is_pt_market": True,
        },
        # PT-sUSDe-31JUL2025/USDC market (91.5% LLTV) - expired but verified on-chain
        "0xbc552f0b14dd6f8e60b760a534ac1d8613d3539153b4d9675d697e048f2edc7e": {
            "name": "PT-sUSDe-31JUL2025/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collateral_token": "PT-sUSDe-31JUL2025",
            "collateral_token_address": "0x3b3fB9C57858EF816833dC91565EFcd85D96f634",
            "oracle": "0x1376913337ceC523B4DDEAD8a60eDb1fA43fF1E3",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 915000000000000000,  # 91.5%
            "is_pt_market": True,
        },
        # PT-eUSDe/USDe market (86% LLTV)
        "0xe7a06721ca6dce24fce8c5a57d7bb39688dc0f5700e86be29d1f488acab63876": {
            "name": "PT-eUSDe/USDe",
            "loan_token": "USDe",
            "loan_token_address": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
            "collateral_token": "PT-eUSDe",
            "collateral_token_address": "0x308c36baF407f543DaC3A6340b7b6B31079e8e0D",
            "oracle": "0x5D916980D5Ae1737a8330Bf24dF812b2911Aae25",
            "irm": "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            "lltv": 860000000000000000,  # 86%
            "is_pt_market": True,
        },
    },
    "arbitrum": {
        # Arbitrum Morpho Blue markets use a chain-specific AdaptiveCurveIRM at
        # 0x66F30587FB8D4206918deb78ecA7d5eBbafD06DA (different from the Ethereum IRM).
        # Market IDs sourced from blue-api.morpho.org and verified on-chain 2026-04-17.
        # wstETH/USDC market (86% LLTV) - top-TVL Arbitrum market (~$12M supply)
        "0x33e0c8ab132390822b07e5dc95033cf250c963153320b7ffca73220664da2ea0": {
            "name": "wstETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x5979D7b546E38E414F7E9822514be443A4800529",
            "oracle": "0x8e02a9b9Cc29d783b2fCB71C3a72651B591cae31",
            "irm": "0x66F30587FB8D4206918deb78ecA7d5eBbafD06DA",
            "lltv": 860000000000000000,  # 86%
        },
        # WBTC/USDC market (86% LLTV) - top-TVL WBTC market on Arbitrum (~$3.2M supply)
        "0xe6392ff19d10454b099d692b58c361ef93e31af34ed1ef78232e07c78fe99169": {
            "name": "WBTC/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "collateral_token": "WBTC",
            "collateral_token_address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
            "oracle": "0x88193FcB705d29724A40Bb818eCAA47dD5F014d9",
            "irm": "0x66F30587FB8D4206918deb78ecA7d5eBbafD06DA",
            "lltv": 860000000000000000,  # 86%
        },
    },
    "polygon": {
        # Polygon Morpho Blue markets use the chain-specific AdaptiveCurveIRM at
        # 0xe675A2161D4a6E2de2eeD70ac98EEBf257FBF0B0. Market IDs sourced from
        # blue-api.morpho.org (chainId=137, sorted by supply TVL) and verified
        # on-chain 2026-04-17.
        #
        # WBTC/WPOL market (77% LLTV) — top-TVL Polygon Morpho market (~$3.2M supply).
        "0x96e62bd75493006b81dae51d5db3c5af4b3ced65133dab60e70df9dc8e38bf2c": {
            "name": "WBTC/WPOL",
            "loan_token": "WPOL",
            "loan_token_address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
            "collateral_token": "WBTC",
            "collateral_token_address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
            "oracle": "0x624d826C5233A7426C98d1BE789E70583A296b24",
            "irm": "0xe675A2161D4a6E2de2eeD70ac98EEBf257FBF0B0",
            "lltv": 770000000000000000,  # 77%
        },
        # WBTC/USDC market (86% LLTV) — ~$1.7M supply. Used by the intent test because
        # USDC is the loan token (well-known storage slot 9) and WBTC has a clean
        # storage slot 0 on Polygon.
        "0x1cfe584af3db05c7f39d60e458a87a8b2f6b5d8c6125631984ec489f1d13553b": {
            "name": "WBTC/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "collateral_token": "WBTC",
            "collateral_token_address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
            "oracle": "0x15B4e0eE3DC3D20D9d261da2D3E0d2a86A6A6291",
            "irm": "0xe675A2161D4a6E2de2eeD70ac98EEBf257FBF0B0",
            "lltv": 860000000000000000,  # 86%
        },
        # wstETH/WETH market (91.5% LLTV) — ~$1.1M supply. High-LLTV correlated pair.
        "0xb8ae474af3b91c8143303723618b31683b52e9c86566aa54c06f0bc27906bcae": {
            "name": "wstETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
            "oracle": "0x1Dc2444b54945064c131145cD6b8701e3454C63a",
            "irm": "0xe675A2161D4a6E2de2eeD70ac98EEBf257FBF0B0",
            "lltv": 915000000000000000,  # 91.5%
        },
    },
    "base": {
        # cbETH/USDC market (86% LLTV)
        "0xdba352d93a64b17c71104cbddc6aef85cd432322a1446b5b65163cbbc615cd0c": {
            "name": "cbETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "collateral_token": "cbETH",
            "collateral_token_address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
            "oracle": "0x4756c26E01E61c7c2F86b10f4316e179db8F9425",
            "irm": "0x46415998764C29aB2a25CbeA6254146D50D22687",
            "lltv": 860000000000000000,  # 86%
        },
        # wstETH/USDC market (86% LLTV) - https://app.morpho.org/base/market/0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae/wsteth-usdc
        "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae": {
            "name": "wstETH/USDC",
            "loan_token": "USDC",
            "loan_token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "collateral_token": "wstETH",
            "collateral_token_address": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
            "oracle": "0xD7A1abA119a236Fea5BBC5cAC6836465cbe9289A",
            "irm": "0x46415998764C29aB2a25CbeA6254146D50D22687",
            "lltv": 860000000000000000,  # 86%
        },
    },
    "monad": {
        # Markets sourced from Morpho GraphQL API (blue-api.morpho.org) for chainId 143,
        # sorted by supply TVL. All markets use AdaptiveCurveIRM
        # 0x09475a3D6eA8c314c592b1a3799bDE044E2F400F.
        #
        # WETH/wstETH market (94.5% LLTV) — largest Monad Morpho market (~$61.8M supply).
        "0x8bdb7d2c5024d349772884afb3c5c409bc8de58ed63d79618bf48fb57b595060": {
            "name": "wstETH/WETH",
            "loan_token": "WETH",
            "loan_token_address": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
            "collateral_token": "wstETH",
            "collateral_token_address": "0x10Aeaf63194db8d453d4D85a06E5eFE1dd0b5417",
            "oracle": "0xBB16f6B3c5422209ee1d9b0f63761F159C136694",
            "irm": "0x09475a3D6eA8c314c592b1a3799bDE044E2F400F",
            "lltv": 945000000000000000,  # 94.5%
        },
        # WBTC/AUSD market (86% LLTV) — BTC-backed lending (~$13.2M supply).
        "0x0ce0a3398925f5112360db21750912f2a834c5cb90ecf03f461b2e2561320955": {
            "name": "WBTC/AUSD",
            "loan_token": "AUSD",
            "loan_token_address": "0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a",
            "collateral_token": "WBTC",
            "collateral_token_address": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
            "oracle": "0xda77Cf67fFEECC7fc64a4767837D1fFEad1Bc73C",
            "irm": "0x09475a3D6eA8c314c592b1a3799bDE044E2F400F",
            "lltv": 860000000000000000,  # 86%
        },
    },
    "robinhood": {
        # Robinhood Chain Morpho markets (verified on-chain at block 5_610_000, 2026-07-09).
        # Every market on 4663 uses USDG (Global Dollar) as the loan asset and the single
        # AdaptiveCurveIRM 0x2BD3d5965B26B51814AC95127B2b80dD6CcC0fa1 (chain-specific —
        # never copy Ethereum's). There is NO WETH-collateral market on 4663, so the
        # looping demo is a stable-stable loop (USDe collateral / USDG debt).
        #
        # USDe/USDG market — the deep one powering Robinhood Earn: $75.5M supplied /
        # $68.9M borrowed (util 0.91). The looping demo targets this market.
        "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6": {
            "name": "USDe/USDG",
            "loan_token": "USDG",
            "loan_token_address": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
            "collateral_token": "USDe",
            "collateral_token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
            "oracle": "0xE64849bd4AD03DfaBbe02bb521de19997a19055f",
            "irm": "0x2BD3d5965B26B51814AC95127B2b80dD6CcC0fa1",
            "lltv": 915000000000000000,  # 91.5%
        },
        # syrupUSDG/USDG market — secondary, ~$9M supplied. Same loan asset + IRM.
        "0x919a9b6b94dae7c86620eaf7a08e597aae8a4c3a9e9c7671771fbaf62b6b61c7": {
            "name": "syrupUSDG/USDG",
            "loan_token": "USDG",
            "loan_token_address": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
            "collateral_token": "syrupUSDG",
            "collateral_token_address": "0x40858070814a57FdF33a613ae84fE0a8b4a874f7",
            "oracle": "0x152c638fad68913739Ee19Ba8eF47fAEB09DCa91",
            "irm": "0x2BD3d5965B26B51814AC95127B2b80dD6CcC0fa1",
            "lltv": 915000000000000000,  # 91.5%
        },
    },
}

__all__ = ["MORPHO_BLUE_ADDRESS", "MORPHO_BLUE", "MORPHO_BLUE_TOKENS", "MORPHO_MARKETS"]
