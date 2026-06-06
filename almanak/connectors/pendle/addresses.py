"""Pendle contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``PendleGatewayConnector``;
strategy-side connector code reads the dicts directly.

Two surfaces live here:

* ``PENDLE`` — per-chain Pendle Router + supporting infrastructure
  (router static, market factory, YT factory, PT oracle) plus the
  ``market_*`` keys that point at specific PT / YT / LP markets. The
  ``market_*`` entries are dynamic by nature — strategy-side code adds
  new markets here as Pendle ships new expiries.
* ``PENDLE_TOKENS`` — the canonical underlying-token address catalogue
  consumed by the strategy-side adapter.

The contract-kind vocabulary (``router`` / ``router_static`` /
``market_factory`` / ``yt_factory`` / ``pt_oracle`` /
``market_*`` / ``pt_*`` / ``yt_*`` / ``sy_*``) is connector-private —
callers outside this folder should consume the gateway registry, not
guess key names. The connector's contract-monitoring manifest declares
the ``market_*`` prefix for strategy-side monitoring of per-market
addresses.
"""

from __future__ import annotations

PENDLE: dict[str, dict[str, str]] = {
    "arbitrum": {
        # Core contracts
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x263833d47eA3fA4a30f269323aba6a107f9eB14C",
        "market_factory": "0x2FCb47B58350cD377f94d3821e7373Df60bD9Ced",
        "yt_factory": "0x28d4cE244fCE6f26C6A4A0447fFe8A4ccf9F1CcC",
        "pt_oracle": "0x1Fd95db7B7C0067De8D45C0cb35D59796adfD187",
        # Popular markets
        "market_wsteth_26dec2024": "0xf769035a247af48bf55BaA82d8b5e14E02E49A25",
        "market_wsteth_26jun2025": "0x08a152834de126d2ef83D612ff36e4523FD0017F",  # Expired
        "market_wsteth_active": "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B",
        "market_eeth_26dec2024": "0x952083cde7aaa11AB8449057F7de23A970AA8472",
        "market_rseth_26dec2024": "0x6ae79089b2CF4be441480801F9f1CA1a54e3ce9C",
    },
    "ethereum": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x263833d47eA3fA4a30f269323aba6a107f9eB14C",
        "market_factory": "0x1A6fCc85557BC4fB7B534ed835a03EF056552D52",
        "yt_factory": "0xeA1CE3Fd2da6C6BD47C227526be5e54e4E12fE00",
        "pt_oracle": "0x66a1096C6366b2529274dF4f5D8247827fe4CEA8",
    },
    "plasma": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        # fUSDT0 market (Fluid) - expires 26 Feb 2026
        "market_fusdt0_26feb2026": "0x0cb289E9df2d0dCFe13732638C89655fb80C2bE2",
        "pt_fusdt0_26feb2026": "0xbE45F6F17b81571fC30253BDaE0A2A6f7b04D60F",
        "yt_fusdt0_26feb2026": "0xC0f6a41a9837C4d824Bc8d346341DB77e634ae69",
        "sy_fusdt0": "0xfF3CCC1245D59B21B6EC4A597557E748f8311E8c",
    },
    "sonic": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x0013ACc071f732fd6BF8210AB46A3794a7D8945e",
        "market_factory": "0x0AB3ae25c42a2f3748a018556989355D568Fa6d6",  # V6
    },
    "base": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0xB4205a645c7e920BD8504181B1D7f2c5C955C3e7",
        "market_factory": "0x81E80A50E56d10C501fF17B5Fe2F662bd9EA4590",  # V6
    },
    "mantle": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0xCAd502Bb55d1A3F79952F969BFF3f011CF30a94a",
        "market_factory": "0xa35AE21a593CB06959978E20b33Db34163166C79",  # V6
    },
    "bsc": {
        "router": "0x888888888889758F76e7103c6CbF23ABbF58F946",
        "router_static": "0x2700ADB035F82a11899ce1D3f1BF8451c296eABb",
        "market_factory": "0x80cE46449DF1c977f6ba60495125ce282F83DdFB",  # V6
    },
}

PENDLE_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "WSTETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "PENDLE": "0x0c880f6761F1af8d9Aa9C466984b80DAb9a8c9e8",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "WSTETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "PENDLE": "0x808507121B80c02388fAd14726482e061B8da827",
    },
    "plasma": {
        "USDT0": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
        "FUSDT0": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",
        "PENDLE": "0x17Bac5F906c9A0282aC06a59958D85796c831f24",
        "WXPL": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
    },
    "sonic": {
        "wS": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        "WETH": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "USDC": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
    "mantle": {
        "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",  # Canonical Mantle Bridged WETH (deterministic bridge address, not a placeholder)
        "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
    },
}

__all__ = ["PENDLE", "PENDLE_TOKENS"]
