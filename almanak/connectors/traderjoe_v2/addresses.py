"""TraderJoe V2 (Liquidity Book) contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on
``TraderJoeV2GatewayConnector``; strategy-side connector code reads the
dicts directly.

Three address surfaces live here:

* ``TRADERJOE_V2`` — per-chain LBFactory + LBRouter v2.1 addresses.
* ``TRADERJOE_V2_LBPAIRS`` — the per-pair LBPair contracts the static
  Roles manifest needs ``approveForAll`` access to during LP_CLOSE.
* ``TRADERJOE_V2_TOKENS`` — the canonical token address catalogue used
  by the strategy-side adapter.
"""

from __future__ import annotations

TRADERJOE_V2: dict[str, dict[str, str]] = {
    "avalanche": {
        "factory": "0x8e42f2F4101563bF679975178e880FD87d3eFd4e",
        "router": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1
    },
    "arbitrum": {
        "factory": "0x8e42f2F4101563bF679975178e880FD87d3eFd4e",
        "router": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1 (CREATE2 — same address)
    },
    "bsc": {
        "factory": "0x8e42f2F4101563bF679975178e880FD87d3eFd4e",
        "router": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",  # LBRouter v2.1 (CREATE2 — same address)
    },
    "ethereum": {
        "factory": "0xDC8d77b69155c7E68A95a4fb0f06a71FF90B943a",
        "router": "0x9A93a421b74F1c5755b83dD2C211614dC419C44b",  # LBRouter v2.1
    },
}

# TraderJoe V2 LBPair (per-pair) addresses — needed for static permissions on
# Roles modifier. Each pair is a distinct contract; ``approveForAll`` is called
# on the pair, not the router. Without this registry, LP_CLOSE compiles fail
# during synthetic discovery (RPC required to resolve the address) and the
# Safe is unauthorised when a real strategy tries to remove liquidity. See
# #1905. A future improvement would be a dynamic LBFactory.getLBPairInformation
# resolution at manifest-gen time so arbitrary pairs are covered without
# manual registration.
#
# Address verified on-chain via
# ``LBFactory.getLBPairInformation(WAVAX, USDC, 20)`` against the avalanche
# C-Chain RPC (LBFactory ``0x8e42f2F4101563bF679975178e880FD87d3eFd4e``).
TRADERJOE_V2_LBPAIRS: dict[str, list[dict[str, str | int]]] = {
    "avalanche": [
        {
            "tokenX": "WAVAX",
            "tokenY": "USDC",
            "bin_step": 20,
            "address": "0xD446eb1660F766d533BeCeEf890Df7A69d26f7d1",
        },
    ],
    # Arbitrum WETH/USDC LBPair (bin_step=15) — the only WETH/USDC pair with
    # meaningful liquidity on the LBFactory at
    # ``0x8e42f2F4101563bF679975178e880FD87d3eFd4e``. Address verified on-chain
    # via ``LBFactory.getLBPairInformation(WETH, USDC, 15)`` against the
    # arbitrum one RPC and ``LBPair.getReserves()`` (~4 WETH / ~3861 USDC as of
    # 2026-05-14).
    "arbitrum": [
        {
            "tokenX": "WETH",
            "tokenY": "USDC",
            "bin_step": 15,
            "address": "0x69f1216cB2905bf0852f74624D5Fa7b5FC4dA710",
        },
    ],
    # BSC WBNB/USDT LBPair (bin_step=15) — the WBNB/USDT pair whose active bin
    # tracks live market price on the LBFactory at
    # ``0x8e42f2F4101563bF679975178e880FD87d3eFd4e``. Address verified on-chain
    # via ``LBFactory.getLBPairInformation(WBNB, USDT, 15)`` against the BSC
    # public RPC. Token X = WBNB, token Y = USDT (both 18-decimal, Binance-Peg
    # USDT). The active bin's implied price (~$546) matches market for the
    # 2026-05-14 fork block; other bin steps either revert with zero reserves
    # (bs=25) or imply prices an order of magnitude off (bs=50/100), so bs=15
    # is the only viable choice on BSC. See VIB-4377.
    "bsc": [
        {
            "tokenX": "WBNB",
            "tokenY": "USDT",
            "bin_step": 15,
            "address": "0xf258929a659F68ace4732e36F626d6D1544878aC",
        },
    ],
    # Ethereum USDT/USDC LBPair (bin_step=1) — the only TJv2 pair on Ethereum
    # carrying meaningful reserves at the fork block (~497 USDT / ~70 USDC as
    # of 2026-05-14). LBFactory at ``0xDC8d77b69155c7E68A95a4fb0f06a71FF90B943a``.
    # Token X = USDT, token Y = USDC (verified on-chain via
    # ``LBPair.getTokenX/Y()``). Required by ``permission_hints._build_static_permissions``
    # so the Roles manifest authorises ``approveForAll(LBRouter, true)`` on the
    # LBPair during LP_CLOSE. See VIB-4419.
    "ethereum": [
        {
            "tokenX": "USDT",
            "tokenY": "USDC",
            "bin_step": 1,
            "address": "0x47B1CEC2D2370E11B049c73aB6732F03E920C71a",
        },
    ],
}


TRADERJOE_V2_TOKENS: dict[str, dict[str, str]] = {
    "avalanche": {
        "AVAX": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",  # Native
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "JOE": "0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "BTC.b": "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
    },
    "arbitrum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    },
    "bsc": {
        "BNB": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
    },
    "ethereum": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    },
}


__all__ = ["TRADERJOE_V2", "TRADERJOE_V2_LBPAIRS", "TRADERJOE_V2_TOKENS"]
