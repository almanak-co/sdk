"""Uniswap V4 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on
``UniswapV4GatewayConnector``; strategy-side connector code reads the
dicts directly.

OFFICIAL source: https://docs.uniswap.org/contracts/v4/deployments
CRITICAL: Addresses are DIFFERENT per chain. Do NOT copy-paste across chains.
Swaps route through UniversalRouter (command 0x10). LP via PositionManager.
"""

from __future__ import annotations

UNISWAP_V4: dict[str, dict[str, str]] = {
    "ethereum": {
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e",
        "universal_router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
        "state_view": "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227",
    },
    "base": {
        "pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b",
        "position_manager": "0x7C5f5A4bBd8fD63184577525326123B519429bDc",
        "universal_router": "0x6fF5693b99212Da76ad316178A184AB56D299b43",
        "quoter": "0x0d5e0F971ED27FBfF6c2837bf31316121532048D",
        "state_view": "0xA3c0c9b65baD0b08107Aa264b0f3dB444b867A71",
    },
    "arbitrum": {
        "pool_manager": "0x360E68faCcca8cA495c1B759Fd9EEe466db9FB32",
        "position_manager": "0xd88F38F930b7952f2DB2432Cb002E7abbF3dD869",
        "universal_router": "0xA51afAFe0263b40EdaEf0Df8781eA9aa03E381a3",
        "quoter": "0x3972C00f7ed4885e145823eb7C655375d275A1C5",
        "state_view": "0x76Fd297e2D437cd7f76d50F01AfE6160f86e9990",
    },
    "optimism": {
        "pool_manager": "0x9a13F98Cb987694C9F086b1F5eB990EeA8264Ec3",
        "position_manager": "0x3C3Ea4B57a46241e54610e5f022E5c45859A1017",
        "universal_router": "0x851116D9223fabED8E56C0E6b8Ad0c31d98B3507",
        "quoter": "0x1f3131A13296FB91C90870043742C3CDBFF1A8d7",
        "state_view": "0xc18a3169788F4F75A170290584ECA6395C75Ecdb",
    },
    "polygon": {
        "pool_manager": "0x67366782805870060151383F4BbFF9daB53e5cD6",
        "position_manager": "0x1Ec2eBf4F37E7363FDfe3551602425af0B3ceef9",
        "universal_router": "0x1095692A6237d83C6a72F3F5eFEdb9A670C49223",
        "quoter": "0xb3d5c3Dfc3a7aEbFF71895A7191796BFFc2c81b9",
        "state_view": "0x5eA1bD7974c8A611cBAB0bDCAFcB1D9CC9b3BA5a",
    },
    "avalanche": {
        "pool_manager": "0x06380C0e0912312B5150364B9DC4542BA0DbBc85",
        "position_manager": "0xB74b1F14d2754AcfcbBe1a221023a5cf50Ab8ACD",
        "universal_router": "0x94b75331AE8d42C1b61065089B7d48FE14aA73b7",
        "quoter": "0xbE40675BB704506a3c2Ccfb762DCFd1e979845C2",
        "state_view": "0xc3c9e198C735a4b97e3e683f391cCBDD60B69286",
    },
    "bsc": {
        "pool_manager": "0x28e2Ea090877bF75740558f6BFB36A5ffeE9e9dF",
        "position_manager": "0x7A4a5c919aE2541AeD11041A1AEeE68f1287f95b",
        "universal_router": "0x1906c1d672b88cD1B9aC7593301cA990F94Eae07",
        "quoter": "0x9F75dD27D6664c475B90e105573E550ff69437B0",
        "state_view": "0xd13Dd3D6E93f276FAfc9Db9E6BB47C1180aeE0c4",
    },
}

__all__ = ["UNISWAP_V4"]
