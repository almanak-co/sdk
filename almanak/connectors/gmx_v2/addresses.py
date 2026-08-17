"""GMX V2 core contract addresses per chain.

This module contains deployment configuration only: contracts whose identity is
stable connector wiring. It deliberately contains no market or token catalogue.
Live market identity comes from the gateway registry after an exact
``Reader.getMarket`` verification; the one bounded offline permission vector
lives in :mod:`almanak.connectors.gmx_v2.permission_seed` as a generated,
audited artifact (ALM-3199).

The former market aliases and ``GMX_V2_TOKENS`` mirror were hand-maintained
copies of venue/token-registry state. VIB-6155 found five wrong market rows and
VIB-6401 found nine valid long collaterals missing from the token mirror. Their
removal is intentional: runtime compilation derives collateral identity,
symbols, and decimals from the same verified market record.

The contract-kind vocabulary (``exchange_router`` / ``router`` /
``data_store`` / ``order_vault`` / ``reader`` / ``event_emitter``) is
connector-private — callers outside this folder should consume the
gateway registry, not guess key names.
"""

from __future__ import annotations

GMX_V2: dict[str, dict[str, str]] = {
    "arbitrum": {
        "exchange_router": "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41",
        "router": "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6",
        "data_store": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
        "order_vault": "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5",
        "reader": "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789",
        # Central EventEmitter (all keeper-settlement events: OrderExecuted /
        # PositionIncrease / PositionDecrease / PositionFeesCollected). Canonical,
        # matches adapter.GMX_V2_ADDRESSES["arbitrum"]["event_emitter"].
        "event_emitter": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
    },
    # Avalanche addresses verified against
    # https://github.com/gmx-io/gmx-synthetics/tree/main/deployments/avalanche
    # and the live GMX REST markets endpoint
    # (https://avalanche-api.gmxinfra.io/markets) on 2026-04-29 — VIB-1720.
    "avalanche": {
        "exchange_router": "0x8f550E53DFe96C055D5Bdb267c21F268fCAF63B2",
        "router": "0x820F5FfC5b525cD4d88Cd91aCf2c28F16530Cc68",
        "data_store": "0x2F0b22339414ADeD7D5F06f9D604c7fF5b2fe3f6",
        "order_vault": "0xD3D60D22d415aD43b7e64b510D86A30f19B1B12C",
        "reader": "0x62Cb8740E6986B29dC671B2EB596676f60590A5B",
        # Central EventEmitter (see arbitrum note). Matches
        # adapter.GMX_V2_ADDRESSES["avalanche"]["event_emitter"].
        "event_emitter": "0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26",
    },
}

__all__ = ["GMX_V2"]
