"""GMX V2 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``GmxV2GatewayConnector``;
strategy-side connector code reads the dicts directly.

Three surfaces live here:

* ``GMX_V2`` — per-chain core contract + market addresses
  (ExchangeRouter / Router / DataStore / OrderVault / Reader, plus the
  long/short market addresses GMX exposes per pair).
* ``GMX_V2_TOKENS`` — the canonical underlying-token address catalogue
  consumed by the strategy-side adapter (long/short tokens for each
  market — WETH/WBTC/USDC/USDT on Arbitrum, WAVAX/BTC.b/WETH.e/USDC/USDT
  on Avalanche).
* ``GMX_V2_MARKETS`` / ``GMX_V2_INDEX_TOKEN_DECIMALS`` — market-address
  catalogue and index-token decimals used by compiler, adapter, perps read,
  and paper-position readers.

The contract-kind vocabulary (``exchange_router`` / ``router`` /
``data_store`` / ``order_vault`` / ``reader`` / ``<pair>_market``) is
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
        "eth_usd_market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "btc_usd_market": "0x47c031236e19d024b42f8AE6780E44A573170703",
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
        # USDC-collateral perp markets (the AVAX-* and BTC-* native-collateral
        # variants are not exposed yet — strategies that need them should be
        # added with a separate market key).
        "eth_usd_market": "0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7",
        "btc_usd_market": "0xFb02132333A79C8B5Bd0b64E3AbccA5f7fAf2937",
        "avax_usd_market": "0x913C1F46b48b3eD35E7dc3Cf754d4ae8499F31CF",
    },
}

GMX_V2_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    },
    "avalanche": {
        # Long/short tokens used by GMX V2 USDC-collateral markets on Avalanche.
        # WAVAX is the native wrapper; BTC.b and WETH.e are the GMX-listed
        # bridged variants (the Avalanche bridge uses these symbols on-chain).
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "BTC.b": "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        # GMX-listed USDC on Avalanche is native Circle USDC (NOT bridged
        # USDC.e). Verified via the markets endpoint short-token field.
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
    },
}

GMX_V2_MARKETS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "ETH/USD": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "BTC/USD": "0x47c031236e19d024b42f8AE6780E44A573170703",
        "LINK/USD": "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
        "ARB/USD": "0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407",
        "SOL/USD": "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
        # VIB-6155: was 0xC7aBb2C5F3bf3CEB389df0Ebb3cFE90EcE8A1bAa — a
        # transcription slip of the address below (identical through
        # "0xc7Abb2C5f3BF3CEB389dF0E", then divergent). Reader.getMarket()
        # returns the zero Props for the old value: no such market.
        # GMX lists two UNI markets; this is the UNI-long one, which is what
        # the collateral rule ("UNI", "USDC") already declared.
        "UNI/USD": "0xc7Abb2C5f3BF3CEB389dF0Eecd6120D451170B50",
        "DOGE/USD": "0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4",
        "LTC/USD": "0xD9535bB5f58A1a75032416F2dFe7880C30575a41",
        "XRP/USD": "0x0CCB4fAa6f1F1B30911619f1184082aB4E25813c",
        "ATOM/USD": "0x248C35760068cE009a13076D573ed3497A47bCD4",
        "NEAR/USD": "0x63Dc80EE90F26363B3FCD609007CC9e14c8991BE",
        "AAVE/USD": "0x1CbBa6346F110c8A5ea739ef2d1eb182990e4EB2",
        # VIB-6155: was 0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7 — the
        # AVALANCHE ETH/USD market, copy-pasted onto an Arbitrum row. GMX V2
        # market tokens are per-deployment CREATE2 products, so that address
        # is not a market on Arbitrum at all (zero Props).
        "AVAX/USD": "0x7BbBf946883a5701350007320F525c5379B8178A",
        # VIB-6155: was 0xb56E5E2eB50cf5383342914b0C85Fe62DbD861C8 — a REAL
        # Arbitrum market, but the wstETH/WETH SWAP pool (indexToken == 0x0),
        # not OP/USD. This is the class a cross-chain uniqueness check cannot
        # see: the address was unique and live, just not this market. Only
        # Reader.getMarket() distinguishes them, which is why the audit in
        # tests/audit/test_gmx_v2_market_identity.py compares against it.
        "OP/USD": "0x4fDd333FF9cA409df583f306B6F5a7fFdE790739",
        "GMX/USD": "0x55391D178Ce46e7AC8eaAEa50A72D1A5a8A622Da",
    },
    "avalanche": {
        "AVAX/USD": GMX_V2["avalanche"]["avax_usd_market"],
        "ETH/USD": "0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7",
        "BTC/USD": "0xFb02132333A79C8B5Bd0b64E3AbccA5f7fAf2937",
        # VIB-6155: both were addresses that Reader.getMarket() answers with
        # the zero Props on Avalanche — no such markets. Replaced with the
        # live SOL/USD and LTC/USD market tokens enumerated from
        # Reader.getMarkets() on 2026-08-02.
        "SOL/USD": "0xd2eFd1eA687CD78c41ac262B3Bc9B53889ff1F70",
        "LTC/USD": "0xA74586743249243D3b77335E15FE768bA8E1Ec5A",
    },
}


def _market_decimal_key(chain: str, market: str) -> str:
    """Return the normalized decimal-table key for a listed market."""
    return GMX_V2_MARKETS[chain][market].lower()


GMX_V2_INDEX_TOKEN_DECIMALS: dict[str, dict[str, int]] = {
    "arbitrum": {
        _market_decimal_key("arbitrum", "ETH/USD"): 18,
        _market_decimal_key("arbitrum", "BTC/USD"): 8,
        _market_decimal_key("arbitrum", "LINK/USD"): 18,
        _market_decimal_key("arbitrum", "ARB/USD"): 18,
        _market_decimal_key("arbitrum", "SOL/USD"): 9,
        _market_decimal_key("arbitrum", "UNI/USD"): 18,
        _market_decimal_key("arbitrum", "DOGE/USD"): 8,
        _market_decimal_key("arbitrum", "LTC/USD"): 8,
        _market_decimal_key("arbitrum", "XRP/USD"): 6,
        _market_decimal_key("arbitrum", "ATOM/USD"): 6,
        _market_decimal_key("arbitrum", "NEAR/USD"): 24,
        _market_decimal_key("arbitrum", "AAVE/USD"): 18,
        _market_decimal_key("arbitrum", "AVAX/USD"): 18,
        _market_decimal_key("arbitrum", "OP/USD"): 18,
        _market_decimal_key("arbitrum", "GMX/USD"): 18,
    },
    "avalanche": {
        _market_decimal_key("avalanche", "AVAX/USD"): 18,
        _market_decimal_key("avalanche", "ETH/USD"): 18,
        _market_decimal_key("avalanche", "BTC/USD"): 8,
        _market_decimal_key("avalanche", "SOL/USD"): 9,
        _market_decimal_key("avalanche", "LTC/USD"): 8,
    },
}


def index_token_decimals(chain: str | None, market_address: str | None) -> int | None:
    """Index-token decimals for a GMX V2 market, or ``None`` when unresolved (VIB-6110).

    Keyed ``chain → lowercase market address → decimals`` via ``GMX_V2_INDEX_TOKEN_DECIMALS``.
    Unlike ``adapter._get_index_token_decimals`` (which defaults to 18 for size scaling),
    this returns ``None`` on an unknown chain/market so PRICE scaling can fail closed
    (Empty≠Zero) — a wrongly-scaled ``entry_price``/``exit_price`` is worse than an
    unmeasured one. Callers must map ``None`` to an unmeasured price, never to the raw
    GMX-native ratio.
    """
    if not chain or not market_address:
        return None
    return GMX_V2_INDEX_TOKEN_DECIMALS.get(str(chain).lower(), {}).get(str(market_address).lower())


def _assert_gmx_v2_decimal_coverage() -> None:
    """Fail fast when listed market addresses drift away from decimal metadata.

    LOAD-BEARING for money, not just tidiness (VIB-6110). Since PERP_SETTLEMENT
    entry/exit prices are scaled by ``index_token_decimals(chain, market)``, this
    import-time assert is the ONLY thing guaranteeing that a position this SDK
    opened can always resolve its price scale: ``compiler._resolve_market`` and
    ``sdk.get_market_address`` resolve markets exclusively from
    ``GMX_V2_MARKETS``, and this assert guarantees every such market has
    decimals. Weaken it and prices silently become unmeasured (Empty≠Zero keeps
    them from becoming *wrong*, but a settlement stops being measurable).

    It does NOT cover markets we did not open — a third party's market seen in a
    shared keeper tx may be absent from the table, which is why the price path
    still fails closed rather than trusting a default. Replacing this table with
    a chain-backed resolver is VIB-6156.

    It also does NOT check that a listed address IS the market it claims to be —
    it only checks the two local tables agree with each other, so it stays green
    when both name the same wrong address. VIB-6155 found five such rows. The
    only authority for identity is on-chain ``Reader.getMarket()``; the audit
    that compares against it is ``tests/audit/test_gmx_v2_market_identity.py``.
    Note that a cross-chain uniqueness assert would NOT be that guard: it would
    have caught only one of the five (identical CREATE2 addresses across chains
    are legitimately possible, and four of the five rows were unique anyway).
    """
    missing: dict[str, list[str]] = {}
    extra: dict[str, list[str]] = {}
    for chain, markets in GMX_V2_MARKETS.items():
        market_addresses = {address.lower() for address in markets.values()}
        decimal_addresses = set(GMX_V2_INDEX_TOKEN_DECIMALS.get(chain, {}))
        chain_missing = sorted(market_addresses - decimal_addresses)
        chain_extra = sorted(decimal_addresses - market_addresses)
        if chain_missing:
            missing[chain] = chain_missing
        if chain_extra:
            extra[chain] = chain_extra
    if missing or extra:
        raise ValueError(
            f"GMX_V2_INDEX_TOKEN_DECIMALS must exactly cover GMX_V2_MARKETS; missing={missing!r} extra={extra!r}"
        )


_assert_gmx_v2_decimal_coverage()

__all__ = [
    "GMX_V2",
    "GMX_V2_INDEX_TOKEN_DECIMALS",
    "GMX_V2_MARKETS",
    "GMX_V2_TOKENS",
    "index_token_decimals",
]
