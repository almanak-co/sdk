"""Legacy verified-market snapshots for GMX V2 unit tests.

These literals preserve test scenarios that existed while the production
``GMX_V2_MARKETS`` table still shipped. They are not current listing evidence
and the runtime never imports them. Production code has no symbol→address
table: strategies supply addresses or venue labels and the dynamic registry
verifies them. Tests that need a deterministic "verified market" explicitly
prime the process catalog from these rows instead of patching production data.

Long-token addresses marked ``_UNREGISTERED`` mirror the VIB-6401 gap (the
token registry lacked those long tokens); they are deterministic sentinels,
NOT real contracts — no test may submit them anywhere.

Usage::

    from tests.unit.connectors.gmx_v2.market_fixtures import (
        FIXTURE_MARKETS, fake_dynamic_gateway, market_record, prime_catalog,
    )

    prime_catalog()                       # remember every fixture row
    rec = market_record("arbitrum", "ETH/USD")   # one verified record
    gateway = fake_dynamic_gateway("arbitrum")   # serves the rows dynamically
"""

from __future__ import annotations

from types import SimpleNamespace

import grpc

from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.market_metadata import ResolvedGmxMarket
from almanak.gateway.proto import gateway_pb2

#: Deterministic sentinel for long tokens absent from the token registry
#: (VIB-6401). One address per symbol, stable across runs, EIP-55 valid so
#: checksum sweeps over fixture data hold.
_UNREGISTERED = {
    "AAVE": "0x00000000000000000000000000000000000a0001",
    "ARB": "0x00000000000000000000000000000000000A0002",
    "WAVAX": "0x00000000000000000000000000000000000A0003",
    "GMX": "0x00000000000000000000000000000000000a0004",
    "LINK": "0x00000000000000000000000000000000000a0005",
    "OP": "0x00000000000000000000000000000000000A0006",
    "SOL": "0x00000000000000000000000000000000000A0007",
    "UNI": "0x00000000000000000000000000000000000a0008",
}
_TOKEN_DECIMALS = {
    "AAVE": 18,
    "ARB": 18,
    "BTC.B": 8,
    "GMX": 18,
    "LINK": 18,
    "OP": 18,
    "SOL": 9,
    "UNI": 18,
    "USDC": 6,
    "WAVAX": 18,
    "WBTC": 8,
    "WETH": 18,
    "WETH.E": 18,
}


def _row(
    chain: str,
    label: str,
    market_token: str,
    index_token: str,
    index_symbol: str,
    index_token_decimals: int,
    long_symbol: str,
    long_token: str,
    short_symbol: str,
    short_token: str,
) -> tuple[str, ResolvedGmxMarket]:
    return (
        chain,
        ResolvedGmxMarket(
            label=label,
            market_token=market_token,
            # The audit-pinned index-token address (EXPECTED_INDEX_TOKENS in
            # tests/audit/test_gmx_v2_market_identity.py — chain-verified).
            # Synthetic markets pin their venue identifier address; it exists
            # in the Props tuple even when no ERC-20 code lives behind it.
            index_token=index_token,
            index_symbol=index_symbol,
            index_token_decimals=index_token_decimals,
            long_token=long_token or _UNREGISTERED[long_symbol],
            long_token_symbol=long_symbol,
            long_token_decimals=_TOKEN_DECIMALS[long_symbol.upper()],
            short_token=short_token,
            short_token_symbol=short_symbol,
            short_token_decimals=_TOKEN_DECIMALS[short_symbol.upper()],
        ),
    )


FIXTURE_MARKETS: tuple[tuple[str, ResolvedGmxMarket], ...] = (
    _row(
        "arbitrum",
        "AAVE/USD",
        "0x1CbBa6346F110c8A5ea739ef2d1eb182990e4EB2",
        "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196",
        "AAVE",
        18,
        "AAVE",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "ARB/USD",
        "0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407",
        "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "ARB",
        18,
        "ARB",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "ATOM/USD",
        "0x248C35760068cE009a13076D573ed3497A47bCD4",
        "0x7D7F1765aCbaF847b9A1f7137FE8Ed4931FbfEbA",
        "ATOM",
        6,
        "WETH",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "AVAX/USD",
        "0x7BbBf946883a5701350007320F525c5379B8178A",
        "0x565609fAF65B92F7be02468acF86f8979423e514",
        "AVAX",
        18,
        "WAVAX",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "BTC/USD",
        "0x47c031236e19d024b42f8AE6780E44A573170703",
        "0x47904963fc8b2340414262125aF798B9655E58Cd",
        "BTC",
        8,
        "WBTC",
        "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "DOGE/USD",
        "0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4",
        "0xC4da4c24fd591125c3F47b340b6f4f76111883d8",
        "DOGE",
        8,
        "WETH",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "ETH/USD",
        "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "ETH",
        18,
        "WETH",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "GMX/USD",
        "0x55391D178Ce46e7AC8eaAEa50A72D1A5a8A622Da",
        "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
        "GMX",
        18,
        "GMX",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "LINK/USD",
        "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
        "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",
        "LINK",
        18,
        "LINK",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "LTC/USD",
        "0xD9535bB5f58A1a75032416F2dFe7880C30575a41",
        "0xB46A094Bc4B0adBD801E14b9DB95e05E28962764",
        "LTC",
        8,
        "WETH",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "NEAR/USD",
        "0x63Dc80EE90F26363B3FCD609007CC9e14c8991BE",
        "0x1FF7F3EFBb9481Cbd7db4F932cBCD4467144237C",
        "NEAR",
        24,
        "WETH",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "OP/USD",
        "0x4fDd333FF9cA409df583f306B6F5a7fFdE790739",
        "0xaC800FD6159c2a2CB8fC31EF74621eB430287a5A",
        "OP",
        18,
        "OP",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "SOL/USD",
        "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
        "0x2bcC6D6CdBbDC0a4071e48bb3B969b06B3330c07",
        "SOL",
        9,
        "SOL",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "UNI/USD",
        "0xc7Abb2C5f3BF3CEB389dF0Eecd6120D451170B50",
        "0xFa7F8980b0f1E64A2062791cc3b0871572f1F7f0",
        "UNI",
        18,
        "UNI",
        "",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "arbitrum",
        "XRP/USD",
        "0x0CCB4fAa6f1F1B30911619f1184082aB4E25813c",
        "0xc14e065b0067dE91534e032868f5Ac6ecf2c6868",
        "XRP",
        6,
        "WETH",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    ),
    _row(
        "avalanche",
        "AVAX/USD",
        "0x913C1F46b48b3eD35E7dc3Cf754d4ae8499F31CF",
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "AVAX",
        18,
        "WAVAX",
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "USDC",
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    ),
    _row(
        "avalanche",
        "BTC/USD",
        "0xFb02132333A79C8B5Bd0b64E3AbccA5f7fAf2937",
        "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
        "BTC",
        8,
        "BTC.b",
        "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
        "USDC",
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    ),
    _row(
        "avalanche",
        "ETH/USD",
        "0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7",
        "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "ETH",
        18,
        "WETH.e",
        "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC",
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    ),
    _row(
        "avalanche",
        "LTC/USD",
        "0xA74586743249243D3b77335E15FE768bA8E1Ec5A",
        "0x8E9C35235C38C44b5a53B56A41eaf6dB9a430cD6",
        "LTC",
        8,
        "WAVAX",
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "USDC",
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    ),
    _row(
        "avalanche",
        "SOL/USD",
        "0xd2eFd1eA687CD78c41ac262B3Bc9B53889ff1F70",
        "0xFE6B19286885a4F7F55AdAD09C3Cd1f906D2478F",
        "SOL",
        9,
        "SOL",
        "",
        "USDC",
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    ),
)


def market_record(chain: str, label: str) -> ResolvedGmxMarket:
    """Return the fixture record for ``(chain, label)`` — KeyError when absent."""
    for row_chain, record in FIXTURE_MARKETS:
        if row_chain == chain and record.label == label:
            return record
    raise KeyError(f"No fixture market {label!r} on {chain!r}")


def market_address(chain: str, label: str) -> str:
    """Return the fixture market-token address for ``(chain, label)``."""
    return market_record(chain, label).market_token


def prime_catalog(*records: ResolvedGmxMarket, chain: str | None = None) -> None:
    """Remember fixture rows in the process catalog.

    With no arguments every fixture row is remembered (both chains). Passing
    explicit ``records`` (with their ``chain``) primes just those.
    """
    if records:
        if chain is None:
            raise ValueError("chain= is required when passing explicit records")
        for record in records:
            market_catalog.remember(chain, record)
        return
    for row_chain, record in FIXTURE_MARKETS:
        market_catalog.remember(row_chain, record)


class _FixtureMarketNotFound(grpc.RpcError):
    """The venue's authoritative "no such market" answer (gRPC NOT_FOUND)."""

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.NOT_FOUND

    def details(self) -> str:
        return "no fixture record for the requested market"


def fake_dynamic_gateway(chain: str, *records: ResolvedGmxMarket, rpc_url: str | None = None) -> SimpleNamespace:
    """A minimal connected dynamic gateway serving fixture records.

    PERP_OPEN compiles are risk-increasing and demand CURRENT venue listing
    (``require_listed=True``): the process-catalog fallback is CLOSE-ONLY, so
    an open with no usable dynamic gateway fails closed by design. Production
    opens always run with a gateway; open-compile tests wire this stub as
    ``gateway_client`` to model that (same SimpleNamespace shape as
    ``test_dynamic_market_registry_vib6561``).

    ``GetPerpMarket`` answers by label or market-token address from ``records``
    (default: every fixture row for ``chain``) and raises gRPC NOT_FOUND for
    anything else — the venue's authoritative miss, mapped by
    ``resolve_market_via_gateway`` to ``GmxMarketNotFound``.
    """
    rows = list(records) if records else [record for row_chain, record in FIXTURE_MARKETS if row_chain == chain]
    by_key: dict[str, ResolvedGmxMarket] = {}
    for record in rows:
        by_key[record.label.lower()] = record
        by_key[record.market_token.lower()] = record

    def get_perp_market(request: object, timeout: float) -> gateway_pb2.PerpMarketResponse:
        del timeout
        record = by_key.get(request.market.lower())  # type: ignore[attr-defined]
        if record is None:
            raise _FixtureMarketNotFound()
        return gateway_pb2.PerpMarketResponse(
            success=True,
            market=gateway_pb2.PerpMarket(
                protocol="gmx_v2",
                chain=chain,
                label=record.label,
                market_token=record.market_token,
                index_token=record.index_token,
                index_symbol=record.index_symbol,
                index_token_decimals=record.index_token_decimals,
                long_token=record.long_token,
                long_token_symbol=record.long_token_symbol,
                long_token_decimals=record.long_token_decimals,
                short_token=record.short_token,
                short_token_symbol=record.short_token_symbol,
                short_token_decimals=record.short_token_decimals,
                verified=True,
            ),
        )

    def rpc_call(request, timeout=None):  # noqa: ANN001 - gRPC stub surface
        """Forward the gateway RpcRequest to a REAL node (the test's fork).

        ``_build_sdk`` adopts any connected gateway and drops the direct
        rpc_url, so a resolution-only fake would leave the SDK pricing keeper
        fees against nothing. With ``rpc_url`` set, chain reads stay REAL
        (the fork) while market resolution stays fixed — the faithful hybrid
        for 4-layer intent tests.
        """
        if rpc_url is None:
            raise AssertionError(
                "fake_dynamic_gateway received a chain read but was built without rpc_url — "
                "pass rpc_url=<fork url> for compile paths that reach the SDK"
            )
        import json as _json
        import urllib.request as _urllib_request

        payload = _json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request.id,
                "method": request.method,
                "params": _json.loads(request.params) if request.params else [],
            }
        ).encode()
        req = _urllib_request.Request(rpc_url, data=payload, headers={"Content-Type": "application/json"})
        with _urllib_request.urlopen(req, timeout=timeout or 10) as resp:
            body = _json.loads(resp.read())
        if "error" in body:
            return SimpleNamespace(success=False, result="", error=_json.dumps(body["error"]))
        return SimpleNamespace(success=True, result=_json.dumps(body.get("result")), error="")

    return SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5),
        market=SimpleNamespace(GetPerpMarket=get_perp_market),
        rpc=SimpleNamespace(Call=rpc_call),
    )


__all__ = [
    "FIXTURE_MARKETS",
    "fake_dynamic_gateway",
    "market_address",
    "market_record",
    "prime_catalog",
]
