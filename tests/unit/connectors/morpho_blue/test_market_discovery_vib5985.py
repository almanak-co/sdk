"""Morpho Blue verified market-discovery capability (VIB-5985).

Connector-level unit coverage of the pure resolution + verification logic:

* the recompute helper reproduces EVERY curated catalog id (keccak integrity),
* offline listing + address-resolved filters,
* verify-PASS returns a verified record, verify-FAIL raises (never a silent
  record), not-found returns None,
* the connector structurally satisfies the capability Protocol.
"""

from __future__ import annotations

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayLendingMarketDiscoveryCapability,
    LendingMarketVerificationError,
)
from almanak.connectors.morpho_blue.addresses import MORPHO_MARKETS
from almanak.connectors.morpho_blue.gateway.market_discovery import (
    list_morpho_markets,
    recompute_morpho_market_id,
    verify_morpho_market,
)
from almanak.connectors.morpho_blue.gateway.provider import MorphoBlueGatewayConnector

_SUSDE_USDC_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"


def _word_addr(addr: str) -> str:
    return addr.lower().replace("0x", "").zfill(64)


def _word_uint(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _payload(loan: str, collateral: str, oracle: str, irm: str, lltv: int) -> str:
    return "0x" + _word_addr(loan) + _word_addr(collateral) + _word_addr(oracle) + _word_addr(irm) + _word_uint(lltv)


def _make_eth_call(payload: str):
    async def _call(to: str, data: str) -> str:
        return payload

    return _call


def test_connector_satisfies_capability_protocol():
    conn = MorphoBlueGatewayConnector()
    assert isinstance(conn, GatewayLendingMarketDiscoveryCapability)
    assert "ethereum" in conn.lending_market_discovery_chains()


# Known-stale catalog entries whose stored id AND recomputed-from-params id both
# return all-zeros from ``idToMarketParams`` on-chain (verified 2026-07-25) — i.e.
# the market simply does not exist at either id. Excluded from the strict
# self-consistency guard below and tracked for a separate data-cleanup. The
# VIB-5985 verification layer already neutralises them: ``GetLendingMarket`` on
# such an id returns NOT_FOUND (never a fabricated record), which is exactly the
# candidate-vs-verified separation this feature exists to enforce.
_KNOWN_STALE_CATALOG_IDS = frozenset(
    {
        # PT-eUSDe/USDe — neither the stored id nor keccak(params) is a live market.
        "0xe7a06721ca6dce24fce8c5a57d7bb39688dc0f5700e86be29d1f488acab63876",
    }
)


@pytest.mark.parametrize("chain", sorted(MORPHO_MARKETS.keys()))
def test_recompute_reproduces_every_catalog_id(chain: str):
    """The recompute helper is the verification primitive — it MUST reproduce
    every curated id (catalog self-consistency), or on-chain verification would
    reject real markets. Doubles as a regression guard on the catalog itself:
    this test caught (and the fix corrected) the weETH/WETH LLTV that was stored
    as 90% but is 86% on-chain (VIB-5985). Known-stale ids are excluded."""
    for market_id, info in MORPHO_MARKETS[chain].items():
        if market_id.lower() in _KNOWN_STALE_CATALOG_IDS:
            continue
        computed = recompute_morpho_market_id(
            loan_token=info["loan_token_address"],
            collateral_token=info["collateral_token_address"],
            oracle=info["oracle"],
            irm=info["irm"],
            lltv=int(info["lltv"]),
        )
        assert computed.lower() == market_id.lower(), f"{chain}:{info.get('name')}"


def test_list_filters_by_resolved_addresses():
    rows = list_morpho_markets(chain="ethereum", collateral_token="sUSDe", loan_token="USDC")
    ids = {r.market_id for r in rows}
    assert _SUSDE_USDC_ID in ids
    assert all(r.verified is False and r.source == "curated_catalog" for r in rows)
    assert all(r.kind == "isolated_pair" for r in rows)
    # Address form resolves identically to the symbol form.
    rows_addr = list_morpho_markets(
        chain="ethereum", collateral_token="0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
    )
    assert _SUSDE_USDC_ID in {r.market_id for r in rows_addr}


def test_list_unknown_symbol_filter_raises():
    with pytest.raises(ValueError, match="cannot resolve token filter"):
        list_morpho_markets(chain="ethereum", loan_token="NOTATOKEN")


def test_list_lltv_bps_filter():
    rows = list_morpho_markets(chain="ethereum", collateral_token="sUSDe", lltv_bps=9150)
    assert rows
    assert all(r.lltv_bps == 9150 for r in rows)


@pytest.mark.asyncio
async def test_verify_pass_returns_verified_record():
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    payload = _payload(
        info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
    )
    record = await verify_morpho_market(
        chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=_make_eth_call(payload)
    )
    assert record is not None
    assert record.verified is True
    assert record.source == "onchain_verify"
    assert record.market_id == _SUSDE_USDC_ID
    assert record.lltv_bps == 9150
    assert record.loan_symbol == "USDC"
    assert record.collateral_symbol == "sUSDe"


@pytest.mark.asyncio
async def test_verify_fail_on_mismatch_raises():
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    # Mutated LLTV → recomputed id will not match the requested id.
    payload = _payload(
        info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], 42
    )
    with pytest.raises(LendingMarketVerificationError, match="verification failed"):
        await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=_make_eth_call(payload))


@pytest.mark.asyncio
async def test_verify_not_found_returns_none():
    payload = "0x" + "0" * 320  # zero loan token = market never created
    record = await verify_morpho_market(
        chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=_make_eth_call(payload)
    )
    assert record is None


@pytest.mark.asyncio
async def test_verify_short_payload_raises():
    with pytest.raises(LendingMarketVerificationError, match="short payload"):
        await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=_make_eth_call("0x1234"))
