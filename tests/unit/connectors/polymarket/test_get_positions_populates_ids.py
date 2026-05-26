"""Acceptance test for SDK-P2-02: ClobClient.get_positions must populate
``market_id`` and ``token_id`` even when the Polymarket Data API returns
them empty.

The Data API ``/positions`` endpoint inconsistently populates ``market``
and ``tokenId``. PM/dashboard can't reconcile positions to strategies,
call ``get_market`` by id, or compute per-market PnL without those fields.
:meth:`ClobClient.get_positions` backfills them from ``conditionId`` via
Gamma's ``/markets?condition_ids=<cid>`` endpoint.
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock

import httpx
import pytest
from eth_account import Account
from pydantic import SecretStr

from almanak.connectors.polymarket import (
    ApiCredentials,
    ClobClient,
    PolymarketConfig,
    SignatureType,
)

# Known YES/NO CLOB token ids from Gamma for this fixture's conditionId.
YES_TOKEN = "111111111111111111111111111111111111111111111111111111111111111111"
NO_TOKEN = "222222222222222222222222222222222222222222222222222222222222222222"


@pytest.fixture
def config_with_credentials() -> PolymarketConfig:
    account = Account.from_key("0x" + "11" * 32)
    secret = base64.b64encode(b"test_secret_key_123").decode()
    return PolymarketConfig(
        wallet_address=account.address,
        signature_type=SignatureType.EOA,
        api_credentials=ApiCredentials(
            api_key="test_api_key",
            secret=SecretStr(secret),
            passphrase=SecretStr("test_passphrase"),
        ),
    )


def _make_mock_http(data_api_positions: list[dict], gamma_markets_by_cid: dict[str, list[dict]]) -> MagicMock:
    """Route ``_request`` calls by URL: Data API → positions payload,
    Gamma ``/markets`` → market lookup by condition_ids filter."""

    def _respond(*, method: str, url: str, params: dict | None = None, **_: object) -> MagicMock:
        response = MagicMock()
        response.status_code = 200
        if "data-api.polymarket.com" in url and url.endswith("/positions"):
            body = data_api_positions
        elif "gamma-api.polymarket.com" in url and url.endswith("/markets"):
            cids = (params or {}).get("condition_ids", "")
            body = gamma_markets_by_cid.get(cids, [])
        else:
            body = []
        response.json.return_value = body
        response.content = b"x" if body else b"[]"
        return response

    mock_http = MagicMock(spec=httpx.Client)
    mock_http.request.side_effect = _respond
    return mock_http


def _gamma_market(mid: str, condition_id: str, yes_tok: str, no_tok: str, question: str) -> dict:
    import json

    return {
        "id": mid,
        "conditionId": condition_id,
        "question": question,
        "slug": f"slug-{mid}",
        "outcomes": json.dumps(["Yes", "No"]),
        "outcomePrices": json.dumps(["0.5", "0.5"]),
        "clobTokenIds": json.dumps([yes_tok, no_tok]),
        "volume": "0",
        "liquidity": "0",
        "active": True,
        "closed": False,
        "enableOrderBook": True,
        "orderPriceMinTickSize": "0.01",
        "orderMinSize": "5",
    }


def test_get_positions_backfills_market_and_token_ids(config_with_credentials):
    """Every returned Position has non-empty market_id/token_id; token_id
    matches the side (YES -> yes_token, NO -> no_token)."""
    cid_yes = "0xe6caabcdef0000000000000000000000000000000000000000000000000000aa"
    cid_no = "0xe6caabcdef0000000000000000000000000000000000000000000000000000bb"

    data_api_positions = [
        {  # YES position with empty market/tokenId — must be backfilled
            "market": "",
            "conditionId": cid_yes,
            "tokenId": "",
            "outcome": "YES",
            "size": "50.57",
            "avgPrice": "0.40",
            "currentPrice": "0.45",
            "realizedPnl": "0",
        },
        {  # NO position where only tokenId is empty
            "market": "market-pre-filled",
            "conditionId": cid_no,
            "tokenId": "",
            "outcome": "NO",
            "size": "10",
            "avgPrice": "0.60",
            "currentPrice": "0.55",
            "realizedPnl": "0",
        },
    ]
    gamma_markets_by_cid = {
        cid_yes: [_gamma_market("mkt-yes", cid_yes, YES_TOKEN, NO_TOKEN, "Will X happen?")],
        cid_no: [_gamma_market("mkt-no", cid_no, "yes-alt", "no-alt", "Will Y happen?")],
    }

    client = ClobClient(
        config_with_credentials,
        http_client=_make_mock_http(data_api_positions, gamma_markets_by_cid),
    )
    positions = client.get_positions()

    assert len(positions) == 2

    yes_pos = next(p for p in positions if p.outcome == "YES")
    assert yes_pos.market_id == "mkt-yes"
    assert yes_pos.token_id == YES_TOKEN
    assert yes_pos.condition_id == cid_yes
    assert yes_pos.market_question == "Will X happen?"

    no_pos = next(p for p in positions if p.outcome == "NO")
    # Pre-filled market id is preserved; only missing token_id is resolved.
    assert no_pos.market_id == "market-pre-filled"
    assert no_pos.token_id == "no-alt"


def test_get_positions_caches_gamma_lookups_per_session(config_with_credentials):
    """N positions against distinct condition_ids -> at most N Gamma calls,
    and repeat calls within the cache TTL add zero."""
    cid = "0xe6caabcdef0000000000000000000000000000000000000000000000000000cc"
    positions_payload = [
        {
            "market": "",
            "conditionId": cid,
            "tokenId": "",
            "outcome": "YES",
            "size": "1",
            "avgPrice": "0.5",
            "currentPrice": "0.5",
            "realizedPnl": "0",
        },
        {  # Same conditionId — second call must hit the cache, not Gamma.
            "market": "",
            "conditionId": cid,
            "tokenId": "",
            "outcome": "NO",
            "size": "2",
            "avgPrice": "0.5",
            "currentPrice": "0.5",
            "realizedPnl": "0",
        },
    ]
    gamma_by_cid = {cid: [_gamma_market("mkt-1", cid, YES_TOKEN, NO_TOKEN, "Q?")]}

    mock_http = _make_mock_http(positions_payload, gamma_by_cid)
    client = ClobClient(config_with_credentials, http_client=mock_http)

    # First call: 1 Data API + 1 Gamma (cid seen twice but cached after first hit).
    client.get_positions()
    # Second call: 1 Data API + 0 Gamma (all cids still cached).
    client.get_positions()

    urls = [call.kwargs["url"] for call in mock_http.request.call_args_list]
    gamma_hits = [u for u in urls if "gamma-api.polymarket.com" in u]
    data_api_hits = [u for u in urls if "data-api.polymarket.com" in u]
    assert len(data_api_hits) == 2
    assert len(gamma_hits) == 1, f"expected 1 Gamma lookup, got {len(gamma_hits)}: {gamma_hits}"


def test_get_positions_keeps_position_when_market_not_found(config_with_credentials):
    """Closed/delisted markets: leave ids empty but keep the position — PnL
    is still real."""
    cid = "0xe6caabcdef0000000000000000000000000000000000000000000000000000dd"
    positions_payload = [
        {
            "market": "",
            "conditionId": cid,
            "tokenId": "",
            "outcome": "YES",
            "size": "7",
            "avgPrice": "0.2",
            "currentPrice": "0.1",
            "realizedPnl": "0",
        },
    ]

    mock_http = _make_mock_http(positions_payload, {})  # Gamma returns []
    client = ClobClient(config_with_credentials, http_client=mock_http)

    positions = client.get_positions()
    assert len(positions) == 1
    assert positions[0].condition_id == cid
    assert positions[0].market_id == ""
    assert positions[0].token_id == ""
    # Unrealized PnL still computed: (0.1 - 0.2) * 7 = -0.7
    from decimal import Decimal

    assert positions[0].unrealized_pnl == Decimal("-0.7")
