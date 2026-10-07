"""Aster Pro agents: two stable agents per wallet, reused across restarts and renewed before expiry.

Aster caps agents per account; a gateway that registered a fresh agent on every
start locked a live account out of withdrawals ("Agent quantity over limit").
"""

from __future__ import annotations

import time
import urllib.parse
from typing import Any

import pytest
from eth_account import Account

from almanak.connectors.aster_perps.gateway.api_client import (
    AGENT_RENEW_MARGIN_SECONDS,
    AsterApiError,
    AsterProApiClient,
    derive_agent,
)

MAIN = Account.create()
IP = "203.0.113.7"


class _Venue:
    """Aster's agent table for one wallet, behind the client's ``_send``."""

    def __init__(self, agents: dict[str, dict[str, Any]] | None = None) -> None:
        self.agents = agents or {}
        self.calls: list[tuple[str, str]] = []

    async def send(self, method: str, path: str, query: str, *, body: bool) -> Any:
        params = dict(urllib.parse.parse_qsl(query))
        self.calls.append((method, path))
        if path == "/fapi/v3/agent" and method == "GET":
            if params["signer"] not in self.agents:
                raise AsterApiError("No agent found", code=-1000)
            return list(self.agents.values())
        if path == "/fapi/v3/agent" and method == "DELETE":
            if self.agents.pop(params["agentAddress"], None) is None:
                raise AsterApiError("agent not found", code=-1000)
            return {"code": 200}
        if path == "/fapi/v3/registerAndApproveAgent":
            self.agents[params["agentAddress"]] = {
                "agentAddress": params["agentAddress"],
                "expired": int(params["expired"]),
                "ipWhitelist": params.get("ipWhitelist", ""),
                "canSpotTrade": params["canSpotTrade"] == "true",
                "canPerpTrade": params["canPerpTrade"] == "true",
                "canWithdraw": params["canWithdraw"] == "true",
            }
            return {"code": 200}
        return []

    def count(self, method: str, path: str) -> int:
        return self.calls.count((method, path))


def _client(venue: _Venue, whitelist: str = IP) -> AsterProApiClient:
    client = AsterProApiClient(MAIN, withdraw_ip_whitelist=whitelist)
    client._send = venue.send  # type: ignore[method-assign]
    return client


def _listed(role: str, *, days: float, perp: bool, withdraw: bool, whitelist: str = "") -> dict[str, Any]:
    address = derive_agent(MAIN, role).address
    return {
        address: {
            "agentAddress": address,
            "expired": int((time.time() + days * 86400) * 1000),
            "ipWhitelist": whitelist,
            "canSpotTrade": False,
            "canPerpTrade": perp,
            "canWithdraw": withdraw,
        }
    }


@pytest.mark.asyncio
async def test_restarts_reuse_one_agent_instead_of_registering_more() -> None:
    venue = _Venue()
    for _ in range(5):
        await _client(venue)._ensure_agent()
    assert list(venue.agents) == [derive_agent(MAIN, "perp").address]
    assert venue.count("POST", "/fapi/v3/registerAndApproveAgent") == 1


@pytest.mark.asyncio
async def test_a_valid_registration_is_reused_without_any_main_wallet_action() -> None:
    venue = _Venue(_listed("perp", days=20, perp=True, withdraw=False))
    await _client(venue)._ensure_agent()
    assert venue.count("DELETE", "/fapi/v3/agent") == 0
    assert venue.count("POST", "/fapi/v3/registerAndApproveAgent") == 0


@pytest.mark.asyncio
async def test_an_agent_close_to_expiry_is_replaced() -> None:
    venue = _Venue(_listed("perp", days=2, perp=True, withdraw=False))
    await _client(venue)._ensure_agent()
    assert venue.count("DELETE", "/fapi/v3/agent") == 1
    [agent] = venue.agents.values()
    assert agent["expired"] / 1000 - time.time() > AGENT_RENEW_MARGIN_SECONDS


@pytest.mark.asyncio
async def test_a_running_gateway_renews_its_agent_before_it_expires() -> None:
    venue = _Venue()
    client = _client(venue)
    first = await client._ensure_agent()
    venue.agents[first.address]["expired"] = int((time.time() + 3600) * 1000)
    client._agent_expires_at = time.time() + 3600
    assert await client._ensure_agent() == first
    assert venue.count("POST", "/fapi/v3/registerAndApproveAgent") == 2


@pytest.mark.asyncio
async def test_withdraw_agent_is_re_registered_when_the_egress_ip_changed() -> None:
    venue = _Venue(
        {
            **_listed("perp", days=20, perp=True, withdraw=False),
            **_listed("withdraw", days=20, perp=False, withdraw=True, whitelist="198.51.100.1"),
        }
    )
    await _client(venue)._ensure_withdraw_agent()
    withdraw = venue.agents[derive_agent(MAIN, "withdraw").address]
    assert withdraw["ipWhitelist"] == IP and withdraw["canWithdraw"] and not withdraw["canPerpTrade"]
    assert len(venue.agents) == 2


@pytest.mark.asyncio
async def test_a_trading_scoped_agent_is_never_reused_as_the_withdraw_agent() -> None:
    venue = _Venue(
        {
            **_listed("perp", days=20, perp=True, withdraw=False),
            **_listed("withdraw", days=20, perp=True, withdraw=True, whitelist=IP),
        }
    )
    await _client(venue)._ensure_withdraw_agent()
    assert not venue.agents[derive_agent(MAIN, "withdraw").address]["canPerpTrade"]


def test_agent_keys_are_per_wallet_and_per_role() -> None:
    other = Account.create()
    assert derive_agent(MAIN, "perp").address == derive_agent(MAIN, "perp").address
    assert derive_agent(MAIN, "perp").address != derive_agent(MAIN, "withdraw").address
    assert derive_agent(MAIN, "perp").address != derive_agent(other, "perp").address
    assert derive_agent(MAIN, "perp").key != MAIN.key
