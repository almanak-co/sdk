"""Unit tests for the Pendle teardown on-chain closure verifier (VIB-3808 / VIB-5487).

The hook (``pendle_teardown_post_condition``) re-reads the residual PT / LP-token
balance via the gateway so a still-funded Pendle holding fails the teardown
closed instead of being reported optimistically. These drive the REAL hook with
a hand-built fake gateway client and assert behaviour:

- LP residual non-zero -> ``closed=False`` + residual map.
- LP zero balance -> ``closed=True``.
- PT path: ``eth_call`` returns a ``readTokens()`` (SY, PT, YT) triple, then the
  PT ``balanceOf`` decides closure (non-zero -> not closed; zero -> closed).
- ``gateway_client=None`` -> fail-closed.
- Unknown ``kind`` -> fail-closed.
- A gateway that raises -> fail-closed (never propagates).
- A balance of ``None`` (gateway/RPC error) -> fail-closed.

Fail-closed means ``closed=False`` with an ``error`` string — an unverifiable
position must never read as closed.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from eth_abi import encode

from almanak.connectors.pendle.teardown_post_condition import pendle_teardown_post_condition
from almanak.framework.teardown.post_conditions import (
    get_teardown_post_condition,
    has_teardown_post_condition,
)

WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"
MARKET = "0x1234567890abcdef1234567890abcdef12345678"  # Pendle market == LP token
SY = "0x000000000000000000000000000000000000005a"
PT = "0x00000000000000000000000000000000000000ce"
YT = "0x00000000000000000000000000000000000000ff"


def _read_tokens_return(sy: str = SY, pt: str = PT, yt: str = YT) -> str:
    """ABI-encode a ``readTokens() -> (SY, PT, YT)`` eth_call return as a hex str."""
    return "0x" + encode(["address", "address", "address"], [sy, pt, yt]).hex()


class _FakeGateway:
    """Hand-built fake gateway client (behaviour, not a mock-of-mocks).

    ``balances`` maps lower-cased token address -> wei balance (or ``None`` to
    simulate a gateway/RPC failure). ``read_tokens`` is the hex return for the
    market's ``readTokens()`` eth_call (or ``None``). ``raise_on`` triggers an
    exception from the named method to exercise the never-raise contract.
    """

    def __init__(
        self,
        *,
        balances: dict[str, int | None] | None = None,
        read_tokens: str | None = None,
        raise_on: str | None = None,
    ) -> None:
        self._balances = {k.lower(): v for k, v in (balances or {}).items()}
        self._read_tokens = read_tokens
        self._raise_on = raise_on
        self.eth_call_calls: list[dict[str, Any]] = []
        self.balance_calls: list[dict[str, Any]] = []

    def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
        if self._raise_on == "eth_call":
            raise RuntimeError("simulated gateway eth_call failure")
        self.eth_call_calls.append({"chain": chain, "to": to, "data": data, "block": block})
        return self._read_tokens

    def query_erc20_balance(self, chain: str, token_address: str, wallet_address: str, block: Any = None) -> int | None:
        if self._raise_on == "query_erc20_balance":
            raise RuntimeError("simulated gateway balance failure")
        self.balance_calls.append(
            {"chain": chain, "token_address": token_address, "wallet_address": wallet_address, "block": block}
        )
        return self._balances.get(token_address.lower())


def _position(
    *,
    chain: str = "ethereum",
    protocol: str = "pendle",
    position_id: str = MARKET,
    details: dict | None = None,
    position_type: Any = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        protocol=protocol,
        position_id=position_id,
        chain=chain,
        details=details if details is not None else {},
        position_type=position_type,
    )


# --------------------------------------------------------------------------- #
# Registration
# --------------------------------------------------------------------------- #
def test_pendle_hook_is_registered() -> None:
    assert has_teardown_post_condition("pendle")
    assert get_teardown_post_condition("pendle") is pendle_teardown_post_condition


# --------------------------------------------------------------------------- #
# LP path: the market address IS the LP token
# --------------------------------------------------------------------------- #
def test_lp_zero_balance_is_closed() -> None:
    gw = _FakeGateway(balances={MARKET: 0})
    pos = _position(details={"kind": "lp", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw, block=123)
    assert result.closed is True
    assert result.error is None
    # LP residual is read against the market address itself, block-pinned.
    assert gw.balance_calls[0]["token_address"] == MARKET
    assert gw.balance_calls[0]["block"] == 123


def test_lp_nonzero_balance_is_not_closed_with_residual() -> None:
    gw = _FakeGateway(balances={MARKET: 4242})
    pos = _position(details={"kind": "lp", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is None
    assert result.residual["token"] == MARKET
    assert result.residual["balance"] == 4242
    assert result.residual["kind"] == "lp"


def test_lp_kind_inferred_from_position_type_when_details_kind_missing() -> None:
    gw = _FakeGateway(balances={MARKET: 0})
    pos = _position(details={"market_id": MARKET}, position_type=SimpleNamespace(value="LP"))
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is True


# --------------------------------------------------------------------------- #
# PT path: resolve PT from market.readTokens() then balanceOf(PT)
# --------------------------------------------------------------------------- #
def test_pt_zero_balance_is_closed() -> None:
    gw = _FakeGateway(balances={PT: 0}, read_tokens=_read_tokens_return())
    pos = _position(details={"kind": "pt", "market_id": MARKET, "asset_symbol": "PT-sUSDe"})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is True
    assert result.error is None
    # readTokens() is called on the market; the balance is read against PT (index 1).
    assert gw.eth_call_calls[0]["to"] == MARKET
    assert gw.eth_call_calls[0]["data"] == "0x2c8ce6bc"
    assert gw.balance_calls[0]["token_address"].lower() == PT.lower()


def test_pt_nonzero_balance_is_not_closed() -> None:
    gw = _FakeGateway(balances={PT: 99}, read_tokens=_read_tokens_return())
    pos = _position(details={"kind": "pt", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.residual["balance"] == 99
    assert result.residual["kind"] == "pt"
    assert result.residual["token"].lower() == PT.lower()


def test_pt_kind_inferred_from_position_type_token() -> None:
    gw = _FakeGateway(balances={PT: 0}, read_tokens=_read_tokens_return())
    pos = _position(details={"market_id": MARKET}, position_type=SimpleNamespace(value="TOKEN"))
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is True


def test_pt_readtokens_none_is_fail_closed() -> None:
    gw = _FakeGateway(balances={PT: 0}, read_tokens=None)
    pos = _position(details={"kind": "pt", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "readTokens" in result.error


def test_pt_readtokens_malformed_is_fail_closed() -> None:
    gw = _FakeGateway(balances={PT: 0}, read_tokens="0xdeadbeef")  # too short to decode 3 addresses
    pos = _position(details={"kind": "pt", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "undecodable" in result.error


def test_pt_zero_pt_address_is_fail_closed() -> None:
    zero = "0x0000000000000000000000000000000000000000"
    gw = _FakeGateway(balances={}, read_tokens=_read_tokens_return(pt=zero))
    pos = _position(details={"kind": "pt", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "zero PT address" in result.error


# --------------------------------------------------------------------------- #
# Fail-closed guards
# --------------------------------------------------------------------------- #
def test_no_gateway_client_is_fail_closed() -> None:
    pos = _position(details={"kind": "lp", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=None)
    assert result.closed is False
    assert result.error is not None and "gateway_client" in result.error


def test_missing_chain_is_fail_closed() -> None:
    gw = _FakeGateway(balances={MARKET: 0})
    pos = _position(chain="", details={"kind": "lp", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "chain" in result.error


def test_unknown_kind_is_fail_closed() -> None:
    gw = _FakeGateway(balances={MARKET: 0})
    pos = _position(details={"kind": "yt", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "yt" in result.error


def test_gateway_balance_raises_is_fail_closed() -> None:
    gw = _FakeGateway(balances={MARKET: 0}, raise_on="query_erc20_balance")
    pos = _position(details={"kind": "lp", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "raised" in result.error


def test_gateway_eth_call_raises_is_fail_closed() -> None:
    gw = _FakeGateway(balances={PT: 0}, read_tokens=_read_tokens_return(), raise_on="eth_call")
    pos = _position(details={"kind": "pt", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "raised" in result.error


def test_balance_none_is_fail_closed() -> None:
    gw = _FakeGateway(balances={MARKET: None})  # gateway returns None -> RPC error
    pos = _position(details={"kind": "lp", "market_id": MARKET})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "None" in result.error


def test_missing_market_address_is_fail_closed() -> None:
    gw = _FakeGateway(balances={})
    pos = _position(position_id="", details={"kind": "lp"})
    result = pendle_teardown_post_condition(pos, WALLET, gateway_client=gw)
    assert result.closed is False
    assert result.error is not None and "market address" in result.error


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
