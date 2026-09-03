"""Unit tests for ``almanak ax lending-market`` — live lending-market
verification (ALM-3474 / VIB-5985).

Exercises the exit-code contract (0 found / 1 not_found / 2 invalid /
4 unavailable-or-unverified) and the epistemic distinction the command exists
to enforce: an unverified curated-catalog CANDIDATE (`ax lending-reserves`) is
never the same thing as an on-chain VERIFIED market — this command is the
only supported promotion path from one to the other (ALM-3474's headline gap:
`GetLendingMarket` existed as a gateway RPC with no CLI/planner entrypoint).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest
from click.testing import CliRunner

from almanak.framework.cli.ax import ax as ax_cli

_MARKET_ID = "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _verified_market_response() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        error="",
        market=SimpleNamespace(
            kind=2,  # LENDING_MARKET_KIND_ISOLATED_PAIR
            protocol="morpho_blue",
            chain="base",
            market_id=_MARKET_ID,
            collateral_token="0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452",
            collateral_symbol="wstETH",
            loan_token="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            loan_symbol="USDC",
            lltv_bps=8600,
            oracle="0xoracle",
            irm="0xirm",
            verified=True,
        ),
    )


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def _patch_stub(monkeypatch: pytest.MonkeyPatch, get_lending_market) -> MagicMock:
    """Route the command's channel + stub construction through fakes."""
    channel = MagicMock(name="channel")
    monkeypatch.setattr(
        "almanak.framework.cli.ax._acquire_gateway_channel",
        lambda ctx: (channel, None),
    )
    stub = SimpleNamespace(GetLendingMarket=get_lending_market)
    monkeypatch.setattr(
        "almanak.gateway.proto.gateway_pb2_grpc.MarketServiceStub",
        lambda ch: stub,
    )
    return channel


def _invoke(runner: CliRunner, *extra_args: str) -> object:
    return runner.invoke(
        ax_cli,
        ["-c", "base", *extra_args, "lending-market", "--protocol", "morpho_blue", "--market-id", _MARKET_ID],
    )


class TestAxLendingMarketFound:
    def test_found_json_payload(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        channel = _patch_stub(monkeypatch, lambda request, timeout: _verified_market_response())
        result = _invoke(runner, "--json")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "found"
        assert payload["protocol"] == "morpho_blue"
        assert payload["chain"] == "base"
        assert payload["kind"] == "isolated_pair"
        assert payload["market_id"] == _MARKET_ID
        assert payload["collateral_symbol"] == "wstETH"
        assert payload["loan_symbol"] == "USDC"
        assert payload["lltv_bps"] == 8600
        assert payload["verified"] is True
        channel.close.assert_called_once()

    def test_found_human_output(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stub(monkeypatch, lambda request, timeout: _verified_market_response())
        result = _invoke(runner)
        assert result.exit_code == 0, result.output
        assert f"{_MARKET_ID} on morpho_blue (base)" in result.output
        assert "verified on-chain" in result.output
        assert "wstETH" in result.output
        assert "USDC" in result.output
        assert "86.00%" in result.output  # lltv_bps=8600 -> 86.00%

    def test_request_threads_exact_fields(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict = {}

        def capture(request, timeout):
            seen["protocol"] = request.protocol
            seen["chain"] = request.chain
            seen["market_id"] = request.market_id
            return _verified_market_response()

        _patch_stub(monkeypatch, capture)
        result = _invoke(runner, "--json")
        assert result.exit_code == 0, result.output
        assert seen == {"protocol": "morpho_blue", "chain": "base", "market_id": _MARKET_ID}


class TestAxLendingMarketNotFound:
    def test_market_not_found_exits_one(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_not_found(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.NOT_FOUND, f"market {_MARKET_ID} does not exist on morpho_blue/base")

        channel = _patch_stub(monkeypatch, raise_not_found)
        result = _invoke(runner, "--json")
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_found"
        assert "does not exist" in payload["error"]
        assert "hint" not in payload  # an authoritative answer needs no inconclusive hint
        channel.close.assert_called_once()


class TestAxLendingMarketInvalid:
    def test_recompute_mismatch_exits_two(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """A market_id that does not hash to its own on-chain params must
        never be promotable — this is the exact defect class ALM-3474
        exists to prevent (a candidate promoted into config unverified)."""

        def raise_mismatch(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, "recomputed market id does not match requested id")

        _patch_stub(monkeypatch, raise_mismatch)
        result = _invoke(runner, "--json")
        assert result.exit_code == 2, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "invalid"
        assert "recomputed" in payload["error"]

    def test_unsupported_protocol_exits_two(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_invalid(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, "unsupported protocol: 'nope'")

        _patch_stub(monkeypatch, raise_invalid)
        result = _invoke(runner, "--json")
        assert result.exit_code == 2, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "invalid"


class TestAxLendingMarketInconclusive:
    """Exit code 4 — the answer is unknown, never 'does not exist'."""

    def test_gateway_unreachable_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.framework.cli.ax._acquire_gateway_channel",
            lambda ctx: (None, "no gateway running on localhost:50051 and auto-start failed: boom"),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "could not verify" in payload["hint"]

    def test_rpc_unavailable_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_unavailable(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "lending-market verification unavailable")

        channel = _patch_stub(monkeypatch, raise_unavailable)
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "hint" in payload
        channel.close.assert_called_once()

    def test_ok_but_unverified_record_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        response = _verified_market_response()
        response.market.verified = False

        channel = _patch_stub(monkeypatch, lambda request, timeout: response)
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unverified"
        channel.close.assert_called_once()

    def test_unexpected_exception_exits_four_not_one(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """A local CLI fault must never surface as exit 1 (authoritative not-found)."""

        def raise_runtime(request, timeout):
            raise RuntimeError("proto field access exploded")

        channel = _patch_stub(monkeypatch, raise_runtime)
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "unexpected CLI failure" in payload["error"]
        channel.close.assert_called_once()

    def test_stub_construction_failure_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        channel = MagicMock(name="channel")
        monkeypatch.setattr(
            "almanak.framework.cli.ax._acquire_gateway_channel",
            lambda ctx: (channel, None),
        )

        def broken_stub(ch):
            raise TypeError("channel does not support unary_unary")

        monkeypatch.setattr("almanak.gateway.proto.gateway_pb2_grpc.MarketServiceStub", broken_stub)
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        channel.close.assert_called_once()
