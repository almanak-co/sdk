"""Unit tests for ``almanak ax perp-market`` — live perp-market resolution (ALM-3179).

Exercises the exit-code contract (0 found / 1 not_found / 2 invalid /
4 unavailable-or-unverified) and the epistemic distinction the command
exists to enforce: an authoritative venue NOT_FOUND is different from
"could not verify", and neither is derivable from static catalogues.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest
from click.testing import CliRunner

from almanak.framework.cli.ax import ax as ax_cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _verified_market_response() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        error="",
        market=SimpleNamespace(
            protocol="gmx_v2",
            chain="arbitrum",
            label="XMR/USD",
            market_token="0x92dD7CB239a02aA7462AEeC91b1a20Ba0A08e5F5",
            index_token="0x0d81d5C0Eaea9Dd7f37a02c8D64D9EAF32BABad8",
            index_symbol="XMR",
            index_token_decimals=12,
            long_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            long_token_symbol="WETH",
            long_token_decimals=18,
            short_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            short_token_symbol="USDC",
            short_token_decimals=6,
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


def _patch_stub(monkeypatch: pytest.MonkeyPatch, get_perp_market) -> MagicMock:
    """Route the command's channel + stub construction through fakes."""
    channel = MagicMock(name="channel")
    monkeypatch.setattr(
        "almanak.framework.cli.ax._acquire_gateway_channel",
        lambda ctx: (channel, None),
    )
    stub = SimpleNamespace(GetPerpMarket=get_perp_market)
    monkeypatch.setattr(
        "almanak.gateway.proto.gateway_pb2_grpc.MarketServiceStub",
        lambda ch: stub,
    )
    return channel


class TestAxPerpMarketFound:
    def test_found_json_payload(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        channel = _patch_stub(monkeypatch, lambda request, timeout: _verified_market_response())
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "found"
        assert payload["label"] == "XMR/USD"
        assert payload["index_symbol"] == "XMR"
        assert payload["index_token_decimals"] == 12
        assert payload["long_token_decimals"] == 18
        assert payload["short_token_decimals"] == 6
        assert payload["verified"] is True
        channel.close.assert_called_once()

    def test_found_human_output(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stub(monkeypatch, lambda request, timeout: _verified_market_response())
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 0, result.output
        assert "XMR/USD on gmx_v2 (arbitrum)" in result.output
        assert "verified on-chain" in result.output
        assert "market_token" in result.output

    def test_unreported_decimals_are_not_rendered_as_zero(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        response = _verified_market_response()
        response.market.HasField = lambda field: field not in {
            "index_token_decimals",
            "long_token_decimals",
            "short_token_decimals",
        }
        _patch_stub(monkeypatch, lambda request, timeout: response)

        json_result = runner.invoke(
            ax_cli,
            ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"],
        )
        assert json_result.exit_code == 0, json_result.output
        payload = json.loads(json_result.output)
        assert payload["index_token_decimals"] is None
        assert payload["long_token_decimals"] is None
        assert payload["short_token_decimals"] is None

        human_result = runner.invoke(ax_cli, ["-c", "arbitrum", "perp-market", "gmx_v2", "XMR/USD"])
        assert human_result.exit_code == 0, human_result.output
        assert human_result.output.count("decimals unreported") == 3

    def test_request_requires_current_listing(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict = {}

        def capture(request, timeout):
            seen["protocol"] = request.protocol
            seen["chain"] = request.chain
            seen["market"] = request.market
            seen["require_listed"] = request.require_listed
            return _verified_market_response()

        _patch_stub(monkeypatch, capture)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 0, result.output
        assert seen == {
            "protocol": "gmx_v2",
            "chain": "arbitrum",
            "market": "XMR/USD",
            "require_listed": True,
        }


class TestAxPerpMarketNotFound:
    def test_venue_not_found_exits_one(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_not_found(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.NOT_FOUND, "market 'ZEC/USD' does not exist on gmx_v2/arbitrum")

        channel = _patch_stub(monkeypatch, raise_not_found)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "ZEC/USD"])
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_found"
        assert "does not exist" in payload["error"]
        # An authoritative venue answer needs no inconclusive hint.
        assert "hint" not in payload
        channel.close.assert_called_once()


class TestAxPerpMarketInvalid:
    def test_unsupported_protocol_exits_two(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_invalid(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, "unsupported protocol: 'nope'")

        _patch_stub(monkeypatch, raise_invalid)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "nope", "BTC/USD"])
        assert result.exit_code == 2, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "invalid"


class TestAxPerpMarketInconclusive:
    """Exit code 4 — the answer is unknown, never 'not listed'."""

    def test_gateway_unreachable_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.framework.cli.ax._acquire_gateway_channel",
            lambda ctx: (None, "no gateway running on localhost:50051 and auto-start failed: boom"),
        )
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "could not verify" in payload["hint"]

    def test_rpc_unavailable_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_unavailable(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "catalogue unavailable")

        channel = _patch_stub(monkeypatch, raise_unavailable)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "hint" in payload
        channel.close.assert_called_once()

    def test_failed_precondition_maps_to_unverified(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_unverified(request, timeout):
            raise _FakeRpcError(
                grpc.StatusCode.FAILED_PRECONDITION,
                "market 'XMR/USD' was returned without on-chain verification",
            )

        _patch_stub(monkeypatch, raise_unverified)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unverified"

    def test_ok_but_unverified_record_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        response = _verified_market_response()
        response.market.verified = False

        channel = _patch_stub(monkeypatch, lambda request, timeout: response)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unverified"
        channel.close.assert_called_once()

    def test_unexpected_exception_exits_four_not_one(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """A local CLI fault must never surface as exit 1 (venue not-found)."""

        def raise_runtime(request, timeout):
            raise RuntimeError("proto field access exploded")

        channel = _patch_stub(monkeypatch, raise_runtime)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "unexpected CLI failure" in payload["error"]
        channel.close.assert_called_once()

    def test_config_load_failure_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """A malformed local config must degrade through the real helper, not escape."""

        def broken_load_config():
            raise RuntimeError("conflicting configuration: GATEWAY_AUTH_TOKEN")

        monkeypatch.setattr("almanak.config.load_config", broken_load_config)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "failed to load gateway configuration" in payload["error"]

    def test_acquisition_raising_exits_four_not_one(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """Even a helper that violates its (None, note) contract must not exit 1."""

        def raise_acquisition(ctx):
            raise RuntimeError("acquisition exploded")

        monkeypatch.setattr("almanak.framework.cli.ax._acquire_gateway_channel", raise_acquisition)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "unexpected CLI failure" in payload["error"]

    def test_stub_construction_failure_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        channel = MagicMock(name="channel")
        monkeypatch.setattr(
            "almanak.framework.cli.ax._acquire_gateway_channel",
            lambda ctx: (channel, None),
        )

        def broken_stub(ch):
            raise TypeError("channel does not support unary_unary")

        monkeypatch.setattr("almanak.gateway.proto.gateway_pb2_grpc.MarketServiceStub", broken_stub)
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "perp-market", "gmx_v2", "XMR/USD"])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        channel.close.assert_called_once()
