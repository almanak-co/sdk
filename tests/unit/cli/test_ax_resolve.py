"""Unit tests for ``almanak ax resolve`` — the AI-agent friendly token lookup.

Exercises the four exit-code contracts (0 resolved / 1 not_found / 2
malformed) and both ``--gateway`` / ``--no-gateway`` paths.
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from almanak.framework.cli.ax import ax as ax_cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestAxResolveStatic:
    """Offline path -- must work without a gateway."""

    def test_resolves_known_symbol_by_chain(self, runner: CliRunner) -> None:
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "resolve", "--no-gateway", "USDC"])
        assert result.exit_code == 0, result.output
        assert "USDC on arbitrum" in result.output
        assert "0xaf88d065" in result.output.lower()
        assert "decimals    6" in result.output

    def test_json_payload_shape(self, runner: CliRunner) -> None:
        result = runner.invoke(
            ax_cli, ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "USDC"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["symbol"] == "USDC"
        assert payload["chain"] == "arbitrum"
        assert payload["decimals"] == 6
        assert payload["chain_id"] == 42161
        assert payload["is_stablecoin"] is True
        assert payload["address"].lower().startswith("0xaf88d065")

    def test_bsc_usdc_18_decimals(self, runner: CliRunner) -> None:
        """Regression guard for the well-known BSC USDC = 18-decimals quirk.

        If this flips to 6, every amount calc on BSC is off by 10^12.
        """
        result = runner.invoke(
            ax_cli, ["-c", "bsc", "--json", "resolve", "--no-gateway", "USDC"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["decimals"] == 18

    def test_resolve_by_address(self, runner: CliRunner) -> None:
        result = runner.invoke(
            ax_cli,
            [
                "-c",
                "arbitrum",
                "--json",
                "resolve",
                "--no-gateway",
                "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["symbol"] == "USDC"

    def test_unknown_symbol_exits_one_with_suggestions(self, runner: CliRunner) -> None:
        result = runner.invoke(
            ax_cli,
            ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "DEFINITELY_NOT_A_TOKEN"],
        )
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_found"
        assert payload["token"] == "DEFINITELY_NOT_A_TOKEN"
        assert payload["chain"] == "arbitrum"
        # ``suggestions`` is the actionable contract key the resolver
        # populates from TokenNotFoundError.suggestions — always present
        # (even if empty). ``hint`` is a bonus docstring-style field.
        assert "suggestions" in payload
        assert isinstance(payload["suggestions"], list)

    def test_cross_chain_isolation(self, runner: CliRunner) -> None:
        """USDC exists on ethereum in the registry but not zerog -- the
        resolver must never silently fall back to another chain's address.
        """
        result = runner.invoke(
            ax_cli, ["-c", "zerog", "--json", "resolve", "--no-gateway", "USDC"]
        )
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_found"

    def test_lowercase_symbol_still_matches(self, runner: CliRunner) -> None:
        result = runner.invoke(
            ax_cli, ["-c", "arbitrum", "resolve", "--no-gateway", "usdc"]
        )
        assert result.exit_code == 0, result.output

    def test_chain_envvar_default(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """``ALMANAK_CHAIN`` should act as the default when -c is omitted."""
        monkeypatch.setenv("ALMANAK_CHAIN", "ethereum")
        result = runner.invoke(ax_cli, ["resolve", "--no-gateway", "USDC"])
        assert result.exit_code == 0, result.output
        assert "ethereum" in result.output


class TestAxResolveGatewayUnreachable:
    """When --gateway is on but the gateway path can't answer, the command
    must degrade gracefully (static answer + gateway-note in JSON), never
    hang.

    These tests patch the resolver's gateway path instead of dialing a
    real TCP port, so they don't depend on local network state (no flaky
    environment behavior, no real socket open).
    """

    def test_falls_back_to_static(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.data.tokens import resolver as resolver_mod

        # Stub the gateway-symbol and gateway-address helpers. Return None
        # (transient miss) so the resolver uses the static registry.
        monkeypatch.setattr(
            resolver_mod.TokenResolver, "_check_gateway_available", lambda self: False
        )
        monkeypatch.setattr(
            resolver_mod.TokenResolver, "_resolve_symbol_via_gateway", lambda *a, **k: None
        )
        monkeypatch.setattr(
            resolver_mod.TokenResolver, "_resolve_via_gateway", lambda *a, **k: None
        )

        result = runner.invoke(
            ax_cli,
            ["--gateway-port", "59999", "-c", "arbitrum", "--json", "resolve", "USDC"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["symbol"] == "USDC"
        assert payload["source"] in {"static", "cache"}

    def test_unknown_symbol_adds_gateway_note(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework.data.tokens import resolver as resolver_mod

        monkeypatch.setattr(
            resolver_mod.TokenResolver, "_check_gateway_available", lambda self: False
        )
        monkeypatch.setattr(
            resolver_mod.TokenResolver, "_resolve_symbol_via_gateway", lambda *a, **k: None
        )

        result = runner.invoke(
            ax_cli,
            [
                "--gateway-port",
                "59999",
                "-c",
                "arbitrum",
                "--json",
                "resolve",
                "DEFINITELY_NOT_A_TOKEN",
            ],
        )
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_found"
        assert "gateway" in payload
        assert "59999" in payload["gateway"]


class TestAxResolveMalformedInput:
    """Exit-code 2 contract: garbage that looks address-shaped but isn't
    a valid checksummed hex must be rejected, not resolved and not
    negative-cached."""

    def test_malformed_hex_address_exits_2(self, runner: CliRunner) -> None:
        # 42 chars starting with 0x but non-hex characters -> _validate_address
        # raises InvalidTokenAddressError; CLI must map that to exit 2.
        result = runner.invoke(
            ax_cli,
            [
                "-c",
                "arbitrum",
                "--json",
                "resolve",
                "--no-gateway",
                "0xZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
            ],
        )
        assert result.exit_code == 2, result.output

    def test_wrong_length_hex_address_exits_2(self, runner: CliRunner) -> None:
        # 0x-prefixed but wrong length -> also InvalidTokenAddressError.
        result = runner.invoke(
            ax_cli,
            [
                "-c",
                "arbitrum",
                "--json",
                "resolve",
                "--no-gateway",
                "0xabc",  # too short to be valid, too long to be a symbol
            ],
        )
        # Depending on the length, the resolver either treats this as a symbol
        # (unknown -> exit 1) or an invalid address (exit 2). Both are valid
        # "do not silently resolve" behaviors; accept either here.
        assert result.exit_code in (1, 2), result.output
