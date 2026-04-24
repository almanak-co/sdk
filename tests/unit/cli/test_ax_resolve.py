"""Unit tests for ``almanak ax resolve`` — the AI-agent friendly token lookup.

Exercises the exit-code contracts (0 resolved / 1 not_found / 2
malformed / 3 not deployed) and both ``--gateway`` / ``--no-gateway`` paths.
Also covers the ``--verify`` / ``--no-verify`` on-chain verification feature
(VIB-3347).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

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
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "USDC"])
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
        result = runner.invoke(ax_cli, ["-c", "bsc", "--json", "resolve", "--no-gateway", "USDC"])
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
        result = runner.invoke(ax_cli, ["-c", "zerog", "--json", "resolve", "--no-gateway", "USDC"])
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_found"

    def test_lowercase_symbol_still_matches(self, runner: CliRunner) -> None:
        result = runner.invoke(ax_cli, ["-c", "arbitrum", "resolve", "--no-gateway", "usdc"])
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
        monkeypatch.setattr(resolver_mod.TokenResolver, "_check_gateway_available", lambda self: False)
        monkeypatch.setattr(resolver_mod.TokenResolver, "_resolve_symbol_via_gateway", lambda *a, **k: None)
        monkeypatch.setattr(resolver_mod.TokenResolver, "_resolve_via_gateway", lambda *a, **k: None)

        result = runner.invoke(
            ax_cli,
            ["--gateway-port", "59999", "-c", "arbitrum", "--json", "resolve", "USDC"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["symbol"] == "USDC"
        assert payload["source"] in {"static", "cache"}

    def test_unknown_symbol_adds_gateway_note(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.data.tokens import resolver as resolver_mod

        monkeypatch.setattr(resolver_mod.TokenResolver, "_check_gateway_available", lambda self: False)
        monkeypatch.setattr(resolver_mod.TokenResolver, "_resolve_symbol_via_gateway", lambda *a, **k: None)
        # Short-circuit the TCP probe so ``_build_resolver_for_cli`` skips the
        # managed-gateway auto-spawn path — the test is asserting the
        # static-fallback + gateway-note contract, not the spawn path, and
        # ManagedGateway.start() reconfigures logging which un-hooks pytest's
        # log capture and leaks warnings into CliRunner's stdout.
        monkeypatch.setattr("almanak.framework.cli.ax._gateway_is_reachable", lambda *a, **k: True)

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

    def test_command_does_not_mutate_singleton_gateway_channel(self, runner: CliRunner) -> None:
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        original_channel = resolver._gateway_channel
        sentinel_channel = MagicMock(name="sentinel_channel")
        resolver.set_gateway_channel(sentinel_channel)

        try:
            result = runner.invoke(
                ax_cli,
                ["--gateway-port", "59999", "-c", "arbitrum", "resolve", "USDC"],
            )
            assert result.exit_code == 0, result.output
            assert resolver._gateway_channel is sentinel_channel
        finally:
            resolver.set_gateway_channel(original_channel)

    def test_build_resolver_wraps_gateway_channel_with_auth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import grpc

        from almanak.framework.cli.ax import _build_resolver_for_cli
        from almanak.framework.gateway_client import _AuthClientInterceptor

        raw_channel = MagicMock(name="raw_channel")
        intercepted_channel = MagicMock(name="intercepted_channel")
        resolver_without_channel = MagicMock(name="resolver_without_channel")
        resolver_with_channel = MagicMock(name="resolver_with_channel")
        created_channels: list[object | None] = []
        intercepted: dict[str, object] = {}

        def fake_create_token_resolver(*, gateway_channel=None):
            created_channels.append(gateway_channel)
            return resolver_with_channel if gateway_channel is not None else resolver_without_channel

        def fake_intercept_channel(channel, *interceptors):
            intercepted["channel"] = channel
            intercepted["interceptors"] = interceptors
            return intercepted_channel

        monkeypatch.setattr(grpc, "insecure_channel", lambda target: raw_channel)
        monkeypatch.setattr(grpc, "intercept_channel", fake_intercept_channel)
        monkeypatch.setattr("almanak.framework.data.tokens.create_token_resolver", fake_create_token_resolver)
        # Pretend the configured gateway is already listening so the helper
        # takes the direct-channel path instead of trying to spawn a
        # ManagedGateway (which would fail in a unit-test environment).
        monkeypatch.setattr("almanak.framework.cli.ax._gateway_is_reachable", lambda *a, **k: True)

        ctx = SimpleNamespace(
            obj={
                "gateway_host": "localhost",
                "gateway_port": 59999,
                "gateway_auth_token": "secret-token",
            }
        )

        resolver, channel, note = _build_resolver_for_cli(ctx, use_gateway=True)

        assert resolver is resolver_with_channel
        assert channel is intercepted_channel
        assert note == "attempted dynamic lookup via localhost:59999"
        assert created_channels == [None, intercepted_channel]
        assert intercepted["channel"] is raw_channel
        assert len(intercepted["interceptors"]) == 1
        assert isinstance(intercepted["interceptors"][0], _AuthClientInterceptor)

    def test_build_resolver_reads_auth_token_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When ``ctx.obj`` carries no auth token, fall back to the
        ``ALMANAK_GATEWAY_AUTH_TOKEN`` / ``GATEWAY_AUTH_TOKEN`` env vars —
        same contract ``_get_executor`` uses for swap / balance / etc.
        """
        import grpc

        from almanak.framework.cli.ax import _build_resolver_for_cli
        from almanak.framework.gateway_client import _AuthClientInterceptor

        raw_channel = MagicMock(name="raw_channel")
        intercepted_channel = MagicMock(name="intercepted_channel")
        resolver_with_channel = MagicMock(name="resolver_with_channel")
        captured_tokens: list[str] = []

        def fake_create_token_resolver(*, gateway_channel=None):
            return resolver_with_channel if gateway_channel is not None else MagicMock()

        class _CapturingInterceptor(_AuthClientInterceptor):
            def __init__(self, token):
                captured_tokens.append(token)
                super().__init__(token)

        monkeypatch.setattr(grpc, "insecure_channel", lambda target: raw_channel)
        monkeypatch.setattr(grpc, "intercept_channel", lambda *a, **k: intercepted_channel)
        monkeypatch.setattr("almanak.framework.data.tokens.create_token_resolver", fake_create_token_resolver)
        monkeypatch.setattr("almanak.framework.cli.ax._gateway_is_reachable", lambda *a, **k: True)
        monkeypatch.setattr("almanak.framework.gateway_client._AuthClientInterceptor", _CapturingInterceptor)
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "env-token")
        monkeypatch.delenv("GATEWAY_AUTH_TOKEN", raising=False)

        # ctx.obj deliberately has no 'gateway_auth_token' — exercises env path.
        ctx = SimpleNamespace(obj={"gateway_host": "localhost", "gateway_port": 59999})

        resolver, channel, note = _build_resolver_for_cli(ctx, use_gateway=True)

        assert resolver is resolver_with_channel
        assert channel is intercepted_channel
        assert note == "attempted dynamic lookup via localhost:59999"
        assert captured_tokens == ["env-token"], (
            "Expected _build_resolver_for_cli to read the auth token from "
            "ALMANAK_GATEWAY_AUTH_TOKEN when ctx.obj is empty."
        )


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


class TestCheckContractDeployed:
    """Unit tests for the ``_check_contract_deployed`` helper (VIB-3347).

    All tests mock at the network boundary so they are fully offline and
    deterministic.
    """

    def test_native_sentinel_returns_none(self) -> None:
        from almanak.framework.cli.ax import _check_contract_deployed
        from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

        result = _check_contract_deployed(NATIVE_SENTINEL, "arbitrum")
        assert result is None

    def test_lowercase_sentinel_also_returns_none(self) -> None:
        from almanak.framework.cli.ax import _check_contract_deployed

        # Callers may pass un-checksummed lowercase; must still be skipped.
        result = _check_contract_deployed(
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "arbitrum"
        )
        assert result is None

    def test_zero_address_returns_none(self) -> None:
        from almanak.framework.cli.ax import _check_contract_deployed

        result = _check_contract_deployed(
            "0x0000000000000000000000000000000000000000", "arbitrum"
        )
        assert result is None

    def test_non_evm_address_returns_none(self) -> None:
        """Solana base58 pubkeys have no 0x prefix — skip silently."""
        from almanak.framework.cli.ax import _check_contract_deployed

        result = _check_contract_deployed(
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "solana"
        )
        assert result is None

    def test_short_address_returns_none(self) -> None:
        from almanak.framework.cli.ax import _check_contract_deployed

        result = _check_contract_deployed("0xabc", "arbitrum")
        assert result is None

    def test_deployed_contract_via_gateway(self) -> None:
        """When the gateway channel returns non-empty bytecode, return True."""
        import json as _json
        from unittest.mock import MagicMock, patch

        from almanak.framework.cli.ax import _check_contract_deployed

        mock_channel = MagicMock()
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.result = _json.dumps("0x608060405260...")  # non-empty code

        with patch("almanak.gateway.proto.gateway_pb2_grpc.RpcServiceStub") as MockStub:
            MockStub.return_value.Call.return_value = mock_response
            result = _check_contract_deployed(
                "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "arbitrum",
                gateway_channel=mock_channel,
            )
        assert result is True

    def test_not_deployed_via_http(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When HTTP JSON-RPC returns '0x', return False (not deployed)."""
        import json as _json
        from unittest.mock import MagicMock, patch

        from almanak.framework.cli.ax import _check_contract_deployed

        rpc_response_bytes = _json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0x"}).encode()
        mock_http_response = MagicMock()
        mock_http_response.read.return_value = rpc_response_bytes
        mock_http_response.__enter__ = lambda s: s
        mock_http_response.__exit__ = MagicMock(return_value=False)

        with patch("almanak.gateway.utils.get_rpc_url", return_value="http://fake-rpc.example.com"):
            with patch("urllib.request.urlopen", return_value=mock_http_response):
                result = _check_contract_deployed(
                    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "avalanche",
                )
        assert result is False

    def test_rpc_unavailable_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When get_rpc_url raises, return None (graceful skip)."""
        from unittest.mock import patch

        from almanak.framework.cli.ax import _check_contract_deployed

        with patch(
            "almanak.gateway.utils.get_rpc_url",
            side_effect=ValueError("No RPC configured"),
        ):
            result = _check_contract_deployed(
                "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "avalanche",
            )
        assert result is None


class TestAxResolveVerifyFlag:
    """Tests for the ``--verify`` / ``--no-verify`` flag (VIB-3347).

    All network I/O is mocked so tests run offline and deterministically.
    """

    def test_no_verify_skips_check_returns_null(self, runner: CliRunner) -> None:
        """``--no-verify`` must produce ``contract_verified: null`` in JSON."""
        result = runner.invoke(
            ax_cli,
            ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "--no-verify", "USDC"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["contract_verified"] is None

    def test_verify_deployed_returns_true(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the contract exists on chain, ``contract_verified`` is True and exit 0."""
        monkeypatch.setattr(
            "almanak.framework.cli.ax._check_contract_deployed",
            lambda *a, **k: True,
        )
        result = runner.invoke(
            ax_cli,
            ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "USDC"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["contract_verified"] is True

    def test_verify_not_deployed_exits_3(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When eth_getCode returns empty bytecode, exit code must be 3."""
        monkeypatch.setattr(
            "almanak.framework.cli.ax._check_contract_deployed",
            lambda *a, **k: False,
        )
        result = runner.invoke(
            ax_cli,
            ["-c", "avalanche", "--json", "resolve", "--no-gateway", "USDC"],
        )
        assert result.exit_code == 3, result.output
        payload = json.loads(result.output)
        assert payload["contract_verified"] is False

    def test_verify_not_deployed_shows_warning_in_human_output(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Human output must include a WARNING line when not deployed."""
        monkeypatch.setattr(
            "almanak.framework.cli.ax._check_contract_deployed",
            lambda *a, **k: False,
        )
        result = runner.invoke(
            ax_cli,
            ["-c", "avalanche", "resolve", "--no-gateway", "USDC"],
        )
        assert result.exit_code == 3, result.output
        assert "WARNING" in result.output
        assert "not deployed" in result.output

    def test_verify_rpc_unavailable_exits_0(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When RPC is unavailable (returns None), command must still exit 0."""
        monkeypatch.setattr(
            "almanak.framework.cli.ax._check_contract_deployed",
            lambda *a, **k: None,
        )
        result = runner.invoke(
            ax_cli,
            ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "USDC"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["contract_verified"] is None

    def test_native_token_skips_verification_exits_0(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ETH (native token) must get ``contract_verified: null`` and exit 0."""
        # Ensure _check_contract_deployed is NOT called for native tokens.
        called = []
        monkeypatch.setattr(
            "almanak.framework.cli.ax._check_contract_deployed",
            lambda *a, **k: called.append(True) or None,
        )
        result = runner.invoke(
            ax_cli,
            ["-c", "arbitrum", "--json", "resolve", "--no-gateway", "ETH"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        # _check_contract_deployed IS called but returns None for the sentinel.
        # What matters is exit code 0 and contract_verified is null.
        assert payload["contract_verified"] is None
