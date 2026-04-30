"""Regression tests for VIB-3748 (BUG-56) — gateway chain alias normalization.

QA April29 Batch 17 surfaced ``Chain 'bsc' is not configured on this gateway.
Configured chains: [bnb]`` for ``pancakeswap_v3_swap_bsc``: the gateway was
started with the alias ``bnb`` while the strategy passed the canonical ``bsc``.

Two layers of defense in the fix:

1. ``GatewaySettings.chains`` is normalized at load time via a Pydantic
   ``field_validator`` (any alias maps to the canonical Chain enum value).
2. ``RpcService._chain_not_configured_error`` also normalizes the incoming
   request chain before comparing — so even if some other config path
   bypasses the validator, request-side normalization saves the call.

These tests pin both layers.
"""

from __future__ import annotations

import pytest

from almanak.gateway.core.settings import GatewaySettings


@pytest.fixture
def _isolated_env(monkeypatch):
    """Strip ALMANAK_GATEWAY_CHAINS from the environment so tests start clean."""
    monkeypatch.delenv("ALMANAK_GATEWAY_CHAINS", raising=False)
    yield


class TestSettingsChainsNormalization:
    """GatewaySettings.chains canonicalizes any alias at load time."""

    def test_bnb_alias_normalizes_to_bsc(self, _isolated_env):
        s = GatewaySettings(chains=["bnb"])
        assert s.chains == ["bsc"]

    def test_bsc_canonical_passes_through(self, _isolated_env):
        s = GatewaySettings(chains=["bsc"])
        assert s.chains == ["bsc"]

    def test_eth_alias_normalizes_to_ethereum(self, _isolated_env):
        s = GatewaySettings(chains=["eth"])
        assert s.chains == ["ethereum"]

    def test_avax_alias_normalizes_to_avalanche(self, _isolated_env):
        s = GatewaySettings(chains=["avax"])
        assert s.chains == ["avalanche"]

    def test_mixed_aliases_all_canonicalize(self, _isolated_env):
        s = GatewaySettings(chains=["bnb", "eth", "arb", "avax"])
        assert s.chains == ["bsc", "ethereum", "arbitrum", "avalanche"]

    def test_comma_separated_string_input(self, _isolated_env):
        # Direct constructor with a CSV string also normalizes (defense in
        # depth alongside the env-var path tested below).
        s = GatewaySettings(chains="bnb,arb,eth")
        assert s.chains == ["bsc", "arbitrum", "ethereum"]

    def test_csv_env_var_normalizes(self, monkeypatch):
        # ``ALMANAK_GATEWAY_CHAINS=bnb,arb`` must work end-to-end. Without
        # ``NoDecode`` on the field, pydantic-settings JSON-decodes complex
        # env vars BEFORE the field validator runs and raises SettingsError.
        # This pins the env-var contract documented in market_service.py.
        monkeypatch.setenv("ALMANAK_GATEWAY_CHAINS", "bnb,arb,eth")
        s = GatewaySettings()
        assert s.chains == ["bsc", "arbitrum", "ethereum"]

    def test_single_chain_env_var(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_CHAINS", "bnb")
        s = GatewaySettings()
        assert s.chains == ["bsc"]

    def test_none_entries_filtered(self, _isolated_env):
        # Defensive: list with None must not become ['none', ...].
        s = GatewaySettings(chains=["bnb", None, "eth"])
        assert s.chains == ["bsc", "ethereum"]

    def test_unknown_alias_passes_through_unchanged(self, _isolated_env):
        # The validator falls open: unknown aliases are stored as-is so
        # the gateway can still surface a clear error at request time.
        # Failing closed (raising at startup) would lock out gateways
        # with future chain support staged in env vars.
        s = GatewaySettings(chains=["totallynewchain"])
        assert s.chains == ["totallynewchain"]

    def test_empty_chains_remains_empty(self, _isolated_env):
        s = GatewaySettings()
        assert s.chains == []

    def test_blanks_and_whitespace_stripped(self, _isolated_env):
        s = GatewaySettings(chains=["  bnb  ", "", "ethereum"])
        assert s.chains == ["bsc", "ethereum"]


class TestRpcServiceChainAliasComparator:
    """RpcService._chain_not_configured_error normalizes the request side too."""

    def _make_servicer(self, configured_chains):
        # Lazy import to keep the test fast — RpcService imports aiohttp/grpc.
        from almanak.gateway.services.rpc_service import RpcServiceServicer

        # Bypass settings load — construct manually with whatever we want.
        settings = GatewaySettings.model_construct(
            chains=configured_chains,
        )
        return RpcServiceServicer(settings=settings)

    def test_request_bsc_matches_configured_bnb(self):
        # Even though settings.chains is canonicalized at load time, this
        # test exercises the request-side normalization in isolation by
        # constructing the servicer with the raw alias bypassing the
        # validator.
        servicer = self._make_servicer(["bnb"])
        # _chain_not_configured_error returns None when the chain is allowed.
        assert servicer._chain_not_configured_error("bsc") is None

    def test_request_bnb_matches_configured_bsc(self):
        servicer = self._make_servicer(["bsc"])
        assert servicer._chain_not_configured_error("bnb") is None

    def test_request_unknown_chain_reports_clear_error(self):
        servicer = self._make_servicer(["bsc"])
        msg = servicer._chain_not_configured_error("solana")
        assert msg is not None
        assert "Chain 'solana' is not configured" in msg
        assert "[bsc]" in msg

    def test_empty_chains_accepts_anything_on_demand(self):
        servicer = self._make_servicer([])
        assert servicer._chain_not_configured_error("bsc") is None
        assert servicer._chain_not_configured_error("anything") is None

    def test_non_string_entries_in_settings_chains_do_not_raise(self):
        # Defense-in-depth: if some path bypasses the validator AND injects a
        # non-string into settings.chains (e.g. None/int via model_construct
        # plus mutation), _canonical must not AttributeError.
        servicer = self._make_servicer([None, 123, "bsc"])
        assert servicer._chain_not_configured_error("bsc") is None
        msg = servicer._chain_not_configured_error("solana")
        assert msg is not None
        assert "Chain 'solana' is not configured" in msg
