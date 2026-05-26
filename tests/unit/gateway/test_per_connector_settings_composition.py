"""Per-connector settings composition (VIB-4812).

``GatewaySettings`` is composed from per-connector ``BaseModel``
fragments via multi-inheritance:

    class GatewaySettings(
        BaseSettings,
        PolymarketGatewaySettings,
        EnsoGatewaySettings,
        PendleGatewaySettings,
    ): ...

These tests pin the load-bearing contracts of that composition:

1. Every field a connector contributes is reachable on ``GatewaySettings``.
2. Each fragment is importable in isolation and has the documented
   defaults — adding a new connector field has only one edit-point.
3. Every existing ``ALMANAK_GATEWAY_<FIELD>`` env var still loads to
   the same value as before. The 7 polymarket + 2 pendle + 1 enso
   env-vars exhaust the surface area moved by Phase 4.
4. The polymarket cache-TTL validator still fires on negative / NaN
   input through the composed class (the validator lives on the
   fragment now; the composed class must inherit it).
"""

from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from almanak.connectors.enso.gateway.settings import EnsoGatewaySettings
from almanak.connectors.pendle.gateway.settings import PendleGatewaySettings
from almanak.connectors.polymarket.gateway.settings import (
    PolymarketGatewaySettings,
)
from almanak.gateway.core.settings import GatewaySettings


# ---------------------------------------------------------------------------
# Fragment defaults — adding a new field changes the fragment in one place.
# ---------------------------------------------------------------------------
class TestPolymarketFragmentDefaults:
    def test_network_default(self) -> None:
        assert PolymarketGatewaySettings().polymarket_network == "mainnet"

    def test_market_cache_ttl_default(self) -> None:
        assert PolymarketGatewaySettings().polymarket_market_cache_ttl_seconds == 60.0

    def test_credentials_default_to_none(self) -> None:
        frag = PolymarketGatewaySettings()
        assert frag.polymarket_wallet_address is None
        assert frag.polymarket_private_key is None
        assert frag.polymarket_api_key is None
        assert frag.polymarket_secret is None
        assert frag.polymarket_passphrase is None


class TestEnsoFragmentDefaults:
    def test_api_key_default_to_none(self) -> None:
        assert EnsoGatewaySettings().enso_api_key is None


class TestPendleFragmentDefaults:
    def test_api_key_default_to_none(self) -> None:
        assert PendleGatewaySettings().pendle_api_key is None

    def test_cache_ttl_default(self) -> None:
        assert PendleGatewaySettings().pendle_api_cache_ttl == 15.0


# ---------------------------------------------------------------------------
# Composition: every fragment field is reachable on ``GatewaySettings``.
# ---------------------------------------------------------------------------
class TestCompositionSurface:
    @pytest.fixture
    def s(self) -> GatewaySettings:
        return GatewaySettings()

    @pytest.mark.parametrize(
        "field",
        [
            "polymarket_network",
            "polymarket_market_cache_ttl_seconds",
            "polymarket_wallet_address",
            "polymarket_private_key",
            "polymarket_api_key",
            "polymarket_secret",
            "polymarket_passphrase",
            "enso_api_key",
            "pendle_api_key",
            "pendle_api_cache_ttl",
        ],
    )
    def test_field_reachable(self, s: GatewaySettings, field: str) -> None:
        assert hasattr(s, field), f"{field} not reachable on GatewaySettings"

    def test_polymarket_defaults_preserved_through_composition(
        self, s: GatewaySettings
    ) -> None:
        assert s.polymarket_network == "mainnet"
        assert s.polymarket_market_cache_ttl_seconds == 60.0

    def test_pendle_defaults_preserved_through_composition(
        self, s: GatewaySettings
    ) -> None:
        assert s.pendle_api_cache_ttl == 15.0


# ---------------------------------------------------------------------------
# Env-var loading: every prefixed env var still binds to the right field.
# This is the byte-identical-compat assertion that protects deployers.
# ---------------------------------------------------------------------------
class TestEnvVarBinding:
    @pytest.mark.parametrize(
        ("env_var", "field", "value", "expected"),
        [
            # Polymarket
            ("ALMANAK_GATEWAY_POLYMARKET_NETWORK", "polymarket_network", "anvil", "anvil"),
            (
                "ALMANAK_GATEWAY_POLYMARKET_MARKET_CACHE_TTL_SECONDS",
                "polymarket_market_cache_ttl_seconds",
                "120.5",
                120.5,
            ),
            (
                "ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS",
                "polymarket_wallet_address",
                "0xWalletAddress",
                "0xWalletAddress",
            ),
            (
                "ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY",
                "polymarket_private_key",
                "0xprivkey",
                "0xprivkey",
            ),
            (
                "ALMANAK_GATEWAY_POLYMARKET_API_KEY",
                "polymarket_api_key",
                "pm-api-key",
                "pm-api-key",
            ),
            (
                "ALMANAK_GATEWAY_POLYMARKET_SECRET",
                "polymarket_secret",
                "pm-secret",
                "pm-secret",
            ),
            (
                "ALMANAK_GATEWAY_POLYMARKET_PASSPHRASE",
                "polymarket_passphrase",
                "pm-pass",
                "pm-pass",
            ),
            # Enso
            (
                "ALMANAK_GATEWAY_ENSO_API_KEY",
                "enso_api_key",
                "enso-test",
                "enso-test",
            ),
            # Pendle
            (
                "ALMANAK_GATEWAY_PENDLE_API_KEY",
                "pendle_api_key",
                "pendle-test",
                "pendle-test",
            ),
            (
                "ALMANAK_GATEWAY_PENDLE_API_CACHE_TTL",
                "pendle_api_cache_ttl",
                "30.0",
                30.0,
            ),
        ],
    )
    def test_env_var_loads_to_field(
        self,
        monkeypatch: pytest.MonkeyPatch,
        env_var: str,
        field: str,
        value: str,
        expected: object,
    ) -> None:
        # Scrub every relevant ALMANAK_GATEWAY_* env var so the test is
        # hermetic against the developer's ``.env``; then set the one
        # under test via ``monkeypatch`` so it auto-reverts.
        for k in (
            "ALMANAK_GATEWAY_POLYMARKET_NETWORK",
            "ALMANAK_GATEWAY_POLYMARKET_MARKET_CACHE_TTL_SECONDS",
            "ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS",
            "ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY",
            "ALMANAK_GATEWAY_POLYMARKET_API_KEY",
            "ALMANAK_GATEWAY_POLYMARKET_SECRET",
            "ALMANAK_GATEWAY_POLYMARKET_PASSPHRASE",
            "ALMANAK_GATEWAY_ENSO_API_KEY",
            "ALMANAK_GATEWAY_PENDLE_API_KEY",
            "ALMANAK_GATEWAY_PENDLE_API_CACHE_TTL",
        ):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv(env_var, value)
        s = GatewaySettings()
        assert getattr(s, field) == expected


# ---------------------------------------------------------------------------
# Validator still fires after composition.
# ---------------------------------------------------------------------------
class TestPolymarketCacheTTLValidator:
    def test_negative_rejected(self) -> None:
        with pytest.raises(
            ValidationError, match="polymarket_market_cache_ttl_seconds must be >= 0"
        ):
            GatewaySettings(polymarket_market_cache_ttl_seconds=-1.0)

    def test_nan_rejected(self) -> None:
        with pytest.raises(
            ValidationError,
            match="polymarket_market_cache_ttl_seconds must be a finite number",
        ):
            GatewaySettings(polymarket_market_cache_ttl_seconds=math.nan)

    def test_positive_accepted(self) -> None:
        s = GatewaySettings(polymarket_market_cache_ttl_seconds=42.0)
        assert s.polymarket_market_cache_ttl_seconds == 42.0

    def test_validator_fires_on_fragment_too(self) -> None:
        """The validator lives on ``PolymarketGatewaySettings`` and must
        also fire when the fragment is constructed standalone."""
        with pytest.raises(ValidationError):
            PolymarketGatewaySettings(polymarket_market_cache_ttl_seconds=-1.0)


# ---------------------------------------------------------------------------
# model_dump() surface — every field is included in the dump (parity with
# the pre-Phase-4 shape). Catches an accidental ``model_config = {"extra":
# "ignore"}`` swallowing a fragment field.
# ---------------------------------------------------------------------------
def test_model_dump_includes_every_per_connector_field() -> None:
    dumped = GatewaySettings().model_dump()
    for field in (
        "polymarket_network",
        "polymarket_market_cache_ttl_seconds",
        "polymarket_wallet_address",
        "polymarket_private_key",
        "polymarket_api_key",
        "polymarket_secret",
        "polymarket_passphrase",
        "enso_api_key",
        "pendle_api_key",
        "pendle_api_cache_ttl",
    ):
        assert field in dumped, f"{field} missing from model_dump()"
