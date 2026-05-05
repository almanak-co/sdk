"""Tests for GatewaySettings env var fallback behavior.

Verifies precedence: ALMANAK_GATEWAY_* takes priority over ALMANAK_* fallbacks.
"""

import os
from unittest.mock import patch

from almanak.gateway.core.settings import GatewaySettings


class TestSettingsFallback:
    """Test _fallback_env_vars hydrates from ALMANAK_* when ALMANAK_GATEWAY_* is missing."""

    def test_eoa_address_fallback(self):
        """ALMANAK_EOA_ADDRESS populates eoa_address when gateway var is unset."""
        env = {"ALMANAK_EOA_ADDRESS": "0xABC123", "ALMANAK_GATEWAY_EOA_ADDRESS": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.eoa_address == "0xABC123"

    def test_safe_address_fallback(self):
        """ALMANAK_SAFE_ADDRESS populates safe_address when gateway var is unset."""
        env = {"ALMANAK_SAFE_ADDRESS": "0xSAFE", "ALMANAK_GATEWAY_SAFE_ADDRESS": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.safe_address == "0xSAFE"

    def test_zodiac_address_fallback(self):
        """ALMANAK_ZODIAC_ADDRESS populates zodiac_roles_address."""
        env = {"ALMANAK_ZODIAC_ADDRESS": "0xZODIAC", "ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.zodiac_roles_address == "0xZODIAC"

    def test_signer_service_url_fallback(self):
        """ALMANAK_SIGNER_SERVICE_URL populates signer_service_url."""
        env = {"ALMANAK_SIGNER_SERVICE_URL": "https://signer.test", "ALMANAK_GATEWAY_SIGNER_SERVICE_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.signer_service_url == "https://signer.test"

    def test_signer_service_jwt_fallback(self):
        """ALMANAK_SIGNER_SERVICE_JWT populates signer_service_jwt."""
        env = {"ALMANAK_SIGNER_SERVICE_JWT": "jwt-tok", "ALMANAK_GATEWAY_SIGNER_SERVICE_JWT": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.signer_service_jwt == "jwt-tok"

    def test_gateway_var_takes_precedence_over_fallback(self):
        """ALMANAK_GATEWAY_EOA_ADDRESS takes priority over ALMANAK_EOA_ADDRESS."""
        env = {
            "ALMANAK_GATEWAY_EOA_ADDRESS": "0xGATEWAY",
            "ALMANAK_EOA_ADDRESS": "0xFALLBACK",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.eoa_address == "0xGATEWAY"

    def test_private_key_fallback(self):
        """ALMANAK_PRIVATE_KEY populates private_key when gateway var is unset."""
        env = {"ALMANAK_PRIVATE_KEY": "0xKEY", "ALMANAK_GATEWAY_PRIVATE_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.private_key == "0xKEY"

    # --- Third-party API key fallbacks (bare name -> Pydantic field) ---

    def test_alchemy_api_key_bare_fallback(self):
        """ALCHEMY_API_KEY populates alchemy_api_key when prefixed var is unset."""
        env = {"ALCHEMY_API_KEY": "alchemy-test-key", "ALMANAK_GATEWAY_ALCHEMY_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.alchemy_api_key == "alchemy-test-key"

    def test_coingecko_api_key_bare_fallback(self):
        """COINGECKO_API_KEY populates coingecko_api_key when prefixed var is unset."""
        env = {"COINGECKO_API_KEY": "cg-test-key", "ALMANAK_GATEWAY_COINGECKO_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.coingecko_api_key == "cg-test-key"

    def test_prefixed_alchemy_key_takes_precedence(self):
        """ALMANAK_GATEWAY_ALCHEMY_API_KEY takes priority over bare ALCHEMY_API_KEY."""
        env = {
            "ALMANAK_GATEWAY_ALCHEMY_API_KEY": "prefixed-key",
            "ALCHEMY_API_KEY": "bare-key",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.alchemy_api_key == "prefixed-key"

    def test_enso_api_key_bare_fallback(self):
        """ENSO_API_KEY populates enso_api_key when prefixed var is unset."""
        env = {"ENSO_API_KEY": "enso-test-key", "ALMANAK_GATEWAY_ENSO_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.enso_api_key == "enso-test-key"

    def test_prefixed_enso_key_takes_precedence(self):
        """ALMANAK_GATEWAY_ENSO_API_KEY takes priority over bare ENSO_API_KEY."""
        env = {
            "ALMANAK_GATEWAY_ENSO_API_KEY": "prefixed-enso-key",
            "ENSO_API_KEY": "bare-enso-key",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.enso_api_key == "prefixed-enso-key"

    def test_portfolio_api_key_fallback(self):
        """ALMANAK_PORTFOLIO_API_KEY populates portfolio_api_key when gateway var is unset."""
        env = {"ALMANAK_PORTFOLIO_API_KEY": "portfolio-test-key", "ALMANAK_GATEWAY_PORTFOLIO_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.portfolio_api_key == "portfolio-test-key"

    def test_zerion_api_key_fallback(self):
        """ZERION_API_KEY also populates portfolio_api_key for local experiments."""
        env = {"ZERION_API_KEY": "zerion-test-key", "ALMANAK_GATEWAY_PORTFOLIO_API_KEY": "", "ALMANAK_PORTFOLIO_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.portfolio_api_key == "zerion-test-key"

    def test_api_keys_empty_when_neither_set(self):
        """Third-party API key fields remain empty when no env vars are set."""
        env = {
            "ALMANAK_GATEWAY_ALCHEMY_API_KEY": "",
            "ALCHEMY_API_KEY": "",
            "ALMANAK_GATEWAY_COINGECKO_API_KEY": "",
            "COINGECKO_API_KEY": "",
            "ALMANAK_GATEWAY_ENSO_API_KEY": "",
            "ENSO_API_KEY": "",
            "ALMANAK_GATEWAY_PORTFOLIO_API_KEY": "",
            "ALMANAK_PORTFOLIO_API_KEY": "",
            "ZERION_API_KEY": "",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert not settings.alchemy_api_key
            assert not settings.coingecko_api_key
            assert not settings.enso_api_key
            assert not settings.portfolio_api_key


class TestPolymarketPrivateKeyFallbackLadder:
    """VIB-3772: ``polymarket_private_key`` resolution ladder.

    Order (first non-empty wins):
        1. ``ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY``
        2. ``POLYMARKET_PRIVATE_KEY``
        3. ``ALMANAK_POLYMARKET_PRIVATE_KEY``
        4. ``ALMANAK_PRIVATE_KEY`` (via resolved ``private_key``)
    """

    # 32-byte hex keys for tests (Polymarket ``Account.from_key`` is not
    # invoked here — settings only stores the string).
    _GATEWAY_KEY = "0x" + "11" * 32
    _BARE_KEY = "0x" + "22" * 32
    _ALMANAK_PM_KEY = "0x" + "33" * 32
    _PRIMARY_KEY = "0x" + "44" * 32

    def _clear_env(self) -> dict[str, str]:
        return {
            "ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY": "",
            "POLYMARKET_PRIVATE_KEY": "",
            "ALMANAK_POLYMARKET_PRIVATE_KEY": "",
            "ALMANAK_GATEWAY_PRIVATE_KEY": "",
            "ALMANAK_PRIVATE_KEY": "",
        }

    def test_rung1_gateway_prefixed_wins(self):
        """ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY beats every other rung."""
        env = self._clear_env() | {
            "ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY": self._GATEWAY_KEY,
            "POLYMARKET_PRIVATE_KEY": self._BARE_KEY,
            "ALMANAK_POLYMARKET_PRIVATE_KEY": self._ALMANAK_PM_KEY,
            "ALMANAK_PRIVATE_KEY": self._PRIMARY_KEY,
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.polymarket_private_key == self._GATEWAY_KEY

    def test_rung2_bare_polymarket_key(self):
        """POLYMARKET_PRIVATE_KEY wins over ALMANAK_POLYMARKET_PRIVATE_KEY and primary key."""
        env = self._clear_env() | {
            "POLYMARKET_PRIVATE_KEY": self._BARE_KEY,
            "ALMANAK_POLYMARKET_PRIVATE_KEY": self._ALMANAK_PM_KEY,
            "ALMANAK_PRIVATE_KEY": self._PRIMARY_KEY,
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.polymarket_private_key == self._BARE_KEY

    def test_rung3_almanak_polymarket_key(self):
        """ALMANAK_POLYMARKET_PRIVATE_KEY wins over the primary signer fallback."""
        env = self._clear_env() | {
            "ALMANAK_POLYMARKET_PRIVATE_KEY": self._ALMANAK_PM_KEY,
            "ALMANAK_PRIVATE_KEY": self._PRIMARY_KEY,
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.polymarket_private_key == self._ALMANAK_PM_KEY

    def test_rung4_falls_back_to_almanak_private_key(self, caplog):
        """The ticket's UX win: ALMANAK_PRIVATE_KEY alone is enough.

        Also locks in the rung-4 INFO operator signal as part of the same
        contract — startup logs are how ops know which env var the gateway
        actually picked up (CodeRabbit nitpick on PR #2075).
        """
        import logging

        env = self._clear_env() | {"ALMANAK_PRIVATE_KEY": self._PRIMARY_KEY}
        with patch.dict(os.environ, env, clear=False):
            with caplog.at_level(logging.INFO, logger="almanak.gateway.core.settings"):
                settings = GatewaySettings()
            assert settings.polymarket_private_key == self._PRIMARY_KEY
            # Sanity: the primary signer key was also resolved correctly.
            assert settings.private_key == self._PRIMARY_KEY
            # Operator-signal contract: rung-4 unification emits a clear INFO
            # log naming the actual signer source.
            assert any(
                "Polymarket signing" in r.getMessage() and "ALMANAK_PRIVATE_KEY" in r.getMessage()
                for r in caplog.records
            )

    def test_rung4_falls_back_to_gateway_prefixed_primary(self, caplog):
        """ALMANAK_GATEWAY_PRIVATE_KEY likewise propagates as the unified key.

        The accompanying INFO log must name ``ALMANAK_GATEWAY_PRIVATE_KEY`` —
        not ``ALMANAK_PRIVATE_KEY`` — so ops can disambiguate the source
        during debugging.
        """
        import logging

        env = self._clear_env() | {"ALMANAK_GATEWAY_PRIVATE_KEY": self._PRIMARY_KEY}
        with patch.dict(os.environ, env, clear=False):
            with caplog.at_level(logging.INFO, logger="almanak.gateway.core.settings"):
                settings = GatewaySettings()
            assert settings.polymarket_private_key == self._PRIMARY_KEY
            assert any(
                "Polymarket signing" in r.getMessage()
                and "ALMANAK_GATEWAY_PRIVATE_KEY" in r.getMessage()
                for r in caplog.records
            )

    def test_missing_all_keys_leaves_field_none(self):
        """When no signer key of any kind is set the field is ``None``.

        The error path is owned by ``PolymarketService._init_unavailable_reason``
        — settings does not raise on missing keys (the gateway can run in
        read-only / market-data-only mode). This test just locks in that
        the fallback ladder does not invent a value out of thin air.
        """
        env = self._clear_env()
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            # ``""`` is allowed (pydantic preserves the explicit empty string
            # when the env var is set to empty); the contract is "no usable
            # signing material", which is what every downstream consumer
            # (``PolymarketService._available``) checks via truthiness.
            assert not settings.polymarket_private_key
            assert not settings.private_key

    def test_polymarket_service_unavailable_message_when_no_keys(self):
        """End-to-end: PolymarketServiceServicer flags the missing-key path with a clear reason."""
        from almanak.gateway.services.polymarket_service import PolymarketServiceServicer

        env = self._clear_env()
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            service = PolymarketServiceServicer(settings=settings)
            assert service._available is False
            reason = service._last_credentials_failure or ""
            # Either branch of _init_unavailable_reason names a private-key env var.
            assert "POLYMARKET_PRIVATE_KEY" in reason or "wallet_address" in reason

    def test_explicit_polymarket_kwarg_beats_almanak_private_key(self):
        """A constructor-level kwarg must still beat the ALMANAK_PRIVATE_KEY rung."""
        env = self._clear_env() | {"ALMANAK_PRIVATE_KEY": self._PRIMARY_KEY}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings(polymarket_private_key=self._GATEWAY_KEY)
            assert settings.polymarket_private_key == self._GATEWAY_KEY

    def test_unification_log_names_actual_source(self, caplog):
        """The unification INFO log identifies the real signer env var.

        CodeRabbit (PR #2075) flagged that hardcoding ``ALMANAK_PRIVATE_KEY``
        in the log was misleading when the unified key actually came from
        ``ALMANAK_GATEWAY_PRIVATE_KEY``. Lock in the dynamic source label so
        ops always see the correct origin during debugging.
        """
        import logging

        # Case 1: bare ALMANAK_PRIVATE_KEY rung.
        env = self._clear_env() | {"ALMANAK_PRIVATE_KEY": self._PRIMARY_KEY}
        with patch.dict(os.environ, env, clear=False):
            with caplog.at_level(logging.INFO, logger="almanak.gateway.core.settings"):
                GatewaySettings()
        messages = [r.getMessage() for r in caplog.records]
        assert any("ALMANAK_PRIVATE_KEY" in m and "Polymarket signing" in m for m in messages)
        assert not any("ALMANAK_GATEWAY_PRIVATE_KEY" in m for m in messages)

        caplog.clear()

        # Case 2: gateway-prefixed primary key — log must name it, not
        # ALMANAK_PRIVATE_KEY.
        env = self._clear_env() | {"ALMANAK_GATEWAY_PRIVATE_KEY": self._PRIMARY_KEY}
        with patch.dict(os.environ, env, clear=False):
            with caplog.at_level(logging.INFO, logger="almanak.gateway.core.settings"):
                GatewaySettings()
        messages = [r.getMessage() for r in caplog.records]
        assert any("ALMANAK_GATEWAY_PRIVATE_KEY" in m and "Polymarket signing" in m for m in messages)


class TestPolymarketWalletAddressFallbackLadder:
    """VIB-3772: ``polymarket_wallet_address`` resolution.

    There is no ``ALMANAK_WALLET_ADDRESS`` to fall back to — the wallet
    address is implied by the signer (local EOA) or the Safe address
    (hosted). Settings just normalises the dedicated env vars; the
    service derives the funder downstream.
    """

    def _clear_env(self) -> dict[str, str]:
        return {
            "ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS": "",
            "POLYMARKET_WALLET_ADDRESS": "",
            "ALMANAK_POLYMARKET_WALLET_ADDRESS": "",
        }

    def test_gateway_prefixed_wins(self):
        env = self._clear_env() | {
            "ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS": "0xGATEWAY",
            "POLYMARKET_WALLET_ADDRESS": "0xBARE",
            "ALMANAK_POLYMARKET_WALLET_ADDRESS": "0xALMANAK",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.polymarket_wallet_address == "0xGATEWAY"

    def test_bare_beats_almanak_prefixed(self):
        env = self._clear_env() | {
            "POLYMARKET_WALLET_ADDRESS": "0xBARE",
            "ALMANAK_POLYMARKET_WALLET_ADDRESS": "0xALMANAK",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.polymarket_wallet_address == "0xBARE"

    def test_almanak_polymarket_wallet_address(self):
        env = self._clear_env() | {"ALMANAK_POLYMARKET_WALLET_ADDRESS": "0xALMANAK"}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.polymarket_wallet_address == "0xALMANAK"

    def test_unset_leaves_none_for_service_to_derive(self):
        env = self._clear_env()
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            # Falsy (None or "") is the contract — downstream
            # ``_resolve_funder_address`` checks truthiness, not identity.
            assert not settings.polymarket_wallet_address


class TestAuthTokenInitKwargPrecedence:
    """VIB-3032: explicit auth_token kwarg must win over ALMANAK_GATEWAY_AUTH_TOKEN env.

    On test networks (anvil/sepolia) the managed-gateway path in
    ``almanak/framework/cli/run.py`` passes ``auth_token=None`` so the server
    does not attach AuthInterceptor while the client runs with
    ``allow_insecure=True``. If pydantic ever re-picked the env value as a
    fallback, every gRPC call would fail UNAUTHENTICATED.
    """

    def test_explicit_none_overrides_env_auth_token(self):
        """Passing auth_token=None as kwarg must win over ALMANAK_GATEWAY_AUTH_TOKEN."""
        env = {"ALMANAK_GATEWAY_AUTH_TOKEN": "from-env-should-lose"}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings(auth_token=None, allow_insecure=True)
            assert settings.auth_token is None
            assert settings.allow_insecure is True

    def test_explicit_token_overrides_env_auth_token(self):
        """Passing a session token kwarg (mainnet path) overrides env token."""
        env = {"ALMANAK_GATEWAY_AUTH_TOKEN": "from-env"}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings(auth_token="session-token-xyz")
            assert settings.auth_token == "session-token-xyz"

    def test_env_still_used_when_no_kwarg_passed(self):
        """Regression sanity: without kwarg, env token is still honoured (non-test paths)."""
        env = {"ALMANAK_GATEWAY_AUTH_TOKEN": "env-token"}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.auth_token == "env-token"
