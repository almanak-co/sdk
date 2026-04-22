"""Tests for OKX OnchainOS portfolio integration."""

import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from almanak.gateway.integrations.base import IntegrationError
from almanak.gateway.integrations.okx import OkxIntegration

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestOkxIntegration:
    """Tests for OkxIntegration."""

    @pytest.fixture
    def okx(self):
        """Create OKX integration with test credentials."""
        return OkxIntegration(
            api_key="test-key",
            api_secret="test-secret",
            api_passphrase="test-passphrase",
            cache_ttl=60,
        )

    def test_initialization(self, okx):
        """OKX integration initializes correctly."""
        assert okx.name == "okx"
        assert okx.default_cache_ttl == 60
        assert okx.is_configured is True

    def test_not_configured_without_credentials(self, monkeypatch):
        """Integration reports not configured when credentials missing."""
        monkeypatch.delenv("OKX_API_KEY", raising=False)
        monkeypatch.delenv("OKX_API_SECRET", raising=False)
        monkeypatch.delenv("OKX_API_PASSPHRASE", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_OKX_API_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_OKX_API_SECRET", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_OKX_API_PASSPHRASE", raising=False)
        okx = OkxIntegration(api_key=None, api_secret=None, api_passphrase=None)
        assert okx.is_configured is False

    def test_not_configured_partial_credentials(self, monkeypatch):
        """Integration reports not configured with partial credentials."""
        monkeypatch.delenv("OKX_API_SECRET", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_OKX_API_SECRET", raising=False)
        okx = OkxIntegration(api_key="key", api_secret=None, api_passphrase="pass")
        assert okx.is_configured is False

    def test_supports_portfolio(self, okx):
        """OKX supports wallet portfolio queries."""
        assert okx.supports_portfolio() is True

    def test_auth_headers_contain_required_fields(self, okx):
        """Auth headers include all 4 OKX access headers."""
        headers = okx._get_auth_headers("GET", "/api/v6/dex/balance/total-value-by-address")
        assert "OK-ACCESS-KEY" in headers
        assert "OK-ACCESS-SIGN" in headers
        assert "OK-ACCESS-TIMESTAMP" in headers
        assert "OK-ACCESS-PASSPHRASE" in headers
        assert headers["OK-ACCESS-KEY"] == "test-key"
        assert headers["OK-ACCESS-PASSPHRASE"] == "test-passphrase"

    def test_signature_is_deterministic(self, okx):
        """Same inputs produce the same HMAC signature."""
        ts = "2026-04-09T12:00:00.000Z"
        sig1 = okx._sign(ts, "GET", "/api/v6/dex/balance/total-value-by-address")
        sig2 = okx._sign(ts, "GET", "/api/v6/dex/balance/total-value-by-address")
        assert sig1 == sig2

    def test_signature_changes_with_path(self, okx):
        """Different paths produce different signatures."""
        ts = "2026-04-09T12:00:00.000Z"
        sig1 = okx._sign(ts, "GET", "/api/v6/dex/balance/total-value-by-address")
        sig2 = okx._sign(ts, "GET", "/api/v6/dex/balance/all-token-balances-by-address")
        assert sig1 != sig2

    def test_chain_id_mapping(self, okx):
        """Chain names map to correct numeric chain IDs."""
        assert okx._CHAIN_IDS["ethereum"] == "1"
        assert okx._CHAIN_IDS["arbitrum"] == "42161"
        assert okx._CHAIN_IDS["base"] == "8453"
        assert okx._CHAIN_IDS["polygon"] == "137"
        assert okx._CHAIN_IDS["avalanche"] == "43114"
        assert okx._CHAIN_IDS["solana"] == "501"

    # -------------------------------------------------------------------------
    # Response normalization tests
    # -------------------------------------------------------------------------

    def test_normalize_total_value(self, okx):
        """Total value response is normalized correctly."""
        payload = {
            "code": "0",
            "msg": "success",
            "data": [{"totalValue": "12345.67"}],
        }
        snapshot = okx._normalize_total_value("0xabc", "ethereum", payload)
        assert snapshot.provider == "okx"
        assert snapshot.wallet_address == "0xabc"
        assert snapshot.chain == "ethereum"
        assert snapshot.total_value_usd == "12345.67"
        assert snapshot.positions == []

    def test_normalize_total_value_empty_data(self, okx):
        """Empty data array returns zero total."""
        payload = {"code": "0", "msg": "success", "data": []}
        snapshot = okx._normalize_total_value("0xabc", "ethereum", payload)
        assert snapshot.total_value_usd == "0"

    @pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
    def test_normalize_total_value_non_finite(self, okx, bad_value):
        """Non-finite totalValue strings are sanitized to zero."""
        payload = {"code": "0", "msg": "success", "data": [{"totalValue": bad_value}]}
        snapshot = okx._normalize_total_value("0xabc", "ethereum", payload)
        assert snapshot.total_value_usd == "0"
        assert snapshot.provider == "okx"
        assert snapshot.wallet_address == "0xabc"
        assert snapshot.chain == "ethereum"

    @pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity", None])
    def test_safe_decimal_non_finite(self, bad_value):
        """Non-finite and None values return Decimal(0)."""
        assert OkxIntegration._safe_decimal(bad_value) == Decimal("0")

    def test_normalize_token_balances(self, okx):
        """Token balances response is normalized into positions."""
        payload = {
            "code": "0",
            "msg": "success",
            "data": [
                {
                    "tokenAssets": [
                        {
                            "chainIndex": "1",
                            "tokenContractAddress": "",
                            "symbol": "ETH",
                            "balance": "1.5",
                            "rawBalance": "1500000000000000000",
                            "tokenPrice": "2000.00",
                            "isRiskToken": False,
                            "address": "0xabc",
                        },
                        {
                            "chainIndex": "1",
                            "tokenContractAddress": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                            "symbol": "USDC",
                            "balance": "500.0",
                            "rawBalance": "500000000",
                            "tokenPrice": "1.00",
                            "isRiskToken": False,
                            "address": "0xabc",
                        },
                    ]
                }
            ],
        }
        snapshot = okx._normalize_token_balances("0xabc", "ethereum", payload)
        assert snapshot.provider == "okx"
        assert len(snapshot.positions) == 2

        eth_pos = snapshot.positions[0]
        assert eth_pos.position_id == "okx:native"
        assert eth_pos.label == "ETH"
        assert eth_pos.value_usd == "3000.000"
        assert eth_pos.token_symbols == ["ETH"]
        assert eth_pos.details["is_risk_token"] is False

        usdc_pos = snapshot.positions[1]
        assert usdc_pos.position_id == "okx:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        assert usdc_pos.label == "USDC"
        assert usdc_pos.value_usd == "500.000"

        # Total should be sum of positions
        assert snapshot.total_value_usd == "3500.000"

    def test_normalize_token_balances_with_risk_token(self, okx):
        """Risk tokens are included but flagged in details."""
        payload = {
            "code": "0",
            "msg": "success",
            "data": [
                {
                    "tokenAssets": [
                        {
                            "chainIndex": "1",
                            "tokenContractAddress": "0xscam",
                            "symbol": "SCAM",
                            "balance": "1000000",
                            "tokenPrice": "0.001",
                            "isRiskToken": True,
                            "address": "0xabc",
                        },
                    ]
                }
            ],
        }
        snapshot = okx._normalize_token_balances("0xabc", "ethereum", payload)
        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].details["is_risk_token"] is True

    def test_extract_data_invalid_payload(self, okx):
        """Invalid payloads return empty list."""
        assert okx._extract_data(None) == []
        assert okx._extract_data("not a dict") == []
        assert okx._extract_data({"code": "0"}) == []
        assert okx._extract_data({"data": "not a list"}) == []

    def test_extract_token_assets(self, okx):
        """Token assets are extracted from nested structure."""
        payload = {"data": [{"tokenAssets": [{"symbol": "ETH"}]}]}
        assert okx._extract_token_assets(payload) == [{"symbol": "ETH"}]

    def test_extract_token_assets_invalid(self, okx):
        """Invalid payloads return empty list for token assets."""
        assert okx._extract_token_assets(None) == []
        assert okx._extract_token_assets({"data": []}) == []
        assert okx._extract_token_assets({"data": [{}]}) == []

    def test_calc_usd_value_normal(self):
        """USD calculation works for normal values."""
        assert OkxIntegration._calc_usd_value("1.5", "2000.00") == "3000.000"

    def test_calc_usd_value_invalid(self):
        """USD calculation returns 0 for invalid inputs."""
        assert Decimal(OkxIntegration._calc_usd_value("invalid", "2000")) == 0
        assert Decimal(OkxIntegration._calc_usd_value("1.5", "invalid")) == 0

    # -------------------------------------------------------------------------
    # Caching tests
    # -------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_get_wallet_portfolio_caches_result(self, okx):
        """Portfolio results are cached."""
        mock_payload = {
            "code": "0",
            "msg": "success",
            "data": [{"totalValue": "5000.00"}],
        }

        with patch.object(okx, "_fetch", return_value=mock_payload) as fetch_mock:
            first = await okx.get_wallet_portfolio("0x1234567890123456789012345678901234567890", "arbitrum")
            second = await okx.get_wallet_portfolio("0x1234567890123456789012345678901234567890", "arbitrum")

        assert first.total_value_usd == "5000.00"
        assert second.cache_hit is True
        fetch_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_wallet_positions_caches_result(self, okx):
        """Position results are cached."""
        mock_payload = {
            "code": "0",
            "msg": "success",
            "data": [
                {
                    "tokenAssets": [
                        {
                            "chainIndex": "42161",
                            "tokenContractAddress": "",
                            "symbol": "ETH",
                            "balance": "1.0",
                            "tokenPrice": "2000.00",
                            "isRiskToken": False,
                            "address": "0xabc",
                        },
                    ]
                }
            ],
        }

        # Mock _fetch to return token payload for balance call, empty for DeFi call
        defi_empty = {"code": "0", "msg": "success", "data": []}

        with patch.object(okx, "_fetch", side_effect=[mock_payload, defi_empty]) as fetch_mock:
            first = await okx.get_wallet_positions("0x1234567890123456789012345678901234567890", "arbitrum")

        assert len(first.positions) == 1
        assert fetch_mock.call_count == 2  # token balances + DeFi platform list

        # Second call should hit cache
        second = await okx.get_wallet_positions("0x1234567890123456789012345678901234567890", "arbitrum")
        assert second.cache_hit is True

    @pytest.mark.asyncio
    async def test_get_wallet_positions_passes_correct_params(self, okx):
        """Fetch is called with correct endpoints for both tokens and DeFi."""
        mock_payload = {"code": "0", "msg": "success", "data": []}

        with patch.object(okx, "_fetch", return_value=mock_payload) as fetch_mock:
            await okx.get_wallet_positions("0xabc", "base")

        assert fetch_mock.call_count == 2
        # First call: token balances
        fetch_mock.assert_any_call(
            "/api/v6/dex/balance/all-token-balances-by-address",
            params={"address": "0xabc", "chains": "8453"},
        )
        # Second call: DeFi platform list
        fetch_mock.assert_any_call(
            "/api/v6/defi/user/asset/platform/list",
            method="POST",
            json_data={"walletAddressList": [{"chainIndex": "8453", "walletAddress": "0xabc"}]},
        )

    # -------------------------------------------------------------------------
    # DeFi API tests
    # -------------------------------------------------------------------------

    def test_extract_platforms(self, okx):
        """Platform list is extracted from DeFi response."""
        payload = {
            "code": "0",
            "data": [
                {
                    "walletIdPlatformList": [
                        {
                            "platformList": [
                                {"analysisPlatformId": "44", "platformName": "Aave V3"},
                                {"analysisPlatformId": "123", "platformName": "Uniswap V3"},
                            ]
                        }
                    ]
                }
            ],
        }
        platforms = okx._extract_platforms(payload, "42161")
        assert len(platforms) == 2
        assert platforms[0] == {"id": "44", "name": "Aave V3"}
        assert platforms[1] == {"id": "123", "name": "Uniswap V3"}

    def test_extract_platforms_empty(self, okx):
        """Empty response returns no platforms."""
        payload = {"code": "0", "data": []}
        assert okx._extract_platforms(payload, "1") == []

    def test_normalize_defi_details(self, okx):
        """DeFi detail response is normalized into positions."""
        platforms = [{"id": "44", "name": "Aave V3"}]
        payload = {
            "code": "0",
            "data": [
                {
                    "walletIdPlatformDetailList": [
                        {
                            "analysisPlatformId": "44",
                            "platformName": "Aave V3",
                            "networkHoldVoList": [
                                {
                                    "chainIndex": "42161",
                                    "investTokenBalanceVoList": [
                                        {
                                            "investmentName": "USDC Supply",
                                            "investmentId": "inv-1",
                                            "investType": 1,
                                            "totalValue": "500.00",
                                            "tokenList": [{"tokenSymbol": "USDC"}],
                                        },
                                        {
                                            "investmentName": "ETH/USDC Pool",
                                            "investmentId": "inv-2",
                                            "investType": 2,
                                            "totalValue": "1200.50",
                                            "tokenList": [
                                                {"tokenSymbol": "ETH"},
                                                {"tokenSymbol": "USDC"},
                                            ],
                                        },
                                    ],
                                    "availableRewards": [
                                        {
                                            "tokenSymbol": "ARB",
                                            "tokenAmount": "10.5",
                                            "currencyAmount": "8.25",
                                        },
                                    ],
                                }
                            ],
                        }
                    ]
                }
            ],
        }

        positions = okx._normalize_defi_details(payload, platforms)
        assert len(positions) == 3  # 2 investments + 1 reward

        # First position: lending (save)
        assert positions[0].protocol == "Aave V3"
        assert positions[0].label == "USDC Supply"
        assert positions[0].position_type == "save"
        assert positions[0].value_usd == "500.00"
        assert positions[0].token_symbols == ["USDC"]

        # Second position: LP pool
        assert positions[1].position_type == "pool"
        assert positions[1].value_usd == "1200.50"
        assert positions[1].token_symbols == ["ETH", "USDC"]

        # Third: reward
        assert positions[2].position_type == "reward"
        assert positions[2].value_usd == "8.25"
        assert positions[2].token_symbols == ["ARB"]
        assert positions[2].details["reward_amount"] == "10.5"

    def test_normalize_defi_zero_rewards_excluded(self, okx):
        """Rewards with zero value are not included."""
        platforms = [{"id": "1", "name": "Proto"}]
        payload = {
            "code": "0",
            "data": [
                {
                    "walletIdPlatformDetailList": [
                        {
                            "analysisPlatformId": "1",
                            "networkHoldVoList": [
                                {
                                    "chainIndex": "1",
                                    "investTokenBalanceVoList": [],
                                    "availableRewards": [
                                        {"tokenSymbol": "X", "tokenAmount": "0", "currencyAmount": "0"},
                                    ],
                                }
                            ],
                        }
                    ]
                }
            ],
        }
        positions = okx._normalize_defi_details(payload, platforms)
        assert len(positions) == 0

    @pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
    def test_normalize_defi_details_non_finite_total_value(self, okx, bad_value):
        """Non-finite totalValue in DeFi positions is sanitized."""
        platforms = [{"id": "1", "name": "Proto"}]
        payload = {
            "code": "0",
            "data": [
                {
                    "walletIdPlatformDetailList": [
                        {
                            "analysisPlatformId": "1",
                            "networkHoldVoList": [
                                {
                                    "chainIndex": "1",
                                    "investTokenBalanceVoList": [
                                        {
                                            "investmentName": "Test",
                                            "investmentId": "inv-1",
                                            "investType": 1,
                                            "totalValue": bad_value,
                                            "tokenList": [{"tokenSymbol": "USDC"}],
                                        },
                                    ],
                                }
                            ],
                        }
                    ]
                }
            ],
        }
        positions = okx._normalize_defi_details(payload, platforms)
        assert len(positions) == 1
        assert Decimal(positions[0].value_usd).is_finite()

    @pytest.mark.asyncio
    async def test_get_wallet_positions_merges_tokens_and_defi(self, okx):
        """get_wallet_positions merges token balances with DeFi positions."""
        token_payload = {
            "code": "0",
            "data": [{"tokenAssets": [
                {"chainIndex": "1", "tokenContractAddress": "", "symbol": "ETH",
                 "balance": "1.0", "tokenPrice": "2000", "isRiskToken": False},
            ]}],
        }
        defi_platform_payload = {
            "code": "0",
            "data": [{"walletIdPlatformList": [{"platformList": [
                {"analysisPlatformId": "44", "platformName": "Aave V3"},
            ]}]}],
        }
        defi_detail_payload = {
            "code": "0",
            "data": [{"walletIdPlatformDetailList": [{
                "analysisPlatformId": "44",
                "networkHoldVoList": [{"chainIndex": "1", "investTokenBalanceVoList": [
                    {"investmentName": "USDC Lend", "investmentId": "x", "investType": 1,
                     "totalValue": "500", "tokenList": [{"tokenSymbol": "USDC"}]},
                ]}],
            }]}],
        }

        with patch.object(okx, "_fetch", side_effect=[token_payload, defi_platform_payload, defi_detail_payload]):
            snap = await okx.get_wallet_positions("0xabc", "ethereum")

        assert len(snap.positions) == 2  # 1 token + 1 DeFi
        assert snap.positions[0].position_type == "token"
        assert snap.positions[0].label == "ETH"
        assert snap.positions[1].position_type == "save"
        assert snap.positions[1].protocol == "Aave V3"
        # Total = 2000 + 500
        assert "2500" in snap.total_value_usd

    # -------------------------------------------------------------------------
    # Schema-invalid response tests
    # -------------------------------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_body", [{}, [], {"msg": "ok"}])
    async def test_fetch_rejects_invalid_response_envelope(self, okx, bad_body):
        """HTTP 200 with missing 'code' key raises IntegrationError."""
        from contextlib import asynccontextmanager

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=bad_body)

        @asynccontextmanager
        async def fake_request(*args, **kwargs):
            yield mock_response

        mock_session = AsyncMock()
        mock_session.request = fake_request

        with patch.object(okx, "_get_session", AsyncMock(return_value=mock_session)), \
             patch.object(okx._rate_limiter, "acquire", AsyncMock(return_value=0)):
            with pytest.raises(IntegrationError, match="Invalid OKX response"):
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert okx._metrics.failed_requests > 0


class TestNormalizeDefiDetails:
    """Characterization tests for ``OkxIntegration._normalize_defi_details``.

    Phase 5e-chars (Track B gate): this class captures the CURRENT observable
    behavior of ``_normalize_defi_details`` before the Phase 5f refactor splits
    the 6-level nested loop into per-level extractors. Every assertion here is
    a contract-freeze: if the refactor changes any of these outputs without an
    explicit behavior-change note, the test must fail.

    Two documented latent bugs are asserted at their CURRENT (buggy) behavior:

    - Issue #1707: ``totalValue == "0"`` silently triggers a recompute from
      ``positionList``. Measured-zero positions are indistinguishable from
      "field missing". Current behavior: recompute. Tests flip when #1707 is fixed.
    - Issue #1708: Rewards can surface twice: once under
      ``positionList[].unclaimFeesDefiTokenInfo`` (position-level) and again
      under ``networkHoldVoList[].availableRewards`` (network-level). When the
      same reward token is present at both levels, the current implementation
      emits two rows. Current behavior: duplicate. Tests flip when #1708 is fixed.
    """

    @pytest.fixture
    def platforms(self) -> list[dict[str, str]]:
        """Minimal platform list mapping id -> protocol name."""
        return [
            {"id": "44", "name": "Aave V3"},
            {"id": "123", "name": "Uniswap V3"},
            {"id": "99", "name": "Compound V3"},
        ]

    # ------------------------------------------------------------------
    # Shape variants: dict vs list-of-entries
    # ------------------------------------------------------------------

    def test_real_api_shape_data_as_dict(self, platforms):
        """Real OKX API: ``data`` is a dict with a single entry.

        Fixture exercises the full pipeline: two platforms, investLogo-derived
        protocol/symbols, position-level rewards, and network-level rewards.
        """
        payload = json.loads((FIXTURES_DIR / "okx_defi_real_shape.json").read_text())
        positions = OkxIntegration._normalize_defi_details(payload, platforms)

        # 2 investments + 1 position-level reward (AAVE) + 1 network-level reward (ARB)
        # from the Aave platform, plus 1 investment from Uniswap = 4 rows total.
        assert len(positions) == 4

        aave_supply = positions[0]
        assert aave_supply.position_id == "okx:defi:44:aave-usdc-supply"
        assert aave_supply.protocol == "Aave"  # from investLogo.bottomRightLogoList
        assert aave_supply.label == "USDC Supply"
        assert aave_supply.position_type == "lending"  # investType 6
        assert aave_supply.value_usd == "1500.75"
        assert aave_supply.token_symbols == ["USDC"]
        assert aave_supply.pool_address == "0xaave-pool"
        assert aave_supply.details == {
            "invest_type": "lending",
            "invest_type_id": 6,
            "investment_id": "aave-usdc-supply",
            "chain_index": "42161",
            "platform_id": "44",
        }

        # Position-level reward (unclaimFeesDefiTokenInfo)
        aave_reward = positions[1]
        assert aave_reward.position_id == "okx:reward:44:aave-usdc-supply:AAVE"
        assert aave_reward.position_type == "reward"
        assert aave_reward.protocol == "Aave"
        assert aave_reward.value_usd == "22.50"
        assert aave_reward.token_symbols == ["AAVE"]
        assert aave_reward.details == {"reward_amount": "0.25", "chain_index": "42161"}

        # Network-level reward (availableRewards)
        arb_reward = positions[2]
        assert arb_reward.position_id == "okx:reward:44:ARB"
        assert arb_reward.position_type == "reward"
        # Network-level reward uses platform_names lookup, NOT investLogo.
        assert arb_reward.protocol == "Aave V3"
        assert arb_reward.value_usd == "15.50"
        assert arb_reward.token_symbols == ["ARB"]

        # Uniswap investment
        uni_pool = positions[3]
        assert uni_pool.protocol == "Uniswap"  # investLogo overrides platform_names
        assert uni_pool.position_type == "pool"  # investType 2
        assert uni_pool.token_symbols == ["ETH", "USDC"]
        assert uni_pool.pool_address == "0xuni-pool-token"  # tokenAddress fallback

    def test_list_of_entries_shape(self, platforms):
        """``data`` as a list of entries is handled identically to dict form."""
        payload = json.loads((FIXTURES_DIR / "okx_defi_list_shape.json").read_text())
        positions = OkxIntegration._normalize_defi_details(payload, platforms)

        # 1 investment + 2 position-level rewards (COMP, WETH)
        assert len(positions) == 3

        compound_market = positions[0]
        assert compound_market.position_id == "okx:defi:99:compound-usdc"
        assert compound_market.protocol == "Compound V3"  # platform_names lookup
        assert compound_market.position_type == "lending"
        # Fixture omits totalValue: _sum_position_values sums
        # 250.50 + 750.25 = 1000.75 across the two assetsTokenList entries.
        assert compound_market.value_usd == "1000.75"
        # No investLogo + no empty middleLogoList => tokenList fallback.
        assert compound_market.token_symbols == ["USDC"]

        comp_reward = positions[1]
        assert comp_reward.position_id == "okx:reward:99:compound-usdc:COMP"
        assert comp_reward.value_usd == "12.00"

        weth_reward = positions[2]
        assert weth_reward.position_id == "okx:reward:99:compound-usdc:WETH"
        assert weth_reward.value_usd == "18.00"

    # ------------------------------------------------------------------
    # Payload-envelope edge cases
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "payload",
        [
            None,
            "not a dict",
            [],
            42,
            {"code": "0"},                   # no 'data' key
            {"code": "0", "data": "nope"},   # data is neither dict nor list
            {"code": "0", "data": None},
            {"code": "0", "data": {}},       # dict without walletIdPlatformDetailList
            {"code": "0", "data": []},
        ],
    )
    def test_malformed_payload_returns_empty(self, platforms, payload):
        """Every non-dict or unusable-shape payload returns an empty list."""
        assert OkxIntegration._normalize_defi_details(payload, platforms) == []

    def test_empty_wallet_id_platform_detail_list(self, platforms):
        """Missing/empty walletIdPlatformDetailList yields no positions."""
        payload = {"code": "0", "data": {"walletIdPlatformDetailList": []}}
        assert OkxIntegration._normalize_defi_details(payload, platforms) == []

        payload = {"code": "0", "data": {"walletIdPlatformDetailList": None}}
        assert OkxIntegration._normalize_defi_details(payload, platforms) == []

    def test_empty_network_hold_list(self, platforms):
        """Missing/empty networkHoldVoList at a platform yields no positions."""
        payload = {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    {"analysisPlatformId": "44", "networkHoldVoList": []},
                    {"analysisPlatformId": "44", "networkHoldVoList": None},
                    {"analysisPlatformId": "44"},  # key absent entirely
                ]
            },
        }
        assert OkxIntegration._normalize_defi_details(payload, platforms) == []

    def test_empty_invest_token_balance_list(self, platforms):
        """Missing investTokenBalanceVoList skips BOTH investments and network-level rewards.

        The ``availableRewards`` loop lives below the ``investTokenBalanceVoList``
        guard in the inner network_hold body, so when the invest list is absent
        the ``continue`` fires and the network-level reward check is never reached.
        """
        payload = {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    {
                        "analysisPlatformId": "44",
                        "networkHoldVoList": [
                            {
                                "chainIndex": "1",
                                # investTokenBalanceVoList absent -> no invest rows,
                                # but availableRewards still scanned at network level.
                                "availableRewards": [
                                    {
                                        "tokenSymbol": "ARB",
                                        "tokenAmount": "5",
                                        "currencyAmount": "10",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
        }
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        # Current behavior: when investTokenBalanceVoList is absent, the inner
        # loop is skipped entirely via `continue`, which ALSO skips the
        # network-level availableRewards check (it lives inside the invest-list
        # loop body). Net: no rewards emitted.
        assert positions == []

    def test_non_list_nested_containers_skipped(self, platforms):
        """Nested containers that are not lists trigger the intermediate guards.

        Covers:
          - ``investTokenBalanceVoList`` as non-list (network_hold guard).
          - ``baseDefiTokenInfos`` as non-list (unclaim-fee guard).
        """
        payload = {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    {
                        "analysisPlatformId": "44",
                        "networkHoldVoList": [
                            # First hold: investTokenBalanceVoList is not a list.
                            {
                                "chainIndex": "1",
                                "investTokenBalanceVoList": "not-a-list",
                            },
                            # Second hold: valid invest, but baseDefiTokenInfos non-list.
                            {
                                "chainIndex": "1",
                                "investTokenBalanceVoList": [
                                    {
                                        "investmentName": "ok",
                                        "investmentId": "ok",
                                        "investType": 1,
                                        "totalValue": "1",
                                        "positionList": [
                                            {
                                                "unclaimFeesDefiTokenInfo": [
                                                    {"baseDefiTokenInfos": "not-a-list"},
                                                    {"baseDefiTokenInfos": None},
                                                ]
                                            }
                                        ],
                                    }
                                ],
                            },
                        ],
                    }
                ]
            },
        }
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        # Only the single valid investment row; no rewards.
        assert len(positions) == 1
        assert positions[0].label == "ok"

    def test_non_dict_entries_skipped_at_every_level(self, platforms):
        """Every nested ``if not isinstance(..., dict): continue`` guard is exercised.

        Mixing scalar/list sentinels into each nested container verifies the
        defensive filters at:
          - detail_list entry
          - network_hold entry
          - invest entry
          - position entry
          - fee_group entry
          - reward entry (both position-level and network-level)
        """
        payload = {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    "not-a-dict-detail",  # skipped
                    {
                        "analysisPlatformId": "44",
                        "networkHoldVoList": [
                            "not-a-dict-network",  # skipped
                            {
                                "chainIndex": "1",
                                "investTokenBalanceVoList": [
                                    "not-a-dict-invest",  # skipped
                                    {
                                        "investmentName": "real",
                                        "investmentId": "r",
                                        "investType": 1,
                                        "totalValue": "50",
                                        "positionList": [
                                            "not-a-dict-pos",  # skipped
                                            {
                                                "unclaimFeesDefiTokenInfo": [
                                                    "not-a-dict-fee-group",  # skipped
                                                    {
                                                        "baseDefiTokenInfos": [
                                                            "not-a-dict-reward",  # skipped
                                                            {
                                                                "tokenSymbol": "R",
                                                                "coinAmount": "1",
                                                                "currencyAmount": "2",
                                                            },
                                                        ]
                                                    },
                                                ]
                                            },
                                        ],
                                    },
                                ],
                                "availableRewards": [
                                    "not-a-dict-net-reward",  # skipped
                                    {
                                        "tokenSymbol": "N",
                                        "tokenAmount": "3",
                                        "currencyAmount": "4",
                                    },
                                ],
                            },
                        ],
                    },
                ]
            },
        }
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        # 1 investment + 1 position-level reward + 1 network-level reward.
        assert len(positions) == 3
        assert positions[0].label == "real"
        assert positions[1].token_symbols == ["R"]
        assert positions[2].token_symbols == ["N"]

    # ------------------------------------------------------------------
    # investLogo handling
    # ------------------------------------------------------------------

    def test_invest_logo_missing(self, platforms):
        """No investLogo: protocol falls back to platform_names, symbols to tokenList."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "no-logo",
                "investmentId": "x",
                "investType": 1,
                "totalValue": "50",
                "tokenList": [{"tokenSymbol": "USDC"}],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert len(positions) == 1
        assert positions[0].protocol == "Aave V3"  # platform_names lookup
        assert positions[0].token_symbols == ["USDC"]

    @pytest.mark.parametrize("non_dict_logo", ["string", 42, ["list"], None])
    def test_invest_logo_non_dict(self, platforms, non_dict_logo):
        """investLogo as non-dict falls back to platform_names + tokenList."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "bad-logo",
                "investmentId": "x",
                "investType": 1,
                "totalValue": "50",
                "investLogo": non_dict_logo,
                "tokenList": [{"tokenSymbol": "DAI"}],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert len(positions) == 1
        assert positions[0].protocol == "Aave V3"
        assert positions[0].token_symbols == ["DAI"]

    def test_invest_logo_only_bottom_right(self, platforms):
        """bottomRightLogoList sets protocol; missing middleLogoList triggers tokenList fallback."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "br-only",
                "investmentId": "x",
                "investType": 1,
                "totalValue": "50",
                "investLogo": {
                    "bottomRightLogoList": [{"tokenName": "Morpho"}],
                    # middleLogoList absent
                },
                "tokenList": [{"tokenSymbol": "USDT"}],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert positions[0].protocol == "Morpho"
        assert positions[0].token_symbols == ["USDT"]

    def test_invest_logo_only_middle(self, platforms):
        """middleLogoList sets symbols; missing bottomRightLogoList falls back to platform_names."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "mid-only",
                "investmentId": "x",
                "investType": 2,
                "totalValue": "50",
                "investLogo": {
                    "middleLogoList": [
                        {"tokenName": "WBTC"},
                        {"tokenName": "WETH"},
                    ],
                },
                "tokenList": [{"tokenSymbol": "SHOULD_NOT_APPEAR"}],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert positions[0].protocol == "Aave V3"  # platform_names fallback
        # middleLogoList wins over tokenList fallback.
        assert positions[0].token_symbols == ["WBTC", "WETH"]

    def test_invest_logo_both_present(self, platforms):
        """Both lists present: protocol from bottomRight, symbols from middle."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "both",
                "investmentId": "x",
                "investType": 2,
                "totalValue": "100",
                "investLogo": {
                    "bottomRightLogoList": [{"tokenName": "Balancer"}],
                    "middleLogoList": [
                        {"tokenName": "ETH"},
                        {"tokenName": "OP"},
                    ],
                },
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert positions[0].protocol == "Balancer"
        assert positions[0].token_symbols == ["ETH", "OP"]

    def test_token_list_fallback_when_middle_logo_empty(self, platforms):
        """Empty middleLogoList triggers tokenList fallback (even with a logo dict)."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "middle-empty",
                "investmentId": "x",
                "investType": 1,
                "totalValue": "10",
                "investLogo": {
                    "bottomRightLogoList": [{"tokenName": "Morpho"}],
                    "middleLogoList": [],  # empty -> fallback triggers
                },
                "tokenList": [{"tokenSymbol": "FRAX"}],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert positions[0].protocol == "Morpho"
        assert positions[0].token_symbols == ["FRAX"]

    # ------------------------------------------------------------------
    # Reward handling
    # ------------------------------------------------------------------

    def test_position_level_rewards_from_unclaim_fees(self, platforms):
        """positionList[].unclaimFeesDefiTokenInfo[] emits one reward row per non-zero reward."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "with-rewards",
                "investmentId": "y",
                "investType": 1,
                "totalValue": "100",
                "positionList": [
                    {
                        "unclaimFeesDefiTokenInfo": [
                            {
                                "baseDefiTokenInfos": [
                                    {
                                        "tokenSymbol": "REW1",
                                        "coinAmount": "1.0",
                                        "currencyAmount": "5.00",
                                    },
                                    {
                                        "tokenSymbol": "ZERO",
                                        "coinAmount": "0",
                                        "currencyAmount": "0",  # filtered out
                                    },
                                    {
                                        "tokenSymbol": "REW2",
                                        "coinAmount": "2.0",
                                        "currencyAmount": "3.00",
                                    },
                                ]
                            }
                        ]
                    }
                ],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        # 1 investment + 2 non-zero rewards
        assert len(positions) == 3
        reward_symbols = [p.token_symbols[0] for p in positions if p.position_type == "reward"]
        assert reward_symbols == ["REW1", "REW2"]

    def test_duplicate_rewards_at_network_and_position_level_issue_1708(self, platforms):
        """Issue #1708: same reward present at both levels produces two rows.

        Current (buggy) behavior: a reward token that appears in both
        ``positionList[].unclaimFeesDefiTokenInfo`` AND
        ``networkHoldVoList[].availableRewards`` is emitted twice with
        different position_ids. When #1708 is fixed, this test must flip to
        assert a single row.
        """
        payload = {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    {
                        "analysisPlatformId": "44",
                        "networkHoldVoList": [
                            {
                                "chainIndex": "1",
                                "investTokenBalanceVoList": [
                                    {
                                        "investmentName": "dup-reward",
                                        "investmentId": "inv-dup",
                                        "investType": 1,
                                        "totalValue": "100",
                                        "positionList": [
                                            {
                                                "unclaimFeesDefiTokenInfo": [
                                                    {
                                                        "baseDefiTokenInfos": [
                                                            {
                                                                "tokenSymbol": "ARB",
                                                                "coinAmount": "5",
                                                                "currencyAmount": "10",
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ],
                                    }
                                ],
                                "availableRewards": [
                                    {
                                        "tokenSymbol": "ARB",
                                        "tokenAmount": "5",
                                        "currencyAmount": "10",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
        }
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        rewards = [p for p in positions if p.position_type == "reward"]
        # Duplicate-by-design today: position-level + network-level rows coexist.
        assert len(rewards) == 2
        reward_ids = sorted(r.position_id for r in rewards)
        assert reward_ids == [
            "okx:reward:44:ARB",            # network-level row
            "okx:reward:44:inv-dup:ARB",    # position-level row
        ]

    def test_available_rewards_uses_token_amount_then_coin_amount(self, platforms):
        """availableRewards: ``tokenAmount`` preferred, falls back to ``coinAmount``."""
        payload = {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    {
                        "analysisPlatformId": "44",
                        "networkHoldVoList": [
                            {
                                "chainIndex": "1",
                                "investTokenBalanceVoList": [
                                    {"investmentName": "anchor", "investmentId": "a",
                                     "investType": 1, "totalValue": "1"}
                                ],
                                "availableRewards": [
                                    {"tokenSymbol": "A", "tokenAmount": "1.0", "currencyAmount": "5"},
                                    {"tokenSymbol": "B", "coinAmount": "2.0", "currencyAmount": "6"},
                                    {"tokenSymbol": "C", "currencyAmount": "7"},  # defaults to "0"
                                ],
                            }
                        ],
                    }
                ]
            },
        }
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        rewards = [p for p in positions if p.position_type == "reward"]
        assert len(rewards) == 3
        amounts = {p.token_symbols[0]: p.details["reward_amount"] for p in rewards}
        assert amounts == {"A": "1.0", "B": "2.0", "C": "0"}

    # ------------------------------------------------------------------
    # _sum_position_values fallback
    # ------------------------------------------------------------------

    def test_sum_position_values_fallback_when_total_value_missing(self, platforms):
        """totalValue absent -> _sum_position_values aggregates assets."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "summed",
                "investmentId": "s",
                "investType": 1,
                # totalValue omitted
                "positionList": [
                    {
                        "assetsTokenList": [
                            {"tokenSymbol": "A", "currencyAmount": "10.25"},
                            {"tokenSymbol": "B", "currencyAmount": "5.75"},
                        ]
                    }
                ],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert len(positions) == 1
        # 10.25 + 5.75 = 16.00
        assert Decimal(positions[0].value_usd) == Decimal("16.00")

    def test_measured_zero_total_value_silently_recomputes_issue_1707(self, platforms):
        """Issue #1707: ``totalValue == "0"`` triggers _sum_position_values recompute.

        Current (buggy) behavior: a measured-zero totalValue is indistinguishable
        from "field missing" and is silently overwritten by the sum of
        positionList assets. When #1707 is fixed, totalValue "0" must be
        preserved verbatim and this assertion must flip.
        """
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "zero-measured",
                "investmentId": "z",
                "investType": 1,
                "totalValue": "0",  # measured zero
                "positionList": [
                    {
                        "assetsTokenList": [
                            {"tokenSymbol": "A", "currencyAmount": "42.00"},
                        ]
                    }
                ],
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert len(positions) == 1
        # Current buggy behavior: totalValue "0" silently recomputed from assets.
        assert positions[0].value_usd == "42.00"

    # ------------------------------------------------------------------
    # investType label mapping
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "invest_type,expected_label",
        [
            (1, "save"),
            (2, "pool"),
            (3, "farm"),
            (4, "vault"),
            (5, "stake"),
            (6, "lending"),
            (7, "lock"),
            (8, "leveraged_farming"),
        ],
    )
    def test_known_invest_types_map_to_labels(self, platforms, invest_type, expected_label):
        """All 8 known investType IDs map to their canonical string labels."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "t",
                "investmentId": "t",
                "investType": invest_type,
                "totalValue": "1",
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert positions[0].position_type == expected_label
        assert positions[0].details["invest_type"] == expected_label
        assert positions[0].details["invest_type_id"] == invest_type

    def test_unknown_invest_type_produces_type_n_label(self, platforms):
        """Unknown investType ids produce the fallback ``type_<n>`` label."""
        payload = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "unknown",
                "investmentId": "u",
                "investType": 99,
                "totalValue": "1",
            },
        )
        positions = OkxIntegration._normalize_defi_details(payload, platforms)
        assert positions[0].position_type == "type_99"
        assert positions[0].details["invest_type_id"] == 99
        # investType missing entirely defaults to 0 -> "type_0".
        payload2 = self._single_invest_payload(
            platform_id="44",
            invest={
                "investmentName": "no-type",
                "investmentId": "v",
                "totalValue": "1",
            },
        )
        positions2 = OkxIntegration._normalize_defi_details(payload2, platforms)
        assert positions2[0].position_type == "type_0"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _single_invest_payload(*, platform_id: str, invest: dict, chain_index: str = "1") -> dict:
        """Build a minimal ``data: dict`` payload with a single invest row."""
        return {
            "code": "0",
            "data": {
                "walletIdPlatformDetailList": [
                    {
                        "analysisPlatformId": platform_id,
                        "networkHoldVoList": [
                            {
                                "chainIndex": chain_index,
                                "investTokenBalanceVoList": [invest],
                            }
                        ],
                    }
                ]
            },
        }
