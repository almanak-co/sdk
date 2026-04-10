"""Tests for OKX OnchainOS portfolio integration."""

from unittest.mock import patch

import pytest

from almanak.gateway.integrations.okx import OkxIntegration


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
        okx = OkxIntegration(api_key=None, api_secret=None, api_passphrase=None)
        assert okx.is_configured is False

    def test_not_configured_partial_credentials(self, monkeypatch):
        """Integration reports not configured with partial credentials."""
        monkeypatch.delenv("OKX_API_SECRET", raising=False)
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
        assert OkxIntegration._calc_usd_value("invalid", "2000") == "0"
        assert OkxIntegration._calc_usd_value("1.5", "invalid") == "0"

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
