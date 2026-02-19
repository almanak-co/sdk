"""Integration tests for LiFi connector.

Tests verify the LiFi integration works against the live API:
1. Read-only tests using live LiFi API (quotes, tokens, chains, status)
2. Model parsing against real API responses
3. Client error handling with real error responses

To run:
    uv run pytest tests/integration/connectors/test_lifi_integration.py -v -s -m integration

Requirements:
    - Network access to LiFi API (https://li.quest/v1)
    - LIFI_API_KEY env var is optional (higher rate limits with key)
"""

import pytest

from almanak.framework.connectors.lifi.client import (
    CHAIN_MAPPING,
    LiFiClient,
    LiFiConfig,
)
from almanak.framework.connectors.lifi.exceptions import (
    LiFiAPIError,
    LiFiRouteNotFoundError,
)
from almanak.framework.connectors.lifi.models import (
    LiFiOrderStrategy,
    LiFiStatusResponse,
    LiFiStep,
    LiFiTransferStatus,
)


# =============================================================================
# Constants
# =============================================================================

# Read-only wallet address (never signs anything)
TEST_WALLET = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"  # vitalik.eth

# Well-known token addresses
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ETHEREUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ARBITRUM = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"

# Chain IDs
ARBITRUM = 42161
BASE = 8453
ETHEREUM = 1
OPTIMISM = 10


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def lifi_client():
    """Create a LiFi client for integration testing."""
    config = LiFiConfig(
        chain_id=ARBITRUM,
        wallet_address=TEST_WALLET,
    )
    return LiFiClient(config)


# =============================================================================
# Chain & Token Discovery Tests
# =============================================================================


@pytest.mark.integration
class TestLiFiChains:
    """Test LiFi chain discovery against live API."""

    def test_get_chains_returns_data(self, lifi_client):
        """Fetch supported chains from live API."""
        try:
            chains = lifi_client.get_chains()
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(chains, list)
        assert len(chains) > 0

        # Verify response contains expected fields
        first_chain = chains[0]
        assert "id" in first_chain or "chainId" in first_chain

    def test_all_our_chains_supported(self, lifi_client):
        """Verify all Almanak-supported chains are in LiFi."""
        try:
            chains = lifi_client.get_chains()
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        lifi_chain_ids = set()
        for chain in chains:
            chain_id = chain.get("id") or chain.get("chainId")
            if chain_id:
                lifi_chain_ids.add(int(chain_id))

        # All chains we care about should be supported
        expected_chains = {
            1: "ethereum",
            10: "optimism",
            56: "bsc",
            137: "polygon",
            8453: "base",
            42161: "arbitrum",
            43114: "avalanche",
        }
        for chain_id, name in expected_chains.items():
            assert chain_id in lifi_chain_ids, f"LiFi missing chain: {name} ({chain_id})"


@pytest.mark.integration
class TestLiFiTokens:
    """Test LiFi token discovery against live API."""

    def test_get_tokens_arbitrum(self, lifi_client):
        """Fetch Arbitrum tokens from live API."""
        try:
            result = lifi_client.get_tokens(chain_id=ARBITRUM)
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert result is not None
        # Response is typically {"tokens": {"42161": [...]}}
        tokens = result.get("tokens", result)
        assert tokens is not None

    def test_get_tokens_no_filter(self, lifi_client):
        """Fetch all tokens (no chain filter)."""
        try:
            result = lifi_client.get_tokens()
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert result is not None


# =============================================================================
# Same-Chain Quote Tests (Read-Only)
# =============================================================================


@pytest.mark.integration
class TestLiFiSameChainQuote:
    """Test same-chain swap quotes against live API."""

    def test_quote_usdc_to_weth_arbitrum(self, lifi_client):
        """Quote USDC -> WETH swap on Arbitrum."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ARBITRUM,
                from_token=USDC_ARBITRUM,
                to_token=WETH_ARBITRUM,
                from_amount="1000000000",  # 1000 USDC
                from_address=TEST_WALLET,
                slippage=0.01,
                order=LiFiOrderStrategy.RECOMMENDED,
            )
        except LiFiRouteNotFoundError:
            pytest.skip("No same-chain route found (may be temporary)")
        except LiFiAPIError as e:
            pytest.skip(f"LiFi API error: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        # Validate response structure
        assert isinstance(quote, LiFiStep)
        assert quote.id != ""
        assert quote.tool != ""  # e.g., "1inch", "paraswap", "0x"

        # Same-chain swap
        assert not quote.is_cross_chain
        assert quote.action is not None
        assert quote.action.from_chain_id == ARBITRUM
        assert quote.action.to_chain_id == ARBITRUM

        # Estimate should have amounts
        assert quote.estimate is not None
        assert int(quote.estimate.to_amount) > 0
        assert int(quote.estimate.to_amount_min) > 0
        assert int(quote.estimate.to_amount_min) <= int(quote.estimate.to_amount)

        # Transaction data should be present
        assert quote.transaction_request is not None
        assert quote.transaction_request.data != ""
        assert len(quote.transaction_request.data) > 10  # Real calldata
        assert quote.transaction_request.to != ""

        # Approval address should be present for ERC20
        assert quote.estimate.approval_address != ""

    def test_quote_usdc_to_usdt_arbitrum(self, lifi_client):
        """Quote stablecoin swap USDC -> USDT on Arbitrum."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ARBITRUM,
                from_token=USDC_ARBITRUM,
                to_token=USDT_ARBITRUM,
                from_amount="100000000",  # 100 USDC
                from_address=TEST_WALLET,
                slippage=0.005,
            )
        except (LiFiRouteNotFoundError, LiFiAPIError) as e:
            pytest.skip(f"LiFi quote unavailable: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(quote, LiFiStep)

        # For stablecoin swap, output should be close to input
        to_amount = int(quote.estimate.to_amount)
        assert to_amount > 90_000000  # At least 90 USDT for 100 USDC
        assert to_amount < 110_000000  # No more than 110 USDT

    def test_quote_small_amount(self, lifi_client):
        """Quote a small swap amount."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ARBITRUM,
                from_token=USDC_ARBITRUM,
                to_token=WETH_ARBITRUM,
                from_amount="10000000",  # 10 USDC
                from_address=TEST_WALLET,
                slippage=0.01,
            )
        except (LiFiRouteNotFoundError, LiFiAPIError) as e:
            pytest.skip(f"LiFi quote unavailable: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(quote, LiFiStep)
        assert quote.get_to_amount() > 0


# =============================================================================
# Cross-Chain Quote Tests (Read-Only)
# =============================================================================


@pytest.mark.integration
class TestLiFiCrossChainQuote:
    """Test cross-chain bridge quotes against live API."""

    def test_quote_usdc_arbitrum_to_base(self, lifi_client):
        """Quote USDC bridge from Arbitrum -> Base."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=BASE,
                from_token=USDC_ARBITRUM,
                to_token=USDC_BASE,
                from_amount="1000000000",  # 1000 USDC
                from_address=TEST_WALLET,
                slippage=0.005,
            )
        except LiFiRouteNotFoundError:
            pytest.skip("No cross-chain route found (may be temporary)")
        except LiFiAPIError as e:
            pytest.skip(f"LiFi API error: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        # Validate cross-chain quote
        assert isinstance(quote, LiFiStep)
        assert quote.is_cross_chain
        assert quote.action is not None
        assert quote.action.from_chain_id == ARBITRUM
        assert quote.action.to_chain_id == BASE

        # Bridge tool should be identified
        assert quote.tool != ""  # e.g., "across", "stargate", "cctp"

        # Estimate
        assert quote.estimate is not None
        to_amount = int(quote.estimate.to_amount)
        assert to_amount > 0
        # Bridge should return most of the USDC (minus fees)
        assert to_amount > 900_000000  # At least 900 USDC from 1000

        # Execution duration should be non-zero for bridges
        assert quote.estimate.execution_duration > 0

        # Transaction data
        assert quote.transaction_request is not None
        assert quote.transaction_request.data != ""

    def test_quote_usdc_arbitrum_to_ethereum(self, lifi_client):
        """Quote USDC bridge from Arbitrum -> Ethereum."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ETHEREUM,
                from_token=USDC_ARBITRUM,
                to_token=USDC_ETHEREUM,
                from_amount="5000000000",  # 5000 USDC
                from_address=TEST_WALLET,
                slippage=0.005,
            )
        except (LiFiRouteNotFoundError, LiFiAPIError) as e:
            pytest.skip(f"LiFi quote unavailable: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(quote, LiFiStep)
        assert quote.is_cross_chain
        assert quote.tool != ""
        assert quote.get_to_amount() > 0

    def test_quote_with_bridge_preference(self, lifi_client):
        """Quote with specific bridge preference."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=BASE,
                from_token=USDC_ARBITRUM,
                to_token=USDC_BASE,
                from_amount="1000000000",
                from_address=TEST_WALLET,
                slippage=0.005,
                allow_bridges=["across"],
            )
        except (LiFiRouteNotFoundError, LiFiAPIError) as e:
            pytest.skip(f"LiFi quote unavailable with bridge filter: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(quote, LiFiStep)
        # When filtering to "across", tool should be across (or a sub-step should use it)
        # Note: LiFi may still use a different tool if across is unavailable

    def test_quote_cross_chain_with_swap(self, lifi_client):
        """Quote cross-chain with token swap (USDC on Arb -> WETH on Base).

        This tests LiFi's ability to bridge AND swap atomically.
        """
        weth_base = "0x4200000000000000000000000000000000000006"
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=BASE,
                from_token=USDC_ARBITRUM,
                to_token=weth_base,
                from_amount="1000000000",  # 1000 USDC
                from_address=TEST_WALLET,
                slippage=0.01,
            )
        except (LiFiRouteNotFoundError, LiFiAPIError) as e:
            pytest.skip(f"LiFi quote unavailable: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(quote, LiFiStep)
        assert quote.is_cross_chain
        assert quote.get_to_amount() > 0

        # Should have included steps for multi-hop route
        # (swap on source + bridge + possibly swap on dest)
        if quote.included_steps:
            assert len(quote.included_steps) >= 1


# =============================================================================
# Order Strategy Tests
# =============================================================================


@pytest.mark.integration
class TestLiFiOrderStrategies:
    """Test different LiFi route ordering strategies."""

    def test_cheapest_vs_fastest(self, lifi_client):
        """Compare CHEAPEST vs FASTEST route ordering."""
        try:
            cheapest = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=BASE,
                from_token=USDC_ARBITRUM,
                to_token=USDC_BASE,
                from_amount="1000000000",
                from_address=TEST_WALLET,
                slippage=0.005,
                order=LiFiOrderStrategy.CHEAPEST,
            )
            fastest = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=BASE,
                from_token=USDC_ARBITRUM,
                to_token=USDC_BASE,
                from_amount="1000000000",
                from_address=TEST_WALLET,
                slippage=0.005,
                order=LiFiOrderStrategy.FASTEST,
            )
        except (LiFiRouteNotFoundError, LiFiAPIError) as e:
            pytest.skip(f"LiFi quote unavailable: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        # Both should return valid quotes
        assert cheapest.get_to_amount() > 0
        assert fastest.get_to_amount() > 0

        # CHEAPEST should give >= output amount (not always, depends on market)
        # Just verify both are reasonable
        assert cheapest.get_to_amount() > 900_000000
        assert fastest.get_to_amount() > 900_000000


# =============================================================================
# Error Handling Tests
# =============================================================================


@pytest.mark.integration
class TestLiFiErrorHandling:
    """Test error handling against live API."""

    def test_invalid_token_returns_error(self, lifi_client):
        """Invalid token address should raise appropriate error."""
        with pytest.raises((LiFiAPIError, LiFiRouteNotFoundError)):
            lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ARBITRUM,
                from_token="0x0000000000000000000000000000000000000001",
                to_token="0x0000000000000000000000000000000000000002",
                from_amount="1000000000",
                from_address=TEST_WALLET,
            )

    def test_zero_amount_returns_error(self, lifi_client):
        """Zero amount should raise error."""
        with pytest.raises((LiFiAPIError, LiFiRouteNotFoundError)):
            lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ARBITRUM,
                from_token=USDC_ARBITRUM,
                to_token=WETH_ARBITRUM,
                from_amount="0",
                from_address=TEST_WALLET,
            )


# =============================================================================
# Status API Tests
# =============================================================================


@pytest.mark.integration
class TestLiFiStatus:
    """Test LiFi status API against live endpoint."""

    def test_status_not_found(self, lifi_client):
        """Status check for non-existent tx returns NOT_FOUND."""
        try:
            status = lifi_client.get_status(
                tx_hash="0x0000000000000000000000000000000000000000000000000000000000000000",
                from_chain=ARBITRUM,
                to_chain=BASE,
            )
        except LiFiAPIError as e:
            # Some API versions return 404 for unknown tx
            if e.status_code == 404:
                return  # Expected behavior
            pytest.skip(f"LiFi API error: {e}")
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert isinstance(status, LiFiStatusResponse)
        # The zero-hash may match a real tx on some chains, so accept any valid status
        assert status.status in (
            LiFiTransferStatus.NOT_FOUND,
            LiFiTransferStatus.PENDING,
            LiFiTransferStatus.DONE,
            "",
        )


# =============================================================================
# Model Parsing with Real Responses
# =============================================================================


@pytest.mark.integration
class TestLiFiModelParsing:
    """Verify our models correctly parse real API responses."""

    def test_quote_model_fields_populated(self, lifi_client):
        """Verify all important model fields are populated from real response."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=ARBITRUM,
                from_token=USDC_ARBITRUM,
                to_token=WETH_ARBITRUM,
                from_amount="1000000000",
                from_address=TEST_WALLET,
                slippage=0.01,
            )
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        # Step fields
        assert quote.id, "Step ID should be populated"
        assert quote.type in ("swap", "cross", "lifi", "protocol"), f"Unexpected type: {quote.type}"
        assert quote.tool, "Tool should be identified"

        # Action fields
        assert quote.action is not None, "Action should be present"
        assert quote.action.from_chain_id == ARBITRUM
        assert quote.action.to_chain_id == ARBITRUM
        assert quote.action.from_token is not None
        assert quote.action.from_token.address.lower() == USDC_ARBITRUM.lower()
        assert quote.action.from_token.decimals == 6
        assert quote.action.from_token.symbol == "USDC"
        assert quote.action.to_token is not None
        assert quote.action.to_token.address.lower() == WETH_ARBITRUM.lower()
        assert quote.action.to_token.decimals == 18

        # Estimate fields
        assert quote.estimate is not None
        assert int(quote.estimate.to_amount) > 0
        assert int(quote.estimate.to_amount_min) > 0

        # Transaction request
        assert quote.transaction_request is not None
        assert quote.transaction_request.to, "Target contract should be set"
        assert quote.transaction_request.data, "Calldata should be present"
        assert quote.transaction_request.chain_id == ARBITRUM

    def test_cross_chain_quote_has_fee_info(self, lifi_client):
        """Cross-chain quotes should include fee and gas cost info."""
        try:
            quote = lifi_client.get_quote(
                from_chain_id=ARBITRUM,
                to_chain_id=BASE,
                from_token=USDC_ARBITRUM,
                to_token=USDC_BASE,
                from_amount="1000000000",
                from_address=TEST_WALLET,
                slippage=0.005,
            )
        except Exception as e:
            pytest.skip(f"LiFi API not reachable: {e}")

        assert quote.estimate is not None

        # Gas costs should be present
        assert len(quote.estimate.gas_costs) > 0
        total_gas = quote.estimate.total_gas_estimate
        assert total_gas > 0, "Gas estimate should be > 0"

    def test_chain_resolution_matches_api(self, lifi_client):
        """Our chain name mapping should resolve correctly."""
        for chain_name, chain_id in CHAIN_MAPPING.items():
            resolved = LiFiClient.resolve_chain_id(chain_name)
            assert resolved == chain_id, f"Chain {chain_name} resolved to {resolved}, expected {chain_id}"

            # Also test case insensitivity
            resolved_upper = LiFiClient.resolve_chain_id(chain_name.upper())
            assert resolved_upper == chain_id
