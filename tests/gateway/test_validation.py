"""Tests for gateway validation utilities.

These tests verify that the validation module correctly rejects malicious inputs
and prevents injection attacks.
"""

import pytest

from almanak.gateway.validation import (
    ALLOWED_CHAINS,
    MAX_BATCH_SIZE,
    MAX_GRAPHQL_QUERY_LENGTH,
    MAX_STATE_SIZE_BYTES,
    ValidationError,
    validate_address,
    validate_batch_size,
    validate_chain,
    validate_deployment_id,
    validate_graphql_query,
    validate_positive_int,
    validate_rpc_method,
    validate_state_size,
    validate_symbol,
    validate_token_id,
    validate_tx_hash,
)


class TestChainValidation:
    """Tests for chain validation."""

    def test_valid_chains(self):
        """Test that valid chains pass validation."""
        for chain in ALLOWED_CHAINS:
            assert validate_chain(chain) == chain.lower()

    def test_chain_normalization(self):
        """Test that chains are normalized to lowercase."""
        assert validate_chain("ETHEREUM") == "ethereum"
        assert validate_chain("Arbitrum") == "arbitrum"
        assert validate_chain("  base  ") == "base"

    def test_invalid_chain_rejected(self):
        """Test that invalid chains are rejected."""
        with pytest.raises(ValidationError) as exc:
            validate_chain("invalid_chain")
        assert "not allowed" in str(exc.value)

    def test_injection_attempt_chain(self):
        """Test that injection attempts in chain are rejected."""
        injection_attempts = [
            "ethereum; DROP TABLE users;",
            "arbitrum\n--",
            "base' OR '1'='1",
            "../../../etc/passwd",
            "ethereum<script>alert(1)</script>",
        ]
        for attempt in injection_attempts:
            with pytest.raises(ValidationError):
                validate_chain(attempt)

    def test_empty_chain_rejected(self):
        """Test that empty chain is rejected."""
        with pytest.raises(ValidationError):
            validate_chain("")


class TestAllowedChainsTrustBoundary:
    """Pin ``ALLOWED_CHAINS`` against an explicit historical snapshot.

    VIB-4801 derives ``ALLOWED_CHAINS`` from ``ChainRegistry.all()`` so the
    gateway allowlist tracks the single source of truth for supported
    chains. This is convenient, but it also means a *future* descriptor-only
    chain addition would implicitly widen the gateway trust boundary
    without any explicit gateway review.

    This guard pins the set against a snapshot of the pre-VIB-4801,
    hand-maintained literal. Any drift (added chain, removed chain) trips
    the test — adding a new chain requires deliberately editing this
    snapshot, which forces an explicit gateway-side acknowledgement.

    Mirror test under ``tests/unit/core/test_chain_registry.py`` covers the
    same invariant from the registry side; this is the gateway-directory
    surface so reviewers of ``almanak/gateway/`` see the boundary guard
    next to the validation code it protects.
    """

    # Pre-VIB-4801 hand-maintained set from almanak/gateway/validation.py.
    HISTORICAL_ALLOWED_CHAINS = frozenset(
        {
            "ethereum",
            "arbitrum",
            "base",
            "optimism",
            "polygon",
            "avalanche",
            "bsc",
            "sonic",
            "plasma",
            "linea",
            "blast",
            "mantle",
            "berachain",
            "solana",
            "monad",
            "xlayer",
            "zerog",
            "hyperevm",
        }
    )

    def test_allowed_chains_matches_historical_snapshot(self):
        """ALLOWED_CHAINS must be byte-identical to the pre-VIB-4801 set.

        If this test fails it means a chain was added or removed from the
        registry without an explicit update here. Adding a chain to the
        gateway boundary is a security review event — update the snapshot
        deliberately, do not silently let the registry widen the
        allowlist.
        """
        added = ALLOWED_CHAINS - self.HISTORICAL_ALLOWED_CHAINS
        removed = self.HISTORICAL_ALLOWED_CHAINS - ALLOWED_CHAINS
        assert ALLOWED_CHAINS == self.HISTORICAL_ALLOWED_CHAINS, (
            f"ALLOWED_CHAINS drifted from the pre-VIB-4801 snapshot. "
            f"Added: {sorted(added)}; removed: {sorted(removed)}. "
            f"Adding a chain widens the gateway trust boundary — review "
            f"the new chain's descriptor and update HISTORICAL_ALLOWED_CHAINS "
            f"in this test deliberately."
        )


class TestAddressValidation:
    """Tests for Ethereum address validation."""

    def test_valid_address(self):
        """Test that valid addresses pass validation."""
        valid = "0x1234567890123456789012345678901234567890"
        assert validate_address(valid) == valid

    def test_invalid_address_format(self):
        """Test that invalid address formats are rejected."""
        invalid_addresses = [
            "1234567890123456789012345678901234567890",  # Missing 0x
            "0x123456789012345678901234567890123456789",  # Too short
            "0x12345678901234567890123456789012345678901",  # Too long
            "0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG",  # Invalid hex
        ]
        for addr in invalid_addresses:
            with pytest.raises(ValidationError):
                validate_address(addr)

    def test_injection_attempt_address(self):
        """Test that injection attempts in address are rejected."""
        injection_attempts = [
            "0x1234567890123456789012345678901234567890; DROP TABLE",
            "0x1234567890123456789012345678901234567890\n--",
            "0x1234567890123456789012345678901234567890' OR '1'='1",
        ]
        for attempt in injection_attempts:
            with pytest.raises(ValidationError):
                validate_address(attempt)


class TestDeploymentIdValidation:
    """Tests for deployment ID validation."""

    def test_valid_deployment_ids(self):
        """Test that valid deployment IDs pass validation."""
        valid_ids = [
            "my-strategy",
            "strategy_123",
            "Test-Strategy_v2",
            "a" * 128,  # Max length
        ]
        for sid in valid_ids:
            assert validate_deployment_id(sid) == sid

    def test_invalid_deployment_id_format(self):
        """Test that invalid deployment ID formats are rejected."""
        invalid_ids = [
            "strategy with spaces",
            " strategy",
            "strategy ",
            "strategy.with.dots",
            "strategy/with/slashes",
            "a" * 129,  # Too long
        ]
        for sid in invalid_ids:
            with pytest.raises(ValidationError):
                validate_deployment_id(sid)

    def test_path_traversal_deployment_id(self):
        """Test that path traversal attempts are rejected."""
        traversal_attempts = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
            "strategy/../secret",
        ]
        for attempt in traversal_attempts:
            with pytest.raises(ValidationError):
                validate_deployment_id(attempt)

    def test_sql_injection_deployment_id(self):
        """Test that SQL injection attempts are rejected."""
        injection_attempts = [
            "strategy'; DROP TABLE strategies; --",
            "1 OR 1=1",
            "strategy UNION SELECT * FROM users",
        ]
        for attempt in injection_attempts:
            with pytest.raises(ValidationError):
                validate_deployment_id(attempt)


class TestRpcMethodValidation:
    """Tests for RPC method validation."""

    def test_allowed_methods(self):
        """Test that allowed RPC methods pass validation."""
        allowed = [
            "eth_call",
            "eth_getBalance",
            "eth_blockNumber",
            "eth_sendRawTransaction",
        ]
        for method in allowed:
            assert validate_rpc_method(method) == method

    def test_dangerous_methods_rejected(self):
        """Test that dangerous RPC methods are rejected."""
        dangerous_methods = [
            "debug_traceTransaction",
            "admin_addPeer",
            "personal_unlockAccount",
            "miner_start",
            "eth_signTransaction",  # Could expose private key usage
        ]
        for method in dangerous_methods:
            with pytest.raises(ValidationError) as exc:
                validate_rpc_method(method)
            assert "not allowed" in str(exc.value)

    def test_arbitrary_method_rejected(self):
        """Test that arbitrary method names are rejected."""
        arbitrary_methods = [
            "custom_method",
            "exploit_vulnerability",
            "steal_keys",
        ]
        for method in arbitrary_methods:
            with pytest.raises(ValidationError):
                validate_rpc_method(method)


class TestSymbolValidation:
    """Tests for trading symbol validation."""

    def test_valid_symbols(self):
        """Test that valid symbols pass validation."""
        valid_symbols = ["BTCUSDT", "ETHUSDC", "BTC", "ETH"]
        for symbol in valid_symbols:
            assert validate_symbol(symbol) == symbol.upper()

    def test_symbol_normalization(self):
        """Test that symbols are normalized to uppercase."""
        assert validate_symbol("btcusdt") == "BTCUSDT"
        assert validate_symbol("  ethusdc  ") == "ETHUSDC"

    def test_invalid_symbol_format(self):
        """Test that invalid symbol formats are rejected."""
        invalid_symbols = [
            "BTC-USDT",  # Contains dash
            "BTC_USDT",  # Contains underscore
            "BTC USDT",  # Contains space
            "A" * 21,  # Too long
        ]
        for symbol in invalid_symbols:
            with pytest.raises(ValidationError):
                validate_symbol(symbol)

    def test_injection_attempt_symbol(self):
        """Test that injection attempts in symbol are rejected."""
        injection_attempts = [
            "BTCUSDT; DROP TABLE",
            "BTCUSDT\n--",
            "BTCUSDT' OR '1'='1",
        ]
        for attempt in injection_attempts:
            with pytest.raises(ValidationError):
                validate_symbol(attempt)


class TestTokenIdValidation:
    """Tests for CoinGecko token ID validation."""

    def test_valid_token_ids(self):
        """Test that valid token IDs pass validation."""
        valid_ids = ["ethereum", "bitcoin", "usd-coin", "wrapped-bitcoin"]
        for tid in valid_ids:
            assert validate_token_id(tid) == tid.lower()

    def test_token_id_normalization(self):
        """Test that token IDs are normalized to lowercase."""
        assert validate_token_id("ETHEREUM") == "ethereum"
        assert validate_token_id("  Bitcoin  ") == "bitcoin"

    def test_invalid_token_id_format(self):
        """Test that invalid token ID formats are rejected."""
        invalid_ids = [
            "token_with_underscore",
            "token.with.dots",
            "token with spaces",
            "a" * 65,  # Too long
        ]
        for tid in invalid_ids:
            with pytest.raises(ValidationError):
                validate_token_id(tid)


class TestTxHashValidation:
    """Tests for transaction hash validation."""

    def test_valid_tx_hash(self):
        """Test that valid tx hashes pass validation."""
        valid = "0x" + "a" * 64
        assert validate_tx_hash(valid) == valid

    def test_invalid_tx_hash_format(self):
        """Test that invalid tx hash formats are rejected."""
        invalid_hashes = [
            "a" * 64,  # Missing 0x
            "0x" + "a" * 63,  # Too short
            "0x" + "a" * 65,  # Too long
            "0x" + "g" * 64,  # Invalid hex
        ]
        for h in invalid_hashes:
            with pytest.raises(ValidationError):
                validate_tx_hash(h)


class TestStateSizeValidation:
    """Tests for state size validation."""

    def test_valid_state_size(self):
        """Test that valid state sizes pass validation."""
        data = b"x" * 1000
        assert validate_state_size(data) == data

    def test_max_state_size(self):
        """Test that max state size is enforced."""
        # Just under limit should pass
        data = b"x" * (MAX_STATE_SIZE_BYTES - 1)
        assert validate_state_size(data) == data

        # Over limit should fail
        data = b"x" * (MAX_STATE_SIZE_BYTES + 1)
        with pytest.raises(ValidationError) as exc:
            validate_state_size(data)
        assert "exceeds maximum" in str(exc.value)


class TestBatchSizeValidation:
    """Tests for batch size validation."""

    def test_valid_batch_size(self):
        """Test that valid batch sizes pass validation."""
        items = list(range(50))
        assert validate_batch_size(items) == items

    def test_max_batch_size(self):
        """Test that max batch size is enforced."""
        # At limit should pass
        items = list(range(MAX_BATCH_SIZE))
        assert validate_batch_size(items) == items

        # Over limit should fail
        items = list(range(MAX_BATCH_SIZE + 1))
        with pytest.raises(ValidationError) as exc:
            validate_batch_size(items)
        assert "exceeds maximum" in str(exc.value)


class TestGraphQLQueryValidation:
    """Tests for GraphQL query validation."""

    def test_valid_query(self):
        """Test that valid queries pass validation."""
        query = "{ pools(first: 10) { id token0 { symbol } } }"
        assert validate_graphql_query(query) == query

    def test_query_length_limit(self):
        """Test that query length limit is enforced."""
        # Just under limit should pass
        query = "a" * (MAX_GRAPHQL_QUERY_LENGTH - 1)
        assert validate_graphql_query(query) == query

        # Over limit should fail
        query = "a" * (MAX_GRAPHQL_QUERY_LENGTH + 1)
        with pytest.raises(ValidationError) as exc:
            validate_graphql_query(query)
        assert "exceeds maximum" in str(exc.value)

    def test_introspection_blocked(self):
        """Test that introspection queries are blocked."""
        introspection_queries = [
            "{ __schema { types { name } } }",
            '{ __type(name: "User") { fields { name } } }',
            "query IntrospectionQuery { __schema { queryType { name } } }",
        ]
        for query in introspection_queries:
            with pytest.raises(ValidationError) as exc:
                validate_graphql_query(query)
            assert "introspection" in str(exc.value).lower()

    def test_empty_query_rejected(self):
        """Test that empty query is rejected."""
        with pytest.raises(ValidationError):
            validate_graphql_query("")


class TestPositiveIntValidation:
    """Tests for positive integer validation."""

    def test_valid_positive_int(self):
        """Test that valid positive integers pass validation."""
        assert validate_positive_int(0, "field") == 0
        assert validate_positive_int(100, "field") == 100

    def test_negative_int_rejected(self):
        """Test that negative integers are rejected."""
        with pytest.raises(ValidationError) as exc:
            validate_positive_int(-1, "field")
        assert "non-negative" in str(exc.value)

    def test_max_value_enforced(self):
        """Test that max value is enforced when specified."""
        assert validate_positive_int(100, "field", max_value=100) == 100

        with pytest.raises(ValidationError) as exc:
            validate_positive_int(101, "field", max_value=100)
        assert "exceeds maximum" in str(exc.value)


# NOTE: ``resolve_deployment_id`` was removed in VIB-4722 — per blueprint 29 the
# gateway no longer translates identity (the runner passes the canonical
# ``deployment_id`` and every SQL filters it directly). The deployment-mode
# env contract is covered by tests/unit/deployment/test_deployment.py.
