"""Tests for Solana-specific gateway validation (VIB-369).

Verifies:
1. is_solana_chain() helper correctly identifies Solana family chains
2. validate_rpc_method() dispatches to Solana allowlist when chain="solana"
3. validate_rpc_method() still uses EVM allowlist when chain is None or EVM
4. validate_tx_hash() accepts base58 signatures for Solana chains
5. validate_tx_hash() rejects EVM hex hashes for Solana chains (and vice versa)
6. validate_address_for_chain() dispatches correctly
"""

import pytest

from almanak.gateway.validation import (
    ALLOWED_RPC_METHODS,
    ALLOWED_SOLANA_RPC_METHODS,
    SOLANA_TX_SIGNATURE_PATTERN,
    ValidationError,
    is_solana_chain,
    validate_address_for_chain,
    validate_rpc_method,
    validate_tx_hash,
)

# --- Real-world test fixtures ---

# Valid Solana transaction signature (base58, 88 chars)
VALID_SOLANA_SIGNATURE = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXvA"
# Shorter valid base58 signature (87 chars)
VALID_SOLANA_SIGNATURE_87 = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXv"
# Valid EVM tx hash
VALID_EVM_TX_HASH = "0x" + "a1" * 32
# Valid Solana address (base58, 32-44 chars)
VALID_SOLANA_ADDRESS = "So11111111111111111111111111111111111111112"
# Valid EVM address
VALID_EVM_ADDRESS = "0x" + "ab" * 20


class TestIsSolanaChain:
    """is_solana_chain() helper function."""

    def test_solana_returns_true(self):
        assert is_solana_chain("solana") is True

    def test_solana_case_insensitive(self):
        assert is_solana_chain("Solana") is True
        assert is_solana_chain("SOLANA") is True

    def test_solana_with_whitespace(self):
        assert is_solana_chain("  solana  ") is True

    def test_evm_chains_return_false(self):
        for chain in ("arbitrum", "ethereum", "base", "optimism", "polygon"):
            assert is_solana_chain(chain) is False, f"Expected False for {chain}"

    def test_empty_string_returns_false(self):
        assert is_solana_chain("") is False

    def test_unknown_chain_returns_false(self):
        assert is_solana_chain("unknown_chain") is False


class TestValidateRpcMethodSolana:
    """validate_rpc_method() with chain-aware Solana dispatch."""

    def test_solana_method_accepted_for_solana_chain(self):
        """Solana RPC methods pass when chain='solana'."""
        for method in ("getBalance", "getAccountInfo", "getTransaction", "sendTransaction", "getSlot"):
            result = validate_rpc_method(method, chain="solana")
            assert result == method

    def test_all_solana_methods_accepted(self):
        """Every method in ALLOWED_SOLANA_RPC_METHODS passes."""
        for method in ALLOWED_SOLANA_RPC_METHODS:
            result = validate_rpc_method(method, chain="solana")
            assert result == method

    def test_evm_method_rejected_for_solana_chain(self):
        """EVM-specific methods are rejected when chain='solana'."""
        with pytest.raises(ValidationError, match="not allowed for Solana"):
            validate_rpc_method("eth_call", chain="solana")

    def test_debug_method_rejected_for_solana_chain(self):
        """Dangerous methods are blocked for Solana too."""
        with pytest.raises(ValidationError, match="not allowed for Solana"):
            validate_rpc_method("debug_traceTransaction", chain="solana")

    def test_evm_method_accepted_for_evm_chain(self):
        """EVM methods still work when chain='arbitrum'."""
        result = validate_rpc_method("eth_call", chain="arbitrum")
        assert result == "eth_call"

    def test_solana_method_rejected_for_evm_chain(self):
        """Solana methods are rejected for EVM chains."""
        with pytest.raises(ValidationError, match="not allowed"):
            validate_rpc_method("getBalance", chain="arbitrum")

    def test_no_chain_uses_evm_allowlist(self):
        """When chain is None, uses default EVM allowlist."""
        result = validate_rpc_method("eth_call")
        assert result == "eth_call"

    def test_no_chain_rejects_solana_methods(self):
        """When chain is None, Solana methods are rejected."""
        with pytest.raises(ValidationError, match="not allowed"):
            validate_rpc_method("getBalance")

    def test_empty_method_rejected(self):
        """Empty method raises ValidationError regardless of chain."""
        with pytest.raises(ValidationError, match="required"):
            validate_rpc_method("", chain="solana")


class TestValidateTxHashSolana:
    """validate_tx_hash() with chain-aware Solana dispatch."""

    def test_solana_signature_accepted(self):
        """Valid 88-char base58 signature passes for Solana."""
        result = validate_tx_hash(VALID_SOLANA_SIGNATURE, chain="solana")
        assert result == VALID_SOLANA_SIGNATURE

    def test_solana_signature_87_chars_accepted(self):
        """Valid 87-char base58 signature passes for Solana."""
        result = validate_tx_hash(VALID_SOLANA_SIGNATURE_87, chain="solana")
        assert result == VALID_SOLANA_SIGNATURE_87

    def test_evm_hex_rejected_for_solana(self):
        """EVM 0x-prefixed hash is rejected for Solana chain."""
        with pytest.raises(ValidationError, match="Solana signature format"):
            validate_tx_hash(VALID_EVM_TX_HASH, chain="solana")

    def test_short_base58_rejected_for_solana(self):
        """Too-short base58 string rejected for Solana."""
        with pytest.raises(ValidationError, match="Solana signature format"):
            validate_tx_hash("shortSig123", chain="solana")

    def test_evm_hash_accepted_for_evm_chain(self):
        """EVM hex hash passes for EVM chains."""
        result = validate_tx_hash(VALID_EVM_TX_HASH, chain="arbitrum")
        assert result == VALID_EVM_TX_HASH

    def test_solana_sig_rejected_for_evm_chain(self):
        """Solana base58 signature rejected for EVM chains."""
        with pytest.raises(ValidationError, match="invalid format"):
            validate_tx_hash(VALID_SOLANA_SIGNATURE, chain="arbitrum")

    def test_no_chain_uses_evm_validation(self):
        """When chain is None, uses EVM hex validation."""
        result = validate_tx_hash(VALID_EVM_TX_HASH)
        assert result == VALID_EVM_TX_HASH

    def test_empty_hash_rejected(self):
        """Empty tx hash raises ValidationError."""
        with pytest.raises(ValidationError, match="required"):
            validate_tx_hash("", chain="solana")

    def test_solana_signature_pattern_rejects_invalid_base58(self):
        """Base58 pattern rejects chars not in the base58 alphabet (0, O, I, l)."""
        # '0' is not in base58
        invalid = "0" * 88
        assert SOLANA_TX_SIGNATURE_PATTERN.match(invalid) is None

        # 'O' (uppercase O) is not in base58
        invalid_O = "O" * 88
        assert SOLANA_TX_SIGNATURE_PATTERN.match(invalid_O) is None

        # 'I' (uppercase I) is not in base58
        invalid_I = "I" * 88
        assert SOLANA_TX_SIGNATURE_PATTERN.match(invalid_I) is None

        # 'l' (lowercase L) is not in base58
        invalid_l = "l" * 88
        assert SOLANA_TX_SIGNATURE_PATTERN.match(invalid_l) is None


class TestValidateAddressForChainSolana:
    """validate_address_for_chain() Solana vs EVM dispatch."""

    def test_solana_address_accepted(self):
        result = validate_address_for_chain(VALID_SOLANA_ADDRESS, "solana")
        assert result == VALID_SOLANA_ADDRESS

    def test_evm_address_rejected_for_solana(self):
        with pytest.raises(ValidationError, match="Solana address format"):
            validate_address_for_chain(VALID_EVM_ADDRESS, "solana")

    def test_evm_address_accepted_for_evm(self):
        result = validate_address_for_chain(VALID_EVM_ADDRESS, "arbitrum")
        assert result == VALID_EVM_ADDRESS

    def test_solana_address_rejected_for_evm(self):
        with pytest.raises(ValidationError, match="invalid address format"):
            validate_address_for_chain(VALID_SOLANA_ADDRESS, "arbitrum")


class TestSolanaRpcMethodCoverage:
    """Verify critical Solana RPC methods are in the allowlist."""

    @pytest.mark.parametrize(
        "method",
        [
            "getBalance",
            "getAccountInfo",
            "getTransaction",
            "sendTransaction",
            "simulateTransaction",
            "getLatestBlockhash",
            "getSlot",
            "getBlockHeight",
            "getTokenAccountBalance",
            "getTokenAccountsByOwner",
            "getSignatureStatuses",
            "getRecentPrioritizationFees",
            "getMinimumBalanceForRentExemption",
        ],
    )
    def test_critical_solana_method_in_allowlist(self, method):
        """Critical Solana methods must be in the allowlist."""
        assert method in ALLOWED_SOLANA_RPC_METHODS


class TestEvmRpcUnchanged:
    """Ensure EVM RPC validation is unchanged by Solana additions."""

    def test_evm_methods_still_accepted_without_chain(self):
        for method in ("eth_call", "eth_getBalance", "eth_blockNumber", "eth_sendRawTransaction"):
            result = validate_rpc_method(method)
            assert result == method

    def test_evm_methods_still_accepted_with_evm_chain(self):
        for method in ("eth_call", "eth_getLogs", "eth_estimateGas"):
            result = validate_rpc_method(method, chain="arbitrum")
            assert result == method

    def test_dangerous_methods_still_blocked(self):
        for method in ("debug_traceTransaction", "admin_addPeer", "personal_sendTransaction"):
            with pytest.raises(ValidationError):
                validate_rpc_method(method)

    def test_evm_tx_hash_still_works(self):
        result = validate_tx_hash(VALID_EVM_TX_HASH)
        assert result == VALID_EVM_TX_HASH
