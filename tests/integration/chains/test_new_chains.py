"""On-chain integration tests for new chain support (Blast, Mantle, Berachain).

Tests verify that the new chains can be forked with Anvil and that chain IDs
are correctly configured in both constants.py and config.py.

Note: These chains (Blast, Mantle, Berachain) are not supported by Alchemy
and cannot be auto-started by the test fixtures. To run these tests, start
Anvil manually:

    # Blast
    anvil --fork-url https://rpc.blast.io --chain-id 81457

    # Mantle
    anvil --fork-url https://rpc.mantle.xyz --chain-id 5000

    # Berachain
    anvil --fork-url https://rpc.berachain.com --chain-id 80094

Then run:
    uv run pytest tests/integration/chains/test_new_chains.py -v -s
"""

import subprocess

import pytest
from web3 import Web3

from almanak.core.constants import CHAIN_IDS as CORE_CHAIN_IDS
from almanak.core.constants import Chain, get_chain_id
from almanak.framework.execution.config import CHAIN_IDS as CONFIG_CHAIN_IDS

# =============================================================================
# Constants
# =============================================================================

# Default Anvil RPC (for manually started Anvil)
DEFAULT_ANVIL_RPC = "http://localhost:8545"

# Chain configuration for new chains
NEW_CHAINS = {
    "blast": {
        "chain_id": 81457,
        "rpc_url": "https://rpc.blast.io",
        "native_token": "ETH",
        "chain_enum": Chain.BLAST,
    },
    "mantle": {
        "chain_id": 5000,
        "rpc_url": "https://rpc.mantle.xyz",
        "native_token": "MNT",
        "chain_enum": Chain.MANTLE,
    },
    "berachain": {
        "chain_id": 80094,
        "rpc_url": "https://rpc.berachain.com",
        "native_token": "BERA",
        "chain_enum": Chain.BERACHAIN,
    },
}


# =============================================================================
# Helper Functions
# =============================================================================


def is_anvil_running(rpc_url: str = DEFAULT_ANVIL_RPC) -> bool:
    """Check if Anvil is running and responding."""
    try:
        web3 = Web3(Web3.HTTPProvider(rpc_url))
        return web3.is_connected()
    except Exception:
        return False


def get_anvil_chain_id(rpc_url: str = DEFAULT_ANVIL_RPC) -> int | None:
    """Get the chain ID from running Anvil instance."""
    try:
        web3 = Web3(Web3.HTTPProvider(rpc_url))
        if web3.is_connected():
            return web3.eth.chain_id
        return None
    except Exception:
        return None


def fund_native_token(wallet: str, amount_wei: int, rpc_url: str = DEFAULT_ANVIL_RPC) -> None:
    """Fund a wallet with native token using Anvil RPC."""
    amount_hex = hex(amount_wei)
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, amount_hex, "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


# =============================================================================
# Unit Tests for Chain ID Consistency
# =============================================================================


class TestChainIDConsistency:
    """Tests verifying that chain IDs are consistent across the codebase."""

    def test_blast_chain_id_in_core_constants(self):
        """Verify Blast chain ID is correctly defined in core/constants.py."""
        assert Chain.BLAST in CORE_CHAIN_IDS, "Chain.BLAST should be in CORE_CHAIN_IDS"
        assert CORE_CHAIN_IDS[Chain.BLAST] == 81457, (
            f"Blast chain ID should be 81457, got {CORE_CHAIN_IDS[Chain.BLAST]}"
        )

    def test_mantle_chain_id_in_core_constants(self):
        """Verify Mantle chain ID is correctly defined in core/constants.py."""
        assert Chain.MANTLE in CORE_CHAIN_IDS, "Chain.MANTLE should be in CORE_CHAIN_IDS"
        assert CORE_CHAIN_IDS[Chain.MANTLE] == 5000, (
            f"Mantle chain ID should be 5000, got {CORE_CHAIN_IDS[Chain.MANTLE]}"
        )

    def test_berachain_chain_id_in_core_constants(self):
        """Verify Berachain chain ID is correctly defined in core/constants.py."""
        assert Chain.BERACHAIN in CORE_CHAIN_IDS, "Chain.BERACHAIN should be in CORE_CHAIN_IDS"
        assert CORE_CHAIN_IDS[Chain.BERACHAIN] == 80094, (
            f"Berachain chain ID should be 80094, got {CORE_CHAIN_IDS[Chain.BERACHAIN]}"
        )

    def test_blast_chain_id_in_config(self):
        """Verify Blast chain ID is correctly defined in execution/config.py."""
        assert "blast" in CONFIG_CHAIN_IDS, "blast should be in CONFIG_CHAIN_IDS"
        assert CONFIG_CHAIN_IDS["blast"] == 81457, f"Blast chain ID should be 81457, got {CONFIG_CHAIN_IDS['blast']}"

    def test_mantle_chain_id_in_config(self):
        """Verify Mantle chain ID is correctly defined in execution/config.py."""
        assert "mantle" in CONFIG_CHAIN_IDS, "mantle should be in CONFIG_CHAIN_IDS"
        assert CONFIG_CHAIN_IDS["mantle"] == 5000, f"Mantle chain ID should be 5000, got {CONFIG_CHAIN_IDS['mantle']}"

    def test_berachain_chain_id_in_config(self):
        """Verify Berachain chain ID is correctly defined in execution/config.py."""
        assert "berachain" in CONFIG_CHAIN_IDS, "berachain should be in CONFIG_CHAIN_IDS"
        assert CONFIG_CHAIN_IDS["berachain"] == 80094, (
            f"Berachain chain ID should be 80094, got {CONFIG_CHAIN_IDS['berachain']}"
        )

    def test_blast_chain_ids_match(self):
        """Verify Blast chain ID matches between constants.py and config.py."""
        core_id = CORE_CHAIN_IDS[Chain.BLAST]
        config_id = CONFIG_CHAIN_IDS["blast"]
        assert core_id == config_id, f"Blast chain ID mismatch: constants.py={core_id}, config.py={config_id}"

    def test_mantle_chain_ids_match(self):
        """Verify Mantle chain ID matches between constants.py and config.py."""
        core_id = CORE_CHAIN_IDS[Chain.MANTLE]
        config_id = CONFIG_CHAIN_IDS["mantle"]
        assert core_id == config_id, f"Mantle chain ID mismatch: constants.py={core_id}, config.py={config_id}"

    def test_berachain_chain_ids_match(self):
        """Verify Berachain chain ID matches between constants.py and config.py."""
        core_id = CORE_CHAIN_IDS[Chain.BERACHAIN]
        config_id = CONFIG_CHAIN_IDS["berachain"]
        assert core_id == config_id, f"Berachain chain ID mismatch: constants.py={core_id}, config.py={config_id}"

    def test_get_chain_id_function_blast(self):
        """Verify get_chain_id() works for Blast with various inputs."""
        # Test with Chain enum
        assert get_chain_id(Chain.BLAST) == 81457

        # Test with string
        assert get_chain_id("blast") == 81457
        assert get_chain_id("BLAST") == 81457
        assert get_chain_id("Blast") == 81457

        # Test with int (passthrough)
        assert get_chain_id(81457) == 81457

    def test_get_chain_id_function_mantle(self):
        """Verify get_chain_id() works for Mantle with various inputs."""
        # Test with Chain enum
        assert get_chain_id(Chain.MANTLE) == 5000

        # Test with string
        assert get_chain_id("mantle") == 5000
        assert get_chain_id("MANTLE") == 5000
        assert get_chain_id("Mantle") == 5000

        # Test with int (passthrough)
        assert get_chain_id(5000) == 5000

    def test_get_chain_id_function_berachain(self):
        """Verify get_chain_id() works for Berachain with various inputs."""
        # Test with Chain enum
        assert get_chain_id(Chain.BERACHAIN) == 80094

        # Test with string
        assert get_chain_id("berachain") == 80094
        assert get_chain_id("BERACHAIN") == 80094
        assert get_chain_id("Berachain") == 80094

        # Test alias
        assert get_chain_id("bera") == 80094
        assert get_chain_id("BERA") == 80094

        # Test with int (passthrough)
        assert get_chain_id(80094) == 80094


# =============================================================================
# On-Chain Integration Tests (Require Anvil)
# =============================================================================


@pytest.mark.blast
class TestBlastFork:
    """On-chain integration tests for Blast mainnet fork.

    Requires Anvil running with:
        anvil --fork-url https://rpc.blast.io --chain-id 81457
    """

    def test_blast_fork(self):
        """
        Test: Fork Blast mainnet and verify chain ID 81457.

        Validates:
        1. Anvil is running and connected
        2. Chain ID is 81457 (Blast mainnet)
        3. Can query block number (fork is functional)
        4. Can fund a wallet with native ETH
        """
        if not is_anvil_running(DEFAULT_ANVIL_RPC):
            pytest.skip("Anvil is not running. Start with: anvil --fork-url https://rpc.blast.io --chain-id 81457")

        web3 = Web3(Web3.HTTPProvider(DEFAULT_ANVIL_RPC))
        chain_id = web3.eth.chain_id

        # Skip if not on Blast fork
        if chain_id != 81457:
            pytest.skip(
                f"Anvil must be forked from Blast mainnet (chain ID 81457). Current chain ID: {chain_id}. Start with: anvil --fork-url https://rpc.blast.io --chain-id 81457"
            )

        # Verify chain ID matches expected
        assert chain_id == 81457, f"Expected chain ID 81457, got {chain_id}"
        print(f"\nBlast fork verified: chain ID = {chain_id}")

        # Verify fork is functional by checking block number
        block_number = web3.eth.block_number
        assert block_number > 0, "Block number should be positive"
        print(f"Current block number: {block_number}")

        # Test wallet funding (proves Anvil cheat codes work)
        test_wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        fund_amount = 100 * 10**18  # 100 ETH

        fund_native_token(test_wallet, fund_amount, DEFAULT_ANVIL_RPC)
        balance = web3.eth.get_balance(Web3.to_checksum_address(test_wallet))

        assert balance >= fund_amount, f"Wallet should have at least {fund_amount} wei, got {balance}"
        print(f"Test wallet funded with {balance / 10**18:.2f} ETH")

        # Verify chain ID matches our constants
        assert chain_id == CORE_CHAIN_IDS[Chain.BLAST], "Chain ID should match CORE_CHAIN_IDS"
        assert chain_id == CONFIG_CHAIN_IDS["blast"], "Chain ID should match CONFIG_CHAIN_IDS"


@pytest.mark.mantle
class TestMantleFork:
    """On-chain integration tests for Mantle mainnet fork.

    Requires Anvil running with:
        anvil --fork-url https://rpc.mantle.xyz --chain-id 5000
    """

    def test_mantle_fork(self):
        """
        Test: Fork Mantle mainnet and verify chain ID 5000.

        Validates:
        1. Anvil is running and connected
        2. Chain ID is 5000 (Mantle mainnet)
        3. Can query block number (fork is functional)
        4. Can fund a wallet with native MNT
        """
        if not is_anvil_running(DEFAULT_ANVIL_RPC):
            pytest.skip("Anvil is not running. Start with: anvil --fork-url https://rpc.mantle.xyz --chain-id 5000")

        web3 = Web3(Web3.HTTPProvider(DEFAULT_ANVIL_RPC))
        chain_id = web3.eth.chain_id

        # Skip if not on Mantle fork
        if chain_id != 5000:
            pytest.skip(
                f"Anvil must be forked from Mantle mainnet (chain ID 5000). Current chain ID: {chain_id}. Start with: anvil --fork-url https://rpc.mantle.xyz --chain-id 5000"
            )

        # Verify chain ID matches expected
        assert chain_id == 5000, f"Expected chain ID 5000, got {chain_id}"
        print(f"\nMantle fork verified: chain ID = {chain_id}")

        # Verify fork is functional by checking block number
        block_number = web3.eth.block_number
        assert block_number > 0, "Block number should be positive"
        print(f"Current block number: {block_number}")

        # Test wallet funding (proves Anvil cheat codes work)
        test_wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        fund_amount = 100 * 10**18  # 100 MNT

        fund_native_token(test_wallet, fund_amount, DEFAULT_ANVIL_RPC)
        balance = web3.eth.get_balance(Web3.to_checksum_address(test_wallet))

        assert balance >= fund_amount, f"Wallet should have at least {fund_amount} wei, got {balance}"
        print(f"Test wallet funded with {balance / 10**18:.2f} MNT")

        # Verify chain ID matches our constants
        assert chain_id == CORE_CHAIN_IDS[Chain.MANTLE], "Chain ID should match CORE_CHAIN_IDS"
        assert chain_id == CONFIG_CHAIN_IDS["mantle"], "Chain ID should match CONFIG_CHAIN_IDS"


@pytest.mark.berachain
class TestBerachainFork:
    """On-chain integration tests for Berachain mainnet fork.

    Requires Anvil running with:
        anvil --fork-url https://rpc.berachain.com --chain-id 80094
    """

    def test_berachain_fork(self):
        """
        Test: Fork Berachain mainnet and verify chain ID 80094.

        Validates:
        1. Anvil is running and connected
        2. Chain ID is 80094 (Berachain mainnet)
        3. Can query block number (fork is functional)
        4. Can fund a wallet with native BERA
        """
        if not is_anvil_running(DEFAULT_ANVIL_RPC):
            pytest.skip("Anvil is not running. Start with: anvil --fork-url https://rpc.berachain.com --chain-id 80094")

        web3 = Web3(Web3.HTTPProvider(DEFAULT_ANVIL_RPC))
        chain_id = web3.eth.chain_id

        # Skip if not on Berachain fork
        if chain_id != 80094:
            pytest.skip(
                f"Anvil must be forked from Berachain mainnet (chain ID 80094). Current chain ID: {chain_id}. Start with: anvil --fork-url https://rpc.berachain.com --chain-id 80094"
            )

        # Verify chain ID matches expected
        assert chain_id == 80094, f"Expected chain ID 80094, got {chain_id}"
        print(f"\nBerachain fork verified: chain ID = {chain_id}")

        # Verify fork is functional by checking block number
        block_number = web3.eth.block_number
        assert block_number > 0, "Block number should be positive"
        print(f"Current block number: {block_number}")

        # Test wallet funding (proves Anvil cheat codes work)
        test_wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        fund_amount = 100 * 10**18  # 100 BERA

        fund_native_token(test_wallet, fund_amount, DEFAULT_ANVIL_RPC)
        balance = web3.eth.get_balance(Web3.to_checksum_address(test_wallet))

        assert balance >= fund_amount, f"Wallet should have at least {fund_amount} wei, got {balance}"
        print(f"Test wallet funded with {balance / 10**18:.2f} BERA")

        # Verify chain ID matches our constants
        assert chain_id == CORE_CHAIN_IDS[Chain.BERACHAIN], "Chain ID should match CORE_CHAIN_IDS"
        assert chain_id == CONFIG_CHAIN_IDS["berachain"], "Chain ID should match CONFIG_CHAIN_IDS"


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
