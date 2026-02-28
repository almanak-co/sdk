"""Fixtures for Base intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances.
"""

import pytest
from web3 import Web3

from tests.conftest_gateway import AnvilFixture
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.execution.signer import LocalKeySigner
from almanak.framework.execution.simulator import DirectSimulator
from almanak.framework.execution.submitter import PublicMempoolSubmitter
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    TEST_PRIVATE_KEY,
    TEST_SUBMITTER_MAX_RETRIES,
    TEST_TX_TIMEOUT_SECONDS,
    TEST_WALLET,
    TEST_WEB3_REQUEST_TIMEOUT,
    _wrap_native_token,
    fund_erc20_token,
    fund_native_token,
    get_token_decimals,
)

CHAIN_NAME = "base"
REQUIRED_CHAIN_ID = 8453


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for Base on the current fork instance."""
    config = CHAIN_CONFIGS[CHAIN_NAME]

    # Fund with 100 native tokens
    fund_native_token(TEST_WALLET, 100 * 10**18, rpc_url)

    # Fund with common tokens
    for token_symbol, token_address in config.get("tokens", {}).items():
        balance_slot = config.get("balance_slots", {}).get(token_symbol)
        if balance_slot is not None:
            try:
                decimals = get_token_decimals(web3, token_address)
                # Use wrapping for wrapped native tokens (more reliable than storage slot manipulation)
                if token_symbol in ("WETH", "WAVAX", "WMATIC", "WBNB"):
                    wrap_amount = 10 * (10**decimals)
                    _wrap_native_token(TEST_WALLET, token_address, wrap_amount, rpc_url)
                else:
                    amount = 100_000 * (10**decimals)
                    fund_erc20_token(TEST_WALLET, token_address, amount, balance_slot, rpc_url)
            except Exception as e:
                print(f"Warning: Could not fund {token_symbol}: {e}")

    return TEST_WALLET


@pytest.fixture(scope="module")
def anvil_instance(anvil_base: AnvilFixture) -> AnvilFixture:
    """Expose the chain-specific AnvilFixture for shared recovery logic."""
    return anvil_base


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_base: AnvilFixture) -> str:
    """Get the Anvil RPC URL for Base chain."""
    return f"http://127.0.0.1:{anvil_base.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for Base."""
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))
    assert w3.is_connected(), f"Anvil not responding at {anvil_rpc_url}"
    actual_id = w3.eth.chain_id
    assert actual_id == REQUIRED_CHAIN_ID, f"Expected chain {REQUIRED_CHAIN_ID}, got {actual_id}"
    return w3


@pytest.fixture(scope="module")
def test_private_key() -> str:
    """Return test private key."""
    return TEST_PRIVATE_KEY


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3, anvil_rpc_url: str) -> str:
    """Fund the test wallet with native token and common ERC20s."""
    return _seed_wallet_state(web3, anvil_rpc_url)


@pytest.fixture(scope="module")
def reseed_wallet_state(web3: Web3, anvil_instance: AnvilFixture):
    """Return a callable that re-seeds balances on demand (for fork recovery)."""

    def _reseed() -> str:
        return _seed_wallet_state(web3, anvil_instance.get_rpc_url())

    return _reseed


@pytest.fixture
def orchestrator(test_private_key: str, anvil_rpc_url: str) -> ExecutionOrchestrator:
    """Create ExecutionOrchestrator for testing."""
    signer = LocalKeySigner(private_key=test_private_key)
    submitter = PublicMempoolSubmitter(
        rpc_url=anvil_rpc_url,
        max_retries=TEST_SUBMITTER_MAX_RETRIES,
        timeout_seconds=TEST_TX_TIMEOUT_SECONDS,
    )
    simulator = DirectSimulator()

    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain=CHAIN_NAME,
        rpc_url=anvil_rpc_url,
        tx_timeout_seconds=TEST_TX_TIMEOUT_SECONDS,
    )
