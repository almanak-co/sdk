"""Fixtures for Monad intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances. Mirrors the
Base conftest pattern — see tests/intents/base/conftest.py for the reference.

Monad-specific notes:
- Native token is MON; WMON is the wrapped-native ERC20 (WETH9-style).
- WETH on Monad is bridged from Ethereum (NOT native-wrappable).
- Public RPC ``https://rpc.monad.xyz`` is used as the fork source; Alchemy's
  Monad mainnet endpoint requires per-app enablement on the dashboard.
"""

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.execution.signer import LocalKeySigner
from almanak.framework.execution.simulator import DirectSimulator
from almanak.framework.execution.submitter import PublicMempoolSubmitter
from tests.conftest_gateway import AnvilFixture
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    TEST_PRIVATE_KEY,
    TEST_SUBMITTER_MAX_RETRIES,
    TEST_TX_TIMEOUT_SECONDS,
    TEST_WALLET,
    TEST_WEB3_REQUEST_TIMEOUT,
    _retry_rpc_call,
    _wrap_native_token,
    fund_erc20_token,
    fund_native_token,
    get_token_decimals,
    make_intent_test_web3,
    seed_wallet_state_with_recovery,
)

CHAIN_NAME = "monad"
REQUIRED_CHAIN_ID = 143

# Tokens that wrap native MON — fund via deposit rather than storage-slot manipulation.
_NATIVE_WRAPPERS = {"WMON"}


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for Monad on the current fork instance."""
    config = CHAIN_CONFIGS[CHAIN_NAME]

    # Clear any delegation code on the test wallet (guard against unusual EIP-7702
    # state on forks — mirrors Base seed logic).
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    wallet_code = w3.eth.get_code(Web3.to_checksum_address(TEST_WALLET))
    if len(wallet_code) > 0:
        _retry_rpc_call(w3, "anvil_setCode", [TEST_WALLET, "0x"])

    # Fund with 100 native MON.
    fund_native_token(TEST_WALLET, 100 * 10**18, rpc_url)

    # Fund with common tokens.
    for token_symbol, token_address in config.get("tokens", {}).items():
        balance_slot = config.get("balance_slots", {}).get(token_symbol)
        if balance_slot is None:
            continue
        try:
            decimals = get_token_decimals(web3, token_address)
            if token_symbol in _NATIVE_WRAPPERS:
                # Wrap 10 MON -> 10 WMON via WETH9-style deposit().
                _wrap_native_token(TEST_WALLET, token_address, 10 * (10**decimals), rpc_url)
            else:
                # Direct storage-slot override for bridged / non-native ERC20s.
                amount = 100_000 * (10**decimals)
                fund_erc20_token(TEST_WALLET, token_address, amount, balance_slot, rpc_url)
        except Exception as e:  # noqa: BLE001 — funding is best-effort
            print(f"Warning: Could not fund {token_symbol} on monad: {e}")

    return TEST_WALLET


@pytest.fixture(scope="module")
def anvil_instance(anvil_monad: AnvilFixture) -> AnvilFixture:
    """Expose the chain-specific AnvilFixture for shared recovery logic."""
    return anvil_monad


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_monad: AnvilFixture) -> str:
    """Get the Anvil RPC URL for Monad."""
    return f"http://127.0.0.1:{anvil_monad.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for Monad."""
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
def funded_wallet(web3: Web3, anvil_rpc_url: str, anvil_instance: AnvilFixture) -> str:
    """Fund the test wallet with native MON, WMON (wrap), and ERC20s (storage)."""
    return seed_wallet_state_with_recovery(
        seed_wallet_state=_seed_wallet_state,
        web3=web3,
        rpc_url=anvil_rpc_url,
        anvil_instance=anvil_instance,
        chain_name=CHAIN_NAME,
    )


@pytest.fixture(scope="module")
def reseed_wallet_state(anvil_instance: AnvilFixture):
    """Return a callable that re-seeds balances on demand (for fork recovery)."""

    def _reseed() -> str:
        rpc_url = anvil_instance.get_rpc_url()
        return _seed_wallet_state(make_intent_test_web3(rpc_url), rpc_url)

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
