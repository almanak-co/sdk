"""Fixtures for 0G Chain intent tests (Jaine DEX).

Uses gateway's Anvil fixture for zerog. The test wallet is funded with native
A0GI only — storage slots for 0G ERC20s are not yet mapped, so ERC20 balances
are acquired on-demand by swapping from native via Jaine.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.execution.signer import LocalKeySigner
from almanak.framework.execution.simulator import DirectSimulator
from almanak.framework.execution.submitter import PublicMempoolSubmitter
from tests.conftest_gateway import AnvilFixture
from tests.intents.conftest import (
    TEST_PRIVATE_KEY,
    TEST_SUBMITTER_MAX_RETRIES,
    TEST_TX_TIMEOUT_SECONDS,
    TEST_WALLET,
    TEST_WEB3_REQUEST_TIMEOUT,
    fund_native_token,
)

CHAIN_NAME = "zerog"
REQUIRED_CHAIN_ID = 16661


@pytest.fixture(scope="module")
def anvil_instance(anvil_zerog: AnvilFixture) -> AnvilFixture:
    return anvil_zerog


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_zerog: AnvilFixture) -> str:
    return f"http://127.0.0.1:{anvil_zerog.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))
    assert w3.is_connected(), f"Anvil not responding at {anvil_rpc_url}"
    actual_id = w3.eth.chain_id
    assert actual_id == REQUIRED_CHAIN_ID, f"Expected chain {REQUIRED_CHAIN_ID}, got {actual_id}"
    return w3


@pytest.fixture(scope="module")
def test_private_key() -> str:
    return TEST_PRIVATE_KEY


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3, anvil_rpc_url: str) -> str:
    """Fund the test wallet with 100 native A0GI.

    ERC20 balance-slot seeding is intentionally omitted: storage layouts for 0G
    tokens (W0G, USDC.e, ...) are not yet mapped. Tests must acquire ERC20s by
    swapping from native via Jaine.
    """
    fund_native_token(TEST_WALLET, 100 * 10**18, anvil_rpc_url)
    return TEST_WALLET


@pytest.fixture(scope="session")
def price_oracle() -> dict[str, Decimal]:
    """Static price oracle for 0G tests.

    No CoinGecko listing for A0GI/USDC.e on 0G; use a fixed stub. Slippage is
    protected by the bilateral balance assertions in each test, not by oracle
    precision.
    """
    # A0GI trades around $0.6 on Jaine (verified via QuoterV2: 0.05 W0G -> ~0.031 USDC.e).
    # The value is not load-bearing for correctness — slippage protection comes
    # from the bilateral balance assertions — but must be within the compiler's
    # price-impact guard tolerance (default 30%) of the on-chain quote.
    return {
        "A0GI": Decimal("0.6"),
        "0G": Decimal("0.6"),
        "W0G": Decimal("0.6"),
        "USDC.e": Decimal("1"),
    }


@pytest.fixture
def orchestrator(test_private_key: str, anvil_rpc_url: str) -> ExecutionOrchestrator:
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
