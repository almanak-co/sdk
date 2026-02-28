"""Fixtures for Polygon intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances.

Polygon-specific considerations:
- Gas prices: Polygon mainnet gas prices can be very high (100-1000+ gwei).
  Anvil forks preserve the block's base fee, so the EIP-1559 formula (2x base fee)
  often exceeds the default 500 gwei cap.  We lower the base fee on the Anvil fork
  to 30 gwei (normal Polygon range) so the gas price guard passes.
- WETH: Polygon WETH (0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619) is a PoS-bridged
  UChildERC20Proxy token, NOT a wrapped native token.  It cannot be obtained by
  wrapping MATIC.  We fund it via storage slot manipulation (slot 0, the _balances
  mapping in the ERC20 base contract) AND verify the balance afterwards to catch
  incorrect slots early.
"""

import pytest
from web3 import Web3

from tests.conftest_gateway import AnvilFixture
from almanak.framework.execution.orchestrator import ExecutionOrchestrator, TransactionRiskConfig
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
    fund_erc20_token,
    fund_native_token,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "polygon"
REQUIRED_CHAIN_ID = 137

# Polygon mainnet base fees can exceed 300 gwei.  The EIP-1559 formula
# (2 * base_fee + priority_fee) produces 600+ gwei, which exceeds the
# default gas price cap.  We reset the Anvil fork's base fee to a
# sensible value so the orchestrator's gas guard doesn't block txs.
_ANVIL_BASE_FEE_WEI = 30 * 10**9  # 30 gwei -- within normal Polygon range


def _lower_anvil_base_fee(rpc_url: str) -> None:
    """Set the next block's base fee on the Anvil fork to a reasonable value.

    Polygon mainnet can have very high base fees (300+ gwei) which, after the
    EIP-1559 doubling formula, exceed the orchestrator's gas price cap.  By
    resetting the base fee on the fork we avoid false-positive gas guard
    failures in tests.

    Uses in-process Web3 provider RPC instead of subprocess (cast).
    """
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))
    base_fee_hex = hex(_ANVIL_BASE_FEE_WEI)
    w3.provider.make_request("anvil_setNextBlockBaseFeePerGas", [base_fee_hex])
    # Mine a block so the new base fee takes effect
    w3.provider.make_request("evm_mine", [])


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for Polygon on the current fork instance."""
    config = CHAIN_CONFIGS[CHAIN_NAME]
    # Required tokens for Polygon intent tests - fixture must fail if these can't be funded
    required_tokens = {"USDC", "WETH"}
    failed_tokens: list[str] = []

    # Ensure sane base fee on every reseed (important after fork restart)
    _lower_anvil_base_fee(rpc_url)

    # Fund with 100 native tokens (MATIC on Polygon)
    fund_native_token(TEST_WALLET, 100 * 10**18, rpc_url)

    # Fund with common tokens
    # Note: WETH on Polygon is bridged (not wrapped native), so all tokens
    # use storage slot funding. After each token, verify balance to catch
    # incorrect storage slots early.
    for token_symbol, token_address in config.get("tokens", {}).items():
        balance_slot = config.get("balance_slots", {}).get(token_symbol)
        if balance_slot is not None:
            try:
                decimals = get_token_decimals(web3, token_address)
                amount = 100_000 * (10**decimals)
                fund_erc20_token(TEST_WALLET, token_address, amount, balance_slot, rpc_url)

                # Verify the balance was actually set (catches wrong storage slots)
                actual_balance = get_token_balance(web3, token_address, TEST_WALLET)
                if actual_balance == 0:
                    print(
                        f"WARNING: {token_symbol} balance is 0 after funding with slot {balance_slot}. "
                        f"Storage slot may be incorrect for {token_address}"
                    )
                    failed_tokens.append(token_symbol)
                else:
                    from tests.intents.conftest import format_token_amount
                    print(f"  Funded {token_symbol}: {format_token_amount(actual_balance, decimals)}")
            except Exception as e:
                print(f"Warning: Could not fund {token_symbol}: {e}")
                failed_tokens.append(token_symbol)

    # Fail fast if required tokens couldn't be funded
    missing_required = set(failed_tokens) & required_tokens
    if missing_required:
        raise AssertionError(
            f"Failed to fund required tokens: {missing_required}. "
            f"All failed tokens: {failed_tokens}"
        )

    return TEST_WALLET


@pytest.fixture(scope="module")
def anvil_instance(anvil_polygon: AnvilFixture) -> AnvilFixture:
    """Expose the chain-specific AnvilFixture for shared recovery logic."""
    return anvil_polygon


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_polygon: AnvilFixture) -> str:
    """Get the Anvil RPC URL for Polygon chain."""
    rpc_url = f"http://127.0.0.1:{anvil_polygon.port}"
    # Lower base fee immediately after fork so all subsequent operations
    # (funding, gas estimation, execution) see a reasonable gas price.
    _lower_anvil_base_fee(rpc_url)
    return rpc_url


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for Polygon."""
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
    """Fund the test wallet with native token and common ERC20s.

    Polygon-specific: WETH and USDT are PoS-bridged UChildERC20Proxy tokens.
    They cannot be wrapped from native MATIC, so we use storage slot manipulation
    and verify the resulting balance.
    """
    return _seed_wallet_state(web3, anvil_rpc_url)


@pytest.fixture(scope="module")
def reseed_wallet_state(web3: Web3, anvil_instance: AnvilFixture):
    """Return a callable that re-seeds balances on demand (for fork recovery)."""

    def _reseed() -> str:
        return _seed_wallet_state(web3, anvil_instance.get_rpc_url())

    return _reseed


@pytest.fixture
def orchestrator(test_private_key: str, anvil_rpc_url: str) -> ExecutionOrchestrator:
    """Create ExecutionOrchestrator for testing.

    Uses a permissive TransactionRiskConfig because Polygon gas prices on
    Anvil forks can still spike above chain-specific caps even after
    lowering the base fee.  The gas price guard is a production safety net,
    not something intent tests need to exercise.
    """
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
        tx_risk_config=TransactionRiskConfig.permissive(),
    )
