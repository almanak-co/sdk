"""Fixtures for Polygon intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances.

Phase G.1 pilot: when a test carries the ``@pytest.mark.uses_zodiac(...)``
marker, the ``funded_wallet`` and ``orchestrator`` fixtures below substitute
the Safe address and a ``ZodiacOrchestrator`` respectively, so the same test
body runs unchanged through Safe + Roles + ``execTransactionWithRole``.
Unmarked tests see the original EOA behaviour.

Polygon-specific considerations:
- Gas prices: Polygon mainnet gas prices can be very high (100-1000+ gwei).
  Intent tests rely on Anvil startup forcing ``--block-base-fee-per-gas 0``.
  Keep module setup free of extra admin RPCs like ``evm_mine`` so a degraded
  shared fork can be recovered by the generic restart path instead of hanging
  during fixture setup.
- WETH: Polygon WETH (0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619) is a PoS-bridged
  UChildERC20Proxy token, NOT a wrapped native token.  It cannot be obtained by
  wrapping MATIC.  We fund it via storage slot manipulation (slot 0, the _balances
  mapping in the ERC20 base contract) AND verify the balance afterwards to catch
  incorrect slots early.
"""

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator, TransactionRiskConfig
from almanak.framework.execution.signer import LocalKeySigner
from almanak.framework.execution.simulator import DirectSimulator
from almanak.framework.execution.submitter import PublicMempoolSubmitter
from tests.conftest_gateway import AnvilFixture
from tests.intents._permission_onchain_harness import ZodiacOrchestrator
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    TEST_POLYGON_WEB3_REQUEST_TIMEOUT,
    TEST_PRIVATE_KEY,
    TEST_SUBMITTER_MAX_RETRIES,
    TEST_TX_TIMEOUT_SECONDS,
    TEST_WALLET,
    ZodiacContext,
    fund_erc20_token,
    fund_native_token,
    get_token_balance,
    get_token_decimals,
    make_intent_test_web3,
    reset_fork_to_pristine,
    seed_wallet_state_with_recovery,
)

CHAIN_NAME = "polygon"
REQUIRED_CHAIN_ID = 137


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for Polygon on the current fork instance."""
    config = CHAIN_CONFIGS[CHAIN_NAME]
    # Required tokens for Polygon intent tests - fixture must fail if these can't be funded
    required_tokens = {"USDC", "WETH"}
    failed_tokens: list[str] = []

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
    """Get the Anvil RPC URL for Polygon chain.

    Keep this fixture side-effect free so a wedged shared fork can still be
    recovered by the generic seeding/restart helpers instead of hanging in
    module setup on an admin RPC.
    """
    return f"http://127.0.0.1:{anvil_polygon.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for Polygon."""
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url, request_kwargs={"timeout": TEST_POLYGON_WEB3_REQUEST_TIMEOUT}))
    assert w3.is_connected(), f"Anvil not responding at {anvil_rpc_url}"
    actual_id = w3.eth.chain_id
    assert actual_id == REQUIRED_CHAIN_ID, f"Expected chain {REQUIRED_CHAIN_ID}, got {actual_id}"
    return w3


@pytest.fixture(scope="module")
def test_private_key() -> str:
    """Return test private key."""
    return TEST_PRIVATE_KEY


@pytest.fixture(scope="module")
def _eoa_funded_wallet(web3: Web3, anvil_rpc_url: str, anvil_instance: AnvilFixture) -> str:
    """Module-scoped EOA funding (original ``funded_wallet`` behaviour).

    Polygon-specific: WETH and USDT are PoS-bridged UChildERC20Proxy tokens.
    They cannot be wrapped from native MATIC, so we use storage slot manipulation
    and verify the resulting balance.

    Kept as a private fixture so the function-scoped ``funded_wallet`` below can
    delegate to it for unmarked tests without duplicating the module-scoped
    seeding work. For tests with the ``uses_zodiac`` marker, ``funded_wallet``
    returns the Safe address instead and this fixture's side effect (seeding
    the EOA) is still useful — the member EOA uses its balance to pay gas
    when signing ``execTransactionWithRole``.

    Reverts the fork to session pristine state first so each test module gets a
    clean slate independent of prior modules on the same chain (VIB-3059).
    """
    reset_fork_to_pristine(web3)
    return seed_wallet_state_with_recovery(
        seed_wallet_state=_seed_wallet_state,
        web3=web3,
        rpc_url=anvil_rpc_url,
        anvil_instance=anvil_instance,
        chain_name=CHAIN_NAME,
    )


@pytest.fixture
def funded_wallet(
    _eoa_funded_wallet: str,
    zodiac_safe: ZodiacContext | None,
) -> str:
    """Return the wallet tests should treat as the token holder.

    When ``@pytest.mark.uses_zodiac(...)`` is set: returns the per-test Safe
    address from ``zodiac_safe``. The Safe has already been seeded with the
    same CHAIN_CONFIGS ERC-20 balances the EOA path normally receives, so
    tests that read ``funded_wallet`` purely as a token holder keep working.

    Without the marker: returns ``TEST_WALLET`` (the EOA), preserving the
    original module-scoped behaviour.

    Tests that use ``funded_wallet`` as an *EOA signer* outside the
    orchestrator (raw ``web3.eth.send_transaction({"from": funded_wallet})``)
    will need to route through the orchestrator when marked with
    ``uses_zodiac`` — the Safe cannot produce raw signatures on arbitrary
    calls. The pilot tests already go through ``orchestrator.execute(...)``
    so this surfaces naturally during G.2 rollout rather than silently.
    """
    if zodiac_safe is not None:
        # The module-scoped EOA fixture has already run (pytest resolves
        # dependencies in order); we depend on it so EOA funding happens for
        # gas, but we return the Safe.
        _ = _eoa_funded_wallet
        return zodiac_safe.safe_address
    return _eoa_funded_wallet


@pytest.fixture(scope="module")
def reseed_wallet_state(anvil_instance: AnvilFixture):
    """Return a callable that re-seeds balances on demand (for fork recovery)."""

    def _reseed() -> str:
        rpc_url = anvil_instance.get_rpc_url()
        return _seed_wallet_state(make_intent_test_web3(rpc_url), rpc_url)

    return _reseed


@pytest.fixture
def orchestrator(
    test_private_key: str,
    anvil_rpc_url: str,
    web3: Web3,
    zodiac_safe: ZodiacContext | None,
):
    """Create the execution orchestrator for this test.

    Returns a ``ZodiacOrchestrator`` when ``@pytest.mark.uses_zodiac(...)`` is
    set — the marker's manifest has been applied on-chain (see
    ``zodiac_safe``) and each tx in the bundle will be routed through
    ``Roles.execTransactionWithRole`` signed by the member EOA. The outward
    contract (``async def execute(action_bundle) -> ExecutionResult``) is
    identical, so unchanged tests run unchanged.

    Without the marker: returns the standard ``ExecutionOrchestrator`` with a
    permissive ``TransactionRiskConfig`` because Polygon fork gas heuristics
    can still diverge from production caps even with the startup base-fee
    override. The gas price guard is a production safety net, not something
    intent tests need to exercise.
    """
    if zodiac_safe is not None:
        return ZodiacOrchestrator(
            web3=web3,
            roles_address=zodiac_safe.roles_address,
            role_key=zodiac_safe.role_key,
            member_eoa=zodiac_safe.member_eoa,
            member_private_key=zodiac_safe.member_private_key,
            chain=CHAIN_NAME,
            rpc_url=anvil_rpc_url,
        )
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
