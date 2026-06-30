"""Fixtures for Arbitrum intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances.

Phase G.1 pilot: when a test carries the ``@pytest.mark.uses_zodiac(...)``
marker, the ``funded_wallet`` and ``orchestrator`` fixtures below substitute
the Safe address and a ``ZodiacOrchestrator`` respectively, so the same test
body runs unchanged through Safe + Roles + ``execTransactionWithRole``.
Unmarked tests see the original EOA behaviour.
"""

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.execution.signer import LocalKeySigner
from almanak.framework.execution.simulator import DirectSimulator
from almanak.framework.execution.submitter import PublicMempoolSubmitter
from tests.conftest_gateway import AnvilFixture
from tests.intents._permission_onchain_harness import ZodiacOrchestrator
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    TEST_PRIVATE_KEY,
    TEST_SUBMITTER_MAX_RETRIES,
    TEST_TX_TIMEOUT_SECONDS,
    TEST_WALLET,
    ZodiacContext,
    _wrap_native_token,
    fund_erc20_token,
    fund_native_token,
    get_token_decimals,
    make_intent_test_web3,
    reset_fork_to_pristine,
    seed_arbitrum_susdai,
    seed_wallet_state_with_recovery,
    web3_request_timeout,
)

CHAIN_NAME = "arbitrum"
REQUIRED_CHAIN_ID = 42161


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for Arbitrum on the current fork instance."""
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

    # Fund sUSDai for the live Pendle sUSDai-market intent tests (the Arbitrum
    # wstETH Pendle market expired 2026-06-25). sUSDai is intentionally NOT in
    # CHAIN_CONFIGS["arbitrum"]["tokens"] (the price-oracle fixture requires a
    # CoinGecko id per token there, and sUSDai has none) — it is seeded via the
    # shared helper, from both this EOA seed and the Zodiac Safe seed.
    seed_arbitrum_susdai(TEST_WALLET, web3, rpc_url)

    return TEST_WALLET


@pytest.fixture(scope="module")
def anvil_instance(anvil_arbitrum: AnvilFixture) -> AnvilFixture:
    """Expose the chain-specific AnvilFixture for shared recovery logic."""
    return anvil_arbitrum


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_arbitrum: AnvilFixture) -> str:
    """Get the Anvil RPC URL for Arbitrum chain."""
    return f"http://127.0.0.1:{anvil_arbitrum.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for Arbitrum."""
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url, request_kwargs={"timeout": web3_request_timeout(CHAIN_NAME)}))
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
    _zodiac_intent_recorder: list,
):
    """Create the execution orchestrator for this test.

    Returns a ``ZodiacOrchestrator`` by default — under the opt-out model,
    every intent test routes through Safe + Roles + ``execTransactionWithRole``
    unless it carries ``@pytest.mark.no_zodiac(reason="...")``. The
    orchestrator generates a manifest at execute-time from the intents
    recorded by ``_zodiac_intent_recorder`` and applies new targets to Roles
    incrementally, so multi-step tests (open-then-close, supply-then-borrow)
    extend the authorisation scope as they go.

    For ``no_zodiac``-marked tests: returns the standard ``ExecutionOrchestrator``.
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
            safe_address=zodiac_safe.safe_address,
            owner_eoa=zodiac_safe.owner_eoa,
            owner_private_key=zodiac_safe.owner_private_key,
            recorded_intents=_zodiac_intent_recorder,
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
    )
