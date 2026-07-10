"""Fixtures for Robinhood Chain (4663) intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances. Mirrors the
Base conftest pattern (the reference default-on-Zodiac conftest) — see
``tests/intents/base/conftest.py``.

Robinhood-specific notes:
- Native token is ETH; WETH (``0x0Bd7…AD73``) is the canonical wrapped-native
  ERC20. It is WETH9-style (``deposit()`` / ``withdraw()`` present in the
  runtime bytecode), so it funds via the native-wrap path like other chains'
  WETH. NOTE: ``WETH9()`` reverts on the NPM / SwapRouter02 periphery — WETH is
  resolved from the chain descriptor, never from the periphery.
- The chain's canonical stable is USDG (Global Dollar, Paxos, 6 dec). There is
  NO Circle-USDC / Tether-USDT with real liquidity on 4663.
- The Anvil fork is pinned to block 5,610,000 (``anvil_robinhood`` in
  ``tests/conftest_gateway.py``) — the block where the WETH/USDG fee-500 pool
  liquidity and the canonical Safe + Zodiac Roles v2 stack (VIB-5708) were
  verified on-chain. Every intent test therefore routes through
  Safe + Roles + ``execTransactionWithRole`` by default (opt out with
  ``@pytest.mark.no_zodiac``).
- Public RPC ``https://rpc.mainnet.chain.robinhood.com`` forks the chain when an
  Alchemy ``robinhood-mainnet`` app is not enabled on the local key.
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
    TEST_WEB3_REQUEST_TIMEOUT,
    ZodiacContext,
    _retry_rpc_call,
    _wrap_native_token,
    fund_erc20_token,
    fund_native_token,
    get_token_decimals,
    make_intent_test_web3,
    reset_fork_to_pristine,
    seed_wallet_state_with_recovery,
)

CHAIN_NAME = "robinhood"
REQUIRED_CHAIN_ID = 4663

# Tokens that wrap native ETH — fund via WETH9 deposit() rather than storage-slot
# manipulation (more reliable for the EOA path).
_NATIVE_WRAPPERS = {"WETH"}


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for Robinhood on the current fork instance."""
    config = CHAIN_CONFIGS[CHAIN_NAME]

    # Clear any delegation code on the test wallet (guard against unusual
    # EIP-7702 state on forks — mirrors the Base/Monad seed logic). Reuse the
    # already-connected instance; no second HTTP provider needed.
    wallet_code = web3.eth.get_code(Web3.to_checksum_address(TEST_WALLET))
    if len(wallet_code) > 0:
        _retry_rpc_call(web3, "anvil_setCode", [TEST_WALLET, "0x"])

    # Fund with 100 native ETH.
    fund_native_token(TEST_WALLET, 100 * 10**18, rpc_url)

    # Fund with the chain's stock tokens.
    for token_symbol, token_address in config.get("tokens", {}).items():
        balance_slot = config.get("balance_slots", {}).get(token_symbol)
        if balance_slot is None:
            continue
        try:
            decimals = get_token_decimals(web3, token_address)
            if token_symbol in _NATIVE_WRAPPERS:
                # Wrap 10 ETH -> 10 WETH via WETH9-style deposit().
                _wrap_native_token(TEST_WALLET, token_address, 10 * (10**decimals), rpc_url)
            else:
                # Direct storage-slot override for the ERC20 stable (USDG).
                amount = 100_000 * (10**decimals)
                fund_erc20_token(TEST_WALLET, token_address, amount, balance_slot, rpc_url)
        except Exception as e:  # noqa: BLE001 — funding is best-effort
            print(f"Warning: Could not fund {token_symbol} on robinhood: {e}")

    return TEST_WALLET


@pytest.fixture(scope="module")
def anvil_instance(anvil_robinhood: AnvilFixture) -> AnvilFixture:
    """Expose the chain-specific AnvilFixture for shared recovery logic."""
    return anvil_robinhood


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_robinhood: AnvilFixture) -> str:
    """Get the Anvil RPC URL for Robinhood."""
    return f"http://127.0.0.1:{anvil_robinhood.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for Robinhood."""
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
def _eoa_funded_wallet(web3: Web3, anvil_rpc_url: str, anvil_instance: AnvilFixture) -> str:
    """Module-scoped EOA funding (original ``funded_wallet`` behaviour).

    Kept private so the function-scoped ``funded_wallet`` below can delegate to
    it for unmarked tests without duplicating the module-scoped seeding work.
    For default-on Zodiac tests, ``funded_wallet`` returns the Safe address
    instead and this fixture's side effect (seeding the EOA) still pays the
    member EOA's gas for signing ``execTransactionWithRole``.

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

    Default-on Zodiac: returns the per-test Safe address from ``zodiac_safe``
    (seeded with the same CHAIN_CONFIGS balances the EOA path receives). With
    ``@pytest.mark.no_zodiac`` set, ``zodiac_safe`` is ``None`` and this returns
    the EOA (``TEST_WALLET``).
    """
    if zodiac_safe is not None:
        _ = _eoa_funded_wallet  # ensure EOA gas funding ran
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

    Returns a ``ZodiacOrchestrator`` by default — every intent test routes
    through Safe + Roles + ``execTransactionWithRole`` unless it carries
    ``@pytest.mark.no_zodiac(reason="...")``. The manifest is derived at
    execute-time from the intents the test compiles (recorded via
    ``_zodiac_intent_recorder``) and applied to Roles incrementally, so
    multi-step tests (open-then-close) extend the authorisation scope as they
    go. For ``no_zodiac``-marked tests: returns the standard
    ``ExecutionOrchestrator``.
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
