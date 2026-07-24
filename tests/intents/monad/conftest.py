"""Fixtures for Monad intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances. Mirrors the
Base conftest pattern (the reference default-on-Zodiac conftest) — see
``tests/intents/base/conftest.py``.

Default-on Zodiac (Phase G, VIB-5967): every intent test runs through Safe +
Roles + ``execTransactionWithRole`` by default. The ``funded_wallet`` and
``orchestrator`` fixtures below substitute the Safe address and a
``ZodiacOrchestrator`` respectively, so the same test body runs unchanged
through the Safe path. Tests that carry ``@pytest.mark.no_zodiac(reason="...")``
opt out and retain the original EOA behaviour (standard ``ExecutionOrchestrator``
signing for ``TEST_WALLET``). The canonical Safe v1.4.1 + Zodiac Roles v2 stack
is deployed at the standard CREATE2 addresses on Monad mainnet (verified via
``eth_getCode`` against ``https://rpc.monad.xyz``, 2026-07-24).

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
def _eoa_funded_wallet(web3: Web3, anvil_rpc_url: str, anvil_instance: AnvilFixture) -> str:
    """Module-scoped EOA funding (original ``funded_wallet`` behaviour).

    Kept private so the function-scoped ``funded_wallet`` below can delegate to
    it for ``no_zodiac``-marked tests without duplicating the module-scoped
    seeding work. For default-on Zodiac tests, ``funded_wallet`` returns the
    Safe address instead and this fixture's side effect (seeding the EOA)
    still pays the member EOA's gas for signing ``execTransactionWithRole``.

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

    Tests that use ``funded_wallet`` as an *EOA signer* outside the
    orchestrator (raw ``web3.eth.send_transaction({"from": funded_wallet})``)
    only work under ``no_zodiac`` — the Safe cannot produce raw signatures on
    arbitrary calls. Default-on tests must route through
    ``orchestrator.execute(...)`` so this surfaces naturally rather than
    silently.
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
    multi-step tests (open-then-close, supply-then-withdraw) extend the
    authorisation scope as they go. For ``no_zodiac``-marked tests: returns
    the standard ``ExecutionOrchestrator``.
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
