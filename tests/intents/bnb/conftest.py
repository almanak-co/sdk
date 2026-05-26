"""Fixtures for BNB Chain intent tests.

Uses gateway's Anvil fixtures to avoid duplicate fork instances.

Phase G.1 pilot: when a test carries the ``@pytest.mark.uses_zodiac(...)``
marker, the ``funded_wallet`` and ``orchestrator`` fixtures below substitute
the Safe address and a ``ZodiacOrchestrator`` respectively, so the same test
body runs unchanged through Safe + Roles + ``execTransactionWithRole``.
Unmarked tests see the original EOA behaviour.
"""

from decimal import Decimal

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
    _wrap_native_token,
    fund_erc20_token,
    fund_native_token,
    get_token_decimals,
    make_intent_test_web3,
    reset_fork_to_pristine,
    seed_wallet_state_with_recovery,
)

CHAIN_NAME = "bsc"
REQUIRED_CHAIN_ID = 56


def _seed_wallet_state(web3: Web3, rpc_url: str) -> str:
    """Seed test wallet balances for BNB on the current fork instance."""
    config = CHAIN_CONFIGS[CHAIN_NAME]

    # Fund with 100 native tokens (BNB)
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
def anvil_instance(anvil_bsc: AnvilFixture) -> AnvilFixture:
    """Expose the chain-specific AnvilFixture for shared recovery logic."""
    return anvil_bsc


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_bsc: AnvilFixture) -> str:
    """Get the Anvil RPC URL for BNB chain."""
    return f"http://127.0.0.1:{anvil_bsc.port}"


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Connect to gateway's Anvil fork for BNB chain."""
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


# =============================================================================
# PancakeSwap Perps test helpers (VIB-2874)
# =============================================================================
# The ApolloX router on BSC uses a two-phase oracle-fill flow:
#   1. User signs openMarketTradeBNB -> emits MarketPendingTrade(tradeHash, ...)
#      + an earlier log carrying the priceRequestId.
#   2. Off-chain keeper (holding PRICE_FEEDER_ROLE) calls
#      PriceFacadeFacet.requestPriceCallback(priceRequestId, price) which
#      internally invokes TradingOpenFacet.marketTradeCallback to settle the
#      pending trade into an open position (or refund it).
# To test the close path on a local fork we must simulate step (2). The keeper
# fulfillment event (topic 0x0a6da834...) carries the priceRequestId in
# topics[1]; this helper extracts it, impersonates a PRICE_FEEDER_ROLE holder,
# and submits the fill at a caller-chosen price.


def pcs_perps_extract_price_request_id(receipt: dict) -> str | None:
    """Extract the PCS Perps priceRequestId from an open/close TX receipt.

    The priceRequestId is a bytes32 hash emitted alongside MarketPendingTrade
    (by the PairsManager/TradingCore layer) as the topic[1] of a log whose
    topic[0] is 0x0a6da834... and whose topic[2] is the pair-base address.

    Accepts web3.py HexBytes or plain hex strings in the topics list.
    """

    def _to_hex(value) -> str:
        if isinstance(value, str):
            s = value
        elif hasattr(value, "hex"):
            s = value.hex()
        else:
            s = str(value)
        if not s.startswith("0x") and not s.startswith("0X"):
            s = "0x" + s
        return s.lower()

    target = "0x0a6da83417411689fd88436e7fa57a7cf1cf635a35194c0658314d4a037382af"
    for log in receipt.get("logs", []) or []:
        topics = log.get("topics", []) or []
        if len(topics) < 2:
            continue
        if _to_hex(topics[0]) == target:
            return _to_hex(topics[1])
    return None


def pcs_perps_keeper_fulfill(web3, price_request_id: str, price_1e8: int) -> dict:
    """Impersonate a PancakeSwap Perps PRICE_FEEDER_ROLE holder and fulfill a price request.

    This simulates the off-chain keeper's action so tests can run end-to-end
    (pending -> settled) without waiting for a real keeper.

    Args:
        web3: Web3 instance connected to the Anvil fork.
        price_request_id: bytes32 hash (0x-prefixed 64-char hex) from
            pcs_perps_extract_price_request_id().
        price_1e8: oracle price to supply, scaled by 1e8 (uint64 range).

    Returns:
        The transaction receipt dict from the fulfill call.

    Raises:
        AssertionError: if the Anvil fork is not in a state where impersonation
            can succeed, or if the fulfill TX reverts.
    """
    from almanak.core.contracts import PANCAKESWAP_PERPS
    from almanak.connectors.pancakeswap_perps.sdk import (
        _check_address as _addr_ok,  # noqa: F401 (sanity import)
    )

    router = PANCAKESWAP_PERPS["bsc"]["router"]
    # Known mainnet holder of PRICE_FEEDER_ROLE on ApolloX Diamond.
    keeper = Web3.to_checksum_address("0x2b7363708984aa25a90450cfca7bedaf6804115c")

    # Impersonate + fund via Anvil RPC extensions
    web3.provider.make_request("anvil_impersonateAccount", [keeper])
    web3.provider.make_request("anvil_setBalance", [keeper, hex(10 * 10**18)])

    # Diagnostic: confirm the keeper holds PRICE_FEEDER_ROLE on this fork state.
    from eth_utils import keccak
    role = "0x" + keccak(b"PRICE_FEEDER_ROLE").hex()
    has_role_calldata = (
        bytes.fromhex("91d14854")  # hasRole(bytes32,address)
        + bytes.fromhex(role[2:])
        + bytes.fromhex("000000000000000000000000" + keeper[2:].lower())
    )
    res = web3.eth.call({"to": router, "data": "0x" + has_role_calldata.hex()})
    has_role = int.from_bytes(res, "big") != 0
    assert has_role, f"Impersonated keeper {keeper} lacks PRICE_FEEDER_ROLE on this fork"

    # Build calldata: requestPriceCallback(bytes32,uint64) — selector 0x2103188a
    # Manually encode to avoid pulling in an eth_abi dep here.
    assert price_request_id.startswith("0x") and len(price_request_id) == 66, (
        f"Invalid price_request_id: {price_request_id!r}"
    )
    # bytes32 is already 32 bytes; uint64 left-padded to 32 bytes.
    calldata = (
        bytes.fromhex("2103188a")
        + bytes.fromhex(price_request_id[2:])
        + (price_1e8).to_bytes(32, "big")
    )

    # Use raw JSON-RPC eth_sendTransaction so the Anvil node signs as the
    # impersonated keeper. web3.eth.send_transaction tries to pre-validate /
    # route through local signers which interacts badly with impersonation.
    response = web3.provider.make_request(
        "eth_sendTransaction",
        [
            {
                "from": keeper,
                "to": router,
                "data": "0x" + calldata.hex(),
                "gas": "0x" + format(2_000_000, "x"),
            }
        ],
    )
    if "error" in response:
        raise AssertionError(
            f"Keeper fulfill reverted: {response['error']}. "
            f"priceRequestId={price_request_id}, price={price_1e8}"
        )
    tx_hash_hex = response["result"]
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash_hex, timeout=30)
    assert receipt["status"] == 1, f"Keeper fulfill TX {tx_hash_hex} reverted post-mining"
    return dict(receipt)


# =============================================================================
# Aster/PCS Perps setup helper — open a position via Intent through the
# orchestrator. Used by close-side tests that need a tradeHash to exist on
# chain before they can exercise PERP_CLOSE.
# =============================================================================


async def open_aster_perps_position_via_intent(
    *,
    orchestrator: ExecutionOrchestrator,
    web3: Web3,
    funded_wallet: str,
    anvil_rpc_url: str,
    perps_price_oracle: dict,
    protocol: str,  # "aster_perps" or "pancakeswap_perps"
    market: str,
    collateral_amount,  # Decimal
    size_usd,  # Decimal
    is_long: bool = True,
) -> dict:
    """Open a native-BNB-margin perp position through the orchestrator.

    Replaces the legacy ``web3.eth.account.sign_transaction`` setup pattern
    that 4 close-side tests previously used. That pattern signed an
    ``openMarketTradeBNB`` calldata with ``test_private_key`` and
    ``tx['from'] = funded_wallet`` — fine while ``funded_wallet`` was the EOA,
    but broken under default-on Zodiac (where ``funded_wallet`` becomes the
    Safe and the EOA can't sign for it).

    Routing through the orchestrator works the same regardless of fixture
    mode: under Zodiac it wraps the call into ``execTransactionWithRole``;
    under ``no_zodiac`` it submits directly. Returns the open receipt dict
    so callers can extract the tradeHash via ``AsterPerpsReceiptParser``.

    Args:
        orchestrator: function-scoped orchestrator from the test fixture.
        web3: Web3 instance bound to the Anvil fork (used to size gas).
        funded_wallet: wallet that owns the position (Safe under Zodiac, EOA
            under ``no_zodiac``).
        anvil_rpc_url: RPC URL the IntentCompiler reads from.
        perps_price_oracle: in-memory price map (BTC, BNB, USDT, …).
        protocol: ``aster_perps`` (broker_id=0) or ``pancakeswap_perps``
            (broker_id=2). Routes to the same Diamond router with different
            broker attribution in calldata.
        market: e.g. ``"BTC/USD"``.
        collateral_amount: native BNB amount as Decimal.
        size_usd: notional size in USD as Decimal.
        is_long: long/short flag.

    Returns:
        Receipt dict from the open TX, suitable for
        ``AsterPerpsReceiptParser.parse_receipt(...)``.
    """
    from almanak.framework.intents.compiler import IntentCompiler
    from almanak.framework.intents.perp_intents import PerpOpenIntent

    intent = PerpOpenIntent(
        market=market,
        collateral_token="BNB",
        collateral_amount=collateral_amount,
        size_usd=size_usd,
        is_long=is_long,
        max_slippage=Decimal("0.01"),
        protocol=protocol,
        leverage=Decimal("1"),
    )
    compiler = IntentCompiler(
        chain="bsc",
        wallet_address=funded_wallet,
        price_oracle=perps_price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", (
        f"Setup PerpOpenIntent failed to compile: {compilation.error}"
    )
    assert compilation.action_bundle is not None
    execution = await orchestrator.execute(compilation.action_bundle)
    assert execution.success, f"Setup PerpOpen execution failed: {execution.error}"
    assert len(execution.transaction_results) == 1
    tx_result = execution.transaction_results[0]
    assert tx_result.receipt is not None, "Setup PerpOpen TX produced no receipt"
    return tx_result.receipt.to_dict()
