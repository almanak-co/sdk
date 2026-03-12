"""Fixtures for Solana intent tests.

Solana intent tests differ from EVM intent tests:
- No Anvil fork — uses solana-test-validator via SolanaForkManager
- No ERC20 storage slots — SPL tokens minted via modified mint authority
- No Web3 — uses SolanaRpcClient + solders for account queries
- Compilation tests hit REAL APIs (Jupiter, Kamino, Raydium) — no mocks

Test layers:
1. Compilation tests (always run): verify Intent -> ActionBundle via real APIs
2. Execution tests (require solana-test-validator): sign, submit, verify on-chain
   - Layer 2: Execution success
   - Layer 3: Receipt parser integration
   - Layer 4: Exact balance deltas

When solana-test-validator is NOT installed, compilation tests still prove
the full intent -> adapter -> API -> ActionBundle pipeline works.
Execution tests are gated behind @requires_solana_validator.
"""

import os
import shutil
from decimal import Decimal

import pytest
import pytest_asyncio

CHAIN_NAME = "solana"

# A valid Solana wallet address for compilation tests (does not need funds for compile-only)
# This is a well-known devnet test wallet; no private key needed for compilation
TEST_SOLANA_WALLET = "KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9"

# Solana token mints (mainnet-beta)
SOLANA_TOKENS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "JITOSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
}

SOLANA_TOKEN_DECIMALS = {
    "SOL": 9,
    "WSOL": 9,
    "USDC": 6,
    "USDT": 6,
    "JUP": 6,
    "RAY": 6,
}

# Placeholder prices for compilation (same as JupiterAdapter._get_placeholder_prices)
SOLANA_PLACEHOLDER_PRICES = {
    "SOL": Decimal("150"),
    "WSOL": Decimal("150"),
    "USDC": Decimal("1"),
    "USDT": Decimal("1"),
    "JUP": Decimal("1"),
    "RAY": Decimal("2"),
    "ORCA": Decimal("0.5"),
    "BONK": Decimal("0.00002"),
    "WIF": Decimal("1.5"),
    "JTO": Decimal("3"),
    "PYTH": Decimal("0.4"),
    "MSOL": Decimal("170"),
    "JITOSOL": Decimal("170"),
}

# Check if solana-test-validator is available
HAS_SOLANA_VALIDATOR = shutil.which("solana-test-validator") is not None

requires_solana_validator = pytest.mark.skipif(
    not HAS_SOLANA_VALIDATOR,
    reason="solana-test-validator not installed",
)

# Check if Jupiter API is reachable (best-effort, cached for session)
_jupiter_api_ok: bool | None = None


def _check_jupiter_api() -> bool:
    """Check if Jupiter API is reachable."""
    global _jupiter_api_ok
    if _jupiter_api_ok is not None:
        return _jupiter_api_ok
    try:
        import requests

        resp = requests.get("https://lite-api.jup.ag/v1/health", timeout=5)
        _jupiter_api_ok = resp.status_code == 200
    except Exception:
        _jupiter_api_ok = False
    return _jupiter_api_ok


requires_jupiter_api = pytest.mark.skipif(
    not _check_jupiter_api(),
    reason="Jupiter API not reachable",
)


# =============================================================================
# Helpers for balance queries (used in Layer 4 verification)
# =============================================================================


async def get_sol_balance(fork_manager, wallet_address: str) -> int:
    """Get SOL balance in lamports."""
    return await fork_manager._get_sol_balance(wallet_address)


async def get_spl_token_balance(fork_manager, wallet_address: str, mint: str) -> int:
    """Get SPL token balance in raw units (e.g., 1_000_000 for 1 USDC).

    Returns 0 if no token account exists.
    """
    from solders.pubkey import Pubkey

    mint_pk = Pubkey.from_string(mint)
    owner_pk = Pubkey.from_string(wallet_address)
    ata = fork_manager._derive_ata(owner_pk, mint_pk)

    balance_str = await fork_manager._get_token_balance(str(ata))
    if not balance_str or balance_str == "0":
        return 0

    # _get_token_balance returns uiAmountString (human-readable).
    # Convert back to raw units using known decimals.
    # Find decimals from mint address
    decimals = 6  # default
    for symbol, mint_addr in SOLANA_TOKENS.items():
        if mint_addr == mint and symbol in SOLANA_TOKEN_DECIMALS:
            decimals = SOLANA_TOKEN_DECIMALS[symbol]
            break

    return int(Decimal(balance_str) * Decimal(10**decimals))


# =============================================================================
# Compilation Fixtures (always available)
# =============================================================================


@pytest.fixture(scope="module")
def chain_name() -> str:
    """Chain name for Solana."""
    return CHAIN_NAME


@pytest.fixture(scope="module")
def solana_wallet() -> str:
    """Solana wallet address for compilation tests."""
    return TEST_SOLANA_WALLET


@pytest.fixture(scope="module")
def price_oracle() -> dict[str, Decimal]:
    """Placeholder price oracle for intent compilation.

    Uses placeholder prices because we don't have a gateway
    for Solana CoinGecko integration yet.
    """
    return SOLANA_PLACEHOLDER_PRICES


@pytest.fixture(scope="module")
def solana_compiler(solana_wallet, price_oracle):
    """IntentCompiler configured for Solana.

    Uses allow_placeholder_prices=True since we don't have
    gateway-backed price feeds for Solana yet.
    """
    from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

    config = IntentCompilerConfig(allow_placeholder_prices=True)

    return IntentCompiler(
        chain="solana",
        wallet_address=solana_wallet,
        price_oracle=price_oracle,
        config=config,
    )


# =============================================================================
# Execution Fixtures (require solana-test-validator)
# =============================================================================


@pytest_asyncio.fixture(scope="session")
async def solana_fork():
    """Session-scoped SolanaForkManager.

    Starts solana-test-validator with cloned mainnet accounts.
    Shared across all execution tests in the session.
    """
    if not HAS_SOLANA_VALIDATOR:
        pytest.skip("solana-test-validator not installed")

    from almanak.framework.anvil.solana_fork_manager import SolanaForkManager

    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    mgr = SolanaForkManager(
        rpc_url=rpc,
        validator_port=18899,  # Non-default port to avoid conflicts
        faucet_port=19900,
    )
    started = await mgr.start()
    if not started:
        pytest.skip("solana-test-validator failed to start (network issue?)")
    yield mgr
    await mgr.stop()


@pytest_asyncio.fixture(scope="module")
async def funded_solana_wallet(solana_fork):
    """Module-scoped funded wallet for execution tests.

    Creates a fresh keypair, funds it with:
    - 100 SOL (native, for gas + swap input)
    - 10,000 USDC (for swap/lending tests)
    - 10,000 USDT (for swap tests)

    Returns (wallet_address, private_key_base58) tuple.
    """
    from solders.keypair import Keypair

    kp = Keypair()
    wallet_address = str(kp.pubkey())
    private_key = str(kp)

    # Fund SOL
    ok = await solana_fork.fund_wallet(wallet_address, Decimal("100"))
    assert ok, "Failed to fund SOL"

    # Fund SPL tokens
    ok = await solana_fork.fund_tokens(
        wallet_address,
        {"USDC": Decimal("10000"), "USDT": Decimal("10000")},
    )
    assert ok, "Failed to fund SPL tokens"

    return wallet_address, private_key


@pytest.fixture()
def solana_orchestrator(funded_solana_wallet, solana_fork):
    """Function-scoped SolanaOrchestratorAdapter for execution tests."""
    from almanak.framework.execution.solana.orchestrator_adapter import SolanaOrchestratorAdapter

    wallet_address, private_key = funded_solana_wallet
    return SolanaOrchestratorAdapter(
        wallet_address=wallet_address,
        rpc_url=solana_fork.get_rpc_url(),
        private_key=private_key,
    )


@pytest.fixture()
def execution_compiler(funded_solana_wallet, price_oracle):
    """IntentCompiler configured with the funded wallet (for execution tests)."""
    from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

    wallet_address, _ = funded_solana_wallet
    config = IntentCompilerConfig(allow_placeholder_prices=True)

    return IntentCompiler(
        chain="solana",
        wallet_address=wallet_address,
        price_oracle=price_oracle,
        config=config,
    )
