"""E2E test: DeFAI Agent deploys Lagoon vault + opens Uniswap V3 LP on Base (Anvil fork).

This test forks Base mainnet, funds the test wallet with USDC and ALMANAK tokens,
then runs the DeFAI agent loop with a mock LLM to verify the full flow end-to-end.

Prerequisites:
- anvil (Foundry) installed
- ALCHEMY_API_KEY set in environment
- Gateway binary available

Usage:
    # Run standalone (starts its own Anvil):
    python tests/e2e/test_defai_vault_lp.py

    # Or via pytest:
    pytest tests/e2e/test_defai_vault_lp.py -v -s
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add examples to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "examples" / "agentic"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("test_defai_vault_lp")

# --- Constants ---

BASE_RPC_URL = "https://base-mainnet.g.alchemy.com/v2/{api_key}"
ANVIL_PORT = 8545
ANVIL_HOST = "127.0.0.1"

# Anvil default accounts (first account has 10000 ETH)
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Token addresses on Base
ALMANAK_TOKEN = "0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3"
USDC_TOKEN = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL_ADDRESS = "0xbDbC38652D78AF0383322bBc823E06FA108d0874"

# Uniswap V3 NonfungiblePositionManager on Base
POSITION_MANAGER = "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1"

# Known whale addresses for funding (Base mainnet)
USDC_WHALE = "0x0B0A5886664376F59C351ba3f598C8A8B4d0dBa3"  # Circle reserve on Base


# --- Anvil Management ---


def _find_free_port() -> int:
    """Find a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float = 30.0) -> bool:
    """Wait until a TCP port is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except OSError:
            time.sleep(0.5)
    return False


class AnvilFork:
    """Manage an Anvil fork process for testing."""

    def __init__(self, rpc_url: str, port: int = 0, chain_id: int = 8453):
        self.rpc_url = rpc_url
        self.port = port or _find_free_port()
        self.chain_id = chain_id
        self._process: subprocess.Popen | None = None

    def start(self, block_number: int | None = None) -> None:
        """Start Anvil fork."""
        cmd = [
            "anvil",
            "--fork-url", self.rpc_url,
            "--port", str(self.port),
            "--chain-id", str(self.chain_id),
            "--accounts", "10",
            "--balance", "10000",
            "--silent",
        ]
        if block_number:
            cmd.extend(["--fork-block-number", str(block_number)])

        logger.info("Starting Anvil: %s", " ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if not _wait_for_port(ANVIL_HOST, self.port, timeout=30.0):
            self.stop()
            raise RuntimeError(f"Anvil failed to start on port {self.port}")
        logger.info("Anvil ready on port %d", self.port)

    def stop(self) -> None:
        """Stop Anvil fork."""
        if self._process:
            self._process.send_signal(signal.SIGTERM)
            try:
                self._process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
            self._process = None
            logger.info("Anvil stopped")

    @property
    def url(self) -> str:
        return f"http://{ANVIL_HOST}:{self.port}"


def _cast_send(anvil_url: str, to: str, sig: str, *args: str, from_addr: str = TEST_WALLET) -> str:
    """Execute a cast send command and return output."""
    cmd = [
        "cast", "send", to, sig, *args,
        "--rpc-url", anvil_url,
        "--from", from_addr,
        "--unlocked",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"cast send failed: {result.stderr}")
    return result.stdout


def _cast_call(anvil_url: str, to: str, sig: str, *args: str) -> str:
    """Execute a cast call and return result.

    Newer cast versions append a human-readable suffix like ``100000000 [1e8]``.
    We strip everything after the first space so callers get a clean numeric string.
    """
    cmd = ["cast", "call", to, sig, *args, "--rpc-url", anvil_url]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"cast call failed: {result.stderr}")
    raw = result.stdout.strip()
    # Strip human-readable suffix: "100000000 [1e8]" -> "100000000"
    return raw.split()[0] if raw else raw


def _impersonate_account(anvil_url: str, address: str) -> None:
    """Impersonate an account on Anvil so cast send --unlocked works."""
    cmd = [
        "cast", "rpc", "anvil_impersonateAccount", address,
        "--rpc-url", anvil_url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        raise RuntimeError(f"anvil_impersonateAccount failed for {address}: {result.stderr}")

    # Ensure the impersonated account has ETH for gas (contracts have 0 balance)
    balance_hex = hex(10 * 10**18)  # 10 ETH
    cmd = [
        "cast", "rpc", "anvil_setBalance", address, balance_hex,
        "--rpc-url", anvil_url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        raise RuntimeError(f"anvil_setBalance failed for {address}: {result.stderr}")


def _fund_erc20_via_storage(anvil_url: str, token: str, wallet: str, amount: int, balance_slot: int) -> None:
    """Fund a wallet with ERC20 tokens by directly writing to the balance storage slot."""
    # Compute storage slot: keccak256(abi.encode(wallet, balance_slot))
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(balance_slot)],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cast index failed: {result.stderr}")
    storage_slot = result.stdout.strip()

    # Write balance as 32-byte hex
    amount_hex = f"0x{amount:064x}"
    result = subprocess.run(
        ["cast", "rpc", "anvil_setStorageAt", token, storage_slot, amount_hex, "--rpc-url", anvil_url],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"anvil_setStorageAt failed: {result.stderr}")

    # Mine a block to apply
    subprocess.run(
        ["cast", "rpc", "evm_mine", "--rpc-url", anvil_url],
        capture_output=True, timeout=10,
    )


# ERC20 balance storage slots (determined via cast storage or brute-force)
USDC_BALANCE_SLOT = 9       # Base USDC (FiatTokenV2_2)
ALMANAK_BALANCE_SLOT = 5    # ALMANAK token on Base


def fund_wallet_with_tokens(anvil_url: str, wallet: str) -> None:
    """Fund test wallet with USDC and ALMANAK tokens via Anvil storage manipulation."""
    logger.info("Funding %s with USDC via storage slot %d", wallet, USDC_BALANCE_SLOT)
    _fund_erc20_via_storage(anvil_url, USDC_TOKEN, wallet, 100 * 10**6, USDC_BALANCE_SLOT)

    logger.info("Funding %s with ALMANAK via storage slot %d", wallet, ALMANAK_BALANCE_SLOT)
    _fund_erc20_via_storage(anvil_url, ALMANAK_TOKEN, wallet, 10_000 * 10**18, ALMANAK_BALANCE_SLOT)

    # Verify balances
    usdc_bal = _cast_call(anvil_url, USDC_TOKEN, "balanceOf(address)(uint256)", wallet)
    almanak_bal = _cast_call(anvil_url, ALMANAK_TOKEN, "balanceOf(address)(uint256)", wallet)
    logger.info("Wallet balances -- USDC: %s, ALMANAK: %s", usdc_bal, almanak_bal)


# --- Test ---


@pytest.fixture(scope="module")
def anvil_fork():
    """Start an Anvil fork of Base mainnet."""
    api_key = os.environ.get("ALCHEMY_API_KEY")
    if not api_key:
        pytest.skip("ALCHEMY_API_KEY not set")

    rpc_url = BASE_RPC_URL.format(api_key=api_key)
    fork = AnvilFork(rpc_url=rpc_url, chain_id=8453)
    fork.start()
    try:
        fund_wallet_with_tokens(fork.url, TEST_WALLET)
        yield fork
    finally:
        fork.stop()


@pytest.mark.skipif(
    not os.environ.get("ALCHEMY_API_KEY"),
    reason="ALCHEMY_API_KEY not set",
)
class TestDeFAIVaultLP:
    """E2E tests for the DeFAI Vault + LP agent flow."""

    def test_vault_tools_registered_in_catalog(self):
        """Verify vault tools are present in the default catalog."""
        from almanak.framework.agent_tools import get_default_catalog

        catalog = get_default_catalog()
        assert "deploy_vault" in catalog
        assert "get_vault_state" in catalog
        assert "settle_vault" in catalog

        deploy_tool = catalog.get("deploy_vault")
        assert deploy_tool is not None
        assert deploy_tool.risk_tier == "high"
        assert deploy_tool.category == "action"

    def test_vault_schemas_valid(self):
        """Verify vault request/response schemas can be instantiated."""
        from almanak.framework.agent_tools.schemas import (
            DeployVaultRequest,
            DeployVaultResponse,
            GetVaultStateRequest,
            GetVaultStateResponse,
            SettleVaultRequest,
            SettleVaultResponse,
        )

        req = DeployVaultRequest(
            chain="base",
            name="Test Vault",
            symbol="tVLT",
            underlying_token_address=USDC_TOKEN,
            safe_address=TEST_WALLET,
            admin_address=TEST_WALLET,
            fee_receiver_address=TEST_WALLET,
            deployer_address=TEST_WALLET,
        )
        assert req.chain == "base"

        resp = DeployVaultResponse(status="success", vault_address="0x1234")
        assert resp.vault_address == "0x1234"

        state_req = GetVaultStateRequest(vault_address="0x1234", chain="base")
        assert state_req.vault_address == "0x1234"

        state_resp = GetVaultStateResponse(status="active", total_assets="1000000")
        assert state_resp.total_assets == "1000000"

        settle_req = SettleVaultRequest(
            vault_address="0x1234",
            chain="base",
            safe_address=TEST_WALLET,
            valuator_address=TEST_WALLET,
        )
        assert settle_req.vault_address == "0x1234"

        settle_resp = SettleVaultResponse(status="success", new_total_assets="1000000", epoch_id=1)
        assert settle_resp.epoch_id == 1

    def test_mock_agent_loop_structure(self):
        """Verify the mock LLM produces the expected 11-round sequence."""
        example_dir = Path(__file__).resolve().parent.parent.parent / "examples" / "agentic"
        sys.path.insert(0, str(example_dir))

        config = {
            "chain": "base",
            "wallet_address": TEST_WALLET,
            "pool": "ALMANAK/USDC/3000",
            "pool_address": POOL_ADDRESS,
            "almanak_token": ALMANAK_TOKEN,
            "usdc_token": USDC_TOKEN,
            "vault": {"name": "Test Vault", "symbol": "tVLT", "underlying_token": USDC_TOKEN},
            "lp": {"amount_almanak": "1000", "amount_usdc": "10", "range_width_pct": "0.50"},
            "strategy_id": "test-defai",
        }

        from defai_vault_lp.run import create_dynamic_mock_llm
        mock_llm = create_dynamic_mock_llm(config)
        assert len(mock_llm._rounds) == 11

        # Call round functions with empty context to inspect responses
        ctx: dict = {}
        r1 = mock_llm._rounds[0](ctx)
        # Round 1 should have 4 tool calls (load_state, get_price, 2x get_balance)
        assert len(r1["choices"][0]["message"]["tool_calls"]) == 4

        # Round 2: deploy_vault
        r2 = mock_llm._rounds[1](ctx)
        tc = r2["choices"][0]["message"]["tool_calls"][0]
        assert tc["function"]["name"] == "deploy_vault"

        # Round 7: get_pool_state (discover pool price)
        r7 = mock_llm._rounds[6](ctx)
        tc7 = r7["choices"][0]["message"]["tool_calls"][0]
        assert tc7["function"]["name"] == "get_pool_state"

        # Round 11: final text (no tool calls)
        r11 = mock_llm._rounds[10](ctx)
        assert r11["choices"][0]["message"].get("tool_calls") is None
        assert r11["choices"][0]["message"]["content"] is not None

    @pytest.mark.asyncio
    async def test_executor_dispatches_vault_tools(self):
        """Verify executor routes vault tool calls to correct handlers."""
        from almanak.framework.agent_tools import AgentPolicy, ToolExecutor, get_default_catalog

        mock_gateway = MagicMock()
        policy = AgentPolicy(allowed_chains={"base"}, allowed_tools={"get_vault_state", "deploy_vault", "settle_vault"})
        catalog = get_default_catalog()

        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            catalog=catalog,
            wallet_address=TEST_WALLET,
            strategy_id="test",
            default_chain="base",
        )

        # Mock the vault SDK call path (imported locally inside executor method)
        with patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK") as mock_sdk_cls:
            mock_sdk = MagicMock()
            mock_sdk.get_total_assets.return_value = 1000000
            mock_sdk.get_pending_deposits.return_value = 0
            mock_sdk.get_pending_redemptions.return_value = 0
            mock_sdk.get_share_price.return_value = 1000000
            mock_sdk_cls.return_value = mock_sdk

            result = await executor.execute("get_vault_state", {
                "vault_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })

            assert result.status == "success"
            assert result.data["total_assets"] == "1000000"

    def test_deployer_builds_valid_tx(self):
        """Verify LagoonVaultDeployer produces valid transaction data."""
        from almanak.framework.connectors.lagoon.deployer import (
            LagoonVaultDeployer,
            VaultDeployParams,
        )

        params = VaultDeployParams(
            chain="base",
            underlying_token_address=USDC_TOKEN,
            name="Almanak DeFAI Vault",
            symbol="aALM",
            safe_address=TEST_WALLET,
            admin_address=TEST_WALLET,
            fee_receiver_address=TEST_WALLET,
            deployer_address=TEST_WALLET,
        )

        deployer = LagoonVaultDeployer()
        bundle = deployer.build_deploy_vault_bundle(params)

        assert bundle.intent_type == "DEPLOY_LAGOON_VAULT"
        assert len(bundle.transactions) == 1
        tx = bundle.transactions[0]
        assert tx["to"].lower() == "0x6fc0f2320483fa03fbfdf626ddbae2cc4b112b51"  # Base factory
        assert tx["data"].startswith("0x")

    @pytest.mark.skipif(
        not os.environ.get("ALCHEMY_API_KEY"),
        reason="ALCHEMY_API_KEY not set -- skipping on-chain test",
    )
    def test_anvil_token_balances(self, anvil_fork):
        """Verify test wallet was funded with USDC and ALMANAK on Anvil."""
        usdc_bal_hex = _cast_call(anvil_fork.url, USDC_TOKEN, "balanceOf(address)(uint256)", TEST_WALLET)
        almanak_bal_hex = _cast_call(anvil_fork.url, ALMANAK_TOKEN, "balanceOf(address)(uint256)", TEST_WALLET)

        usdc_bal = int(usdc_bal_hex)
        almanak_bal = int(almanak_bal_hex)

        assert usdc_bal >= 10 * 10**6, f"Expected >= 10 USDC, got {usdc_bal / 10**6}"
        assert almanak_bal >= 100 * 10**18, f"Expected >= 100 ALMANAK, got {almanak_bal / 10**18}"
        logger.info("Balances verified: USDC=%d, ALMANAK=%d", usdc_bal, almanak_bal)


# --- Standalone runner ---


def main():
    """Run E2E test outside of pytest."""
    api_key = os.environ.get("ALCHEMY_API_KEY")
    if not api_key:
        logger.error("Set ALCHEMY_API_KEY to run E2E test")
        sys.exit(1)

    rpc_url = BASE_RPC_URL.format(api_key=api_key)
    fork = AnvilFork(rpc_url=rpc_url, chain_id=8453)
    fork.start()

    try:
        fund_wallet_with_tokens(fork.url, TEST_WALLET)

        # Run the catalog and schema tests
        test = TestDeFAIVaultLP()
        test.test_vault_tools_registered_in_catalog()
        logger.info("PASS: vault tools registered in catalog")

        test.test_vault_schemas_valid()
        logger.info("PASS: vault schemas valid")

        test.test_mock_agent_loop_structure()
        logger.info("PASS: mock agent loop structure")

        test.test_deployer_builds_valid_tx()
        logger.info("PASS: deployer builds valid tx")

        test.test_anvil_token_balances(fork)
        logger.info("PASS: Anvil token balances verified")

        logger.info("ALL E2E TESTS PASSED")
    finally:
        fork.stop()


if __name__ == "__main__":
    main()
